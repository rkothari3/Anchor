"""Raft leader election and log replication (Ongaro & Ousterhout, "In
Search of an Understandable Consensus Algorithm"). The message types are
the protobufs in proto/spine.proto. In-memory only: no persistence and no
log compaction, which are plumbing rather than the algorithm.

Everything runs on one asyncio event loop, so state needs no locks.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Protocol

from dssd.spinepb import (
    AppendEntriesArgs,
    AppendEntriesReply,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
)

class Transport(Protocol):
    """How a Raft node reaches its peers: gRPC in production, a fake in tests."""

    async def request_vote(self, peer_id: str, args: RequestVoteArgs) -> RequestVoteReply: ...

    async def append_entries(self, peer_id: str, args: AppendEntriesArgs) -> AppendEntriesReply: ...


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class Config:
    id: str
    peers: list[str] = field(default_factory=list)  # the other members' ids
    group: str = ""  # which Raft group this is, when one node hosts several (shards)
    election_timeout_min: float = 0.15
    election_timeout_max: float = 0.3
    heartbeat_interval: float = 0.05


class Raft:
    def __init__(self, config: Config, transport: Transport, apply: Callable[[LogEntry], None]) -> None:
        self.cfg = config
        self._transport = transport
        self._apply = apply  # called with each committed entry, in log order

        self._current_term = 0
        self._voted_for = ""
        self._log: list[LogEntry] = [LogEntry(term=0, index=0)]  # log[0] is a sentinel
        self._role = Role.FOLLOWER
        self._leader_id = ""
        self._commit_index = 0
        self._last_applied = 0
        self._next_index: dict[str, int] = {}  # leader only: next entry to send each peer
        self._match_index: dict[str, int] = {}  # leader only: highest entry known replicated on each peer

        self._last_contact = 0.0
        self._election_timeout = self._new_election_timeout()
        self._last_heartbeat_sent = 0.0
        self._apply_signal = asyncio.Event()
        self._tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        self._last_contact = asyncio.get_running_loop().time()
        self._spawn(self._run())
        self._spawn(self._applier())

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    def state(self) -> tuple[int, bool]:
        """(current_term, is_leader)"""
        return self._current_term, self._role == Role.LEADER

    def leader_hint(self) -> tuple[str, int]:
        """(leader_id, term) of the leader we last heard from; may be stale or empty."""
        return self._leader_id, self._current_term

    def last_applied(self) -> int:
        """Index of the last entry handed to `apply`."""
        return self._last_applied

    def last_index(self) -> int:
        """Index of the last entry in our log, applied or not."""
        return len(self._log) - 1

    def propose(self, command: bytes) -> tuple[int, int, bool]:
        """Appends command to the log if we're the leader. Returns (index, term, is_leader)."""
        if self._role != Role.LEADER:
            return 0, 0, False
        entry = LogEntry(term=self._current_term, index=len(self._log), command=command)
        self._log.append(entry)
        return entry.index, entry.term, True

    # --- helpers ---

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _now(self) -> float:
        return asyncio.get_running_loop().time()

    def _new_election_timeout(self) -> float:
        return random.uniform(self.cfg.election_timeout_min, self.cfg.election_timeout_max)

    def _majority(self) -> int:
        return (len(self.cfg.peers) + 1) // 2 + 1

    # --- timers ---

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(0.01)
            if self._role == Role.LEADER:
                if self._now() - self._last_heartbeat_sent >= self.cfg.heartbeat_interval:
                    self._last_heartbeat_sent = self._now()
                    for peer in self.cfg.peers:
                        self._spawn(self._replicate_to(peer, self._current_term))
            elif self._now() - self._last_contact >= self._election_timeout:
                self._start_election()

    # --- leader election ---

    def _start_election(self) -> None:
        self._role = Role.CANDIDATE
        self._current_term += 1
        self._voted_for = self.cfg.id
        self._leader_id = ""
        self._last_contact = self._now()
        self._election_timeout = self._new_election_timeout()
        term = self._current_term
        last = self._log[-1]
        args = RequestVoteArgs(
            group=self.cfg.group,
            term=term,
            candidate_id=self.cfg.id,
            last_log_index=last.index,
            last_log_term=last.term,
        )
        votes = 1  # our own

        async def ask(peer: str) -> None:
            nonlocal votes
            try:
                reply = await asyncio.wait_for(self._transport.request_vote(peer, args), 2 * self.cfg.heartbeat_interval)
            except Exception:  # noqa: BLE001 - unreachable peer: no vote this round
                return
            if reply.term > self._current_term:
                self._become_follower(reply.term)
            elif self._role == Role.CANDIDATE and self._current_term == term and reply.vote_granted:
                votes += 1
                if votes >= self._majority():
                    self._become_leader()

        for peer in self.cfg.peers:
            self._spawn(ask(peer))

    def _become_follower(self, term: int) -> None:
        self._current_term = term
        self._voted_for = ""
        self._role = Role.FOLLOWER
        self._last_contact = self._now()

    def _become_leader(self) -> None:
        if self._role != Role.CANDIDATE:
            return
        self._role = Role.LEADER
        self._leader_id = self.cfg.id
        self._next_index = {p: len(self._log) for p in self.cfg.peers}
        self._match_index = {p: 0 for p in self.cfg.peers}
        self._last_heartbeat_sent = 0.0  # heartbeat right away
        # Raft only counts replicas for entries from the *current* term
        # (§5.4.2), so a previous leader's already-replicated entries stay
        # uncommitted until this leader commits something. An empty no-op
        # commits them right away instead of waiting for the next proposal.
        self.propose(b"")

    # --- RPC handlers (called by the gRPC servicer) ---

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        if args.term > self._current_term:
            self._become_follower(args.term)
        last = self._log[-1]
        log_ok = (args.last_log_term, args.last_log_index) >= (last.term, last.index)
        granted = (
            args.term == self._current_term and self._voted_for in ("", args.candidate_id) and log_ok
        )
        if granted:
            self._voted_for = args.candidate_id
            self._last_contact = self._now()
        return RequestVoteReply(term=self._current_term, vote_granted=granted)

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        if args.term > self._current_term:
            self._become_follower(args.term)
        if args.term < self._current_term:
            return AppendEntriesReply(term=self._current_term, success=False)  # a stale leader

        self._role = Role.FOLLOWER
        self._leader_id = args.leader_id
        self._last_contact = self._now()

        # Log matching: we must already have the entry right before the new ones.
        if args.prev_log_index >= len(self._log) or self._log[args.prev_log_index].term != args.prev_log_term:
            return AppendEntriesReply(term=self._current_term, success=False)

        for i, entry in enumerate(args.entries):
            index = args.prev_log_index + 1 + i
            if index < len(self._log) and self._log[index].term == entry.term:
                continue  # already have it
            self._log = self._log[:index] + list(args.entries[i:])  # drop any conflicting suffix
            break

        if args.leader_commit > self._commit_index:
            self._commit_index = min(args.leader_commit, len(self._log) - 1)
            self._apply_signal.set()
        return AppendEntriesReply(term=self._current_term, success=True)

    # --- replication (leader side) ---

    async def _replicate_to(self, peer: str, term: int) -> None:
        if self._role != Role.LEADER or self._current_term != term:
            return
        next_index = self._next_index[peer]
        args = AppendEntriesArgs(
            group=self.cfg.group,
            term=term,
            leader_id=self.cfg.id,
            prev_log_index=next_index - 1,
            prev_log_term=self._log[next_index - 1].term,
            entries=self._log[next_index:],
            leader_commit=self._commit_index,
        )
        try:
            reply = await asyncio.wait_for(self._transport.append_entries(peer, args), 2 * self.cfg.heartbeat_interval)
        except Exception:  # noqa: BLE001 - unreachable peer: retry on the next heartbeat
            return

        if reply.term > self._current_term:
            self._become_follower(reply.term)
        elif self._role != Role.LEADER or self._current_term != term:
            return
        elif reply.success:
            self._match_index[peer] = args.prev_log_index + len(args.entries)
            self._next_index[peer] = self._match_index[peer] + 1
            self._advance_commit_index()
        elif self._next_index[peer] > 1:
            self._next_index[peer] -= 1  # back up one entry and try again

    def _advance_commit_index(self) -> None:
        """Commit the newest current-term entry that a majority has (§5.4.2);
        everything before it commits with it."""
        for n in range(len(self._log) - 1, self._commit_index, -1):
            if self._log[n].term != self._current_term:
                continue
            if 1 + sum(1 for p in self.cfg.peers if self._match_index[p] >= n) >= self._majority():
                self._commit_index = n
                self._apply_signal.set()
                return

    # --- delivering committed entries ---

    async def _applier(self) -> None:
        while True:
            await self._apply_signal.wait()
            self._apply_signal.clear()
            while self._last_applied < self._commit_index:
                self._last_applied += 1
                self._apply(self._log[self._last_applied])
