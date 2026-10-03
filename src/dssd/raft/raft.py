"""Raft leader election and log replication (Ongaro & Ousterhout, "In
Search of an Understandable Consensus Algorithm"). Intentionally omits
log compaction/snapshotting and durable persistence: those are plumbing,
not the core algorithm this module exists to demonstrate.

Like the swim package, all state lives on one asyncio event loop, so no
locks are needed: nothing here awaits in the middle of a read-modify-write.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass, field
from enum import IntEnum

from .types import (
    AppendEntriesArgs,
    AppendEntriesReply,
    ApplyMsg,
    LogEntry,
    RequestVoteArgs,
    RequestVoteReply,
    Transport,
)


class Role(IntEnum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


@dataclass
class Config:
    id: str
    peers: list[str] = field(default_factory=list)  # ids of other cluster members; excludes id

    election_timeout_min: float = 0.15
    election_timeout_max: float = 0.3
    heartbeat_interval: float = 0.05


class Raft:
    """Leader election and log replication for one cluster member. All
    state is in memory only."""

    def __init__(self, config: Config, transport: Transport, apply_queue: asyncio.Queue[ApplyMsg]) -> None:
        self.cfg = config
        self._transport = transport
        self._apply_queue = apply_queue

        self._current_term = 0
        self._voted_for = ""
        self._log: list[LogEntry] = [LogEntry(term=0, index=0, command=b"")]  # log[0] is a sentinel
        self._role = Role.FOLLOWER
        self._leader_id = ""

        self._commit_index = 0
        self._last_applied = 0

        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}

        self._last_contact = 0.0
        self._election_timeout = _rand_election_timeout(config)
        self._last_heartbeat_sent = 0.0

        self._apply_signal = asyncio.Event()
        self._tasks: list[asyncio.Task] = []
        self._background: set[asyncio.Task] = set()
        self._stopped = False

    async def start(self) -> None:
        self._last_contact = asyncio.get_event_loop().time()
        self._tasks.append(asyncio.create_task(self._run()))
        self._tasks.append(asyncio.create_task(self._applier()))

    async def stop(self) -> None:
        """Shuts the node down. Safe to call more than once."""
        if self._stopped:
            return
        self._stopped = True
        for t in self._tasks:
            t.cancel()
        pending = [*self._tasks, *self._background]
        await asyncio.gather(*pending, return_exceptions=True)

    def state(self) -> tuple[int, bool]:
        """Returns (current_term, is_leader)."""
        return self._current_term, self._role == Role.LEADER

    def last_index(self) -> int:
        """Index of the last entry in this node's log, applied or not."""
        return len(self._log) - 1

    def leader_hint(self) -> tuple[str, int]:
        """Returns (leader_id, term) for the leader this instance last
        heard from. May be stale or empty during an election."""
        return self._leader_id, self._current_term

    def propose(self, command: bytes) -> tuple[int, int, bool]:
        """Appends command to the log if this node is currently leader.
        Returns (index, term, is_leader)."""
        if self._role != Role.LEADER:
            return 0, 0, False
        index = len(self._log)
        term = self._current_term
        self._log.append(LogEntry(term=term, index=index, command=command))
        return index, term, True

    # --- internals ---

    def _spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    def _majority(self) -> int:
        cluster_size = len(self.cfg.peers) + 1
        return cluster_size // 2 + 1

    def _last_log_info(self) -> tuple[int, int]:
        last = self._log[-1]
        return last.index, last.term

    # --- timers ---

    async def _run(self) -> None:
        try:
            while True:
                await asyncio.sleep(0.01)
                self._tick()
        except asyncio.CancelledError:
            pass

    def _tick(self) -> None:
        now = asyncio.get_event_loop().time()
        if self._role == Role.LEADER:
            if now - self._last_heartbeat_sent >= self.cfg.heartbeat_interval:
                self._last_heartbeat_sent = now
                self._broadcast_append_entries()
            return
        if now - self._last_contact >= self._election_timeout:
            self._start_election()

    # --- leader election ---

    def _start_election(self) -> None:
        self._role = Role.CANDIDATE
        self._current_term += 1
        term = self._current_term
        self._voted_for = self.cfg.id
        self._leader_id = ""
        self._last_contact = asyncio.get_event_loop().time()
        self._election_timeout = _rand_election_timeout(self.cfg)
        last_log_index, last_log_term = self._last_log_info()

        votes = 1
        majority = self._majority()

        async def request(peer: str) -> None:
            nonlocal votes
            try:
                reply = await asyncio.wait_for(
                    self._transport.request_vote(
                        peer,
                        RequestVoteArgs(
                            term=term,
                            candidate_id=self.cfg.id,
                            last_log_index=last_log_index,
                            last_log_term=last_log_term,
                        ),
                    ),
                    timeout=2 * self.cfg.heartbeat_interval,
                )
            except Exception:  # noqa: BLE001 - any RPC failure just means "peer unreachable this round"
                return

            if reply.term > self._current_term:
                self._become_follower(reply.term)
                return
            if self._role != Role.CANDIDATE or self._current_term != term or not reply.vote_granted:
                return

            votes += 1
            if votes >= majority:
                self._become_leader()

        for peer in self.cfg.peers:
            self._spawn(request(peer))

    def _become_follower(self, term: int) -> None:
        self._current_term = term
        self._voted_for = ""
        self._role = Role.FOLLOWER
        self._last_contact = asyncio.get_event_loop().time()

    def _become_leader(self) -> None:
        if self._role != Role.CANDIDATE:
            return
        self._role = Role.LEADER
        self._leader_id = self.cfg.id
        last_index = len(self._log) - 1
        self._next_index = {p: last_index + 1 for p in self.cfg.peers}
        self._match_index = {p: 0 for p in self.cfg.peers}
        self._last_heartbeat_sent = 0.0  # force an immediate heartbeat

        # A prior leader's entries can be physically replicated to a
        # majority yet still uncommitted in every survivor's eyes: per
        # the Raft paper §5.4.2, an entry only commits via direct
        # majority-counting when it's from the *current* leader's term,
        # so an old, already-safe entry stays invisible until this new
        # leader commits something of its own - at which point it's
        # retroactively covered too. Proposing an empty no-op entry
        # immediately closes that gap instead of waiting on whatever the
        # application happens to propose next (which, if it naively
        # encodes "current state" while stale entries are still
        # unapplied, can clobber them the moment they finally apply).
        self.propose(b"")

    # --- RPC handlers ---

    def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        if args.term > self._current_term:
            self._become_follower(args.term)
        if args.term < self._current_term:
            return RequestVoteReply(term=self._current_term, vote_granted=False)

        last_log_index, last_log_term = self._last_log_info()
        log_up_to_date = args.last_log_term > last_log_term or (
            args.last_log_term == last_log_term and args.last_log_index >= last_log_index
        )

        if self._voted_for in ("", args.candidate_id) and log_up_to_date:
            self._voted_for = args.candidate_id
            self._last_contact = asyncio.get_event_loop().time()
            return RequestVoteReply(term=self._current_term, vote_granted=True)
        return RequestVoteReply(term=self._current_term, vote_granted=False)

    def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        if args.term > self._current_term:
            self._become_follower(args.term)
        if args.term < self._current_term:
            return AppendEntriesReply(term=self._current_term, success=False)

        self._role = Role.FOLLOWER
        self._leader_id = args.leader_id
        self._last_contact = asyncio.get_event_loop().time()

        if args.prev_log_index > 0 and (
            args.prev_log_index >= len(self._log) or self._log[args.prev_log_index].term != args.prev_log_term
        ):
            return AppendEntriesReply(term=self._current_term, success=False)

        for i, entry in enumerate(args.entries):
            idx = args.prev_log_index + 1 + i
            if idx < len(self._log):
                if self._log[idx].term == entry.term:
                    continue
                self._log = self._log[:idx]
            self._log.extend(args.entries[i:])
            break

        if args.leader_commit > self._commit_index:
            self._commit_index = min(args.leader_commit, len(self._log) - 1)
            self._apply_signal.set()

        return AppendEntriesReply(term=self._current_term, success=True)

    # --- log replication (leader side) ---

    def _broadcast_append_entries(self) -> None:
        if self._role != Role.LEADER:
            return
        term = self._current_term
        leader_commit = self._commit_index
        for peer in self.cfg.peers:
            self._spawn(self._replicate_to(peer, term, leader_commit))

    async def _replicate_to(self, peer: str, term: int, leader_commit: int) -> None:
        if self._role != Role.LEADER or self._current_term != term:
            return
        next_idx = self._next_index[peer]
        prev_log_index = next_idx - 1
        prev_log_term = self._log[prev_log_index].term
        entries = tuple(self._log[next_idx:])

        try:
            reply = await asyncio.wait_for(
                self._transport.append_entries(
                    peer,
                    AppendEntriesArgs(
                        term=term,
                        leader_id=self.cfg.id,
                        prev_log_index=prev_log_index,
                        prev_log_term=prev_log_term,
                        entries=entries,
                        leader_commit=leader_commit,
                    ),
                ),
                timeout=2 * self.cfg.heartbeat_interval,
            )
        except Exception:  # noqa: BLE001 - any RPC failure just means "peer unreachable this round"
            return

        if reply.term > self._current_term:
            self._become_follower(reply.term)
            return
        if self._role != Role.LEADER or self._current_term != term:
            return

        if reply.success:
            self._match_index[peer] = prev_log_index + len(entries)
            self._next_index[peer] = self._match_index[peer] + 1
            self._advance_commit_index()
        elif self._next_index[peer] > 1:
            self._next_index[peer] -= 1

    def _advance_commit_index(self) -> None:
        """Implements Raft's commit rule (§5.4.2): a leader may only
        commit an entry from its own current term once it's replicated
        on a majority; earlier-term entries are committed only as a side
        effect of committing a later entry that covers them."""
        for n in range(len(self._log) - 1, self._commit_index, -1):
            if self._log[n].term != self._current_term:
                continue
            count = 1 + sum(1 for p in self.cfg.peers if self._match_index[p] >= n)
            if count >= self._majority():
                self._commit_index = n
                self._apply_signal.set()
                break

    # --- applying committed entries ---

    async def _applier(self) -> None:
        try:
            while True:
                await self._apply_signal.wait()
                self._apply_signal.clear()
                while self._last_applied < self._commit_index:
                    self._last_applied += 1
                    entry = self._log[self._last_applied]
                    await self._apply_queue.put(ApplyMsg(index=entry.index, term=entry.term, command=entry.command))
        except asyncio.CancelledError:
            pass


def _rand_election_timeout(cfg: Config) -> float:
    span = cfg.election_timeout_max - cfg.election_timeout_min
    if span <= 0:
        return cfg.election_timeout_min
    return cfg.election_timeout_min + random.random() * span
