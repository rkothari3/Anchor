"""Replicates a shard's agent registry through that shard's own Raft
log. Every replica - not just the leader - applies committed entries, so
whichever replica wins the next election already has the full,
up-to-date state and can resume serving immediately: this is what makes
"no agent loss on kill" possible, as opposed to leader-election-only
coordination (which the DiLoCo trainer used, since there the model
state lived with the workers, not the coordinator).
"""

from __future__ import annotations

import asyncio
import json

from dssd import raft

from .world import AgentState


def encode_tick(agents: dict[str, AgentState]) -> bytes:
    return json.dumps(
        [{"id": a.id, "x": a.x, "y": a.y, "vx": a.vx, "vy": a.vy} for a in agents.values()]
    ).encode()


def decode_tick(data: bytes) -> dict[str, AgentState]:
    return {a["id"]: AgentState(**a) for a in json.loads(data)}


class ShardStateMachine:
    """Applies a shard's committed Raft log entries to maintain a
    replicated agent registry, and lets the current leader propose the
    next tick's state."""

    def __init__(self, shard_raft: raft.Raft, apply_queue: asyncio.Queue[raft.ApplyMsg]) -> None:
        self.raft = shard_raft
        self._apply_queue = apply_queue
        self.agents: dict[str, AgentState] = {}
        self._last_applied_index = 0
        self._apply_task: asyncio.Task | None = None
        # Guards every "read agents, mutate, propose" sequence. Without
        # it, the periodic tick loop and an incoming HandOff RPC can
        # interleave: both read/mutate the same shared dict and each
        # proposes a full snapshot, and whichever's proposal is applied
        # last silently wins - a classic lost-update race, not a Raft
        # bug. Holding this for the whole sequence (including
        # propose_and_confirm's wait) serializes access per shard.
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        self._apply_task = asyncio.create_task(self._apply_loop())

    async def stop(self) -> None:
        if self._apply_task is not None:
            self._apply_task.cancel()
            await asyncio.gather(self._apply_task, return_exceptions=True)

    async def _apply_loop(self) -> None:
        try:
            while True:
                msg = await self._apply_queue.get()
                if msg.command:
                    self.agents = decode_tick(msg.command)
                # An empty command is the no-op every new leader proposes
                # on election (see raft.Raft._become_leader) - it exists
                # purely to unstick older, already-safe entries; it must
                # not itself be treated as "replace agents with nothing."
                self._last_applied_index = msg.index
        except asyncio.CancelledError:
            pass

    def caught_up(self) -> bool:
        """True once every entry in this replica's log has been applied.
        A freshly elected leader's in-memory agents lag behind its log
        until then, and a snapshot proposed from that stale state would
        erase the entries still waiting to apply (an agent lost on
        failover). Callers that mutate-then-snapshot must check this
        (or ``wait_caught_up``) while holding ``lock``.
        """
        return self._last_applied_index >= self.raft.last_index()

    def propose_tick(self) -> bool:
        """Proposes the current in-memory agent state as the next log
        entry. Only takes effect if this node is still the shard's
        leader; returns whether it was."""
        _, _, is_leader = self.raft.propose(encode_tick(self.agents))
        return is_leader

    async def propose_and_confirm(self, timeout: float = 2.0) -> bool:
        """Proposes the current in-memory agent state and waits for it
        to actually commit, rather than just entering the log. Used
        wherever a caller needs a real durability guarantee (e.g. hand-off)
        instead of best-effort - once this returns True, the state is
        replicated to a majority and will survive this node dying a
        moment later.
        """
        index, term, is_leader = self.raft.propose(encode_tick(self.agents))
        if not is_leader:
            return False
        return await self._wait_applied(index, term, timeout)

    async def wait_caught_up(self, timeout: float = 1.0) -> bool:
        """Waits for this replica to apply its whole log, for one-shot
        callers (spawn, hand-off) that can't just skip and retry the way
        the tick loop does. False if leadership is lost or time runs out."""
        term, is_leader = self.raft.state()
        return is_leader and await self._wait_applied(self.raft.last_index(), term, timeout)

    async def _wait_applied(self, index: int, term: int, timeout: float) -> bool:
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            current_term, current_is_leader = self.raft.state()
            if current_term != term or not current_is_leader:
                return False  # superseded before applying
            if self._last_applied_index >= index:
                return True
            await asyncio.sleep(0.01)
        return False
