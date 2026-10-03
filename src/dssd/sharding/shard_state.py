"""One shard: its own Raft group plus the agent registry that group
replicates. Every replica applies every committed entry, so whichever
replica wins the next election already has the full state. That is
what makes "no agent loss when a region server dies" possible.

Each log entry is a full snapshot of the shard's agents (JSON).
ponytail: whole-state snapshots, so the log grows with every tick; switch
to per-agent deltas + log compaction if shards ever hold many agents.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import asdict

from dssd import raft
from dssd.spinepb import LogEntry

from .world import AgentState


class ShardStateMachine:
    def __init__(self, config: raft.Config, transport: raft.Transport) -> None:
        self._apply_queue: asyncio.Queue[LogEntry] = asyncio.Queue()
        self.raft = raft.Raft(config, transport, self._apply_queue)
        self.agents: dict[str, AgentState] = {}
        self._last_applied_index = 0
        self._apply_task: asyncio.Task | None = None
        # Every "read agents, change them, propose a snapshot" sequence holds
        # this lock, so the tick loop and an incoming hand-off can't
        # interleave and silently overwrite each other's changes.
        self.lock = asyncio.Lock()

    async def start(self) -> None:
        await self.raft.start()
        self._apply_task = asyncio.create_task(self._apply_loop())

    async def stop(self) -> None:
        if self._apply_task is not None:
            self._apply_task.cancel()
            await asyncio.gather(self._apply_task, return_exceptions=True)
        await self.raft.stop()

    def is_leader(self) -> bool:
        return self.raft.state()[1]

    async def _apply_loop(self) -> None:
        while True:
            entry = await self._apply_queue.get()
            if entry.command:  # empty = a new leader's no-op, not "no agents"
                self.agents = {a["id"]: AgentState(**a) for a in json.loads(entry.command)}
            self._last_applied_index = entry.index

    def caught_up(self) -> bool:
        """True once every entry in our log has been applied. Until then a
        new leader's agents are stale, and snapshotting them would erase the
        entries still waiting to apply (an agent lost on failover). Check
        this while holding `lock`, before changing agents."""
        return self._last_applied_index >= self.raft.last_index()

    def propose_tick(self) -> None:
        """Proposes the current agents as the next entry. Fire-and-forget:
        if it never commits, the next tick's snapshot supersedes it."""
        self.raft.propose(self._snapshot())

    async def add_agent(self, agent: AgentState) -> bool:
        """Adds an agent durably: returns True only once it's committed on a
        majority, so it survives this node dying a moment later. Used both
        to spawn agents and to accept hand-offs from neighbouring shards."""
        async with self.lock:
            if not await self.wait_caught_up():
                return False
            before = dict(self.agents)
            self.agents[agent.id] = agent
            index, _, is_leader = self.raft.propose(self._snapshot())
            if is_leader and await self._wait_applied(index):
                return True
            self.agents = before
            return False

    async def wait_caught_up(self) -> bool:
        """Waits for caught_up(), for callers that can't just skip and retry later."""
        return await self._wait_applied(self.raft.last_index())

    def _snapshot(self) -> bytes:
        return json.dumps([asdict(a) for a in self.agents.values()]).encode()

    async def _wait_applied(self, index: int, timeout: float = 2.0) -> bool:
        """Waits until entry `index` is applied here, while we stay leader of the same term."""
        term, is_leader = self.raft.state()
        deadline = asyncio.get_running_loop().time() + timeout
        while is_leader and self.raft.state() == (term, True):
            if self._last_applied_index >= index:
                return True
            if asyncio.get_running_loop().time() >= deadline:
                return False
            await asyncio.sleep(0.01)
        return False
