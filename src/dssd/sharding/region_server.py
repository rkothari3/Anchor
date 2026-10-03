"""Runs the simulation for every shard this node currently leads: move the
agents one tick, hand off any that crossed into a neighbouring shard, and
replicate the result through that shard's Raft log.
"""

from __future__ import annotations

import asyncio
import logging

import grpc

from .handoff import request_handoff
from .shard_state import ShardStateMachine
from .world import AgentState, GridConfig

logger = logging.getLogger("region")


class RegionServer:
    def __init__(
        self,
        grid: GridConfig,
        shards: dict[str, ShardStateMachine],
        peer_addrs: dict[str, str],  # node id -> grpc addr, for hand-offs
        tick_interval: float = 0.2,
        dt: float = 1.0,
    ) -> None:
        self.grid = grid
        self.shards = shards
        self.peer_addrs = peer_addrs
        self.tick_interval = tick_interval
        self.dt = dt

    async def spawn_agent(self, agent: AgentState) -> bool:
        """Places an agent in its shard, if we lead that shard. True once durable."""
        shard = self.shards[agent.shard_id(self.grid)]
        return shard.is_leader() and await shard.add_agent(agent)

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.tick_interval)
            await self.tick()

    async def tick(self) -> None:
        # Phase 1: move agents in every shard we lead and replicate. Done for
        # all shards before any hand-off, so an agent handed from one of our
        # shards to another of ours can't be moved twice in one tick.
        departures: list[tuple[ShardStateMachine, AgentState, str]] = []
        for shard_id, shard in self.shards.items():
            if not shard.is_leader():
                continue
            async with shard.lock:
                if not shard.caught_up():
                    continue  # still applying a previous leader's entries; try next tick
                for agent in shard.agents.values():
                    agent.step(self.dt, self.grid)
                    if agent.shard_id(self.grid) != shard_id:
                        departures.append((shard, agent, agent.shard_id(self.grid)))
                # Crossers stay ours until the destination confirms; we only
                # remove them after (a crash in between duplicates, never loses).
                shard.propose_tick()

        # Phase 2: hand off each crosser; remove it here only once accepted.
        for shard, agent, dest in departures:
            accepted, reason = await self._handoff(agent, dest)
            if accepted:
                async with shard.lock:
                    # Apply our own earlier proposals first: otherwise an older
                    # snapshot applying later would bring a departed agent back.
                    if await shard.wait_caught_up():
                        shard.agents.pop(agent.id, None)
                        shard.propose_tick()
            logger.info("handoff %s -> %s: %s", agent.id, dest, "accepted" if accepted else f"deferred ({reason})")

    async def _handoff(self, agent: AgentState, dest_shard_id: str) -> tuple[bool, str]:
        leader_id, term = self.shards[dest_shard_id].raft.leader_hint()
        if leader_id not in self.peer_addrs:
            return False, "destination leader unknown"
        try:
            return await request_handoff(self.peer_addrs[leader_id], dest_shard_id, term, agent)
        except grpc.aio.AioRpcError as err:
            return False, str(err.code())
