"""Ties the pieces together: for each shard this node currently leads,
advance its agents one tick, hand off any that crossed into a
neighboring shard's region, and replicate the result.
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
        node_id: str,
        grid: GridConfig,
        state_machines: dict[str, ShardStateMachine],
        peer_grpc_addrs: dict[str, str],
        tick_interval: float = 0.2,
        dt: float = 1.0,
    ) -> None:
        self.node_id = node_id
        self.grid = grid
        self.state_machines = state_machines
        self.peer_grpc_addrs = peer_grpc_addrs
        self.tick_interval = tick_interval
        self.dt = dt
        self._stopped = False

    async def spawn_agent(self, agent: AgentState) -> bool:
        """Injects an agent into whichever shard it currently belongs
        to, if this node happens to be that shard's leader. Returns
        whether it durably took effect here.

        Unlike the routine tick loop (which fires-and-forgets each
        propose, since a missed tick is harmless - the next one just
        catches up), this is a one-time, no-retry placement: it waits
        for the entry to actually commit, the same way HandOff does,
        so a caller that sees True back can't lose the agent to a leader
        crash a moment later.
        """
        shard_id = agent.shard_id(self.grid)
        sm = self.state_machines.get(shard_id)
        if sm is None or not sm.raft.state()[1]:
            return False
        async with sm.lock:
            if not await sm.wait_caught_up():
                return False
            sm.agents[agent.id] = agent
            if await sm.propose_and_confirm():
                return True
            del sm.agents[agent.id]
            return False

    def stop(self) -> None:
        self._stopped = True

    async def run(self) -> None:
        while not self._stopped:
            await asyncio.sleep(self.tick_interval)
            await self._tick_once()

    async def _tick_once(self) -> None:
        # Two phases across every shard this node owns, not interleaved
        # per shard: if a single node leads more than one shard, an
        # agent handed off from shard A to shard B mid-cycle must not
        # then also get advanced during B's movement phase in this same
        # cycle - it would move twice in one tick. Computing every
        # shard's movement before attempting any hand-off makes that
        # impossible: a hand-off's destination was already ticked (or
        # skipped, if not ours) before it can receive anything new.
        departures: list[tuple[str, ShardStateMachine, AgentState, str]] = []
        for shard_id, sm in self.state_machines.items():
            if not sm.raft.state()[1]:
                continue
            async with sm.lock:
                if not sm.caught_up():
                    continue
                for agent in list(sm.agents.values()):
                    agent.step(self.dt, self.grid)
                    new_shard_id = agent.shard_id(self.grid)
                    if new_shard_id != shard_id:
                        departures.append((shard_id, sm, agent, new_shard_id))
                # Commit this tick's movement first, including agents
                # that have geometrically crossed a boundary: they stay
                # authoritatively ours until a hand-off actually
                # confirms elsewhere, per our commit-at-destination-
                # before-remove-at-source policy.
                sm.propose_tick()

        touched: set[ShardStateMachine] = set()
        for shard_id, sm, agent, dest_shard_id in departures:
            accepted, reason = await self._handoff(agent, dest_shard_id)
            if accepted:
                async with sm.lock:
                    sm.agents.pop(agent.id, None)
                touched.add(sm)
                logger.info("handoff of %s from %s to %s accepted", agent.id, shard_id, dest_shard_id)
            else:
                logger.info("handoff of %s from %s to %s deferred: %s", agent.id, shard_id, dest_shard_id, reason)

        for sm in touched:
            async with sm.lock:
                sm.propose_tick()

    async def _handoff(self, agent: AgentState, dest_shard_id: str) -> tuple[bool, str]:
        dest_sm = self.state_machines.get(dest_shard_id)
        if dest_sm is None:
            return False, f"unknown shard {dest_shard_id}"
        leader_id, term = dest_sm.raft.leader_hint()
        dest_addr = self.peer_grpc_addrs.get(leader_id)
        if not dest_addr:
            return False, "destination leader unknown"
        try:
            return await request_handoff(dest_addr, dest_shard_id, term, agent)
        except grpc.aio.AioRpcError as err:
            return False, str(err.code())
