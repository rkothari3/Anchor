"""Cross-shard agent hand-off, fenced by the destination's Raft term so
a stale or since-deposed leader can't accept one - the same
epoch-fencing pattern used for the DiLoCo trainer's leader gate.

Consistency policy: the destination commits the agent to its own
replicated log *before* acknowledging acceptance (see
ShardStateMachine.propose_and_confirm), and the caller only removes the
agent from its source shard after seeing that acceptance. A crash
between those two steps can produce a transient duplicate (the agent
briefly exists in both shards) but never a loss - duplication is the
safer failure mode to prefer here.
"""

from __future__ import annotations

import grpc

from dssd import regionpb

from .shard_state import ShardStateMachine
from .world import AgentState


class RegionOwnerService(regionpb.RegionOwnerServicer):
    def __init__(self, state_machines: dict[str, ShardStateMachine]) -> None:
        self._state_machines = state_machines

    async def HandOff(self, request: regionpb.HandOffRequest, context) -> regionpb.HandOffResponse:
        sm = self._state_machines.get(request.shard_id)
        if sm is None:
            return regionpb.HandOffResponse(accepted=False, reason=f"unknown shard {request.shard_id}")

        term, is_leader = sm.raft.state()
        if not is_leader:
            return regionpb.HandOffResponse(accepted=False, reason="not the leader for this shard")
        if term != request.term:
            return regionpb.HandOffResponse(accepted=False, reason=f"stale term: current is {term}")

        a = request.agent
        async with sm.lock:
            if not await sm.wait_caught_up():
                return regionpb.HandOffResponse(accepted=False, reason="leader could not catch up its log")
            previous = dict(sm.agents)
            sm.agents[a.id] = AgentState(id=a.id, x=a.x, y=a.y, vx=a.vx, vy=a.vy)

            if await sm.propose_and_confirm():
                return regionpb.HandOffResponse(accepted=True)

            sm.agents = previous
            return regionpb.HandOffResponse(accepted=False, reason="lost leadership before the hand-off committed")


async def request_handoff(
    dest_addr: str, shard_id: str, term: int, agent: AgentState, timeout: float = 3.0
) -> tuple[bool, str]:
    """Calls HandOff on whatever node is believed to be shard_id's
    current leader. Returns (accepted, reason)."""
    async with grpc.aio.insecure_channel(dest_addr) as channel:
        stub = regionpb.RegionOwnerStub(channel)
        reply = await stub.HandOff(
            regionpb.HandOffRequest(
                shard_id=shard_id,
                term=term,
                agent=regionpb.AgentState(id=agent.id, x=agent.x, y=agent.y, vx=agent.vx, vy=agent.vy),
            ),
            timeout=timeout,
        )
    return reply.accepted, reply.reason
