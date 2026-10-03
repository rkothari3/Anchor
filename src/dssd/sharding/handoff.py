"""Cross-shard agent hand-off, fenced by the destination's Raft term so a
deposed leader can't accept one.

Policy: the destination commits the agent to its own log *before* saying
yes, and the source removes it only after hearing yes. A crash between
those two steps can briefly duplicate an agent but never lose one.
"""

from __future__ import annotations

from dataclasses import asdict

import grpc

from dssd import regionpb

from .shard_state import ShardStateMachine
from .world import AgentState


class RegionOwnerService(regionpb.RegionOwnerServicer):
    def __init__(self, shards: dict[str, ShardStateMachine]) -> None:
        self._shards = shards

    async def HandOff(self, request: regionpb.HandOffRequest, context) -> regionpb.HandOffResponse:
        shard = self._shards.get(request.shard_id)
        if shard is None:
            return regionpb.HandOffResponse(accepted=False, reason=f"unknown shard {request.shard_id}")
        term, is_leader = shard.raft.state()
        if not is_leader:
            return regionpb.HandOffResponse(accepted=False, reason="not the leader for this shard")
        if term != request.term:
            return regionpb.HandOffResponse(accepted=False, reason=f"stale term: current is {term}")
        a = request.agent
        if await shard.add_agent(AgentState(id=a.id, x=a.x, y=a.y, vx=a.vx, vy=a.vy)):
            return regionpb.HandOffResponse(accepted=True)
        return regionpb.HandOffResponse(accepted=False, reason="lost leadership before the hand-off committed")


async def request_handoff(dest_addr: str, shard_id: str, term: int, agent: AgentState) -> tuple[bool, str]:
    """Asks shard_id's (believed) leader to take the agent. Returns (accepted, reason)."""
    async with grpc.aio.insecure_channel(dest_addr) as channel:
        request = regionpb.HandOffRequest(shard_id=shard_id, term=term, agent=regionpb.AgentState(**asdict(agent)))
        reply = await regionpb.RegionOwnerStub(channel).HandOff(request, timeout=3.0)
    return reply.accepted, reply.reason
