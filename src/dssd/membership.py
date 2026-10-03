"""The gRPC glue around SWIM and Raft: how Raft nodes reach each other
across processes, and the read-only cluster view (GetMembers).
"""

from __future__ import annotations

import grpc

from dssd import spinepb
from dssd.raft import Raft
from dssd.swim import Node


class GRPCTransport:
    """raft.Transport over gRPC, to peers named in a fixed id -> address table."""

    def __init__(self, addrs: dict[str, str]) -> None:
        self._addrs = addrs
        self._channels: dict[str, grpc.aio.Channel] = {}

    def _stub(self, peer_id: str) -> spinepb.RaftStub:
        if peer_id not in self._channels:
            self._channels[peer_id] = grpc.aio.insecure_channel(self._addrs[peer_id])
        return spinepb.RaftStub(self._channels[peer_id])

    async def request_vote(self, peer_id: str, args: spinepb.RequestVoteArgs) -> spinepb.RequestVoteReply:
        return await self._stub(peer_id).RequestVote(args)

    async def append_entries(self, peer_id: str, args: spinepb.AppendEntriesArgs) -> spinepb.AppendEntriesReply:
        return await self._stub(peer_id).AppendEntries(args)

    async def close(self) -> None:
        for channel in self._channels.values():
            await channel.close()


class RaftService(spinepb.RaftServicer):
    """Serves Raft RPCs for every Raft group this process hosts, keyed by
    group name: one group "" for a DiLoCo worker, one per shard for a
    region server."""

    def __init__(self, groups: dict[str, Raft]) -> None:
        self._groups = groups

    async def RequestVote(self, request, context) -> spinepb.RequestVoteReply:
        return self._groups[request.group].handle_request_vote(request)

    async def AppendEntries(self, request, context) -> spinepb.AppendEntriesReply:
        return self._groups[request.group].handle_append_entries(request)


class MembershipService(spinepb.MembershipServicer):
    def __init__(self, swim_node: Node) -> None:
        self._swim = swim_node

    async def GetMembers(self, request, context) -> spinepb.GetMembersResponse:
        members = [
            spinepb.Member(id=m.id, addr=m.addr, state=m.state.name, incarnation=m.incarnation)
            for m in self._swim.members()
        ]
        return spinepb.GetMembersResponse(members=members)
