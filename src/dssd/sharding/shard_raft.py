"""Per-shard leader election: one raft.Raft instance per shard, all
hosted by the same set of nodes, dispatched over gRPC by shard_id. Reuses
the generic Raft implementation from the membership spine unchanged -
"ownership of a region" is just "who won that shard's election."
"""

from __future__ import annotations

import asyncio

import grpc

from dssd import raft, regionpb


class ChannelPool:
    """Caches gRPC channels to peers, shared across every shard's
    transport on this node (they're all talking to the same processes)."""

    def __init__(self, peer_addrs: dict[str, str]) -> None:
        self._peer_addrs = peer_addrs
        self._channels: dict[str, grpc.aio.Channel] = {}

    def stub(self, peer_id: str) -> regionpb.ShardRaftStub:
        channel = self._channels.get(peer_id)
        if channel is None:
            channel = grpc.aio.insecure_channel(self._peer_addrs[peer_id])
            self._channels[peer_id] = channel
        return regionpb.ShardRaftStub(channel)

    async def close(self) -> None:
        for channel in self._channels.values():
            await channel.close()


class ShardRaftTransport:
    """A raft.Transport for one specific shard, over a shared ChannelPool."""

    def __init__(self, shard_id: str, channels: ChannelPool) -> None:
        self._shard_id = shard_id
        self._channels = channels

    async def request_vote(self, peer_id: str, args: raft.RequestVoteArgs) -> raft.RequestVoteReply:
        reply = await self._channels.stub(peer_id).RequestVote(
            regionpb.ShardVoteRequest(
                shard_id=self._shard_id,
                term=args.term,
                candidate_id=args.candidate_id,
                last_log_index=args.last_log_index,
                last_log_term=args.last_log_term,
            )
        )
        return raft.RequestVoteReply(term=reply.term, vote_granted=reply.vote_granted)

    async def append_entries(self, peer_id: str, args: raft.AppendEntriesArgs) -> raft.AppendEntriesReply:
        reply = await self._channels.stub(peer_id).AppendEntries(
            regionpb.ShardAppendRequest(
                shard_id=self._shard_id,
                term=args.term,
                leader_id=args.leader_id,
                prev_log_index=args.prev_log_index,
                prev_log_term=args.prev_log_term,
                entries=[
                    regionpb.ShardLogEntry(term=e.term, index=e.index, command=e.command) for e in args.entries
                ],
                leader_commit=args.leader_commit,
            )
        )
        return raft.AppendEntriesReply(term=reply.term, success=reply.success)


class ShardRaftManager(regionpb.ShardRaftServicer):
    """Hosts one Raft instance per shard in a static, known-upfront grid.
    Every node participates in every shard's election: a deliberate
    scoping choice, not an oversight."""

    def __init__(
        self,
        node_id: str,
        peer_ids: list[str],
        shard_ids: list[str],
        channels: ChannelPool,
        **raft_kwargs,
    ) -> None:
        self.shards: dict[str, raft.Raft] = {}
        self.apply_queues: dict[str, asyncio.Queue] = {}
        for shard_id in shard_ids:
            queue: asyncio.Queue = asyncio.Queue()
            transport = ShardRaftTransport(shard_id, channels)
            cfg = raft.Config(id=node_id, peers=list(peer_ids), **raft_kwargs)
            self.shards[shard_id] = raft.Raft(cfg, transport, queue)
            self.apply_queues[shard_id] = queue

    async def start(self) -> None:
        for r in self.shards.values():
            await r.start()

    async def stop(self) -> None:
        await asyncio.gather(*(r.stop() for r in self.shards.values()))

    async def RequestVote(self, request: regionpb.ShardVoteRequest, context) -> regionpb.ShardVoteReply:
        reply = self.shards[request.shard_id].handle_request_vote(
            raft.RequestVoteArgs(
                term=request.term,
                candidate_id=request.candidate_id,
                last_log_index=request.last_log_index,
                last_log_term=request.last_log_term,
            )
        )
        return regionpb.ShardVoteReply(term=reply.term, vote_granted=reply.vote_granted)

    async def AppendEntries(self, request: regionpb.ShardAppendRequest, context) -> regionpb.ShardAppendReply:
        entries = tuple(raft.LogEntry(term=e.term, index=e.index, command=e.command) for e in request.entries)
        reply = self.shards[request.shard_id].handle_append_entries(
            raft.AppendEntriesArgs(
                term=request.term,
                leader_id=request.leader_id,
                prev_log_index=request.prev_log_index,
                prev_log_term=request.prev_log_term,
                entries=entries,
                leader_commit=request.leader_commit,
            )
        )
        return regionpb.ShardAppendReply(term=reply.term, success=reply.success)
