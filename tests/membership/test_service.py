import asyncio

import grpc

from dssd import spinepb
from dssd.membership import GRPCTransport, MembershipService, RaftService
from dssd.raft import Config as RaftConfig
from dssd.raft import Raft
from dssd.swim import Config as SwimConfig
from dssd.swim import Node
from tests.support import eventually


class ClusterMember:
    """One process's worth of spine: SWIM, Raft, and the gRPC server for both."""

    def __init__(self, id: str) -> None:
        self.id = id
        self.swim = Node(
            SwimConfig(id=id, protocol_period=0.05, ping_timeout=0.05, indirect_ping_count=2, suspicion_timeout=0.2)
        )
        self.server = grpc.aio.server()
        self.grpc_addr = f"127.0.0.1:{self.server.add_insecure_port('127.0.0.1:0')}"
        self.channel = grpc.aio.insecure_channel(self.grpc_addr)
        self.client = spinepb.MembershipStub(self.channel)

    async def start(self, addrs: dict[str, str]) -> None:
        peers = {id: addr for id, addr in addrs.items() if id != self.id}
        self.transport = GRPCTransport(peers)
        config = RaftConfig(
            id=self.id,
            peers=list(peers),
            election_timeout_min=0.15,
            election_timeout_max=0.3,
            heartbeat_interval=0.05,
        )
        self.raft = Raft(config, self.transport, asyncio.Queue())
        spinepb.add_MembershipServicer_to_server(MembershipService(self.swim), self.server)
        spinepb.add_RaftServicer_to_server(RaftService({"": self.raft}), self.server)
        await self.swim.start()
        await self.raft.start()
        await self.server.start()

    async def stop(self) -> None:
        await self.channel.close()
        await self.server.stop(None)
        await self.raft.stop()
        await self.swim.stop()
        await self.transport.close()

    async def states(self) -> dict[str, int]:
        resp = await self.client.GetMembers(spinepb.GetMembersRequest())
        return {m.id: m.state for m in resp.members}


async def start_cluster(n: int) -> list[ClusterMember]:
    members = [ClusterMember(f"m{i}") for i in range(n)]
    addrs = {m.id: m.grpc_addr for m in members}
    for m in members:
        await m.start(addrs)
    for m in members[1:]:
        await m.swim.join(members[0].swim.addr)
    return members


async def test_members_converge_over_grpc():
    members = await start_cluster(4)
    try:
        for m in members:
            async def all_alive(m=m) -> bool:
                states = await m.states()
                return len(states) == len(members) and all(s == spinepb.ALIVE for s in states.values())

            await eventually(all_alive)
    finally:
        await asyncio.gather(*(m.stop() for m in members))


async def test_leader_elected_over_grpc():
    members = await start_cluster(3)
    try:
        def agreed_leader() -> str:
            hints = {m.raft.leader_hint()[0] for m in members}
            return hints.pop() if len(hints) == 1 else ""

        await eventually(agreed_leader)
        assert any(m.id == agreed_leader() and m.raft.state()[1] for m in members)
    finally:
        await asyncio.gather(*(m.stop() for m in members))


async def test_stopped_member_shows_dead():
    members = await start_cluster(3)
    try:
        for m in members:
            async def all_alive(m=m) -> bool:
                states = await m.states()
                return list(states.values()).count(spinepb.ALIVE) == len(members)

            await eventually(all_alive)

        victim = members[2]
        await victim.stop()

        async def victim_dead() -> bool:
            return (await members[0].states()).get(victim.id) == spinepb.DEAD

        await eventually(victim_dead)
    finally:
        await asyncio.gather(*(m.stop() for m in members[:2]))
