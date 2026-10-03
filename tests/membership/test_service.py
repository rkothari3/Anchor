import asyncio

import grpc

from dssd import spinepb
from dssd.membership import GRPCTransport, Service
from dssd.raft import Config as RaftConfig
from dssd.raft import Raft
from dssd.swim import Config as SwimConfig
from dssd.swim import Node
from tests.support import eventually


class ClusterMember:
    def __init__(self, member_id: str) -> None:
        self.id = member_id
        self.swim_node: Node
        self.raft_node: Raft
        self.service: Service
        self.transport: GRPCTransport
        self.server: grpc.aio.Server
        self.channel: grpc.aio.Channel
        self.client: spinepb.MembershipStub
        self.grpc_addr = ""

    async def stop(self) -> None:
        await self.channel.close()
        await self.server.stop(None)
        await self.service.stop()
        await self.raft_node.stop()
        await self.swim_node.stop()
        await self.transport.close()


async def start_cluster(n: int) -> list[ClusterMember]:
    members = [ClusterMember(f"m{i}") for i in range(n)]

    for m in members:
        m.swim_node = Node(
            SwimConfig(
                id=m.id,
                protocol_period=0.02,
                ping_timeout=0.015,
                indirect_ping_count=2,
                suspicion_timeout=0.08,
            )
        )
        await m.swim_node.start()

    for m in members:
        m.server = grpc.aio.server()
        port = m.server.add_insecure_port("127.0.0.1:0")
        m.grpc_addr = f"127.0.0.1:{port}"

    addrs = {m.id: m.grpc_addr for m in members}

    for m in members:
        peer_addrs = {id_: addr for id_, addr in addrs.items() if id_ != m.id}
        m.transport = GRPCTransport(peer_addrs)
        queue: asyncio.Queue = asyncio.Queue()
        m.raft_node = Raft(
            RaftConfig(
                id=m.id,
                peers=list(peer_addrs.keys()),
                election_timeout_min=0.04,
                election_timeout_max=0.08,
                heartbeat_interval=0.01,
            ),
            m.transport,
            queue,
        )
        m.service = Service(m.swim_node, m.raft_node)
        spinepb.add_MembershipServicer_to_server(m.service, m.server)
        spinepb.add_RaftServicer_to_server(m.service, m.server)
        m.service.start()
        await m.raft_node.start()
        await m.server.start()

    for m in members[1:]:
        await m.swim_node.join(members[0].swim_node.addr)

    for m in members:
        m.channel = grpc.aio.insecure_channel(m.grpc_addr)
        m.client = spinepb.MembershipStub(m.channel)

    return members


async def stop_cluster(members: list[ClusterMember]) -> None:
    await asyncio.gather(*(m.stop() for m in members))


async def test_members_converge_over_grpc():
    members = await start_cluster(4)
    try:
        for m in members:
            async def alive_count(m=m) -> bool:
                resp = await m.client.GetMembers(spinepb.GetMembersRequest())
                return sum(1 for mem in resp.members if mem.state == spinepb.ALIVE) == len(members)

            await eventually(alive_count)
    finally:
        await stop_cluster(members)


async def test_leader_elected_over_grpc():
    members = await start_cluster(3)
    try:
        async def has_leader() -> bool:
            resp = await members[0].client.GetLeader(spinepb.GetLeaderRequest())
            return resp.leader_id != ""

        await eventually(has_leader)
    finally:
        await stop_cluster(members)


async def test_quorum_shrinks_after_failure():
    members = await start_cluster(4)
    try:
        for m in members:
            async def full(m=m) -> bool:
                resp = await m.client.GetQuorum(spinepb.GetQuorumRequest())
                return len(resp.members) == len(members)

            await eventually(full)

        victim = members[3]
        await victim.stop()

        survivor = members[0]

        async def shrunk() -> bool:
            resp = await survivor.client.GetQuorum(spinepb.GetQuorumRequest())
            return len(resp.members) == len(members) - 1

        await eventually(shrunk)
    finally:
        await stop_cluster(members[:3])


async def test_watch_streams_failure_event():
    members = await start_cluster(3)
    stream = None
    try:
        for m in members:
            async def alive_count(m=m) -> bool:
                resp = await m.client.GetMembers(spinepb.GetMembersRequest())
                return sum(1 for mem in resp.members if mem.state == spinepb.ALIVE) == len(members)

            await eventually(alive_count)

        survivor = members[0]
        stream = survivor.client.Watch(spinepb.WatchRequest())

        victim = members[2]
        victim_id = victim.id
        await victim.stop()

        found = False
        try:
            async with asyncio.timeout(3.0):
                async for event in stream:
                    if event.type == spinepb.FAILED and event.member.id == victim_id:
                        found = True
                        break
        except TimeoutError:
            pass
        assert found, "did not observe FAILED event for victim"
    finally:
        if stream is not None:
            stream.cancel()
        await stop_cluster(members[:2])
