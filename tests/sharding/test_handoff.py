
import grpc

from dssd import regionpb
from dssd.sharding.handoff import RegionOwnerService, request_handoff
from dssd.sharding.shard_raft import ChannelPool, ShardRaftManager
from dssd.sharding.shard_state import ShardStateMachine
from dssd.sharding.world import AgentState
from tests.support import eventually


class NodeHarness:
    def __init__(self, node_id: str) -> None:
        self.id = node_id
        self.manager: ShardRaftManager
        self.state_machines: dict[str, ShardStateMachine]
        self.owner: RegionOwnerService
        self.channels: ChannelPool
        self.server: grpc.aio.Server
        self.grpc_addr = ""


async def start_cluster(node_ids: list[str], shard_ids: list[str]) -> list[NodeHarness]:
    nodes = [NodeHarness(nid) for nid in node_ids]

    for n in nodes:
        n.server = grpc.aio.server()
        port = n.server.add_insecure_port("127.0.0.1:0")
        n.grpc_addr = f"127.0.0.1:{port}"

    addrs = {n.id: n.grpc_addr for n in nodes}

    for n in nodes:
        peer_ids = [nid for nid in node_ids if nid != n.id]
        peer_addrs = {nid: addrs[nid] for nid in peer_ids}
        n.channels = ChannelPool(peer_addrs)
        n.manager = ShardRaftManager(
            node_id=n.id,
            peer_ids=peer_ids,
            shard_ids=shard_ids,
            channels=n.channels,
            election_timeout_min=0.10,
            election_timeout_max=0.30,
            heartbeat_interval=0.05,
        )
        n.state_machines = {
            shard_id: ShardStateMachine(n.manager.shards[shard_id], n.manager.apply_queues[shard_id])
            for shard_id in shard_ids
        }
        n.owner = RegionOwnerService(n.state_machines)
        regionpb.add_ShardRaftServicer_to_server(n.manager, n.server)
        regionpb.add_RegionOwnerServicer_to_server(n.owner, n.server)
        await n.manager.start()
        for sm in n.state_machines.values():
            await sm.start()
        await n.server.start()

    return nodes


async def stop_cluster(nodes: list[NodeHarness]) -> None:
    for n in nodes:
        for sm in n.state_machines.values():
            await sm.stop()
        await n.manager.stop()
        await n.server.stop(None)
        await n.channels.close()


async def leader_of(nodes: list[NodeHarness], shard_id: str) -> NodeHarness:
    for n in nodes:
        if n.manager.shards[shard_id].state()[1]:
            return n
    raise AssertionError(f"no leader for shard {shard_id}")


async def test_handoff_accepted_by_correct_leader():
    nodes = await start_cluster(["n0", "n1", "n2"], ["dst"])
    try:
        await eventually(lambda: any(n.manager.shards["dst"].state()[1] for n in nodes))
        leader = await leader_of(nodes, "dst")
        term, _ = leader.manager.shards["dst"].state()

        agent = AgentState(id="a1", x=1.0, y=2.0, vx=0.1, vy=0.0)
        accepted, reason = await request_handoff(leader.grpc_addr, "dst", term, agent)
        assert accepted, reason

        for n in nodes:
            def has_agent(n=n) -> bool:
                return "a1" in n.state_machines["dst"].agents
            await eventually(has_agent)
    finally:
        await stop_cluster(nodes)


async def test_handoff_rejected_when_not_leader():
    nodes = await start_cluster(["n0", "n1", "n2"], ["dst"])
    try:
        await eventually(lambda: any(n.manager.shards["dst"].state()[1] for n in nodes))
        leader = await leader_of(nodes, "dst")
        follower = next(n for n in nodes if n is not leader)
        term, _ = leader.manager.shards["dst"].state()

        agent = AgentState(id="a1", x=1.0, y=2.0, vx=0.1, vy=0.0)
        accepted, reason = await request_handoff(follower.grpc_addr, "dst", term, agent)
        assert not accepted
        assert "leader" in reason
    finally:
        await stop_cluster(nodes)


async def test_handoff_rejected_on_stale_term():
    nodes = await start_cluster(["n0", "n1", "n2"], ["dst"])
    try:
        await eventually(lambda: any(n.manager.shards["dst"].state()[1] for n in nodes))
        leader = await leader_of(nodes, "dst")
        term, _ = leader.manager.shards["dst"].state()

        agent = AgentState(id="a1", x=1.0, y=2.0, vx=0.1, vy=0.0)
        accepted, reason = await request_handoff(leader.grpc_addr, "dst", term + 1, agent)
        assert not accepted
        assert "term" in reason
    finally:
        await stop_cluster(nodes)
