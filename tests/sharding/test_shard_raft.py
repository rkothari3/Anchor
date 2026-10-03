
import grpc

from dssd import regionpb
from dssd.sharding.shard_raft import ChannelPool, ShardRaftManager
from tests.support import eventually


class NodeHarness:
    def __init__(self, node_id: str) -> None:
        self.id = node_id
        self.manager: ShardRaftManager
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
            # A narrower election range (e.g. 0.04-0.08, fine for a
            # single Raft group) produces persistent split-vote
            # livelocks once many shards' elections start at nearly the
            # same instant: with a 10ms tick, that range only has ~4-8
            # distinguishable slots, and collisions get likely at scale.
            election_timeout_min=0.10,
            election_timeout_max=0.30,
            # This matters more: raft.Raft's RPC timeout is 2x this
            # value. Stage 1's raft tests got away with 0.01 (a 20ms RPC
            # timeout) because they ran over an in-memory fake transport
            # with ~0 latency. Real gRPC, with 8 shards' worth of
            # concurrent traffic sharing one process, routinely needs
            # more than 20ms - and 0.01 reproduces the exact same
            # "election storm" pathology as too-narrow SWIM timeouts did
            # in the membership spine: everyone times out, nobody
            # converges. raft.Config's own default is already 0.05;
            # this test had just copied the tighter test-only value.
            heartbeat_interval=0.05,
        )
        regionpb.add_ShardRaftServicer_to_server(n.manager, n.server)
        await n.manager.start()
        await n.server.start()

    return nodes


async def stop_cluster(nodes: list[NodeHarness]) -> None:
    for n in nodes:
        await n.manager.stop()
        await n.server.stop(None)
        await n.channels.close()


async def test_each_shard_elects_its_own_leader():
    nodes = await start_cluster(["n0", "n1", "n2"], ["0-0", "0-1"])
    try:
        for shard_id in ("0-0", "0-1"):

            def one_leader(shard_id=shard_id) -> bool:
                leaders = [n for n in nodes if n.manager.shards[shard_id].state()[1]]
                assert len(leaders) <= 1, f"shard {shard_id} has {len(leaders)} leaders"
                return len(leaders) == 1

            await eventually(one_leader, timeout=3.0)
    finally:
        await stop_cluster(nodes)


async def test_shards_can_have_different_leaders():
    # Not guaranteed every run, but with independent randomized election
    # timeouts per shard it's overwhelmingly likely across many shards;
    # use enough shards that "all same leader" would be a coincidence.
    shard_ids = [f"0-{i}" for i in range(8)]
    nodes = await start_cluster(["n0", "n1", "n2"], shard_ids)
    try:
        for shard_id in shard_ids:

            def one_leader(shard_id=shard_id) -> bool:
                leaders = [n for n in nodes if n.manager.shards[shard_id].state()[1]]
                return len(leaders) == 1

            await eventually(one_leader, timeout=3.0)

        leader_ids = set()
        for shard_id in shard_ids:
            for n in nodes:
                if n.manager.shards[shard_id].state()[1]:
                    leader_ids.add(n.id)
        assert len(leader_ids) > 1, "expected leadership to spread across more than one node"
    finally:
        await stop_cluster(nodes)
