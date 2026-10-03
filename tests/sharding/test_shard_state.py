import asyncio

import grpc

from dssd import raft, regionpb
from dssd.sharding.region_server import RegionServer
from dssd.sharding.shard_raft import ChannelPool, ShardRaftManager
from dssd.sharding.shard_state import ShardStateMachine
from dssd.sharding.world import AgentState, GridConfig


async def eventually(cond, timeout: float = 3.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while True:
        if cond():
            return
        if loop.time() >= deadline:
            assert cond(), "condition not met within timeout"
        await asyncio.sleep(0.01)


class NodeHarness:
    def __init__(self, node_id: str) -> None:
        self.id = node_id
        self.manager: ShardRaftManager
        self.state_machines: dict[str, ShardStateMachine]
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
        regionpb.add_ShardRaftServicer_to_server(n.manager, n.server)
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


async def test_tick_replicates_agent_state_to_all_nodes():
    nodes = await start_cluster(["n0", "n1", "n2"], ["0-0"])
    try:
        await eventually(lambda: any(n.manager.shards["0-0"].state()[1] for n in nodes))
        leader = await leader_of(nodes, "0-0")

        agents = {"a1": AgentState(id="a1", x=1.0, y=2.0, vx=0.0, vy=0.0)}
        leader.state_machines["0-0"].agents = agents
        ok = leader.state_machines["0-0"].propose_tick()
        assert ok

        def all_converged() -> bool:
            return all(n.state_machines["0-0"].agents.keys() == {"a1"} for n in nodes)

        await eventually(all_converged)
        for n in nodes:
            a1 = n.state_machines["0-0"].agents["a1"]
            assert a1.x == 1.0 and a1.y == 2.0
    finally:
        await stop_cluster(nodes)


async def test_new_leader_retains_state_after_original_leader_dies():
    nodes = await start_cluster(["n0", "n1", "n2"], ["0-0"])
    try:
        await eventually(lambda: any(n.manager.shards["0-0"].state()[1] for n in nodes))
        first_leader = await leader_of(nodes, "0-0")

        agents = {"a1": AgentState(id="a1", x=5.0, y=5.0, vx=1.0, vy=0.0)}
        first_leader.state_machines["0-0"].agents = agents
        assert first_leader.state_machines["0-0"].propose_tick()

        survivors = [n for n in nodes if n is not first_leader]

        def survivors_converged() -> bool:
            return all(n.state_machines["0-0"].agents.keys() == {"a1"} for n in survivors)

        await eventually(survivors_converged)

        await first_leader.manager.stop()
        await first_leader.server.stop(None)

        def new_leader_elected() -> bool:
            return any(n.manager.shards["0-0"].state()[1] for n in survivors)

        await eventually(new_leader_elected)
        new_leader = await leader_of(survivors, "0-0")

        # The new leader must already have the agent - no loss, and no
        # need to somehow "recover" it from the dead node.
        assert "a1" in new_leader.state_machines["0-0"].agents
        assert new_leader.state_machines["0-0"].agents["a1"].x == 5.0
    finally:
        for n in nodes:
            if n is first_leader:
                continue
            for sm in n.state_machines.values():
                await sm.stop()
            await n.manager.stop()
            await n.server.stop(None)
            await n.channels.close()


class _AgreeablePeers:
    async def request_vote(self, peer_id: str, args: raft.RequestVoteArgs) -> raft.RequestVoteReply:
        return raft.RequestVoteReply(term=args.term, vote_granted=True)

    async def append_entries(self, peer_id: str, args: raft.AppendEntriesArgs) -> raft.AppendEntriesReply:
        return raft.AppendEntriesReply(term=args.term, success=True)


async def test_leader_refuses_to_snapshot_until_its_log_is_applied():
    # A just-elected leader has committed entries it hasn't applied yet,
    # so its in-memory agents are stale. Snapshotting from that state
    # would erase the unapplied entries - an agent lost on failover.
    queue: asyncio.Queue[raft.ApplyMsg] = asyncio.Queue()
    cfg = raft.Config(
        id="n", peers=["p1", "p2"], election_timeout_min=0.02, election_timeout_max=0.04, heartbeat_interval=0.01
    )
    node = raft.Raft(cfg, _AgreeablePeers(), queue)
    sm = ShardStateMachine(node, queue)
    region = RegionServer("n", GridConfig(width=10.0, height=10.0, cols=1, rows=1), {"0-0": sm}, {})
    await node.start()  # sm.start() deliberately not called yet: nothing gets applied
    try:
        await eventually(lambda: node.state()[1])
        await eventually(lambda: not sm.caught_up())  # the election no-op is committed but unapplied

        before = node.last_index()
        assert not await region.spawn_agent(AgentState(id="a", x=1.0, y=1.0, vx=0.0, vy=0.0))
        assert node.last_index() == before, "proposed a snapshot from stale state"

        await sm.start()
        await eventually(sm.caught_up)
        assert await region.spawn_agent(AgentState(id="a", x=1.0, y=1.0, vx=0.0, vy=0.0))
    finally:
        await sm.stop()
        await node.stop()
