import asyncio

import grpc

from dssd import regionpb
from dssd.sharding.handoff import RegionOwnerService
from dssd.sharding.region_server import RegionServer
from dssd.sharding.shard_raft import ChannelPool, ShardRaftManager
from dssd.sharding.shard_state import ShardStateMachine
from dssd.sharding.world import AgentState, GridConfig, shard_id_for
from tests.support import eventually


class NodeHarness:
    def __init__(self, node_id: str) -> None:
        self.id = node_id
        self.manager: ShardRaftManager
        self.state_machines: dict[str, ShardStateMachine]
        self.region: RegionServer
        self.channels: ChannelPool
        self.server: grpc.aio.Server
        self.grpc_addr = ""
        self.run_task: asyncio.Task | None = None


async def start_cluster(node_ids: list[str], grid: GridConfig, shard_ids: list[str]) -> list[NodeHarness]:
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
        owner = RegionOwnerService(n.state_machines)
        n.region = RegionServer(
            node_id=n.id,
            grid=grid,
            state_machines=n.state_machines,
            peer_grpc_addrs=dict(addrs),
            tick_interval=0.05,
            dt=1.0,
        )
        regionpb.add_ShardRaftServicer_to_server(n.manager, n.server)
        regionpb.add_RegionOwnerServicer_to_server(owner, n.server)
        await n.manager.start()
        for sm in n.state_machines.values():
            await sm.start()
        await n.server.start()
        n.run_task = asyncio.create_task(n.region.run())

    return nodes


async def stop_cluster(nodes: list[NodeHarness]) -> None:
    for n in nodes:
        n.region.stop()
        if n.run_task is not None:
            n.run_task.cancel()
    for n in nodes:
        if n.run_task is not None:
            await asyncio.gather(n.run_task, return_exceptions=True)
        for sm in n.state_machines.values():
            await sm.stop()
        await n.manager.stop()
        await n.server.stop(None)
        await n.channels.close()


def total_agents(nodes: list[NodeHarness], shard_ids: list[str]) -> int:
    # Only leaders' views are authoritative; sum unique agent ids across
    # whichever node leads each shard.
    seen: set[str] = set()
    for shard_id in shard_ids:
        for n in nodes:
            sm = n.state_machines[shard_id]
            if sm.raft.state()[1]:
                seen.update(sm.agents.keys())
                break
    return len(seen)


async def test_agent_crosses_shard_boundary_via_handoff():
    grid = GridConfig(width=20.0, height=10.0, cols=2, rows=1)
    shard_ids = ["0-0", "0-1"]
    nodes = await start_cluster(["n0", "n1", "n2"], grid, shard_ids)
    try:
        for shard_id in shard_ids:
            await eventually(lambda shard_id=shard_id: any(n.manager.shards[shard_id].state()[1] for n in nodes))

        assert shard_id_for(5.0, 5.0, grid) == "0-0"
        agent = AgentState(id="crosser", x=5.0, y=5.0, vx=3.0, vy=0.0)

        spawned = False
        for n in nodes:
            if await n.region.spawn_agent(agent):
                spawned = True
                break
        assert spawned

        await eventually(lambda: total_agents(nodes, shard_ids) == 1)

        def crossed_into_0_1() -> bool:
            for n in nodes:
                sm = n.state_machines["0-1"]
                if sm.raft.state()[1] and "crosser" in sm.agents:
                    return True
            return False

        await eventually(crossed_into_0_1, timeout=15.0)

        # No duplication: it must be gone from 0-0 by the time it's in 0-1.
        assert total_agents(nodes, shard_ids) == 1
    finally:
        await stop_cluster(nodes)
