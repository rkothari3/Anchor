"""The README's Stage 3 threshold for spatial sharding: "kill a
region-server and see <1s hand-off with no agent loss." This test
measures that directly rather than just asserting it structurally.
"""

import asyncio
import time

import grpc

from dssd import regionpb
from dssd.sharding.handoff import RegionOwnerService
from dssd.sharding.region_server import RegionServer
from dssd.sharding.shard_raft import ChannelPool, ShardRaftManager
from dssd.sharding.shard_state import ShardStateMachine
from dssd.sharding.world import AgentState, GridConfig
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
        self.stopped = False

    async def stop(self) -> None:
        if self.stopped:
            return
        self.stopped = True
        self.region.stop()
        if self.run_task is not None:
            self.run_task.cancel()
            await asyncio.gather(self.run_task, return_exceptions=True)
        for sm in self.state_machines.values():
            await sm.stop()
        await self.manager.stop()
        await self.server.stop(None)
        await self.channels.close()


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


def total_agents(nodes: list[NodeHarness], shard_ids: list[str]) -> int:
    seen: set[str] = set()
    for shard_id in shard_ids:
        for n in nodes:
            if n.stopped:
                continue
            sm = n.state_machines[shard_id]
            if sm.raft.state()[1]:
                seen.update(sm.agents.keys())
                break
    return len(seen)


async def test_kill_region_server_recovers_in_under_one_second():
    grid = GridConfig(width=20.0, height=20.0, cols=2, rows=2)
    shard_ids = ["0-0", "0-1", "1-0", "1-1"]
    nodes = await start_cluster(["r0", "r1", "r2"], grid, shard_ids)
    try:
        # Stationary agents: this test's job is to isolate and measure
        # "kill a leader, does its replicated state survive, how fast is
        # a new leader ready" - the README's literal threshold. Whether
        # an in-flight cross-shard hand-off also survives a concurrent
        # kill is a separate, harder property already covered by
        # test_region_server.py's continuously-bouncing-agent test.
        agents = [
            AgentState(id="a1", x=5.0, y=5.0, vx=0.0, vy=0.0),
            AgentState(id="a2", x=15.0, y=15.0, vx=0.0, vy=0.0),
            AgentState(id="a3", x=2.0, y=18.0, vx=0.0, vy=0.0),
            AgentState(id="a4", x=18.0, y=2.0, vx=0.0, vy=0.0),
        ]
        for shard_id in shard_ids:
            await eventually(lambda shard_id=shard_id: any(n.manager.shards[shard_id].state()[1] for n in nodes))

        for agent in agents:
            for n in nodes:
                if await n.region.spawn_agent(agent):
                    break

        await eventually(lambda: total_agents(nodes, shard_ids) == 4)

        # Find whichever node owns the most shards - the worst case to kill.
        def owned_shards(n: NodeHarness) -> list[str]:
            return [sid for sid in shard_ids if n.state_machines[sid].raft.state()[1]]

        victim = max(nodes, key=lambda n: len(owned_shards(n)))
        victim_shards = owned_shards(victim)
        assert victim_shards, "victim owns no shards; test setup is wrong"
        survivors = [n for n in nodes if n is not victim]

        start = time.monotonic()
        await victim.stop()

        def recovered() -> bool:
            for shard_id in victim_shards:
                if not any(n.state_machines[shard_id].raft.state()[1] for n in survivors):
                    return False
            return total_agents(survivors, shard_ids) == 4

        await eventually(recovered, timeout=5.0)
        elapsed = time.monotonic() - start

        assert elapsed < 1.0, f"recovery took {elapsed:.3f}s, want <1s"
    finally:
        await asyncio.gather(*(n.stop() for n in nodes))
