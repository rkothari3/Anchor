"""Runs one region-server node: participates in every shard's Raft
election for a static grid, owns (as leader) whichever shards it wins,
and simulates agent movement + hand-off for those it owns.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import grpc

from dssd import regionpb
from dssd.addr import parse_peers, split_addr
from dssd.shutdown import install_shutdown_handler

from .handoff import RegionOwnerService
from .region_server import RegionServer
from .shard_raft import ChannelPool, ShardRaftManager
from .shard_state import ShardStateMachine
from .world import AgentState, GridConfig

logger = logging.getLogger("region-cli")


def all_shard_ids(grid: GridConfig) -> list[str]:
    return [f"{row}-{col}" for row in range(grid.rows) for col in range(grid.cols)]


def parse_spawn(spec: str) -> AgentState:
    agent_id, x, y, vx, vy = spec.split(",")
    return AgentState(id=agent_id, x=float(x), y=float(y), vx=float(vx), vy=float(vy))


async def run(args: argparse.Namespace) -> None:
    peers = parse_peers(args.peer)
    peers.pop(args.id, None)

    grid = GridConfig(width=args.width, height=args.height, cols=args.cols, rows=args.rows)
    shard_ids = all_shard_ids(grid)

    server = grpc.aio.server()
    grpc_host, _ = split_addr(args.grpc_addr)
    grpc_port = server.add_insecure_port(args.grpc_addr)
    self_addr = f"{grpc_host}:{grpc_port}"
    peer_addrs = dict(peers)
    peer_addrs[args.id] = self_addr

    channels = ChannelPool(peers)
    manager = ShardRaftManager(
        node_id=args.id,
        peer_ids=list(peers.keys()),
        shard_ids=shard_ids,
        channels=channels,
    )
    state_machines = {
        shard_id: ShardStateMachine(manager.shards[shard_id], manager.apply_queues[shard_id])
        for shard_id in shard_ids
    }
    owner = RegionOwnerService(state_machines)
    region = RegionServer(args.id, grid, state_machines, peer_addrs, tick_interval=args.tick_interval)

    regionpb.add_ShardRaftServicer_to_server(manager, server)
    regionpb.add_RegionOwnerServicer_to_server(owner, server)

    await manager.start()
    for sm in state_machines.values():
        await sm.start()
    await server.start()
    logger.info("region %s up: grpc=%s shards=%s", args.id, self_addr, shard_ids)

    for spec in args.spawn:
        agent = parse_spawn(spec)
        await asyncio.sleep(1.0)  # give elections a moment to settle
        placed = await region.spawn_agent(agent)
        if placed:
            logger.info("spawned %s at (%.1f, %.1f) in shard %s", agent.id, agent.x, agent.y, agent.shard_id(grid))

    run_task = asyncio.create_task(region.run())

    async def log_progress() -> None:
        while True:
            await asyncio.sleep(2.0)
            owned = [sid for sid, sm in state_machines.items() if sm.raft.state()[1]]
            logger.info(
                "region %s: owns %s agents=%d",
                args.id,
                owned,
                sum(len(state_machines[sid].agents) for sid in owned),
            )

    progress_task = asyncio.create_task(log_progress())

    stop_requested = asyncio.Event()
    install_shutdown_handler(stop_requested)
    await stop_requested.wait()
    logger.info("region %s shutting down", args.id)

    region.stop()
    progress_task.cancel()
    run_task.cancel()
    await asyncio.gather(run_task, progress_task, return_exceptions=True)
    await server.stop(grace=2)
    for sm in state_machines.values():
        await sm.stop()
    await manager.stop()
    await channels.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", required=True, help="unique region-server id")
    parser.add_argument("--grpc-addr", default="127.0.0.1:0", help="TCP address for the gRPC shard-raft/region API")
    parser.add_argument("--peer", action="append", default=[], help="peer as id=grpc-host:port; repeat for each peer")
    parser.add_argument("--width", type=float, default=20.0)
    parser.add_argument("--height", type=float, default=20.0)
    parser.add_argument("--cols", type=int, default=2)
    parser.add_argument("--rows", type=int, default=2)
    parser.add_argument("--tick-interval", type=float, default=0.2)
    parser.add_argument(
        "--spawn",
        action="append",
        default=[],
        help="agent to spawn as id,x,y,vx,vy; repeat for each agent (only takes effect on whichever node owns its shard)",
    )
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
