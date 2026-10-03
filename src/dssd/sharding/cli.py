"""Runs one region server: takes part in every shard's Raft election,
simulates the shards it wins, and hands agents to neighbours.

Example (three terminals):
  dssd-region --id r0 --grpc-addr 127.0.0.1:9000 --peer r1=127.0.0.1:9001 --peer r2=127.0.0.1:9002 --spawn a,5,5,1,0
  dssd-region --id r1 --grpc-addr 127.0.0.1:9001 --peer r0=127.0.0.1:9000 --peer r2=127.0.0.1:9002 --spawn a,5,5,1,0
  dssd-region --id r2 --grpc-addr 127.0.0.1:9002 --peer r0=127.0.0.1:9000 --peer r1=127.0.0.1:9001 --spawn a,5,5,1,0
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import grpc

from dssd import raft, regionpb, spinepb
from dssd.addr import parse_peers, self_addr
from dssd.membership import GRPCTransport, RaftService
from dssd.shutdown import install_shutdown_handler

from .handoff import RegionOwnerService
from .region_server import RegionServer
from .shard_state import ShardStateMachine
from .world import AgentState, GridConfig, all_shard_ids

logger = logging.getLogger("region")


async def run(args: argparse.Namespace) -> None:
    peers = parse_peers(args.peer)
    peers.pop(args.id, None)
    grid = GridConfig(width=args.width, height=args.height, cols=args.cols, rows=args.rows)

    transport = GRPCTransport(peers)
    shards = {
        shard_id: ShardStateMachine(raft.Config(id=args.id, peers=list(peers), group=shard_id), transport)
        for shard_id in all_shard_ids(grid)
    }
    server = grpc.aio.server()
    port = server.add_insecure_port(args.grpc_addr)
    spinepb.add_RaftServicer_to_server(RaftService({sid: s.raft for sid, s in shards.items()}), server)
    regionpb.add_RegionOwnerServicer_to_server(RegionOwnerService(shards), server)
    for shard in shards.values():
        await shard.start()
    await server.start()
    logger.info("region %s up on port %d, shards %s", args.id, port, list(shards))

    region = RegionServer(grid, shards, {**peers, args.id: self_addr(args.grpc_addr, port)}, tick_interval=args.tick_interval)
    await asyncio.sleep(1.0)  # let the elections settle before spawning
    for spec in args.spawn:
        agent_id, x, y, vx, vy = spec.split(",")
        agent = AgentState(agent_id, float(x), float(y), float(vx), float(vy))
        if await region.spawn_agent(agent):  # only the shard's leader can place it
            logger.info("spawned %s in shard %s", agent.id, agent.shard_id(grid))
    run_task = asyncio.create_task(region.run())

    stop_requested = asyncio.Event()
    install_shutdown_handler(stop_requested)
    await stop_requested.wait()
    run_task.cancel()
    await asyncio.gather(run_task, return_exceptions=True)
    await server.stop(grace=2)
    for shard in shards.values():
        await shard.stop()
    await transport.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--id", required=True, help="unique region-server id")
    parser.add_argument("--grpc-addr", default="127.0.0.1:0", help="TCP address for the gRPC API")
    parser.add_argument("--peer", action="append", default=[], help="peer as id=grpc-host:port; repeat for each peer")
    parser.add_argument("--width", type=float, default=20.0)
    parser.add_argument("--height", type=float, default=20.0)
    parser.add_argument("--cols", type=int, default=2)
    parser.add_argument("--rows", type=int, default=2)
    parser.add_argument("--tick-interval", type=float, default=0.2)
    parser.add_argument("--spawn", action="append", default=[], help="agent as id,x,y,vx,vy; repeat for each agent")
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
