"""Runs one node of the shared membership/quorum service: a SWIM failure
detector over UDP and a Raft leader election over gRPC, exposed to
applications through a single gRPC API.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import grpc

from dssd import spinepb
from dssd.addr import parse_peers, split_addr
from dssd.membership import GRPCTransport, Service
from dssd.raft import Config as RaftConfig
from dssd.raft import Raft
from dssd.shutdown import install_shutdown_handler
from dssd.swim import Config as SwimConfig
from dssd.swim import Node

logger = logging.getLogger("member")


async def run(args: argparse.Namespace) -> None:
    peers = parse_peers(args.peer)
    peers.pop(args.id, None)

    swim_host, swim_port = split_addr(args.swim_addr)
    swim_node = Node(SwimConfig(id=args.id, bind_host=swim_host, bind_port=swim_port))
    await swim_node.start()

    if args.join:
        await swim_node.join(args.join)

    transport = GRPCTransport(peers)
    apply_queue: asyncio.Queue = asyncio.Queue()
    raft_node = Raft(RaftConfig(id=args.id, peers=list(peers.keys())), transport, apply_queue)
    await raft_node.start()

    async def log_applied() -> None:
        while True:
            msg = await apply_queue.get()
            logger.info("raft: applied index=%d term=%d command=%r", msg.index, msg.term, msg.command)

    log_task = asyncio.create_task(log_applied())

    service = Service(swim_node, raft_node)
    service.start()

    server = grpc.aio.server()
    spinepb.add_MembershipServicer_to_server(service, server)
    spinepb.add_RaftServicer_to_server(service, server)
    grpc_host, _ = split_addr(args.grpc_addr)
    grpc_port = server.add_insecure_port(args.grpc_addr)

    await server.start()
    logger.info(
        "member %s up: swim=%s grpc=%s:%d peers=%s",
        args.id,
        swim_node.addr,
        grpc_host,
        grpc_port,
        list(peers.keys()),
    )

    stop_requested = asyncio.Event()
    install_shutdown_handler(stop_requested)

    await stop_requested.wait()
    logger.info("member %s shutting down", args.id)

    log_task.cancel()
    await server.stop(grace=2)
    await service.stop()
    await raft_node.stop()
    await swim_node.stop()
    await transport.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", required=True, help="unique member id")
    parser.add_argument("--swim-addr", default="127.0.0.1:0", help="UDP address for SWIM gossip")
    parser.add_argument("--grpc-addr", default="127.0.0.1:0", help="TCP address for the gRPC membership/raft API")
    parser.add_argument("--join", default="", help="SWIM address of an existing member to bootstrap from")
    parser.add_argument("--peer", action="append", default=[], help="raft peer as id=grpc-host:port; repeat for each peer")
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
