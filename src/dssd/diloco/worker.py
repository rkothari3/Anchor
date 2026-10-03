"""A DiLoCo worker. Each worker runs its own membership-spine node (SWIM +
Raft), trains locally, and syncs with whoever is currently the Raft
leader. Every worker also serves the Trainer gRPC service, but only the
leader answers Sync, so training keeps going without a restart when any
worker, even the leader, dies.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import grpc
import torch

from dssd import spinepb, trainerpb
from dssd.addr import parse_peers, resolve_addr, split_addr
from dssd.membership import GRPCTransport, MembershipService, RaftService
from dssd.raft import Config as RaftConfig
from dssd.raft import Raft
from dssd.shutdown import install_shutdown_handler
from dssd.swim import Config as SwimConfig
from dssd.swim import Node as SwimNode
from dssd.swim import State as SwimState

from .data import CharTokenizer, make_batch, synthetic_corpus
from .model import ModelConfig, TinyGPT
from .outer import pseudo_gradient
from .trainer_service import TrainerService, state_from_pb, state_to_pb

logger = logging.getLogger("worker")


class Worker(trainerpb.TrainerServicer):
    def __init__(
        self,
        worker_id: str,
        swim_node: SwimNode,
        raft_node: Raft,
        peer_addrs: dict[str, str],  # member id -> grpc addr, includes self
        model_cfg: ModelConfig,
        data: torch.Tensor,
        inner_steps: int = 20,
        batch_size: int = 16,
        round_timeout: float = 15.0,
    ) -> None:
        self.id = worker_id
        self._swim = swim_node
        self._raft = raft_node
        self._peer_addrs = peer_addrs
        self._data = data
        self._block_size = model_cfg.block_size
        self._batch_size = batch_size
        self._inner_steps = inner_steps
        self._round_timeout = round_timeout

        self.model = TinyGPT(model_cfg)
        self.global_state = {k: v.clone() for k, v in self.model.state_dict().items()}
        self.round = 0
        self.last_loss: float | None = None
        self._stopped = False

        self._barrier: TrainerService | None = None
        self._barrier_term = -1

    def stop(self) -> None:
        self._stopped = True

    async def _alive_members(self) -> list[str]:
        return [m.id for m in self._swim.members() if m.state == SwimState.ALIVE]

    # --- server side: only the leader runs the outer-step barrier ---

    async def Sync(self, request: trainerpb.SyncRequest, context) -> trainerpb.SyncResponse:
        term, is_leader = self._raft.state()
        if not is_leader or request.term != term:
            # Fencing: a worker talking to a replaced leader (or using an
            # old term) is turned away instead of corrupting the round.
            await context.abort(grpc.StatusCode.FAILED_PRECONDITION, f"not the leader for term {request.term}")
        if self._barrier_term != term:
            # New term: start a fresh barrier from this node's own latest
            # global state, so training continues where the group left off.
            global_state = {k: v.clone() for k, v in self.global_state.items()}
            self._barrier = TrainerService(self._alive_members, global_state, round_timeout=self._round_timeout)
            self._barrier_term = term
        assert self._barrier is not None
        return await self._barrier.Sync(request, context)

    async def Status(self, request, context) -> trainerpb.StatusResponse:
        resp = trainerpb.StatusResponse(round=self.round)
        if self.last_loss is not None:
            resp.loss = self.last_loss
        return resp

    # --- client side: train locally, then sync with the leader ---

    async def run(self) -> None:
        while not self._stopped:
            # The PyTorch loop is CPU-bound with no awaits; running it in a
            # thread keeps this process's SWIM/Raft heartbeats flowing.
            await asyncio.to_thread(self._inner_train)
            await self._outer_sync()

    def _inner_train(self) -> None:
        self.model.load_state_dict(self.global_state)
        optimizer = torch.optim.AdamW(self.model.parameters(), lr=3e-3)
        for _ in range(self._inner_steps):
            x, y = make_batch(self._data, self._block_size, self._batch_size)
            optimizer.zero_grad()
            _, loss = self.model(x, y)
            loss.backward()
            optimizer.step()
        self.last_loss = loss.item()

    async def _outer_sync(self) -> None:
        pseudo_grad = pseudo_gradient(self.global_state, self.model.state_dict())

        while not self._stopped:
            leader_id, term = self._raft.leader_hint()
            leader_addr = self._peer_addrs.get(leader_id)
            if not leader_addr:
                await asyncio.sleep(0.1)  # no leader yet
                continue
            try:
                async with grpc.aio.insecure_channel(leader_addr) as channel:
                    request = trainerpb.SyncRequest(worker_id=self.id, term=term, pseudo_gradient=state_to_pb(pseudo_grad))
                    resp = await trainerpb.TrainerStub(channel).Sync(request, timeout=self._round_timeout + 5.0)
                self.global_state = state_from_pb(resp.global_state)
                self.round = resp.round
                return
            except grpc.aio.AioRpcError as err:
                logger.info("worker %s: sync with %s failed (%s), retrying", self.id, leader_addr, err.code())
                await asyncio.sleep(0.2)


async def run(args: argparse.Namespace) -> None:
    peers = parse_peers(args.peer)
    peers.pop(args.id, None)

    swim_host, swim_port = split_addr(args.swim_addr)
    swim_node = SwimNode(SwimConfig(id=args.id, bind_host=swim_host, bind_port=swim_port))
    await swim_node.start()
    if args.join:
        await swim_node.join(resolve_addr(args.join))

    transport = GRPCTransport(peers)
    raft_node = Raft(RaftConfig(id=args.id, peers=list(peers.keys())), transport, asyncio.Queue())
    await raft_node.start()

    text = synthetic_corpus(args.corpus_length)
    tokenizer = CharTokenizer(text)
    data = torch.tensor(tokenizer.encode(text), dtype=torch.long)
    model_cfg = ModelConfig(vocab_size=tokenizer.vocab_size)

    server = grpc.aio.server()
    grpc_port = server.add_insecure_port(args.grpc_addr)
    peer_addrs = {**peers, args.id: f"127.0.0.1:{grpc_port}"}  # the leader may be this very process
    worker = Worker(
        args.id, swim_node, raft_node, peer_addrs, model_cfg, data, inner_steps=args.inner_steps, batch_size=args.batch_size
    )
    spinepb.add_MembershipServicer_to_server(MembershipService(swim_node), server)
    spinepb.add_RaftServicer_to_server(RaftService({"": raft_node}), server)
    trainerpb.add_TrainerServicer_to_server(worker, server)
    await server.start()
    logger.info("worker %s up: swim=%s grpc port=%d peers=%s", args.id, swim_node.addr, grpc_port, list(peers))

    train_task = asyncio.create_task(worker.run())

    async def log_progress() -> None:
        while True:
            await asyncio.sleep(2.0)
            if worker.last_loss is not None:
                logger.info("worker %s: round=%d loss=%.4f", args.id, worker.round, worker.last_loss)

    progress_task = asyncio.create_task(log_progress())

    stop_requested = asyncio.Event()
    install_shutdown_handler(stop_requested)
    await stop_requested.wait()
    logger.info("worker %s shutting down", args.id)

    worker.stop()
    progress_task.cancel()
    train_task.cancel()
    await asyncio.gather(train_task, progress_task, return_exceptions=True)
    await server.stop(grace=2)
    await raft_node.stop()
    await swim_node.stop()
    await transport.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", required=True, help="unique worker id")
    parser.add_argument("--swim-addr", default="127.0.0.1:0", help="UDP address for SWIM gossip")
    parser.add_argument("--grpc-addr", default="127.0.0.1:0", help="TCP address for the gRPC API")
    parser.add_argument("--join", default="", help="SWIM address of an existing member to bootstrap from")
    parser.add_argument("--peer", action="append", default=[], help="peer as id=grpc-host:port; repeat for each peer")
    parser.add_argument("--inner-steps", type=int, default=20, help="local AdamW steps per outer sync")
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--corpus-length", type=int, default=4000)
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
