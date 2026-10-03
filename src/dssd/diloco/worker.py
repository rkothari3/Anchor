"""A DiLoCo worker: runs its own membership-spine node (SWIM + Raft +
gRPC) alongside the DiLoCo inner/outer training loop, discovering and
following the current Raft leader as the outer-step coordinator so
training keeps going - without a restart - when a worker, even the
leader, dies.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import grpc
import torch

from dssd import dashboardpb, spinepb, trainerpb
from dssd.addr import parse_peers, resolve_addr, split_addr
from dssd.membership import GRPCTransport
from dssd.membership import Service as MembershipService
from dssd.raft import Config as RaftConfig
from dssd.raft import Raft
from dssd.shutdown import install_shutdown_handler
from dssd.swim import Config as SwimConfig
from dssd.swim import Node as SwimNode
from dssd.swim import State as SwimState

from .data import CharTokenizer, make_batch, synthetic_corpus
from .gated_trainer import LeaderGatedTrainerService
from .model import ModelConfig, TinyGPT
from .outer import pseudo_gradient
from .trainer_service import state_from_pb, state_to_pb

logger = logging.getLogger("worker")


class Worker:
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
        outer_lr: float = 0.7,
        outer_momentum: float = 0.9,
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

        self.trainer_servicer = LeaderGatedTrainerService(
            raft_node,
            self._get_quorum,
            self.local_state,
            lr=outer_lr,
            momentum=outer_momentum,
            round_timeout=round_timeout,
        )

    def local_state(self):
        return {k: v.clone() for k, v in self.global_state.items()}

    async def _get_quorum(self) -> list[str]:
        return [m.id for m in self._swim.members() if m.state == SwimState.ALIVE]

    def stop(self) -> None:
        self._stopped = True

    async def run(self) -> None:
        while not self._stopped:
            # Runs on a worker thread: it's synchronous, CPU-bound
            # PyTorch code with no await points, and this process's own
            # SWIM/Raft/gRPC handling shares this event loop - running
            # it inline would stall heartbeats and probes long enough to
            # trigger spurious elections and false failure suspicions.
            await asyncio.to_thread(self._inner_train)
            await self._outer_sync()

    def _inner_train(self) -> None:
        self.model.load_state_dict(self.global_state)
        optimizer = torch.optim.AdamW(self.model.parameters(), lr=3e-3)
        loss = None
        for _ in range(self._inner_steps):
            x, y = make_batch(self._data, self._block_size, self._batch_size)
            optimizer.zero_grad()
            _, loss = self.model(x, y)
            loss.backward()
            optimizer.step()
        self.last_loss = loss.item() if loss is not None else None

    async def _outer_sync(self) -> None:
        pseudo_grad = pseudo_gradient(self.global_state, self.model.state_dict())

        while not self._stopped:
            leader_id, term = self._raft.leader_hint()
            leader_addr = self._peer_addrs.get(leader_id)
            if not leader_addr:
                await asyncio.sleep(0.1)
                continue

            try:
                async with grpc.aio.insecure_channel(leader_addr) as channel:
                    stub = trainerpb.TrainerStub(channel)
                    request = trainerpb.SyncRequest(
                        worker_id=self.id,
                        round=self.round,
                        term=term,
                        pseudo_gradient=state_to_pb(pseudo_grad),
                    )
                    resp = await stub.Sync(request, timeout=self._round_timeout + 5.0)
                self.global_state = state_from_pb(resp.global_state)
                self.round = resp.round
                return
            except grpc.aio.AioRpcError as err:
                logger.info("worker %s: sync with %s failed (%s), retrying", self.id, leader_addr, err.code())
                await asyncio.sleep(0.2)


class WorkerStatusService(dashboardpb.WorkerStatusServicer):
    """Lets external tooling (the live dashboard) read a worker's
    training progress without scraping logs."""

    def __init__(self, worker: Worker) -> None:
        self._worker = worker

    async def GetStatus(self, request, context) -> dashboardpb.GetStatusResponse:
        loss = self._worker.last_loss
        return dashboardpb.GetStatusResponse(
            worker_id=self._worker.id,
            round=self._worker.round,
            loss=loss if loss is not None else 0.0,
            has_loss=loss is not None,
        )


async def run(args: argparse.Namespace) -> None:
    peers = parse_peers(args.peer)
    peers.pop(args.id, None)

    swim_host, swim_port = split_addr(args.swim_addr)
    swim_node = SwimNode(SwimConfig(id=args.id, bind_host=swim_host, bind_port=swim_port))
    await swim_node.start()
    if args.join:
        await swim_node.join(resolve_addr(args.join))

    transport = GRPCTransport(peers)
    apply_queue: asyncio.Queue = asyncio.Queue()
    raft_node = Raft(RaftConfig(id=args.id, peers=list(peers.keys())), transport, apply_queue)
    await raft_node.start()

    membership_service = MembershipService(swim_node, raft_node)
    membership_service.start()

    text = synthetic_corpus(args.corpus_length)
    tokenizer = CharTokenizer(text)
    data = torch.tensor(tokenizer.encode(text), dtype=torch.long)
    model_cfg = ModelConfig(vocab_size=tokenizer.vocab_size, block_size=32, n_embd=32, n_head=2, n_layer=2)

    server = grpc.aio.server()
    spinepb.add_MembershipServicer_to_server(membership_service, server)
    spinepb.add_RaftServicer_to_server(membership_service, server)
    grpc_host, _ = split_addr(args.grpc_addr)
    grpc_port = server.add_insecure_port(args.grpc_addr)
    advertise_host = args.advertise_host or grpc_host

    peer_addrs = dict(peers)
    peer_addrs[args.id] = f"{advertise_host}:{grpc_port}"

    worker = Worker(
        args.id,
        swim_node,
        raft_node,
        peer_addrs,
        model_cfg,
        data,
        inner_steps=args.inner_steps,
        batch_size=args.batch_size,
    )
    trainerpb.add_TrainerServicer_to_server(worker.trainer_servicer, server)
    dashboardpb.add_WorkerStatusServicer_to_server(WorkerStatusService(worker), server)

    await server.start()
    logger.info("worker %s up: swim=%s grpc=%s:%d peers=%s", args.id, swim_node.addr, grpc_host, grpc_port, list(peers.keys()))

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
    await membership_service.stop()
    await raft_node.stop()
    await swim_node.stop()
    await transport.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--id", required=True, help="unique worker id")
    parser.add_argument("--swim-addr", default="127.0.0.1:0", help="UDP address for SWIM gossip")
    parser.add_argument("--grpc-addr", default="127.0.0.1:0", help="TCP address for the gRPC membership/raft/trainer API")
    parser.add_argument(
        "--advertise-host",
        default="",
        help="host to advertise for this worker's own gRPC address, if different from --grpc-addr's "
        "bind host (e.g. bind 0.0.0.0 so kubectl port-forward works, advertise the pod IP to peers)",
    )
    parser.add_argument("--join", default="", help="SWIM address of an existing member to bootstrap from")
    parser.add_argument("--peer", action="append", default=[], help="peer as id=grpc-host:port; repeat for each peer")
    parser.add_argument("--inner-steps", type=int, default=20, help="local AdamW steps per outer sync")
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--corpus-length", type=int, default=4000)
    args = parser.parse_args()

    asyncio.run(run(args))


if __name__ == "__main__":
    main()
