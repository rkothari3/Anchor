import asyncio
from functools import partial

import grpc
import torch

from dssd import dashboardpb, spinepb, trainerpb
from dssd.diloco.data import CharTokenizer, synthetic_corpus
from dssd.diloco.model import ModelConfig
from dssd.diloco.worker import Worker, WorkerStatusService
from dssd.membership import GRPCTransport
from dssd.membership import Service as MembershipService
from dssd.raft import Config as RaftConfig
from dssd.raft import Raft
from dssd.swim import Config as SwimConfig
from dssd.swim import Node as SwimNode
from tests import support

eventually = partial(support.eventually, timeout=15.0)


class WorkerHarness:
    def __init__(self, worker_id: str) -> None:
        self.id = worker_id
        self.swim_node: SwimNode
        self.raft_node: Raft
        self.membership_service: MembershipService
        self.transport: GRPCTransport
        self.server: grpc.aio.Server
        self.worker: Worker
        self.grpc_addr = ""
        self.train_task: asyncio.Task | None = None

    async def stop(self) -> None:
        if self.train_task is not None:
            self.worker.stop()
            self.train_task.cancel()
            await asyncio.gather(self.train_task, return_exceptions=True)
        await self.server.stop(None)
        await self.membership_service.stop()
        await self.raft_node.stop()
        await self.swim_node.stop()
        await self.transport.close()


async def start_cluster(n: int, inner_steps: int = 5, batch_size: int = 8) -> list[WorkerHarness]:
    members = [WorkerHarness(f"w{i}") for i in range(n)]

    for m in members:
        m.swim_node = SwimNode(
            SwimConfig(
                id=m.id,
                protocol_period=0.02,
                ping_timeout=0.015,
                indirect_ping_count=2,
                suspicion_timeout=0.08,
            )
        )
        await m.swim_node.start()

    for m in members:
        m.server = grpc.aio.server()
        port = m.server.add_insecure_port("127.0.0.1:0")
        m.grpc_addr = f"127.0.0.1:{port}"

    addrs = {m.id: m.grpc_addr for m in members}

    text = synthetic_corpus(2000)
    tokenizer = CharTokenizer(text)
    data = torch.tensor(tokenizer.encode(text), dtype=torch.long)
    model_cfg = ModelConfig(vocab_size=tokenizer.vocab_size, block_size=16, n_embd=16, n_head=2, n_layer=1)

    for m in members:
        peer_addrs = {i: a for i, a in addrs.items() if i != m.id}
        m.transport = GRPCTransport(peer_addrs)
        m.raft_node = Raft(
            RaftConfig(
                id=m.id,
                peers=list(peer_addrs.keys()),
                election_timeout_min=0.04,
                election_timeout_max=0.08,
                heartbeat_interval=0.01,
            ),
            m.transport,
            asyncio.Queue(),
        )
        m.membership_service = MembershipService(m.swim_node, m.raft_node)
        m.membership_service.start()

        m.worker = Worker(
            m.id,
            m.swim_node,
            m.raft_node,
            dict(addrs),
            model_cfg,
            data,
            inner_steps=inner_steps,
            batch_size=batch_size,
            round_timeout=5.0,
        )

        spinepb.add_MembershipServicer_to_server(m.membership_service, m.server)
        spinepb.add_RaftServicer_to_server(m.membership_service, m.server)
        trainerpb.add_TrainerServicer_to_server(m.worker.trainer_servicer, m.server)
        dashboardpb.add_WorkerStatusServicer_to_server(WorkerStatusService(m.worker), m.server)

        await m.raft_node.start()
        await m.server.start()

    for m in members[1:]:
        await m.swim_node.join(members[0].swim_node.addr)

    for m in members:
        m.train_task = asyncio.create_task(m.worker.run())

    return members


async def test_workers_converge_and_loss_trends_down():
    members = await start_cluster(3, inner_steps=5, batch_size=8)
    try:
        for m in members:
            await eventually(lambda m=m: m.worker.round >= 1)
        early_losses = {m.id: m.worker.last_loss for m in members}

        for m in members:
            await eventually(lambda m=m: m.worker.round >= 4, timeout=25.0)
        late_losses = {m.id: m.worker.last_loss for m in members}

        for m in members:
            assert late_losses[m.id] < early_losses[m.id], (
                f"{m.id}: loss did not improve ({early_losses[m.id]} -> {late_losses[m.id]})"
            )
    finally:
        await asyncio.gather(*(m.stop() for m in members))


class FakeWorker:
    def __init__(self, worker_id: str, round_: int, last_loss: float | None) -> None:
        self.id = worker_id
        self.round = round_
        self.last_loss = last_loss


async def test_status_service_reports_no_loss_before_first_step():
    service = WorkerStatusService(FakeWorker("w0", 0, None))
    resp = await service.GetStatus(dashboardpb.GetStatusRequest(), None)
    assert resp.worker_id == "w0"
    assert resp.round == 0
    assert resp.has_loss is False


async def test_status_service_reports_current_loss():
    service = WorkerStatusService(FakeWorker("w1", 12, 0.345))
    resp = await service.GetStatus(dashboardpb.GetStatusRequest(), None)
    assert resp.round == 12
    assert resp.has_loss is True
    assert abs(resp.loss - 0.345) < 1e-4
