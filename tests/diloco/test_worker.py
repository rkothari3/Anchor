import asyncio
from functools import partial

import grpc
import pytest
import torch

from dssd import spinepb, trainerpb
from dssd.diloco.data import CharTokenizer, synthetic_corpus
from dssd.diloco.model import ModelConfig
from dssd.diloco.worker import Worker, state_from_pb, state_to_pb
from dssd.membership import GRPCTransport, MembershipService, RaftService
from dssd.raft import Config as RaftConfig
from dssd.raft import Raft
from dssd.swim import Config as SwimConfig
from dssd.swim import Member, State
from dssd.swim import Node as SwimNode
from tests import support

eventually = partial(support.eventually, timeout=15.0)


class WorkerHarness:
    """One in-process worker: SWIM node, Raft node, gRPC server and training loop."""

    def __init__(self, worker_id: str) -> None:
        self.id = worker_id
        self.swim_node: SwimNode
        self.raft_node: Raft
        self.transport: GRPCTransport
        self.server: grpc.aio.Server
        self.worker: Worker
        self.grpc_addr = ""
        self.train_task: asyncio.Task | None = None
        self.stopped = False

    async def stop(self) -> None:
        """Also used to kill a worker mid-training. Safe to call twice."""
        if self.stopped:
            return
        self.stopped = True
        if self.train_task is not None:
            self.train_task.cancel()
            await asyncio.gather(self.train_task, return_exceptions=True)
        await self.server.stop(None)
        await self.raft_node.stop()
        await self.swim_node.stop()
        await self.transport.close()


async def start_cluster(n: int, inner_steps: int = 5, batch_size: int = 8) -> list[WorkerHarness]:
    # n torch loops plus SWIM/Raft share one process; extra intra-op threads
    # only add contention and make heartbeats late.
    torch.set_num_threads(1)
    members = [WorkerHarness(f"w{i}") for i in range(n)]

    for m in members:
        m.swim_node = SwimNode(
            SwimConfig(id=m.id, protocol_period=0.05, ping_timeout=0.05, indirect_ping_count=2, suspicion_timeout=0.3)
        )
        await m.swim_node.start()
        m.server = grpc.aio.server()
        m.grpc_addr = f"127.0.0.1:{m.server.add_insecure_port('127.0.0.1:0')}"

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
                peers=list(peer_addrs),
                election_timeout_min=0.15,
                election_timeout_max=0.3,
                heartbeat_interval=0.03,
            ),
            m.transport,
            lambda entry: None,
        )
        m.worker = Worker(
            m.id,
            m.swim_node,
            m.raft_node,
            addrs,
            model_cfg,
            data,
            inner_steps=inner_steps,
            batch_size=batch_size,
            round_timeout=5.0,
        )
        spinepb.add_MembershipServicer_to_server(MembershipService(m.swim_node), m.server)
        spinepb.add_RaftServicer_to_server(RaftService({"": m.raft_node}), m.server)
        trainerpb.add_TrainerServicer_to_server(m.worker, m.server)
        await m.server.start()
        await m.raft_node.start()

    for m in members[1:]:
        await m.swim_node.join(members[0].swim_node.addr)

    for m in members:
        m.train_task = asyncio.create_task(m.worker.run())

    return members


async def test_workers_converge_and_loss_trends_down():
    torch.manual_seed(0)
    members = await start_cluster(3)
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


def leader(members: list[WorkerHarness]) -> WorkerHarness | None:
    return next((m for m in members if not m.stopped and m.raft_node.state()[1]), None)


async def rounds_advance_past(survivors: list[WorkerHarness], start: dict[str, int], by: int = 2) -> None:
    for m in survivors:
        await eventually(lambda m=m: m.worker.round >= start[m.id] + by, timeout=30.0)


async def test_training_continues_when_a_follower_dies():
    members = await start_cluster(3)
    try:
        await eventually(lambda: leader(members) is not None)
        for m in members:
            await eventually(lambda m=m: m.worker.round >= 2)

        victim = next(m for m in members if m is not leader(members))
        start = {m.id: m.worker.round for m in members}
        await victim.stop()
        await rounds_advance_past([m for m in members if m is not victim], start)
    finally:
        await asyncio.gather(*(m.stop() for m in members))


async def test_training_continues_when_the_leader_dies():
    members = await start_cluster(3)
    try:
        await eventually(lambda: leader(members) is not None)
        for m in members:
            await eventually(lambda m=m: m.worker.round >= 2)

        old_leader = leader(members)
        assert old_leader is not None
        old_term = old_leader.raft_node.state()[0]
        start = {m.id: m.worker.round for m in members}
        await old_leader.stop()
        survivors = [m for m in members if m is not old_leader]

        await eventually(lambda: leader(survivors) is not None)
        new_leader = leader(survivors)
        assert new_leader is not None and new_leader.raft_node.state()[0] > old_term
        await rounds_advance_past(survivors, start)
    finally:
        await asyncio.gather(*(m.stop() for m in members))


class FakeRaft:
    def __init__(self, term: int, is_leader: bool) -> None:
        self.term, self.is_leader = term, is_leader

    def state(self):
        return self.term, self.is_leader

    def leader_hint(self):
        return "", self.term


class FakeSwim:
    def __init__(self, alive: list[str]) -> None:
        self.alive = alive

    def members(self):
        return [Member(id=i, addr="", state=State.ALIVE) for i in self.alive]


class FakeContext:
    async def abort(self, code, details):
        raise RuntimeError(f"aborted: {details}")


def lone_worker(raft: FakeRaft, alive: list[str]) -> Worker:
    """A Worker with no network, for exercising its Sync/Status handlers."""
    cfg = ModelConfig(vocab_size=4, block_size=4, n_embd=4, n_head=1, n_layer=1)
    return Worker("a", FakeSwim(alive), raft, {}, cfg, torch.zeros(64, dtype=torch.long), round_timeout=5.0)  # type: ignore[arg-type]


def sync(worker_id: str, term: int, w: float) -> trainerpb.SyncRequest:
    return trainerpb.SyncRequest(worker_id=worker_id, term=term, pseudo_gradient=state_to_pb({"w": torch.tensor([w])}))


async def test_sync_rejects_non_leader_and_stale_term():
    with pytest.raises(RuntimeError):
        await lone_worker(FakeRaft(1, is_leader=False), ["a"]).Sync(sync("a", 1, 1.0), FakeContext())
    with pytest.raises(RuntimeError):
        await lone_worker(FakeRaft(2, is_leader=True), ["a"]).Sync(sync("a", 1, 1.0), FakeContext())


async def test_new_term_restarts_barrier_from_current_global_state():
    raft = FakeRaft(1, is_leader=True)
    worker = lone_worker(raft, ["a"])
    worker.global_state = {"w": torch.tensor([10.0])}
    first = state_from_pb((await worker.Sync(sync("a", 1, 1.0), FakeContext())).global_state)
    assert first["w"].item() < 10.0  # an outer step was applied

    # A new term (re-election) must reseed from this node's newer state,
    # not keep stepping the old term's barrier.
    raft.term = 2
    worker.global_state = {"w": torch.tensor([100.0])}
    second = state_from_pb((await worker.Sync(sync("a", 2, 1.0), FakeContext())).global_state)
    assert 90.0 < second["w"].item() < 100.0


async def test_new_term_continues_the_round_count():
    raft = FakeRaft(1, is_leader=True)
    worker = lone_worker(raft, ["a"])
    worker.global_state = {"w": torch.tensor([0.0])}
    worker.round = 7  # rounds this node already took part in under the old leader
    raft.term = 2
    resp = await worker.Sync(sync("a", 2, 1.0), FakeContext())
    assert resp.round == 8


async def test_concurrent_first_syncs_share_one_barrier():
    # If each call built its own barrier, neither would see the other and
    # both would hang until round_timeout instead of finalizing together.
    worker = lone_worker(FakeRaft(1, is_leader=True), ["a", "b"])
    worker.global_state = {"w": torch.tensor([0.0])}
    a, b = await asyncio.wait_for(
        asyncio.gather(worker.Sync(sync("a", 1, 2.0), FakeContext()), worker.Sync(sync("b", 1, 4.0), FakeContext())),
        timeout=2.0,
    )
    assert a.round == b.round == 1


async def test_status_reports_loss_only_after_training():
    worker = lone_worker(FakeRaft(1, is_leader=False), ["a"])
    assert not (await worker.Status(trainerpb.StatusRequest(), None)).HasField("loss")
    worker.round, worker.last_loss = 3, 0.5
    resp = await worker.Status(trainerpb.StatusRequest(), None)
    assert resp.round == 3 and resp.loss == pytest.approx(0.5)
