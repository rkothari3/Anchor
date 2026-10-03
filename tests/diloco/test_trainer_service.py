import asyncio

import torch

from dssd import trainerpb
from dssd.diloco.trainer_service import TrainerService, state_from_pb, state_to_pb


class FakeContext:
    async def abort(self, code, details):
        raise RuntimeError(f"aborted: {code}: {details}")


def make_request(worker_id: str, pseudo_grad) -> trainerpb.SyncRequest:
    return trainerpb.SyncRequest(worker_id=worker_id, pseudo_gradient=state_to_pb(pseudo_grad))


async def test_round_finalizes_once_all_alive_workers_submit():
    global_state = {"w": torch.tensor([0.0, 0.0])}

    async def get_quorum():
        return ["a", "b"]

    service = TrainerService(get_quorum, global_state, lr=1.0, momentum=0.0, nesterov=False, round_timeout=5.0)

    grad_a = {"w": torch.tensor([1.0, 0.0])}
    grad_b = {"w": torch.tensor([3.0, 0.0])}

    results = await asyncio.gather(
        service.Sync(make_request("a", grad_a), FakeContext()),
        service.Sync(make_request("b", grad_b), FakeContext()),
    )

    for resp in results:
        state = state_from_pb(resp.global_state)
        assert torch.allclose(state["w"], torch.tensor([-2.0, 0.0]))
        assert resp.round == 1


async def test_round_finalizes_after_timeout_with_partial_quorum():
    global_state = {"w": torch.tensor([0.0])}

    async def get_quorum():
        return ["a", "b"]  # b never submits, e.g. it died mid-round

    service = TrainerService(get_quorum, global_state, lr=1.0, momentum=0.0, nesterov=False, round_timeout=0.05)

    resp = await service.Sync(make_request("a", {"w": torch.tensor([2.0])}), FakeContext())

    state = state_from_pb(resp.global_state)
    assert torch.allclose(state["w"], torch.tensor([-2.0]))
    assert resp.round == 1



async def test_shrinking_quorum_lets_next_round_finalize_without_dead_worker():
    global_state = {"w": torch.tensor([0.0])}
    alive = ["a", "b"]

    async def get_quorum():
        return list(alive)

    service = TrainerService(get_quorum, global_state, lr=1.0, momentum=0.0, nesterov=False, round_timeout=5.0)

    await asyncio.gather(
        service.Sync(make_request("a", {"w": torch.tensor([1.0])}), FakeContext()),
        service.Sync(make_request("b", {"w": torch.tensor([1.0])}), FakeContext()),
    )

    alive.remove("b")  # b died; membership no longer reports it
    resp = await service.Sync(make_request("a", {"w": torch.tensor([2.0])}), FakeContext())

    assert resp.round == 2
