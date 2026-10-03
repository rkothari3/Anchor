import asyncio

import torch

from dssd.diloco.barrier import RoundBarrier


def plain_sgd_barrier(alive: list[str], w: list[float], round_timeout: float = 5.0) -> RoundBarrier:
    # lr=1 and no momentum make the outer step plain subtraction: new = old - avg(grads).
    return RoundBarrier(
        lambda: alive, {"w": torch.tensor(w)}, round_timeout=round_timeout, lr=1.0, momentum=0.0, nesterov=False
    )


async def test_round_closes_once_every_alive_worker_submits():
    barrier = plain_sgd_barrier(["a", "b"], [0.0, 0.0])
    (round_a, state_a), (round_b, state_b) = await asyncio.gather(
        barrier.submit("a", {"w": torch.tensor([1.0, 0.0])}),
        barrier.submit("b", {"w": torch.tensor([3.0, 0.0])}),
    )
    assert round_a == round_b == 1
    assert torch.allclose(state_a["w"], torch.tensor([-2.0, 0.0]))
    assert torch.allclose(state_b["w"], torch.tensor([-2.0, 0.0]))


async def test_round_closes_after_timeout_without_a_dead_worker():
    barrier = plain_sgd_barrier(["a", "b"], [0.0], round_timeout=0.05)  # b never submits: it died
    round_, state = await barrier.submit("a", {"w": torch.tensor([2.0])})
    assert round_ == 1
    assert torch.allclose(state["w"], torch.tensor([-2.0]))


async def test_shrinking_membership_lets_the_next_round_close_immediately():
    alive = ["a", "b"]
    barrier = plain_sgd_barrier(alive, [0.0])
    await asyncio.gather(barrier.submit("a", {"w": torch.tensor([1.0])}), barrier.submit("b", {"w": torch.tensor([1.0])}))
    alive.remove("b")  # SWIM declared b dead
    round_, _ = await asyncio.wait_for(barrier.submit("a", {"w": torch.tensor([2.0])}), timeout=1.0)
    assert round_ == 2
