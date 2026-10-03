"""The leader's DiLoCo outer-step barrier.

Each worker submits its pseudo-gradient and waits. The round closes when
every alive member (per SWIM) has submitted, or when round_timeout runs out
with whoever did; that timeout is what keeps training going when a worker
dies mid-round. Then the averaged pseudo-gradient is applied by the outer
optimizer and every waiting worker gets the same new global state.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from .outer import OuterOptimizer, StateDict, average_pseudo_gradients


class RoundBarrier:
    def __init__(
        self,
        alive_members: Callable[[], list[str]],
        global_state: StateDict,
        round_timeout: float = 10.0,
        start_round: int = 0,
        lr: float = 0.7,
        momentum: float = 0.9,
        nesterov: bool = True,
    ) -> None:
        self._alive_members = alive_members
        self._outer = OuterOptimizer(global_state, lr=lr, momentum=momentum, nesterov=nesterov)
        self._global_state = global_state
        self._round = start_round
        self._round_timeout = round_timeout
        self._pending: dict[str, StateDict] = {}  # worker id -> pseudo-gradient, this round
        self._round_done = asyncio.Event()
        self._timeout_task: asyncio.Task | None = None

    async def submit(self, worker_id: str, pseudo_grad: StateDict) -> tuple[int, StateDict]:
        """Blocks until this round closes; returns (new round number, new global state)."""
        self._pending[worker_id] = pseudo_grad
        # Grab this round's event before finishing it: finishing swaps in a
        # fresh event for the next round, which would never fire for us.
        done = self._round_done
        if set(self._alive_members()) <= self._pending.keys():
            self._finish_round()
        elif self._timeout_task is None:
            self._timeout_task = asyncio.create_task(self._finish_after_timeout())
        await done.wait()
        return self._round, self._global_state

    async def _finish_after_timeout(self) -> None:
        await asyncio.sleep(self._round_timeout)
        self._timeout_task = None  # we're the timer; don't let _finish_round cancel us
        if self._pending:
            self._finish_round()

    def _finish_round(self) -> None:
        self._global_state = self._outer.step(average_pseudo_gradients(list(self._pending.values())))
        self._round += 1
        self._pending = {}
        if self._timeout_task is not None:
            self._timeout_task.cancel()
            self._timeout_task = None
        done, self._round_done = self._round_done, asyncio.Event()
        done.set()
