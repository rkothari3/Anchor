"""Spaces N pod kills evenly across a run's duration and fires them at
the right wall-clock time - the "kill a random pod" driver behind the
0/5/20-kill loss-vs-wall-clock experiments.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

KillFn = Callable[[int], Awaitable[None]]


def kill_schedule(total_kills: int, duration: float) -> list[float]:
    """Kill times, evenly spaced so neither the first nor the last kill
    lands exactly on a run boundary."""
    if total_kills <= 0:
        return []
    step = duration / (total_kills + 1)
    return [step * (i + 1) for i in range(total_kills)]


class ChaosScheduler:
    def __init__(
        self,
        total_kills: int,
        duration: float,
        kill_fn: KillFn,
        now: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._times = kill_schedule(total_kills, duration)
        self._kill_fn = kill_fn
        self._now = now
        self._sleep = sleep

    async def run(self) -> None:
        start = self._now()
        for index, target in enumerate(self._times):
            wait = target - (self._now() - start)
            if wait > 0:
                await self._sleep(wait)
            await self._kill_fn(index)
