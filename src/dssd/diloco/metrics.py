"""Polls worker loss over wall-clock and logs it to CSV - the raw data
behind the loss-vs-wall-clock-at-N-kills comparison plot. Loss is
averaged across whichever workers answer each poll, the standard
local-SGD convention for approximating a single "global" curve from
workers that are mid-inner-loop on their own local batches.
"""

from __future__ import annotations

import asyncio
import csv
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

PollFn = Callable[[], Awaitable[dict[str, float | None]]]


def average_loss(losses: dict[str, float | None]) -> float | None:
    reachable = [loss for loss in losses.values() if loss is not None]
    if not reachable:
        return None
    return sum(reachable) / len(reachable)


@dataclass
class Row:
    elapsed: float
    avg_loss: float | None


class MetricsRecorder:
    def __init__(
        self,
        poll_fn: PollFn,
        interval: float,
        now: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._poll_fn = poll_fn
        self._interval = interval
        self._now = now
        self._sleep = sleep
        self.rows: list[Row] = []

    async def run(self, duration: float) -> None:
        start = self._now()
        while True:
            elapsed = self._now() - start
            if elapsed > duration:
                return
            losses = await self._poll_fn()
            self.rows.append(Row(elapsed=elapsed, avg_loss=average_loss(losses)))
            next_elapsed = elapsed + self._interval
            wait = next_elapsed - (self._now() - start)
            if wait > 0:
                await self._sleep(wait)

    def write_csv(self, path: Path) -> None:
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["elapsed", "avg_loss"])
            for row in self.rows:
                writer.writerow([row.elapsed, row.avg_loss])
