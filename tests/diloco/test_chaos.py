import pytest

from dssd.diloco.chaos import ChaosScheduler, kill_schedule


def test_kill_schedule_spreads_evenly_across_the_run():
    assert kill_schedule(total_kills=3, duration=100.0) == [25.0, 50.0, 75.0]


def test_kill_schedule_empty_for_zero_kills():
    assert kill_schedule(total_kills=0, duration=100.0) == []


def test_kill_schedule_single_kill_at_midpoint():
    assert kill_schedule(total_kills=1, duration=100.0) == [50.0]


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        if seconds > 0:
            self.now += seconds


async def test_scheduler_calls_kill_fn_once_per_scheduled_kill_in_order():
    clock = FakeClock()
    killed: list[int] = []

    async def kill_fn(index: int) -> None:
        killed.append(index)

    scheduler = ChaosScheduler(
        total_kills=3, duration=30.0, kill_fn=kill_fn, now=clock.time, sleep=clock.sleep
    )
    await scheduler.run()

    assert killed == [0, 1, 2]
    assert clock.now == pytest.approx(22.5)  # last scheduled kill at 3/4 * 30


async def test_scheduler_with_zero_kills_does_nothing_and_returns_immediately():
    clock = FakeClock()
    calls = 0

    async def kill_fn(index: int) -> None:
        nonlocal calls
        calls += 1

    scheduler = ChaosScheduler(total_kills=0, duration=30.0, kill_fn=kill_fn, now=clock.time, sleep=clock.sleep)
    await scheduler.run()

    assert calls == 0
    assert clock.now == 0.0


async def test_scheduler_accounts_for_time_a_slow_kill_fn_already_consumed():
    clock = FakeClock()
    killed: list[int] = []

    async def kill_fn(index: int) -> None:
        killed.append(index)
        if index == 0:
            clock.now += 10.0  # kill_fn itself took 10s of wall-clock time

    # schedule: [10.0, 20.0]. After kill 0 fires at t=10 and takes 10s,
    # the clock is already at t=20 - the second kill is due immediately.
    scheduler = ChaosScheduler(total_kills=2, duration=30.0, kill_fn=kill_fn, now=clock.time, sleep=clock.sleep)
    await scheduler.run()

    assert killed == [0, 1]
    assert clock.now == pytest.approx(20.0)  # no extra sleep once already caught up
