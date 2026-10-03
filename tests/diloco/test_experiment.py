import pytest

from dssd.diloco.experiment import average_loss, kill_schedule


def test_kill_schedule_spreads_evenly():
    assert kill_schedule(3, 100.0) == [25.0, 50.0, 75.0]
    assert kill_schedule(1, 100.0) == [50.0]
    assert kill_schedule(0, 100.0) == []


def test_average_loss_ignores_unreachable_workers():
    assert average_loss([1.0, 3.0, None]) == pytest.approx(2.0)
    assert average_loss([None, None]) is None
