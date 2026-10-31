import datetime
import random

from spark import scheduler


TZ = scheduler.TZ


def _dt(hour, minute):
    return datetime.datetime(2026, 10, 12, hour, minute, tzinfo=TZ)


def test_summary_not_yet_before_window_end():
    assert scheduler.should_summarize(_dt(21, 29), "21:30") is False


def test_summary_fires_at_window_end():
    assert scheduler.should_summarize(_dt(21, 30), "21:30") is True


def test_summary_fires_after_window_end():
    assert scheduler.should_summarize(_dt(22, 10), "21:30") is True


def test_summary_respects_already_done_flag():
    assert scheduler.should_summarize(_dt(22, 10), "21:30", summarized=True) is False


def test_compute_slot_run_at_stays_before_latest_start():
    start = _dt(8, 0)
    end = _dt(9, 30)
    run_at = scheduler.compute_slot_run_at(
        start, end, batch_size=4, now=_dt(7, 55), rng=random.Random(2)
    )
    assert start <= run_at
    assert run_at <= end - datetime.timedelta(minutes=11)


def test_compute_slot_run_at_clamps_to_now_when_late():
    start = _dt(14, 0)
    end = _dt(18, 0)
    now = _dt(15, 30)
    run_at = scheduler.compute_slot_run_at(
        start, end, batch_size=4, now=now, rng=random.Random(2)
    )
    assert run_at >= now


def test_compute_slot_run_at_none_when_past_end():
    start = _dt(8, 0)
    end = _dt(9, 30)
    assert (
        scheduler.compute_slot_run_at(
            start, end, batch_size=4, now=_dt(10, 0), rng=random.Random(2)
        )
        is None
    )
