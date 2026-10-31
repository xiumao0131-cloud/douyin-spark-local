import datetime
import random
from zoneinfo import ZoneInfo

from spark import scheduler


TZ = ZoneInfo("Asia/Shanghai")


def _dt(hour, minute):
    return datetime.datetime(2026, 10, 12, hour, minute, tzinfo=TZ)


def test_pick_run_time_stays_inside_window():
    run_time = scheduler.pick_run_time(
        _dt(12, 0),
        "18:00",
        "22:00",
        random_start=True,
        rng=random.Random(5),
    )
    assert run_time.hour >= 18
    assert run_time.replace(tzinfo=None) <= datetime.datetime(2026, 10, 12, 21, 15)


def test_pick_run_time_respects_fixed_start_when_random_start_off():
    run_time = scheduler.pick_run_time(
        _dt(12, 0),
        "18:00",
        "22:00",
        random_start=False,
        rng=random.Random(5),
    )
    assert run_time.hour == 18
    assert run_time.minute == 0


def test_pick_run_time_is_deterministic_for_same_seed():
    first = scheduler.pick_run_time(
        _dt(12, 0),
        "18:00",
        "22:00",
        random_start=True,
        rng=random.Random(42),
    )
    second = scheduler.pick_run_time(
        _dt(12, 0),
        "18:00",
        "22:00",
        random_start=True,
        rng=random.Random(42),
    )
    assert first == second


def test_decision_before_window_is_not_yet():
    decision = scheduler.decide_now(_dt(17, 30), "18:00", "22:00", ran_today=False)
    assert decision == "not_yet"


def test_decision_inside_window_without_run_is_run():
    decision = scheduler.decide_now(_dt(19, 30), "18:00", "22:00", ran_today=False)
    assert decision == "run"


def test_decision_inside_window_after_run_is_done():
    decision = scheduler.decide_now(_dt(19, 30), "18:00", "22:00", ran_today=True)
    assert decision == "done"


def test_decision_after_window_is_too_late():
    decision = scheduler.decide_now(_dt(22, 30), "18:00", "22:00", ran_today=False)
    assert decision == "too_late"
