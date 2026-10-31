import datetime
import sqlite3
from pathlib import Path

from spark import state


def _fresh_state(tmp_path):
    db = tmp_path / "state.sqlite3"
    state.connect(db)
    state.ensure_schema()
    return db


def test_upsert_and_read_day_records(tmp_path):
    _fresh_state(tmp_path)
    state.upsert_record("2026-10-12", "小A", "confirmed", message="你好呀")
    state.upsert_record("2026-10-12", "小B", "pending")
    rows = state.get_day_records("2026-10-12")
    assert rows["小A"]["status"] == "confirmed"
    assert rows["小A"]["message"] == "你好呀"
    assert rows["小B"]["status"] == "pending"


def test_increment_attempts(tmp_path):
    _fresh_state(tmp_path)
    state.upsert_record("2026-10-12", "小A", "failed")
    state.increment_attempts("2026-10-12", "小A")
    state.increment_attempts("2026-10-12", "小A")
    assert state.get_day_records("2026-10-12")["小A"]["attempts"] == 2


def test_last_message_and_usage(tmp_path):
    _fresh_state(tmp_path)
    state.upsert_record("2026-10-11", "小A", "confirmed", message="昨天的话")
    state.upsert_record("2026-10-12", "小A", "confirmed", message="今天的话")
    state.upsert_record("2026-10-12", "小B", "confirmed", message="今天的话")
    assert state.last_message_for("小A") == "今天的话"
    usage = state.message_usage()
    assert usage["今天的话"] == "2026-10-12"
    assert usage["昨天的话"] == "2026-10-11"


def test_day_stats(tmp_path):
    _fresh_state(tmp_path)
    for i in range(4):
        state.upsert_record("2026-10-12", f"好友{i}", "confirmed")
    for i in range(4, 7):
        state.upsert_record("2026-10-12", f"好友{i}", "rejected")
    state.upsert_record("2026-10-12", "好友7", "failed")
    stats = state.day_stats("2026-10-12")
    assert stats["attempted"] == 8
    assert stats["confirmed"] == 4
    assert stats["rejected"] == 3
    assert abs(stats["rejection_ratio"] - 3 / 8) < 1e-9


def test_session_checks(tmp_path):
    _fresh_state(tmp_path)
    assert state.latest_session_check() is None
    state.add_session_check("2026-10-12", ok=True, note="ok")
    state.add_session_check("2026-10-12", ok=False, note="login_required")
    latest = state.latest_session_check()
    assert latest["ok"] == 0
    assert latest["note"] == "login_required"


def test_consecutive_bad_days_counts_only_recent_streak(tmp_path):
    _fresh_state(tmp_path)
    # 10-09 异常、10-10 正常、10-11/10-12 连续异常
    for day, statuses in {
        "2026-10-09": ["rejected", "rejected", "confirmed"],
        "2026-10-10": ["confirmed", "confirmed"],
        "2026-10-11": ["rejected", "rejected", "rejected", "rejected"],
        "2026-10-12": ["rejected", "rejected", "confirmed"],
    }.items():
        for index, status in enumerate(statuses):
            state.upsert_record(day, f"好友{index}", status)
    assert state.consecutive_bad_days_before("2026-10-12", threshold=0.5) == 2
    assert state.consecutive_bad_days_before("2026-10-10", threshold=0.5) == 0


def test_latest_run_today(tmp_path):
    _fresh_state(tmp_path)
    assert state.latest_run_today() is None
    run_id = state.start_run("scheduled")
    assert state.latest_run_today()["status"] == "running"
    state.finish_run(run_id, "completed", "全部成功")
    assert state.latest_run_today()["status"] == "completed"


def test_recover_in_flight_requires_confirmation(tmp_path):
    _fresh_state(tmp_path)
    state.upsert_record("2026-10-12", "小A", "in_flight")
    state.upsert_record("2026-10-12", "小B", "confirmed")
    state.recover_in_flight("2026-10-12")
    rows = state.get_day_records("2026-10-12")
    assert rows["小A"]["status"] == "needs_confirmation"
    assert rows["小B"]["status"] == "confirmed"


def test_connect_returns_cached_connection(tmp_path):
    db = _fresh_state(tmp_path)
    first = state.connect(db)
    second = state.connect(db)
    assert first is second
