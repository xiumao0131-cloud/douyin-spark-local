import pytest

from spark import config, state


@pytest.fixture
def fresh_state(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIG_PATH", tmp_path / "config.json")
    monkeypatch.setattr(config, "REPORTS_DIR", tmp_path / "reports")
    db = tmp_path / "state.sqlite3"
    state.connect(db)
    state.ensure_schema()
    return db


def _seed(fresh_state, statuses, login_ok=True, day="2026-10-12"):
    for index, status in enumerate(statuses):
        state.upsert_record(day, f"好友{index}", status)
    state.add_session_check(day, ok=login_ok, note="测试")


def test_summary_all_good_needs_no_action(fresh_state):
    from spark import service

    _seed(fresh_state, ["confirmed", "confirmed"])
    result = service.build_summary("2026-10-12")
    assert result["problems"] == []
    assert "2/2" in result["text"]
    assert "无需处理" in result["text"]


def test_summary_lists_rejected_and_unconfirmed_and_login(fresh_state):
    from spark import service

    _seed(
        fresh_state,
        ["confirmed", "rejected", "unconfirmed"],
        login_ok=False,
    )
    result = service.build_summary("2026-10-12")
    text = result["text"]
    assert "拒收" in text
    assert "未确认" in text
    assert "登录已失效" in text


def test_summary_marks_empty_day(fresh_state):
    from spark import service

    state.add_session_check("2026-10-12", ok=True, note="测试")
    result = service.build_summary("2026-10-12")
    assert "没有任何发送记录" in result["text"]


def test_summary_unconfirmed_is_not_a_problem(fresh_state):
    from spark import service

    _seed(fresh_state, ["unconfirmed", "unconfirmed", "unconfirmed"])
    result = service.build_summary("2026-10-12")
    assert result["problems"] == []
    assert "未确认 3 人" in result["text"]


def test_summary_and_overview_work_with_slot_plan_and_rows(fresh_state):
    """回归：有真实记录行 + 时段计划时，总结与总览不能再 500。"""
    import asyncio

    from spark import service

    _seed(fresh_state, ["unconfirmed", "confirmed"])
    state.ensure_slot_plan("2026-10-12", ["好友0", "好友1"], ["morning", "noon"])
    result = service.build_summary("2026-10-12")
    assert "时段：" in result["text"]

    from spark.server import overview

    payload = asyncio.run(overview())
    assert "friendsTotal" in payload


def test_summary_missed_slot_with_all_sent_is_not_a_problem(fresh_state):
    import datetime
    import random

    from spark import service, scheduler

    day = "2026-10-12"
    state.upsert_record(day, "好友0", "confirmed")
    state.ensure_slot_plan(day, ["好友0"], ["morning"])
    now = datetime.datetime(2026, 10, 12, 10, 0, tzinfo=scheduler.TZ)
    state.ensure_slot_runs(
        day,
        [{"id": "morning", "start": "08:00", "end": "09:30"}],
        {"morning": 1},
        now,
        rng=random.Random(1),
    )
    result = service.build_summary(day)
    assert not any("错过" in item for item in result["problems"])
    assert "晨间 1/1" in result["text"]


def test_summary_missed_slot_with_unsent_member_flags_problem(fresh_state):
    import datetime
    import random

    from spark import service, scheduler

    day = "2026-10-12"
    state.upsert_record(day, "好友0", "failed")
    state.ensure_slot_plan(day, ["好友0"], ["morning"])
    now = datetime.datetime(2026, 10, 12, 10, 0, tzinfo=scheduler.TZ)
    state.ensure_slot_runs(
        day,
        [{"id": "morning", "start": "08:00", "end": "09:30"}],
        {"morning": 1},
        now,
        rng=random.Random(1),
    )
    result = service.build_summary(day)
    assert any("晨间" in item and "错过" in item for item in result["problems"])


def test_write_summary_file(fresh_state):
    from spark import service

    _seed(fresh_state, ["confirmed", "confirmed"])
    payload = service.build_summary("2026-10-12")
    path = service.write_summary_file("2026-10-12", payload)
    assert path.exists()
    assert "2/2" in path.read_text(encoding="utf-8")
