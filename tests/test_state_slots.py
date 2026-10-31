import datetime
import random
import sqlite3

from spark import scheduler, state


SLOTS = ["morning", "forenoon", "noon", "afternoon", "evening"]


def _fresh(tmp_path):
    db = tmp_path / "state.sqlite3"
    state.connect(db)
    state.ensure_schema()
    return db


def _slot_of(plan, friend):
    for slot, names in plan.items():
        if friend in names:
            return slot
    return None


def test_ensure_slot_plan_balances_and_is_stable(tmp_path):
    _fresh(tmp_path)
    friends = [f"好友{i:02d}" for i in range(18)]
    state.ensure_slot_plan("2026-10-12", friends, SLOTS)
    plan = state.get_slot_plan("2026-10-12")
    counts = [len(plan[slot]) for slot in SLOTS]
    assert sum(counts) == 18
    assert max(counts) - min(counts) <= 1
    assert "新同学" not in plan["morning"]

    # 同日重复调用不重排
    state.ensure_slot_plan("2026-10-12", friends, SLOTS)
    assert state.get_slot_plan("2026-10-12") == plan

    # 新好友补入人数组，已有分组不变
    state.ensure_slot_plan("2026-10-12", friends + ["新同学"], SLOTS)
    plan2 = state.get_slot_plan("2026-10-12")
    for friend in friends:
        assert _slot_of(plan2, friend) == _slot_of(plan, friend)
    assert _slot_of(plan2, "新同学") is not None


def test_slot_plan_differs_between_days(tmp_path):
    _fresh(tmp_path)
    friends = [f"好友{i:02d}" for i in range(18)]
    state.ensure_slot_plan("2026-10-12", friends, SLOTS)
    state.ensure_slot_plan("2026-10-13", friends, SLOTS)
    plan_a = state.get_slot_plan("2026-10-12")
    plan_b = state.get_slot_plan("2026-10-13")
    assert plan_a != plan_b


def test_slot_runs_mark_missed_and_pending(tmp_path):
    _fresh(tmp_path)
    tz = scheduler.TZ
    slots = [
        {"id": "morning", "start": "08:00", "end": "09:30"},
        {"id": "noon", "start": "12:00", "end": "14:00"},
    ]
    now = datetime.datetime(2026, 10, 12, 10, 0, tzinfo=tz)
    state.ensure_slot_runs(
        "2026-10-12",
        slots,
        {"morning": 4, "noon": 4},
        now,
        rng=random.Random(1),
        min_gap=30,
        max_gap=120,
    )
    runs = state.get_slot_runs("2026-10-12")
    assert runs["morning"]["status"] == "missed"
    assert runs["noon"]["status"] == "pending"
    assert runs["noon"]["run_at"]

    state.mark_slot_run("2026-10-12", "noon", "done", note="ok")
    assert state.get_slot_runs("2026-10-12")["noon"]["status"] == "done"


def test_day_notes(tmp_path):
    _fresh(tmp_path)
    state.add_day_note("2026-10-12", "因禁用词跳过 2 条")
    notes = state.get_day_notes("2026-10-12")
    assert "因禁用词跳过 2 条" in notes


def test_schema_migration_adds_slot_columns(tmp_path):
    db = tmp_path / "legacy.sqlite3"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE send_records (
            day TEXT NOT NULL, friend TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
            message TEXT DEFAULT '', attempts INTEGER NOT NULL DEFAULT 0,
            detail TEXT DEFAULT '', receipt TEXT DEFAULT '', sent_at TEXT DEFAULT '',
            updated_at TEXT NOT NULL, PRIMARY KEY (day, friend));
        CREATE TABLE runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, day TEXT NOT NULL, trigger TEXT NOT NULL,
            status TEXT NOT NULL, note TEXT DEFAULT '', started_at TEXT NOT NULL, ended_at TEXT);
        """
    )
    conn.commit()
    conn.close()

    state.connect(db)
    state.ensure_schema()
    state.ensure_schema()  # 幂等
    columns = {
        row[1] for row in sqlite3.connect(db).execute("PRAGMA table_info(send_records)")
    }
    assert "slot" in columns
    runs_columns = {
        row[1] for row in sqlite3.connect(db).execute("PRAGMA table_info(runs)")
    }
    assert "slot" in runs_columns
