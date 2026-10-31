"""SQLite 状态：运行记录、每人每天发送记录、会话体检、好友清单。"""

from __future__ import annotations

import sqlite3
import threading
import random
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from spark import config


TZ = ZoneInfo("Asia/Shanghai")
_connections = {}
_lock = threading.Lock()
_default_path = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    day TEXT NOT NULL,
    trigger TEXT NOT NULL,
    status TEXT NOT NULL,
    note TEXT DEFAULT '',
    started_at TEXT NOT NULL,
    ended_at TEXT
);
CREATE TABLE IF NOT EXISTS send_records (
    day TEXT NOT NULL,
    friend TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    message TEXT DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    detail TEXT DEFAULT '',
    receipt TEXT DEFAULT '',
    sent_at TEXT DEFAULT '',
    updated_at TEXT NOT NULL,
    PRIMARY KEY (day, friend)
);
CREATE TABLE IF NOT EXISTS session_checks (
    day TEXT PRIMARY KEY,
    ok INTEGER NOT NULL,
    note TEXT DEFAULT '',
    checked_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS friends (
    name TEXT PRIMARY KEY,
    enabled INTEGER NOT NULL DEFAULT 1,
    display_name TEXT DEFAULT ''
);
CREATE TABLE IF NOT EXISTS slot_plan (
    day TEXT NOT NULL,
    friend TEXT NOT NULL,
    slot TEXT NOT NULL,
    PRIMARY KEY (day, friend)
);
CREATE TABLE IF NOT EXISTS slot_runs (
    day TEXT NOT NULL,
    slot TEXT NOT NULL,
    run_at TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    note TEXT DEFAULT '',
    PRIMARY KEY (day, slot)
);
CREATE TABLE IF NOT EXISTS day_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    day TEXT NOT NULL,
    note TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

_COLUMN_MIGRATIONS = (
    ("send_records", "slot", "TEXT DEFAULT ''"),
    ("runs", "slot", "TEXT DEFAULT ''"),
)


def today():
    return datetime.now(TZ).strftime("%Y-%m-%d")


def now_iso():
    return datetime.now(TZ).isoformat(timespec="seconds")


def connect(path=None):
    global _default_path
    path = Path(path or _default_path or config.STATE_PATH)
    _default_path = path
    key = str(path)
    with _lock:
        conn = _connections.get(key)
        if conn is None:
            path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(path))
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            _connections[key] = conn
        return conn


def ensure_schema():
    conn = connect()
    conn.executescript(SCHEMA)
    for table, column, ddl in _COLUMN_MIGRATIONS:
        columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    conn.commit()


def start_run(trigger, slot=""):
    conn = connect()
    cursor = conn.execute(
        "INSERT INTO runs (day, trigger, status, started_at, slot) VALUES (?, ?, ?, ?, ?)",
        (today(), trigger, "running", now_iso(), slot),
    )
    conn.commit()
    return cursor.lastrowid


def finish_run(run_id, status, note=""):
    conn = connect()
    conn.execute(
        "UPDATE runs SET status=?, note=?, ended_at=? WHERE id=?",
        (status, note, now_iso(), run_id),
    )
    conn.commit()


def latest_run_today():
    conn = connect()
    return conn.execute(
        "SELECT * FROM runs WHERE day=? ORDER BY id DESC LIMIT 1",
        (today(),),
    ).fetchone()


def recover_in_flight(day):
    """中断时无法确定请求是否已发出，留待人工核实，避免重复发送。"""
    conn = connect()
    conn.execute(
        "UPDATE send_records SET status='needs_confirmation', detail='上轮中断，送达情况待人工核实；不自动重发', updated_at=? WHERE day=? AND status='in_flight'",
        (now_iso(), day),
    )
    conn.commit()


def upsert_record(day, friend, status, message=None, detail=None, receipt=None, sent_at=None, slot=None):
    conn = connect()
    conn.execute(
        """
        INSERT INTO send_records (day, friend, status, message, detail, receipt, sent_at, updated_at, slot)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(day, friend) DO UPDATE SET
            status=excluded.status,
            message=CASE WHEN excluded.message IS NOT NULL THEN excluded.message ELSE message END,
            detail=CASE WHEN excluded.detail IS NOT NULL THEN excluded.detail ELSE detail END,
            receipt=CASE WHEN excluded.receipt IS NOT NULL THEN excluded.receipt ELSE receipt END,
            sent_at=CASE WHEN excluded.sent_at IS NOT NULL THEN excluded.sent_at ELSE sent_at END,
            slot=CASE WHEN excluded.slot IS NOT NULL THEN excluded.slot ELSE slot END,
            updated_at=excluded.updated_at
        """,
        (
            day,
            friend,
            status,
            message,
            detail or "",
            receipt or "",
            sent_at or "",
            now_iso(),
            slot,
        ),
    )
    conn.commit()


def ensure_slot_plan(day, friends, slot_ids, rng=None):
    """生成/补全当日分组：均衡、同日幂等、新好友补入人数最少的组。"""
    conn = connect()
    existing = {
        row["friend"]: row["slot"]
        for row in conn.execute("SELECT friend, slot FROM slot_plan WHERE day=?", (day,))
    }
    counts = {slot: 0 for slot in slot_ids}
    for slot in existing.values():
        if slot in counts:
            counts[slot] += 1
    rng = rng or random.Random(f"{day}:plan")
    new_friends = [str(name) for name in friends if str(name) not in existing]
    rng.shuffle(new_friends)
    for friend in new_friends:
        target = min(counts, key=lambda slot: (counts[slot], rng.random()))
        conn.execute(
            "INSERT OR REPLACE INTO slot_plan (day, friend, slot) VALUES (?, ?, ?)",
            (day, friend, target),
        )
        counts[target] += 1
    conn.commit()
    return get_slot_plan(day)


def get_slot_plan(day):
    conn = connect()
    plan = {}
    for row in conn.execute(
        "SELECT friend, slot FROM slot_plan WHERE day=? ORDER BY rowid", (day,)
    ):
        plan.setdefault(row["slot"], []).append(row["friend"])
    return plan


def ensure_slot_runs(day, slots, batch_sizes, now, rng=None, min_gap=30, max_gap=120, scan_budget=600, retry_delay_minutes=10):
    """为每个时段预排当天的启动时刻；过点的时段直接记 missed。"""
    from spark import scheduler

    conn = connect()
    existing = {
        row["slot"]
        for row in conn.execute("SELECT slot FROM slot_runs WHERE day=?", (day,))
    }
    rng = rng or random.Random(f"{day}:runs")
    for slot in slots:
        slot_id = slot["id"]
        if slot_id in existing:
            continue
        count = int(batch_sizes.get(slot_id, 0))
        if count <= 0:
            conn.execute(
                "INSERT INTO slot_runs (day, slot, run_at, status, note) VALUES (?, ?, '', 'empty', '')",
                (day, slot_id),
            )
            continue
        start_h, start_m = scheduler.parse_time(slot["start"])
        end_h, end_m = scheduler.parse_time(slot["end"])
        start_dt = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
        end_dt = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
        run_at = scheduler.compute_slot_run_at(
            start_dt, end_dt, count, now, rng=rng, min_gap=min_gap, max_gap=max_gap,
            scan_budget=scan_budget, retry_delay_minutes=retry_delay_minutes
        )
        conn.execute(
            "INSERT INTO slot_runs (day, slot, run_at, status) VALUES (?, ?, ?, ?)",
            (
                day,
                slot_id,
                run_at.isoformat(timespec="seconds") if run_at else "",
                "pending" if run_at else "missed",
            ),
        )
    conn.commit()


def get_slot_runs(day):
    conn = connect()
    return {
        row["slot"]: dict(row)
        for row in conn.execute("SELECT * FROM slot_runs WHERE day=?", (day,))
    }


def mark_slot_run(day, slot, status, note="", run_at=None):
    conn = connect()
    conn.execute(
        """INSERT INTO slot_runs (day, slot, status, note, run_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(day, slot) DO UPDATE SET status=excluded.status,
        note=excluded.note, run_at=CASE WHEN ? IS NULL THEN slot_runs.run_at ELSE excluded.run_at END""",
        (day, slot, status, note, run_at or "", run_at),
    )
    conn.commit()


def bump_slot_attempts(day, slot):
    conn = connect()
    conn.execute(
        "UPDATE slot_runs SET attempts=attempts+1 WHERE day=? AND slot=?",
        (day, slot),
    )
    conn.commit()


def add_day_note(day, note):
    conn = connect()
    conn.execute(
        "INSERT INTO day_notes (day, note, created_at) VALUES (?, ?, ?)",
        (day, note, now_iso()),
    )
    conn.commit()


def get_day_notes(day):
    conn = connect()
    return [
        row["note"]
        for row in conn.execute(
            "SELECT note FROM day_notes WHERE day=? ORDER BY id", (day,)
        )
    ]


def increment_attempts(day, friend):
    conn = connect()
    conn.execute(
        "UPDATE send_records SET attempts=attempts+1 WHERE day=? AND friend=?",
        (day, friend),
    )
    conn.commit()


def get_day_records(day):
    conn = connect()
    rows = conn.execute("SELECT * FROM send_records WHERE day=?", (day,)).fetchall()
    return {row["friend"]: dict(row) for row in rows}


def last_message_for(friend):
    conn = connect()
    row = conn.execute(
        "SELECT message FROM send_records WHERE friend=? AND message!='' ORDER BY day DESC LIMIT 1",
        (friend,),
    ).fetchone()
    return row["message"] if row else ""


def message_usage():
    conn = connect()
    rows = conn.execute(
        "SELECT message, MAX(day) AS last_day FROM send_records "
        "WHERE message!='' GROUP BY message"
    ).fetchall()
    return {row["message"]: row["last_day"] for row in rows}


def day_stats(day):
    conn = connect()
    rows = conn.execute(
        "SELECT status, COUNT(*) AS count FROM send_records WHERE day=? GROUP BY status",
        (day,),
    ).fetchall()
    stats = {
        "confirmed": 0,
        "unconfirmed": 0,
        "rejected": 0,
        "failed": 0,
        "pending": 0,
        "skipped": 0,
        "in_flight": 0,
        "needs_confirmation": 0,
    }
    for row in rows:
        stats[row["status"]] = row["count"]
    attempted = stats["confirmed"] + stats["unconfirmed"] + stats["rejected"] + stats["failed"] + stats["needs_confirmation"]
    stats["attempted"] = attempted
    stats["rejection_ratio"] = stats["rejected"] / attempted if attempted else 0.0
    return stats


def consecutive_bad_days_before(day, threshold, limit=30):
    """从 ``day`` 往前数连续"异常日"（有尝试且拒收占比 > threshold）的天数。"""
    conn = connect()
    rows = conn.execute(
        "SELECT DISTINCT day FROM send_records WHERE day<=? ORDER BY day DESC LIMIT ?",
        (day, limit),
    ).fetchall()
    count = 0
    for row in rows:
        stats = day_stats(row["day"])
        if stats["attempted"] == 0:
            continue
        if stats["rejection_ratio"] > threshold:
            count += 1
        else:
            break
    return count


def add_session_check(day, ok, note=""):
    conn = connect()
    conn.execute(
        """
        INSERT INTO session_checks (day, ok, note, checked_at) VALUES (?, ?, ?, ?)
        ON CONFLICT(day) DO UPDATE SET ok=excluded.ok, note=excluded.note, checked_at=excluded.checked_at
        """,
        (day, 1 if ok else 0, note, now_iso()),
    )
    conn.commit()


def latest_session_check():
    conn = connect()
    row = conn.execute("SELECT * FROM session_checks ORDER BY day DESC LIMIT 1").fetchone()
    return dict(row) if row else None


def upsert_friend(name, enabled=True, display_name=None):
    conn = connect()
    conn.execute(
        """
        INSERT INTO friends (name, enabled, display_name) VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            enabled=excluded.enabled,
            display_name=CASE WHEN excluded.display_name IS NOT NULL THEN excluded.display_name ELSE display_name END
        """,
        (name, 1 if enabled else 0, display_name or ""),
    )
    conn.commit()


def list_friends(enabled_only=False):
    conn = connect()
    query = "SELECT * FROM friends"
    if enabled_only:
        query += " WHERE enabled=1"
    return [dict(row) for row in conn.execute(query + " ORDER BY name")]


def set_friend_enabled(name, enabled):
    conn = connect()
    conn.execute(
        "UPDATE friends SET enabled=? WHERE name=?",
        (1 if enabled else 0, name),
    )
    conn.commit()
