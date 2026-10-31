"""手动确认（面板 API）的行为测试。"""

import asyncio

import pytest
from fastapi import HTTPException

from spark import state


def _fresh_state(tmp_path):
    db = tmp_path / "state.sqlite3"
    state.connect(db)
    state.ensure_schema()


def test_manual_confirm_sets_confirmed(tmp_path):
    _fresh_state(tmp_path)
    day = state.today()
    state.upsert_friend("Test Friend D")
    state.upsert_record(
        day,
        "Test Friend D",
        "rejected",
        message="Sample test message",
        detail="server never responded to message/send",
        slot="evening",
    )

    from spark.server import confirm_friend_sent

    result = asyncio.run(confirm_friend_sent("Test Friend D"))
    assert result == {"ok": True, "status": "confirmed"}
    row = state.get_day_records(day)["Test Friend D"]
    assert row["status"] == "confirmed"
    assert "手动确认已送达" in row["detail"]
    assert "server never responded" in row["detail"]
    assert row["sent_at"]
    assert row["slot"] == "evening"


def test_manual_confirm_guards(tmp_path):
    _fresh_state(tmp_path)
    day = state.today()
    state.upsert_friend("小A")
    state.upsert_friend("小B")

    from spark.server import confirm_friend_sent

    # 今天还没有记录 → 409
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(confirm_friend_sent("小A"))
    assert excinfo.value.status_code == 409

    # 已经是已确认 → 409
    state.upsert_record(day, "小A", "confirmed")
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(confirm_friend_sent("小A"))
    assert excinfo.value.status_code == 409

    # skipped 等不支持的状态 → 409
    state.upsert_record(day, "小B", "skipped")
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(confirm_friend_sent("小B"))
    assert excinfo.value.status_code == 409

    # 不存在的名单 → 404
    with pytest.raises(HTTPException) as excinfo:
        asyncio.run(confirm_friend_sent("不存在的好友"))
    assert excinfo.value.status_code == 404
