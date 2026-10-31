import pytest

from spark import config, service, state
from spark.util import binary_receipt_ok, receipt_is_strong


def test_binary_receipt_ok_detects_ok_and_message_id():
    body = '\x08d\x10\x8cP\x18\x00"\x02OK(\x00"$3c019141-0549-4dde-9a90-bd9237bc7e57(\x00'
    assert binary_receipt_ok(body) is True


def test_binary_receipt_ok_requires_both_markers():
    assert binary_receipt_ok("这里只有 OK 没有消息号") is False
    assert binary_receipt_ok('"$3c019141-0549-4dde-9a90-bd9237bc7e57" 但没有成功标记') is False
    assert binary_receipt_ok("") is False


def test_receipt_is_strong_accepts_protobuf_ok():
    receipt = {
        "call": "message_send",
        "httpStatus": 200,
        "ok": True,
        "protobufOk": True,
    }
    assert receipt_is_strong(receipt) is True


def test_receipt_is_strong_unchanged_for_json_and_failures():
    json_receipt = {
        "call": "message_send",
        "httpStatus": 200,
        "ok": True,
        "jsonOk": True,
    }
    assert receipt_is_strong(json_receipt) is True
    weak = {"call": "message_send", "httpStatus": 200, "ok": False}
    assert receipt_is_strong(weak) is False


@pytest.fixture
def fresh_state(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIG_PATH", tmp_path / "config.json")
    db = tmp_path / "state.sqlite3"
    state.connect(db)
    state.ensure_schema()
    return db


def test_reclassify_binary_receipts_promotes_unconfirmed(fresh_state):
    import json

    day = "2026-10-12"
    ok_receipt = {
        "call": "message_send",
        "httpStatus": 200,
        "ok": False,
        "protobufOk": True,
        "reason": '"\x02OK" $3c019141-0549-4dde-9a90-bd9237bc7e57',
    }
    legacy_receipt = {
        "call": "message_send",
        "httpStatus": 200,
        "ok": False,
        "reason": '\x08d\x10\x8cP\x18\x00"\x02OK(\x00"$aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"',
    }
    plain_receipt = {
        "call": "message_send",
        "httpStatus": 200,
        "ok": False,
        "reason": "plain failure",
    }
    state.upsert_record(day, "甲", "unconfirmed", message="hi", receipt=json.dumps(ok_receipt))
    state.upsert_record(day, "乙", "unconfirmed", message="hi", receipt=json.dumps(legacy_receipt))
    state.upsert_record(day, "丙", "unconfirmed", message="hi", receipt=json.dumps(plain_receipt))
    state.upsert_record(day, "丁", "confirmed", message="hi")

    changed = service.reclassify_binary_receipts(day)
    rows = state.get_day_records(day)
    assert changed == 2
    assert rows["甲"]["status"] == "confirmed"
    assert rows["乙"]["status"] == "confirmed"
    assert rows["丙"]["status"] == "unconfirmed"
    assert rows["甲"]["message"] == "hi"
