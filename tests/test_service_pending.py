from spark import service


def _row(status, attempts=1):
    return {"status": status, "attempts": attempts, "message": ""}


def test_select_pending_skips_done_statuses_and_keeps_failed():
    records = {
        "a": _row("confirmed"),
        "b": _row("unconfirmed"),
        "c": _row("failed"),
        "d": _row("pending", 0),
    }
    pending = service.select_pending(["a", "b", "c", "d", "e"], records, cap=2)
    assert pending == ["c", "d", "e"]


def test_select_pending_respects_attempt_cap():
    records = {"a": _row("failed", 2)}
    assert service.select_pending(["a"], records, cap=2) == []


def test_slot_members_all_sent():
    records = {
        "a": _row("confirmed"),
        "b": _row("unconfirmed"),
        "c": _row("failed"),
    }
    assert service.slot_members_all_sent(["a", "b"], records) is True
    assert service.slot_members_all_sent(["a", "c"], records) is False
    assert service.slot_members_all_sent(["a", "x"], records) is False
