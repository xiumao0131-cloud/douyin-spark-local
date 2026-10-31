import datetime
import random
from zoneinfo import ZoneInfo

from spark import messages


def test_validate_rejects_deny_words_case_insensitive():
    ok, reason = messages.validate_message("今天过得怎么样～", deny_words=["微信", "vx"])
    assert ok is True
    bad, bad_reason = messages.validate_message("加个微信聊吧", deny_words=["微信", "vx"])
    assert bad is False
    assert "微信" in bad_reason
    mixed, _ = messages.validate_message("加我Vx", deny_words=["微信", "vx"])
    assert mixed is False


def test_validate_rejects_overlong_message():
    ok, _ = messages.validate_message("短句")
    assert ok is True
    bad, reason = messages.validate_message("哈" * 121)
    assert bad is False
    assert "长度" in reason


def test_pick_assigns_distinct_messages_when_pool_is_large():
    pool = [f"文案{i:03d}" for i in range(50)]
    friends = ["a", "b", "c", "d", "e"]
    picked = messages.pick_messages(
        friends,
        pool,
        used_today=set(),
        last_used_by_friend={},
        rng=random.Random(7),
    )
    assert set(picked) == set(friends)
    assert len(set(picked.values())) == len(friends)


def test_pick_never_repeats_a_friends_last_message():
    pool = ["甲", "乙", "丙", "丁"]
    friends = ["a", "b"]
    picked = messages.pick_messages(
        friends,
        pool,
        used_today=set(),
        last_used_by_friend={"a": "甲", "b": "乙"},
        rng=random.Random(1),
    )
    assert picked["a"] != "甲"
    assert picked["b"] != "乙"


def test_pick_skips_messages_already_used_today():
    pool = ["甲", "乙", "丙"]
    friends = ["a", "b"]
    picked = messages.pick_messages(
        friends,
        pool,
        used_today={"甲"},
        last_used_by_friend={},
        rng=random.Random(2),
    )
    assert "甲" not in picked.values()


def test_pick_falls_back_to_reuse_when_pool_smaller_than_friends():
    pool = ["唯一", "第二"]
    friends = ["a", "b", "c"]
    picked = messages.pick_messages(
        friends,
        pool,
        used_today=set(),
        last_used_by_friend={},
        rng=random.Random(3),
    )
    assert set(picked) == set(friends)
    assert set(picked.values()) == {"唯一", "第二"}


def test_pick_returns_nothing_when_pool_is_empty():
    picked = messages.pick_messages(
        ["a"],
        [],
        used_today=set(),
        last_used_by_friend={},
        rng=random.Random(0),
    )
    assert picked == {}


def test_bucket_for_time_boundaries():
    tz = ZoneInfo("Asia/Shanghai")

    def bucket(hour, minute):
        return messages.bucket_for_time(
            datetime.datetime(2026, 10, 12, hour, minute, tzinfo=tz)
        )

    assert bucket(8, 0) == "morning"
    assert bucket(9, 29) == "morning"
    assert bucket(9, 30) == "forenoon"
    assert bucket(11, 59) == "forenoon"
    assert bucket(12, 0) == "noon"
    assert bucket(13, 59) == "noon"
    assert bucket(14, 0) == "afternoon"
    assert bucket(17, 59) == "afternoon"
    assert bucket(18, 0) == "evening"
    assert bucket(21, 30) == "evening"


def test_pick_excludes_morning_items_in_evening_bucket():
    pool = [
        {"text": "早安呀", "when": "morning"},
        {"text": "晚安啦", "when": "evening"},
        {"text": "今天过得怎么样", "when": "any"},
    ]
    picked = messages.pick_messages(
        ["a"],
        pool,
        used_today=set(),
        last_used_by_friend={},
        rng=random.Random(1),
        bucket="evening",
    )
    assert picked["a"] != "早安呀"


def test_pick_accepts_plain_strings_as_any():
    pool = ["甲", "乙"]
    picked = messages.pick_messages(
        ["a"],
        pool,
        used_today=set(),
        last_used_by_friend={},
        rng=random.Random(2),
        bucket="morning",
    )
    assert picked["a"] in {"甲", "乙"}


def test_pick_falls_back_to_full_pool_when_bucket_empty():
    pool = [{"text": "早安呀", "when": "morning"}]
    picked = messages.pick_messages(
        ["a"],
        pool,
        used_today=set(),
        last_used_by_friend={},
        rng=random.Random(3),
        bucket="evening",
    )
    assert picked["a"] == "早安呀"


def test_filter_valid_pool_skips_deny_words():
    pool = [
        {"text": "正常文案", "when": "any"},
        {"text": "加个QQ呗", "when": "any"},
    ]
    valid, skipped = messages.filter_valid_pool(pool)
    assert [messages.item_text(item) for item in valid] == ["正常文案"]
    assert skipped == 1
