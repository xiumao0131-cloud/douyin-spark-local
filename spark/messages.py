"""文案池与每日挑选规则。"""

from __future__ import annotations

import random

from spark import config


def validate_message(text, deny_words=None, max_length=None):
    """校验单条文案，返回 ``(ok, reason)``。"""
    text = str(text or "").strip()
    if not text:
        return False, "文案为空"
    settings = config.load_config().get("messages", {})
    deny = deny_words if deny_words is not None else settings.get("denyWords", [])
    limit = max_length if max_length is not None else settings.get("maxLength", 120)
    lowered = text.lower()
    for word in deny:
        token = str(word).strip()
        if token and token.lower() in lowered:
            return False, f"命中禁用词：{token}"
    if len(text) > limit:
        return False, f"超过长度上限（{len(text)}>{limit}）"
    return True, ""


VALID_WHEN = {"any", "morning", "forenoon", "noon", "afternoon", "evening"}


def bucket_for_time(dt, slots=None):
    """按时刻返回所属时段 id（morning/forenoon/noon/afternoon/evening）。

    不在任何时段内（如窗口外的手动发送）返回 None。
    """
    if slots is None:
        slots = config.load_config().get("schedule", {}).get("slots", [])
    minutes = dt.hour * 60 + dt.minute
    last_end = None
    for slot in slots:
        start_h, start_m = _parse_hhmm(slot.get("start", "00:00"))
        end_h, end_m = _parse_hhmm(slot.get("end", "00:00"))
        start = start_h * 60 + start_m
        end = end_h * 60 + end_m
        last_end = end
        if start <= minutes < end:
            return str(slot.get("id"))
    if slots and minutes == last_end:
        return str(slots[-1].get("id"))
    return None


def _parse_hhmm(text):
    hour, minute = str(text).split(":", 1)
    return int(hour), int(minute)


def filter_valid_pool(pool):
    """过滤掉命中禁用词/超长的文案，返回 ``(有效条目, 跳过数量)``。"""
    valid = []
    skipped = 0
    for item in pool or []:
        ok, _reason = validate_message(item_text(item))
        if ok:
            valid.append(item)
        else:
            skipped += 1
    return valid, skipped


def item_text(item):
    if isinstance(item, dict):
        return str(item.get("text") or "").strip()
    return str(item or "").strip()


def item_when(item):
    when = item.get("when") if isinstance(item, dict) else "any"
    return str(when) if str(when) in VALID_WHEN else "any"


def pick_messages(friends, pool, used_today, last_used_by_friend, rng=None, bucket=None):
    """为每位好友挑一条文案。

    规则：当天所有好友互不重复；同一好友不与上一条重复；
    先排除当天已用的；池子不够用时才允许复用。
    调用方可把 pool 按"最近使用时间升序"传入以获得 LRU 打散效果。
    """
    rng = rng or random
    friends = [str(item).strip() for item in friends or [] if str(item).strip()]
    if not friends:
        return {}
    unique = []
    seen = set()
    for item in pool or []:
        text = item_text(item)
        if text and text not in seen:
            seen.add(text)
            unique.append(text)
    if not unique:
        return {}

    if bucket is not None:
        filtered = [
            item_text(item)
            for item in pool or []
            if item_text(item) in seen
            and item_when(item) in (bucket, "any")
        ]
        if filtered:
            unique = list(dict.fromkeys(filtered))

    used_now = {str(item) for item in used_today or set()}
    last = {str(key): str(value) for key, value in (last_used_by_friend or {}).items()}
    order = unique[:]
    rng.shuffle(order)

    assigned = {}
    for friend in friends:
        candidate = next(
            (item for item in order if item not in used_now and last.get(friend) != item),
            None,
        )
        if candidate is None:
            candidate = next(
                (item for item in order if last.get(friend) != item),
                order[0],
            )
        assigned[friend] = candidate
        used_now.add(candidate)
    return assigned


def load_pool():
    """读取文案池对象并去重（保持顺序，供 LRU 调用方排序）。"""
    items = config.load_messages().get("items", [])
    pool = []
    seen = set()
    for item in items:
        text = item_text(item)
        if text and text not in seen:
            seen.add(text)
            pool.append({"text": text, "when": item_when(item)})
    return pool
