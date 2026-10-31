"""共享小工具：鉴权 cookie 名单、日期比较、回执判定、id 归一化。"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

SCHEDULE_TZ = ZoneInfo("Asia/Shanghai")

AUTH_COOKIE_NAMES = {
    "sessionid",
    "sessionid_ss",
    "sid_guard",
    "sid_tt",
    "uid_tt",
    "uid_tt_ss",
}

_BINARY_MESSAGE_ID = re.compile(r"\$[0-9a-fA-F-]{16,}")


def binary_receipt_ok(text):
    """抖音 IM 发送接口现在返回 protobuf 二进制回执。

    二进制里带 ``OK`` 状态和消息 ID（形如 ``$uuid``）时，相当于服务端已接收，
    视为强确认证据（与 JSON ``jsonOk`` 等价）。
    """
    sample = str(text or "")
    return "OK" in sample and bool(_BINARY_MESSAGE_ID.search(sample))


def normalize_unique_id(value):
    """从抖音号文本里提取规范化的 id（去空白、去 @ 等）。"""
    text = str(value or "").strip()
    if not text:
        return ""
    if text.lower().startswith("抖音号："):
        text = text[4:]
    if text.lower().startswith("抖音号:"):
        text = text[4:]
    text = text.lstrip("@")
    return text.strip()


def same_calendar_day(left, right):
    """按 Asia/Shanghai 的日历日比较两个时间点是否同一天。"""
    if not left or not right:
        return False

    def as_aware(value):
        if isinstance(value, datetime):
            parsed = value
        else:
            return False
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=SCHEDULE_TZ)
        return parsed.astimezone(SCHEDULE_TZ).date()

    left_day = as_aware(left)
    right_day = as_aware(right)
    if left_day is False or right_day is False:
        return False
    return left_day == right_day


def receipt_is_strong(receipt):
    """服务端回执是否构成强确认（HTTP 2xx + message_send + jsonOk 或 protobufOk）。"""
    if not isinstance(receipt, dict):
        return False
    try:
        http_status = int(receipt.get("httpStatus") or 0)
    except (TypeError, ValueError):
        http_status = 0
    return (
        bool(receipt.get("ok"))
        and 200 <= http_status < 300
        and str(receipt.get("call") or "").strip() == "message_send"
        and (receipt.get("jsonOk") is True or receipt.get("protobufOk") is True)
    )
