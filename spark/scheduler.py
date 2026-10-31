"""发送窗口与运行时机判断（Asia/Shanghai）。"""

from __future__ import annotations

import datetime
import random
from zoneinfo import ZoneInfo


TZ = ZoneInfo("Asia/Shanghai")


def parse_time(hhmm):
    hour_text, minute_text = str(hhmm).split(":", 1)
    return int(hour_text), int(minute_text)


def pick_run_time(now, window_start, window_end, random_start=True, rng=None):
    """计算今天这一轮的运行时刻。

    random_start 时在窗口起点到 (窗口终点-45分钟) 之间均匀取一个时刻，
    给一轮 18 人的发送留足空间；窗口不足 45 分钟则落在起点。
    """
    rng = rng or random
    start_hour, start_minute = parse_time(window_start)
    end_hour, end_minute = parse_time(window_end)
    start = now.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    end = now.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
    if start >= end:
        raise ValueError("windowStart 必须早于 windowEnd")
    if not random_start:
        return start
    latest = end - datetime.timedelta(minutes=45)
    if latest <= start:
        latest = start
    seconds = rng.randint(0, int((latest - start).total_seconds()))
    return start + datetime.timedelta(seconds=seconds)


def decide_now(now, window_start, window_end, ran_today):
    """给定当前时刻判断这轮该做什么：not_yet / run / done / too_late。"""
    start_hour, start_minute = parse_time(window_start)
    end_hour, end_minute = parse_time(window_end)
    start = now.replace(hour=start_hour, minute=start_minute, second=0, microsecond=0)
    end = now.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
    if now < start:
        return "not_yet"
    if now > end:
        return "too_late"
    return "done" if ran_today else "run"


def should_summarize(now, window_end, summarized=False):
    """是否到了生成当日总结的时刻（>= 窗口结束时刻，且今天还没生成过）。"""
    if summarized:
        return False
    end_hour, end_minute = parse_time(window_end)
    end = now.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
    return now >= end


def compute_slot_run_at(
    slot_start,
    slot_end,
    batch_size,
    now,
    rng=None,
    min_gap=30,
    max_gap=120,
    scan_budget=600,
    retry_delay_minutes=10,
):
    """为一个小批量时段计算随机启动时刻。

    - 预留扫描、发送、收尾和一次恢复重试的时间；
    - 若 now 已进入时段，则起点不早于 now；
    - 若 now 已过时段结束，返回 None（该时段记 missed，不跨时段补发）。
    """
    rng = rng or random
    if now >= slot_end:
        return None
    budget = datetime.timedelta(seconds=2 * (batch_size * max_gap + scan_budget + 180) + retry_delay_minutes * 60)
    latest = slot_end - budget
    if latest <= slot_start:
        latest = slot_start
    start_pick = max(slot_start, now)
    if start_pick > latest:
        return start_pick
    span = max(0, int((latest - start_pick).total_seconds()))
    return start_pick + datetime.timedelta(seconds=rng.randint(0, span))
