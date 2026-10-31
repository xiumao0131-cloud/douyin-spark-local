"""本地 Web 面板（FastAPI 单页 + JSON API + 内置调度循环）。"""

from __future__ import annotations

import asyncio
import datetime
import logging
import random
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from spark import config, messages, notify, scheduler, service, state


logger = logging.getLogger(__name__)
_preflight_done_days = set()
_missed_notified_days = set()
_summary_done_days = set()
_login_task = None


async def _scheduler_loop():
    while True:
        try:
            await _tick()
        except Exception:
            logger.exception("scheduler tick failed")
        await asyncio.sleep(60)


async def _tick():
    cfg = config.load_config()
    if cfg.get("risk", {}).get("paused"):
        return
    now = datetime.datetime.now(scheduler.TZ)
    day = now.strftime("%Y-%m-%d")
    window = cfg["schedule"]
    slots = window.get("slots", [])

    start_hour, start_minute = scheduler.parse_time(window["windowStart"])
    preflight_from = now.replace(
        hour=start_hour, minute=start_minute, second=0, microsecond=0
    ) - datetime.timedelta(minutes=30)
    if preflight_from <= now < now.replace(hour=start_hour, minute=start_minute):
        if day not in _preflight_done_days:
            _preflight_done_days.add(day)
            ok, note = await service.probe_session()
            if not ok:
                notify.notify(
                    "抖音续火花：今晚需要扫码续期",
                    f"登录预检失败（{note[:80]}）。今晚发送前请到面板「续期登录」扫码，否则今日不会发送。",
                )

    # ---- 五时段调度 ----
    first_start = now.replace(
        hour=start_hour, minute=start_minute, second=0, microsecond=0
    )
    if slots and now >= first_start:
        friends = [row["name"] for row in state.list_friends(enabled_only=True)]
        slot_ids = [slot["id"] for slot in slots]
        state.ensure_slot_plan(day, friends, slot_ids)
        plan = state.get_slot_plan(day)
        batch_sizes = {slot_id: len(plan.get(slot_id, [])) for slot_id in slot_ids}
        state.ensure_slot_runs(
            day,
            slots,
            batch_sizes,
            now,
            rng=random.Random(f"{day}:runs"),
            min_gap=cfg["send"]["minIntervalSec"],
            max_gap=cfg["send"]["maxIntervalSec"],
            scan_budget=cfg["friendListScan"]["maxScanSeconds"],
            retry_delay_minutes=window.get("retryDelayMinutes", 10),
        )
        runs = state.get_slot_runs(day)
        records = state.get_day_records(day)
        for slot in slots:
            slot_id = slot["id"]
            run = runs.get(slot_id)
            if not run or run["status"] != "pending" or not run["run_at"]:
                continue
            members = plan.get(slot_id, [])
            if service.slot_members_all_sent(members, records):
                state.mark_slot_run(
                    day, slot_id, "done", note="成员均已发送，跳过执行"
                )
                continue
            run_at = datetime.datetime.fromisoformat(run["run_at"])
            if now < run_at:
                continue
            end_hour, end_minute = scheduler.parse_time(slot["end"])
            end_dt = now.replace(hour=end_hour, minute=end_minute, second=0, microsecond=0)
            if now > end_dt:
                state.mark_slot_run(day, slot_id, "missed", note="超过时段未执行")
                continue
            logger.info("slot %s run at %s", slot_id, now.strftime("%H:%M"))
            result = await service.run_round(trigger="scheduled", slot=slot_id)
            now = datetime.datetime.now(scheduler.TZ)
            if result.get("status") == "completed" and service.slot_members_all_sent(
                members, state.get_day_records(day)
            ):
                state.mark_slot_run(
                    day,
                    slot_id,
                    "done",
                    note=f"本时段 {len(members)} 人均已发送",
                )
            elif result.get("status") in ("paused", "busy"):
                continue
            else:
                state.bump_slot_attempts(day, slot_id)
                attempts = state.get_slot_runs(day)[slot_id]["attempts"]
                retry_at = now + datetime.timedelta(minutes=window.get("retryDelayMinutes", 10))
                budget = datetime.timedelta(
                    seconds=batch_sizes.get(slot_id, 1) * cfg["send"]["maxIntervalSec"] + cfg["friendListScan"]["maxScanSeconds"] + 180
                )
                if attempts < window.get("maxSlotAttempts", 3) and result.get("status") not in (service.LOGIN_BLOCKED, service.REJECTED) and retry_at + budget <= end_dt:
                    state.mark_slot_run(
                        day,
                        slot_id,
                        "pending",
                        note=f"重试：{result.get('status')}",
                        run_at=retry_at.isoformat(timespec="seconds"),
                    )
                else:
                    state.mark_slot_run(
                        day, slot_id, "failed", note=str(result.get("status"))
                    )

    # Catch-up has its own persisted retry counter; never rerun already sent targets.
    cfg = config.load_config()
    now = datetime.datetime.now(scheduler.TZ)
    catch_h, catch_m = scheduler.parse_time(window.get("catchUpStart", "20:30"))
    end_h, end_m = scheduler.parse_time(window["windowEnd"])
    catch_start = now.replace(hour=catch_h, minute=catch_m, second=0, microsecond=0)
    day_end = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
    if catch_start <= now < day_end:
        friends = [row["name"] for row in state.list_friends(enabled_only=True)]
        records = state.get_day_records(day)
        pending = service.select_pending(friends, records, cfg["send"]["retryPerFriendPerDay"])
        catch_run = state.get_slot_runs(day).get("catch_up")
        attempts = catch_run["attempts"] if catch_run else 0
        due = not catch_run or not catch_run["run_at"] or now >= datetime.datetime.fromisoformat(catch_run["run_at"])
        if pending and due and (not catch_run or catch_run["status"] == "pending") and attempts < window.get("maxSlotAttempts", 3) and not cfg["risk"]["paused"] and not service._RUN_LOCK.locked():
            # Persist before dispatch, so a process restart cannot reset the retry limit.
            retry_at = now + datetime.timedelta(minutes=window.get("retryDelayMinutes", 10))
            state.mark_slot_run(day, "catch_up", "pending", run_at=retry_at.isoformat(timespec="seconds"))
            state.bump_slot_attempts(day, "catch_up")
            result = await service.run_round(trigger="catch_up", catch_up=True)
            if result.get("status") not in ("busy", "paused"):
                after = datetime.datetime.now(scheduler.TZ)
                retry_at = after + datetime.timedelta(minutes=window.get("retryDelayMinutes", 10))
                done = service.slot_members_all_sent(friends, state.get_day_records(day))
                status = "done" if done else "pending"
                if not done and (attempts + 1 >= window.get("maxSlotAttempts", 3) or retry_at >= day_end or result.get("status") in (service.LOGIN_BLOCKED, service.REJECTED)):
                    status = "failed"
                state.mark_slot_run(day, "catch_up", status, note=str(result.get("status")), run_at=retry_at.isoformat(timespec="seconds"))
                for slot in slots:
                    if service.slot_members_all_sent(state.get_slot_plan(day).get(slot["id"], []), state.get_day_records(day)):
                        state.mark_slot_run(day, slot["id"], "done", note="当天补漏后均已发送")
    now = datetime.datetime.now(scheduler.TZ)
    if scheduler.should_summarize(now, window["windowEnd"]) and day not in _summary_done_days:
        _summary_done_days.add(day)
        payload = service.build_summary(day)
        if service._RUN_LOCK.locked():
            payload["text"] += "（发送仍在进行中）"
            payload["problems"].append("发送仍在进行中")
        service.write_summary_file(day, payload)
        notify.notify(
            "抖音续火花今日总结",
            payload["text"][:160],
        )


@asynccontextmanager
async def lifespan(_app):
    state.ensure_schema()
    asyncio.create_task(_scheduler_loop())
    yield


app = FastAPI(title="抖音续火花", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(config.ROOT / "web")), name="static")


@app.middleware("http")
async def no_cache_static(request, call_next):
    """页面与静态资源禁止缓存：部署新版本后刷新即生效（避免旧 JS 缓存）。"""
    response = await call_next(request)
    path = request.url.path
    if path == "/" or path.startswith("/static"):
        response.headers["Cache-Control"] = "no-store"
    return response


class FriendPayload(BaseModel):
    name: str
    enabled: bool = True


class MessagePayload(BaseModel):
    text: str
    when: str = "any"


class SettingsPayload(BaseModel):
    config: dict


class CookiePayload(BaseModel):
    raw: str


def _login_health():
    check = state.latest_session_check()
    return {
        "day": check["day"] if check else None,
        "ok": bool(check["ok"]) if check else None,
        "note": check["note"] if check else "",
        "checkedAt": check["checked_at"] if check else None,
    }


@app.get("/")
async def index():
    return FileResponse(config.ROOT / "web" / "index.html")


@app.get("/api/overview")
async def overview():
    cfg = config.load_config()
    day = state.today()
    stats = state.day_stats(day)
    friends = state.list_friends()
    today_rows = state.get_day_records(day)
    done = sum(
        1
        for row in friends
        if (today_rows.get(row["name"]) or {}).get("status") == "confirmed"
    )
    last_run = state.latest_run_today()
    candidates = []
    now = datetime.datetime.now(scheduler.TZ)
    plan = state.get_slot_plan(day)
    slots = {slot["id"]: slot for slot in cfg["schedule"].get("slots", [])}
    for slot_id, run in state.get_slot_runs(day).items():
        if run["status"] != "pending" or not run["run_at"]:
            continue
        members = plan.get(slot_id, []) if slot_id != "catch_up" else [row["name"] for row in friends if row["enabled"]]
        if not members or service.slot_members_all_sent(members, today_rows):
            continue
        end_text = slots[slot_id]["end"] if slot_id in slots else cfg["schedule"]["windowEnd"]
        end_h, end_m = scheduler.parse_time(end_text)
        if now < now.replace(hour=end_h, minute=end_m, second=0, microsecond=0):
            candidates.append(datetime.datetime.fromisoformat(run["run_at"]))
    next_time = min(candidates) if candidates else None
    return {
        "day": day,
        "login": _login_health(),
        "stats": stats,
        "friendsTotal": len(friends),
        "confirmedToday": done,
        "poolSize": len(messages.load_pool()),
        "window": (
            f"{cfg['schedule']['windowStart']}–{cfg['schedule']['windowEnd']}"
        ),
        "paused": bool(cfg["risk"].get("paused", False)),
        "nextRunAt": next_time.strftime("%H:%M") if next_time else None,
        "lastRun": {
            "trigger": last_run["trigger"] if last_run else None,
            "status": last_run["status"] if last_run else None,
            "startedAt": last_run["started_at"] if last_run else None,
            "note": last_run["note"] if last_run else "",
        },
    }


@app.get("/api/friends")
async def friends_list():
    day = state.today()
    records = state.get_day_records(day)
    items = []
    for row in state.list_friends():
        record = records.get(row["name"])
        items.append(
            {
                "name": row["name"],
                "enabled": bool(row["enabled"]),
                "today": {
                    "status": record["status"] if record else "pending",
                    "message": record["message"] if record else "",
                    "detail": record["detail"] if record else "",
                    "attempts": record["attempts"] if record else 0,
                    "sentAt": record["sent_at"] if record else "",
                },
                "lastMessage": state.last_message_for(row["name"]),
            }
        )
    return {"friends": items}


@app.post("/api/friends")
async def add_friend(payload: FriendPayload):
    name = payload.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="名字不能为空")
    state.upsert_friend(name, enabled=payload.enabled)
    return {"ok": True}


@app.post("/api/friends/{name}/toggle")
async def toggle_friend(name: str):
    rows = state.list_friends()
    target = next((row for row in rows if row["name"] == name), None)
    if target is None:
        raise HTTPException(status_code=404, detail="好友不存在")
    state.set_friend_enabled(name, not bool(target["enabled"]))
    return {"ok": True, "enabled": not bool(target["enabled"])}


_CONFIRMABLE_STATUSES = {"unconfirmed", "rejected", "failed", "needs_confirmation"}


@app.post("/api/friends/{name}/confirm")
async def confirm_friend_sent(name: str):
    """手动确认某好友今天的消息已送达（弱回执/误判时的兜底，当天不再重发）。"""
    rows = state.list_friends()
    if not any(row["name"] == name for row in rows):
        raise HTTPException(status_code=404, detail="好友不存在")
    day = state.today()
    record = state.get_day_records(day).get(name)
    if record is None:
        raise HTTPException(status_code=409, detail="今天还没有该好友的发送记录")
    if record["status"] == "confirmed":
        raise HTTPException(status_code=409, detail="今天已经确认发送")
    if record["status"] not in _CONFIRMABLE_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=f"当前状态「{record['status']}」不支持手动确认",
        )
    old_detail = (record["detail"] or "").strip()
    note = f"手动确认已送达（面板操作 {state.now_iso()}，原状态 {record['status']}）"
    state.upsert_record(
        day,
        name,
        "confirmed",
        detail=f"{old_detail}；{note}" if old_detail else note,
        sent_at=record["sent_at"] or state.now_iso(),
    )
    return {"ok": True, "status": "confirmed"}


@app.delete("/api/friends/{name}")
async def delete_friend(name: str):
    from spark.state import connect

    conn = connect()
    conn.execute("DELETE FROM friends WHERE name=?", (name,))
    conn.commit()
    return {"ok": True}


@app.get("/api/messages")
async def messages_list():
    data = config.load_messages()
    deny = config.load_config().get("messages", {}).get("denyWords", [])
    for item in data["items"]:
        lowered = item["text"].lower()
        item["risky"] = any(str(word).lower() in lowered for word in deny if word)
    return data


@app.get("/api/messages/pending")
async def pending_messages():
    data = config.load_pending_messages()
    deny = config.load_config().get("messages", {}).get("denyWords", [])
    for item in data["items"]:
        lowered = item["text"].lower()
        item["risky"] = any(str(word).lower() in lowered for word in deny if word)
    return data


@app.post("/api/messages/pending/accept")
async def accept_pending(payload: MessagePayload):
    text = payload.text.strip()
    when = payload.when if payload.when in config.VALID_WHEN_VALUES else "any"
    pending = config.load_pending_messages()
    pending["items"] = [item for item in pending["items"] if item["text"] != text]
    config.save_pending_messages(pending)

    main = config.load_messages()
    if not any(item["text"] == text for item in main["items"]):
        main["items"].append({"text": text, "when": when})
        config.save_messages(main)
    return {
        "ok": True,
        "pending": len(pending["items"]),
        "pool": len(config.load_messages()["items"]),
    }


@app.post("/api/messages/pending/delete")
async def delete_pending(payload: MessagePayload):
    pending = config.load_pending_messages()
    pending["items"] = [item for item in pending["items"] if item["text"] != payload.text]
    config.save_pending_messages(pending)
    return {"ok": True, "pending": len(pending["items"])}


@app.post("/api/messages/pending/accept-all")
async def accept_all_pending():
    pending = config.load_pending_messages()
    main = config.load_messages()
    existing = {item["text"] for item in main["items"]}
    added = 0
    for item in pending["items"]:
        if item["text"] not in existing:
            main["items"].append(item)
            existing.add(item["text"])
            added += 1
    config.save_messages(main)
    config.save_pending_messages({"items": []})
    return {"ok": True, "added": added, "pool": len(config.load_messages()["items"])}


@app.post("/api/messages")
async def add_message(payload: MessagePayload):
    data = config.load_messages()
    text = payload.text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="文案不能为空")
    when = payload.when if payload.when in {"any", "morning", "noon", "evening"} else "any"
    if any(item["text"] == text for item in data["items"]):
        return {"ok": True, "duplicate": True}
    data["items"].append({"text": text, "when": when})
    config.save_messages(data)
    return {"ok": True, "count": len(data["items"])}


@app.post("/api/messages/update")
async def update_message(payload: MessagePayload):
    data = config.load_messages()
    text = payload.text.strip()
    when = payload.when if payload.when in {"any", "morning", "noon", "evening"} else "any"
    for item in data["items"]:
        if item["text"] == text:
            item["when"] = when
            config.save_messages(data)
            return {"ok": True}
    raise HTTPException(status_code=404, detail="文案不存在")


@app.post("/api/messages/delete")
async def delete_message(payload: MessagePayload):
    data = config.load_messages()
    data["items"] = [item for item in data["items"] if item["text"] != payload.text]
    config.save_messages(data)
    return {"ok": True, "count": len(data["items"])}


@app.get("/api/settings")
async def get_settings():
    return config.load_config()


@app.post("/api/settings")
async def save_settings(payload: SettingsPayload):
    return config.save_config(payload.config)


@app.get("/api/summary")
async def summary():
    day = state.today()
    payload = service.build_summary(day)
    path = config.REPORTS_DIR / f"{day}.md"
    payload["written"] = path.exists()
    return payload


@app.get("/api/slots")
async def slots_api():
    cfg = config.load_config()
    day = state.today()
    plan = state.get_slot_plan(day)
    runs = state.get_slot_runs(day)
    records = state.get_day_records(day)
    friends = {row["name"]: row for row in state.list_friends()}
    items = []
    for slot in cfg["schedule"].get("slots", []):
        slot_id = slot["id"]
        members = []
        for name in plan.get(slot_id, []):
            friend_row = friends.get(name)
            record = records.get(name)
            members.append(
                {
                    "name": name,
                    "enabled": bool(friend_row["enabled"]) if friend_row else False,
                    "status": record["status"] if record else "pending",
                    "message": record["message"] if record else "",
                }
            )
        run = runs.get(slot_id)
        sent_count = sum(
            1
            for member in members
            if member["status"] in ("confirmed", "unconfirmed")
        )
        items.append(
            {
                "id": slot_id,
                "label": slot["label"],
                "start": slot["start"],
                "end": slot["end"],
                "runStatus": run["status"] if run else "pending",
                "runAt": run["run_at"] if run else "",
                "sentCount": sent_count,
                "totalCount": len(members),
                "allSent": bool(members) and sent_count == len(members),
                "members": members,
            }
        )
    return {"day": day, "slots": items}


@app.post("/api/actions/send-now")
async def send_now():
    if service._RUN_LOCK.locked():
        return {"started": False, "reason": "busy"}
    asyncio.create_task(service.run_round(trigger="manual"))
    return {"started": True}


@app.post("/api/actions/catch-up")
async def catch_up():
    if service._RUN_LOCK.locked():
        return {"started": False, "reason": "busy"}
    cfg = config.load_config()
    now = datetime.datetime.now(scheduler.TZ)
    decision = scheduler.decide_now(
        now,
        cfg["schedule"]["windowStart"],
        cfg["schedule"]["windowEnd"],
        ran_today=state.latest_run_today() is not None,
    )
    if decision == "run":
        asyncio.create_task(service.run_round(trigger="catchup"))
        return {"started": True}
    return {"started": False, "reason": decision}


@app.post("/api/actions/preflight")
async def preflight():
    ok, note = await service.probe_session()
    return {"ok": ok, "note": note}


@app.post("/api/actions/login")
async def start_login():
    global _login_task
    if _login_task and not _login_task.done():
        return {"started": False, "reason": "login_in_progress"}
    _login_task = asyncio.create_task(service.login_flow())
    return {"started": True}


@app.post("/api/actions/login-cookies")
async def login_cookies(payload: CookiePayload):
    return await service.verify_and_save_cookies(payload.raw)


@app.get("/api/actions/login-status")
async def login_status():
    global _login_task
    if _login_task is None:
        return {"state": "idle"}
    if _login_task.done():
        if _login_task.exception() is None:
            return {"state": "done", "result": _login_task.result()}
        return {"state": "failed", "error": str(_login_task.exception())}
    return {"state": "running"}


@app.post("/api/actions/pause")
async def pause():
    cfg = config.load_config()
    cfg["risk"]["paused"] = True
    config.save_config(cfg)
    return {"paused": True}


@app.post("/api/actions/resume")
async def resume():
    cfg = config.load_config()
    cfg["risk"]["paused"] = False
    config.save_config(cfg)
    return {"paused": False}


@app.get("/api/logs")
async def logs(day: str | None = None):
    day = day or state.today()
    records = state.get_day_records(day)
    items = [
        {
            "friend": row["friend"],
            "status": row["status"],
            "message": row["message"],
            "attempts": row["attempts"],
            "detail": row["detail"],
            "sentAt": row["sent_at"],
        }
        for row in sorted(records.values(), key=lambda item: item["friend"])
    ]
    return {"day": day, "records": items, "stats": state.day_stats(day)}


@app.get("/api/history")
async def history():
    from spark.state import connect

    conn = connect()
    days = conn.execute(
        "SELECT DISTINCT day FROM send_records UNION SELECT DISTINCT day FROM runs ORDER BY day DESC LIMIT 30"
    ).fetchall()
    items = []
    for row in days:
        day = row[0]
        stats = state.day_stats(day)
        stats["day"] = day
        items.append(stats)
    return {"history": items}
