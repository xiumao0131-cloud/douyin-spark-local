"""任务编排：登录、预检、发送轮次、记录、熔断与通知。"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from spark import config, messages, notify, state
from spark.engine import browser, login, send as engine
from spark.util import binary_receipt_ok, receipt_is_strong


TZ = ZoneInfo("Asia/Shanghai")
logger = logging.getLogger(__name__)
_RUN_LOCK = asyncio.Lock()

CONFIRMED = "confirmed"
UNCONFIRMED = "unconfirmed"
REJECTED = "rejected"
FAILED = "failed"
IN_FLIGHT = "in_flight"
LOGIN_BLOCKED = "login_blocked"
NEEDS_CONFIRMATION = "needs_confirmation"


def _now():
    return datetime.now(TZ)


async def _open_session():
    network_mode = await browser.select_douyin_network_mode(engine.CREATOR_HOME_URL)
    playwright, context, _profile = await browser.get_persistent_browser_context(
        "account",
        root=str(config.PROFILE_DIR),
        network_mode=network_mode,
    )
    context.set_default_navigation_timeout(120000)
    context.set_default_timeout(120000)
    secrets = config.load_secrets()
    await engine.apply_stored_cookies_to_profile(
        context,
        secrets.get("cookies") or [],
        "account",
        only_when_empty=True,
    )
    page = await context.new_page()
    return playwright, context, page


async def _close_session(playwright, context):
    if context is not None:
        try:
            await context.close()
        except Exception:
            pass
    if playwright is not None:
        try:
            await playwright.stop()
        except Exception:
            pass


async def _open_chat_page(page):
    await engine.retry_operation(
        "open creator home",
        page.goto,
        retries=3,
        delay=5,
        url=engine.CREATOR_HOME_URL,
        wait_until="domcontentloaded",
    )
    await engine.retry_operation(
        "open chat page",
        page.goto,
        retries=3,
        delay=5,
        url=engine.CREATOR_CHAT_URL,
        wait_until="domcontentloaded",
    )
    try:
        await page.wait_for_selector("#sub-app", timeout=30000)
    except Exception:
        pass
    await asyncio.sleep(3)
    await engine.ensure_not_login_required(page, "account", "open_chat_page")


async def _sync_cookies(context):
    secrets = config.load_secrets()
    merged = await engine.merged_cookies_from_profile(
        context,
        secrets.get("cookies") or [],
    )
    config.save_secrets({**secrets, "cookies": merged})


async def probe_session():
    ok, note = await _probe()
    state.add_session_check(state.today(), ok, "预检：登录正常" if ok else note)
    return ok, note


async def _probe(cookies_override=None):
    """Headless 打开会话页检查登录态，返回 ``(ok, note)``。

    ``cookies_override`` 用于"粘贴 Cookie 续期"的验证：先验证通过、
    再落盘，无效 cookie 不会覆盖现有登录态。
    """
    playwright = context = None
    try:
        playwright, context, page = await _open_session()
        if cookies_override is not None:
            await context.add_cookies(cookies_override)
        await _open_chat_page(page)
        await _sync_cookies(context)
        return True, "登录正常"
    except Exception as exc:
        category = engine.classify_browser_failure("open_chat_page", exc)
        note = f"{category}: {str(exc)[:200]}"
        logger.warning("session probe failed: %s", note)
        return False, note
    finally:
        await _close_session(playwright, context)


async def verify_and_save_cookies(raw):
    """解析粘贴的 Cookie → 验证 → 通过后才写入 secrets。"""
    from spark.engine import cookies as cookies_mod

    try:
        parsed = cookies_mod.parse_cookie_input(raw)
    except cookies_mod.CookieParseError as exc:
        return {"ok": False, "note": f"Cookie 解析失败：{exc}"}
    try:
        names = cookies_mod.require_auth_cookies(parsed)
    except cookies_mod.CookieParseError as exc:
        return {"ok": False, "note": f"缺少关键登录 cookie（{exc}）"}

    ok, note = await _probe(cookies_override=parsed)
    if ok:
        secrets = config.load_secrets()
        config.save_secrets({**secrets, "cookies": parsed})
        state.add_session_check(
            state.today(),
            True,
            f"粘贴 Cookie 续期成功（{', '.join(sorted(names))}）",
        )
        return {
            "ok": True,
            "note": f"Cookie 有效，已保存 {len(parsed)} 条",
            "auth": sorted(names),
        }
    state.add_session_check(state.today(), False, f"粘贴 Cookie 无效：{note[:150]}")
    return {"ok": False, "note": f"Cookie 已失效，未保存：{note}"}


def build_summary(day):
    """生成某天的总结文本与问题清单（纯函数，读状态库）。"""
    stats = state.day_stats(day)
    records = state.get_day_records(day)
    cfg = config.load_config()
    problems = []
    login = state.latest_session_check()
    if login is not None and not login["ok"]:
        problems.append("登录已失效：请扫码或粘贴 Cookie 续期")
    if cfg.get("risk", {}).get("paused"):
        problems.append("风控熔断已暂停：需在面板人工确认恢复")
    if stats["rejected"]:
        problems.append(f"有 {stats['rejected']} 人消息被平台拒收（风控信号）")
    if stats["failed"]:
        problems.append(f"有 {stats['failed']} 人发送失败")
    enabled = [row["name"] for row in state.list_friends(enabled_only=True)]
    planned = [name for names in state.get_slot_plan(day).values() for name in names]
    targets = set(enabled or planned or records.keys())
    counted = [row for name, row in records.items() if name in targets and row["status"] != "skipped"]
    missing = sorted(name for name in targets if (records.get(name) or {}).get("status") not in DONE_STATUSES)
    if missing:
        problems.append("当天尚未完成：" + "、".join(missing))
    uncertain = sorted(name for name in targets if (records.get(name) or {}).get("status") == NEEDS_CONFIRMATION)
    if uncertain:
        problems.append("送达待人工核实（不会自动重发）：" + "、".join(uncertain))
    if not counted:
        problems.append("今天没有任何发送记录")
    confirmed = sum((records.get(name) or {}).get("status") == CONFIRMED for name in targets)
    text = f"平台受理且发送端会话确认 {confirmed}/{len(targets)} 人；收件端是否收到需另行核实"
    if stats["unconfirmed"]:
        text += f"；另有 {stats['unconfirmed']} 人回执较弱，未计入确认"

    slot_bits = []
    plan = state.get_slot_plan(day)
    slot_runs = state.get_slot_runs(day)
    for slot in cfg["schedule"].get("slots", []):
        members = plan.get(slot["id"], [])
        run = slot_runs.get(slot["id"])
        if not members and not run:
            continue
        sent = sum(
            1
            for name in members
            if (records.get(name) or {}).get("status") in ("confirmed", "unconfirmed")
        )
        status = run["status"] if run else "pending"
        if status == "missed" and members and not slot_members_all_sent(members, records):
            problems.append(f"{slot['label']}时段错过未发送")
        slot_bits.append(f"{slot['label']} {sent}/{len(members)}")
    if slot_bits:
        text += "；时段：" + "、".join(slot_bits)
    for note in dict.fromkeys(state.get_day_notes(day)):
        text += f"；{note}"
    if problems:
        text += "；" + "；".join(problems)
    else:
        text += "，全部正常，无需处理"
    return {"day": day, "text": text, "problems": problems, "stats": stats}


def write_summary_file(day, payload):
    config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = config.REPORTS_DIR / f"{day}.md"
    lines = [f"# 今日总结 {day}", "", payload["text"], ""]
    details = []
    for name, row in sorted(state.get_day_records(day).items()):
        if row["status"] != "skipped":
            details.append(f"- {name}：{row['status']}（{(row['message'] or '')[:30]}）")
    if details:
        lines.append("## 明细")
        lines.extend(details)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


async def login_flow():
    """弹出浏览器窗口扫码，成功后保存登录态。"""
    result = await login.scan_login(profile_root=str(config.PROFILE_DIR))
    secrets = config.load_secrets()
    config.save_secrets(
        {
            **secrets,
            "username": result.get("username") or "",
            "unique_id": result.get("unique_id") or "",
            "cookies": result.get("cookies") or [],
        }
    )
    state.add_session_check(state.today(), True, "扫码登录成功")
    return {"username": result.get("username"), "unique_id": result.get("unique_id")}


async def _send_one(page, account_name, friend, message):
    """对单个好友执行一次发送尝试，返回结果字典。"""
    await engine.verify_chat_target(page, friend)
    chat_input, selector_used = await engine.locate_chat_input(page)
    logger.info("chat input selector %s for %s", selector_used, friend)
    before_snapshot = await engine.snapshot_last_own_message(page, chat_input=chat_input)
    lines = message.split("\n")
    for index, line in enumerate(lines):
        await chat_input.type(line, delay=50)
        if index < len(lines) - 1:
            await chat_input.press("Shift+Enter")
    await engine.save_debug_artifacts(page, account_name, friend, "typed-message")

    im_summary = {}
    send_receipt = {}
    observer = None
    try:
        observer = await engine.start_im_send_observer(page, account_name, friend)
        logger.info("pressing Enter to send for %s", friend)
        await chat_input.press("Enter")
        sent_ok, detail = await engine.confirm_message_sent(
            page,
            chat_input,
            message,
            before_snapshot=before_snapshot,
        )
        im_summary = await observer()
        send_receipt = im_summary.get("send_receipt") or {}
        server_ok = bool(send_receipt.get("ok"))
        detail = (
            f"{detail}; im_observer request={im_summary.get('send_request_seen')} "
            f"response={im_summary.get('send_response_seen')} serverOk={server_ok} "
            f"logid={send_receipt.get('logid', '')} reason={send_receipt.get('reason', '')}"
        )
        await engine.save_debug_artifacts(page, account_name, friend, "after-send")
        if im_summary.get("enabled"):
            if not im_summary.get("send_request_seen"):
                raise RuntimeError(f"server send request was not observed; {detail}")
            if not im_summary.get("send_response_seen"):
                raise RuntimeError(f"server send response was not observed; {detail}")
            if not server_ok:
                raise RuntimeError(f"server send receipt rejected; {detail}")
            if not sent_ok:
                return {
                    "status": NEEDS_CONFIRMATION,
                    "detail": (
                        "服务端接受了发送请求，但当前会话没有观察到新消息气泡；"
                        "需人工核实，不自动重发；" + detail
                    ),
                    "receipt": send_receipt,
                    "sent_at": "",
                }
        elif not sent_ok:
            raise RuntimeError(detail)
        return {
            "status": CONFIRMED,
            "detail": detail,
            "receipt": send_receipt,
            "sent_at": state.now_iso(),
        }
    except Exception as exc:
        if observer is not None and not im_summary:
            try:
                im_summary = await observer()
                send_receipt = im_summary.get("send_receipt") or {}
            except Exception:
                return {"status": NEEDS_CONFIRMATION, "detail": "发送观察器异常，送达待人工核实；不自动重发",
                        "receipt": {}, "sent_at": ""}
        detail = ""
        sent_ok, recovery = await engine.detect_message_already_sent(
            page,
            chat_input,
            message,
            before_snapshot=before_snapshot,
        )
        if sent_ok:
            detail = recovery
        server_accepted, reject_reason = engine._server_accepted_send(
            im_summary,
            send_receipt,
        )
        if sent_ok and not server_accepted:
            sent_ok = False
            detail = reject_reason
        if sent_ok:
            status = CONFIRMED if receipt_is_strong(send_receipt) else UNCONFIRMED
            return {
                "status": status,
                "detail": detail,
                "receipt": send_receipt,
                "sent_at": state.now_iso(),
            }

        is_login, login_detail = await engine._page_has_login_required_prompt(page)
        if is_login:
            return {
                "status": LOGIN_BLOCKED,
                "detail": f"login_required: {login_detail[:200]}",
                "receipt": send_receipt,
                "sent_at": "",
            }
        category = engine.classify_browser_failure("send_flow", exc)
        if im_summary.get("send_response_seen") and ("server rejected" in str(exc).lower() or reject_reason):
            return {
                "status": REJECTED,
                "detail": (reject_reason or str(exc))[:300],
                "receipt": send_receipt,
                "sent_at": "",
            }
        if im_summary.get("send_request_seen"):
            return {"status": NEEDS_CONFIRMATION, "detail": "请求已发出但未取得可靠送达证据，需人工核实；不自动重发",
                    "receipt": send_receipt, "sent_at": ""}
        await engine.save_debug_artifacts(page, account_name, friend, "send-error")
        return {
            "status": FAILED,
            "detail": f"{category}: {str(exc)[:300]}",
            "receipt": send_receipt,
            "sent_at": "",
        }


def _record(day, friend, outcome, message, slot=""):
    state.upsert_record(
        day,
        friend,
        outcome["status"],
        message=message,
        detail=outcome.get("detail") or "",
        receipt=json.dumps(outcome.get("receipt") or {}, ensure_ascii=False),
        sent_at=outcome.get("sent_at") or "",
        slot=slot,
    )


DONE_STATUSES = (CONFIRMED, UNCONFIRMED)


def select_pending(friends, records, cap):
    """挑选今天还需要发送的好友。

    confirmed / unconfirmed 都视为"今天已发"（弱确认常见于抖音返回二进制回执，
    200 + 气泡双证据时不再重发，避免一天两条）；只有未尝试、failed 等
    且未达尝试上限的好友才继续发送；拒收和送达待核实不自动重试。
    """
    pending = []
    for name in friends:
        row = records.get(name)
        if row is None:
            pending.append(name)
            continue
        if row["status"] in (*DONE_STATUSES, REJECTED, NEEDS_CONFIRMATION):
            continue
        if row["attempts"] >= cap:
            continue
        pending.append(name)
    return pending


def slot_members_all_sent(members, records):
    """时段成员是否都已发出（confirmed/unconfirmed 都算已发）。"""
    if not members:
        return False
    return all(
        (records.get(name) or {}).get("status") in DONE_STATUSES
        for name in members
    )


def reclassify_binary_receipts(day):
    """把"二进制回执含 OK+消息 ID"的未确认记录重算为确认（一次性维护用）。

    新发送直接按 protobufOk 判强确认；此函数用于修正规则升级之前留下的历史记录。
    """
    changed = 0
    for name, row in state.get_day_records(day).items():
        if row["status"] != UNCONFIRMED:
            continue
        try:
            receipt = json.loads(row["receipt"] or "{}")
        except (json.JSONDecodeError, TypeError):
            receipt = {}
        marker_source = f"{receipt.get('reason') or ''}"
        if receipt.get("protobufOk") is True or binary_receipt_ok(marker_source):
            detail = (row["detail"] or "") + "；按二进制回执（OK+消息ID）重算为确认"
            state.upsert_record(day, name, CONFIRMED, detail=detail)
            changed += 1
    return changed


async def run_round(trigger="manual", rng=None, slot=None, catch_up=False):
    """执行一轮发送。返回 {'status': ..., 'stats'?: ...}。"""
    rng = rng or random
    cfg = config.load_config()
    day = state.today()
    if cfg.get("risk", {}).get("paused"):
        return {"status": "paused"}
    if _RUN_LOCK.locked():
        return {"status": "busy"}

    async with _RUN_LOCK:
        run_id = state.start_run(trigger, slot=slot or "")
        playwright = context = None
        send_cfg = cfg["send"]
        schedule = cfg["schedule"]
        notes = []
        try:
            state.recover_in_flight(day)
            playwright, context, page = await _open_session()
            await _open_chat_page(page)
            await _sync_cookies(context)
            state.add_session_check(day, True, "发送前打开会话页成功")

            enabled = [row["name"] for row in state.list_friends(enabled_only=True)]
            if slot and not catch_up:
                plan = state.get_slot_plan(day)
                members = set(plan.get(slot, []))
                friends = [name for name in enabled if name in members]
            else:
                friends = enabled
            records = state.get_day_records(day)
            pending = select_pending(friends, records, send_cfg["retryPerFriendPerDay"])
            if not pending:
                complete = slot_members_all_sent(friends, records) or not friends
                status = "completed" if complete else "incomplete"
                state.finish_run(run_id, status, "已发或已达尝试上限")
                return {"status": status, "stats": state.day_stats(day)}

            pool = messages.load_pool()
            pool, skipped_invalid = messages.filter_valid_pool(pool)
            if skipped_invalid:
                state.add_day_note(day, f"因禁用词/超长跳过 {skipped_invalid} 条文案")
                notes.append(f"invalid_skipped={skipped_invalid}")
            usage = state.message_usage()
            pool = sorted(
                pool,
                key=lambda item: usage.get(messages.item_text(item), "0000-00-00"),
            )
            last_used = {name: state.last_message_for(name) for name in pending}
            used_today = {row["message"] for row in records.values() if row["message"]}

            deadline = _now() + timedelta(minutes=30)
            end_text = schedule["windowEnd"] if catch_up or not slot else next(
                item["end"] for item in schedule["slots"] if item["id"] == slot
            )
            end_h, end_m = map(int, end_text.split(":"))
            deadline = min(deadline, _now().replace(hour=end_h, minute=end_m, second=0, microsecond=0))
            if deadline <= _now():
                state.finish_run(run_id, "incomplete", "超过发送窗口")
                return {"status": "incomplete"}
            timeout = asyncio.timeout(max(0.1, (deadline - _now()).total_seconds()))
            async with timeout:
                blocked = await _deliver_pending(page, day, pending, pool, used_today, last_used,
                                       send_cfg, cfg, rng, slot, deadline)
            records = state.get_day_records(day)
            complete = slot_members_all_sent(friends, records)
            stats = state.day_stats(day)
            status = blocked or ("completed" if complete else "incomplete")
            state.finish_run(run_id, status, "" if complete else "部分好友尚未发送")
            if status == LOGIN_BLOCKED:
                notify.notify("抖音续火花：登录已失效", "请到面板续期登录，未发好友保留待补漏")
            elif status == REJECTED:
                notify.notify("抖音续火花：平台拒收", "本轮已停止，拒收目标不会自动重试，请到面板查看")
            return {"status": status, "stats": stats}
        except Exception as exc:
            category = engine.classify_browser_failure("open_chat_page", exc)
            detail = f"{category}: {str(exc)[:300]}"
            logger.exception("run_round failed")
            state.recover_in_flight(day)
            status = LOGIN_BLOCKED if "login_required" in detail else "failed"
            notify.notify("抖音续火花：登录已失效" if status == LOGIN_BLOCKED else "抖音续火花：本轮有遗漏", detail)
            state.finish_run(run_id, status, detail)
            return {"status": status, "error": detail}
        finally:
            await _close_session(playwright, context)


async def _deliver_pending(page, day, pending, pool, used_today, last_used, send_cfg, cfg, rng, slot, deadline):
    blocked = None
    async for friend in engine.scroll_and_select_user(
        page, {}, "account", pending,
        engine._normalize_friend_list_scan_config(cfg), index_targets=pending,
    ):
        if _now() >= deadline:
            break
        row = state.get_day_records(day).get(friend)
        if row and (row["status"] in DONE_STATUSES or row["attempts"] >= send_cfg["retryPerFriendPerDay"]):
            continue
        picked = messages.pick_messages(
            [friend], pool, used_today, {friend: last_used.get(friend, "")}, rng,
            bucket=messages.bucket_for_time(_now()),
        )
        message = picked.get(friend)
        if not message:
            state.upsert_record(day, friend, "skipped", detail="当前时段没有可用文案", slot=slot or "")
            continue
        await engine.verify_chat_target(page, friend)
        used_today.add(message)
        state.increment_attempts(day, friend)
        state.upsert_record(day, friend, IN_FLIGHT, message=message, slot=slot or "")
        outcome = await _send_one(page, "account", friend, message)
        _record(day, friend, outcome, message, slot=slot or "")
        if outcome["status"] in (REJECTED, LOGIN_BLOCKED):
            blocked = outcome["status"]
            break
        await asyncio.sleep(rng.randint(send_cfg["minIntervalSec"], send_cfg["maxIntervalSec"]))
    risk = cfg["risk"]
    bad_days = state.consecutive_bad_days_before(day, risk["badDayRatioThreshold"])
    if bad_days >= risk["autoPauseOnConsecutiveBadDays"]:
        updated = config.load_config()
        updated["risk"]["paused"] = True
        config.save_config(updated)
        notify.notify("抖音续火花已自动暂停", "连续异常日达到阈值，请到面板确认后恢复")

    return blocked
