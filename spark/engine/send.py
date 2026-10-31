"""发送链路（从 DouYinSparkFlow 的 core/tasks.py 移植并精简）。

来源：https://github.com/halfwaystudent/douyin-sparkflow
许可：PolyForm Noncommercial License 1.0.0 —— 仅限本人账号的非商业个人使用。
"""
import asyncio
import base64
import hashlib
import json
import logging
import os
import random
import unicodedata
from datetime import datetime, timezone

from spark import config
from spark.engine.browser import (
    get_browser,
    get_persistent_browser_context,
    sanitize_profile_name,
    select_douyin_network_mode,
)
from spark.util import (
    AUTH_COOKIE_NAMES,
    binary_receipt_ok,
    normalize_unique_id,
    receipt_is_strong,
)


logger = logging.getLogger(__name__)
debug_artifacts_dir = config.DEBUG_ARTIFACTS_DIR
debug_artifacts_dir.mkdir(parents=True, exist_ok=True)

CREATOR_HOME_URL = "https://creator.douyin.com/"
CREATOR_CHAT_URL = "https://creator.douyin.com/creator-micro/data/following/chat"


def _persist_friend_index(user, friend_index, updated_at, scan_complete=False, missing_targets=None):
    """旧项目会把好友索引写回账号数据；新应用由调用方自行保存，这里只留日志。"""
    del user, friend_index, updated_at, scan_complete, missing_targets
    logger.debug("friend index persistence handled by caller; skipping legacy write")


async def retry_operation(name, operation, retries=3, delay=2, *args, **kwargs):
    for attempt in range(retries):
        try:
            return await operation(*args, **kwargs)
        except Exception as exc:
            if attempt < retries - 1:
                logger.warning("%s failed, retry %s/%s: %s", name, attempt + 1, retries, exc)
                await asyncio.sleep(delay)
            else:
                logger.error("%s failed after %s attempts: %s", name, retries, exc)
                raise


def _safe_name(value):
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in value)[:80]


def _normalize_target_name(value):
    raw = unicodedata.normalize("NFKC", str(value or ""))
    for token in ("\u200b", "\u200c", "\u200d", "\ufeff"):
        raw = raw.replace(token, "")
    raw = raw.replace("\xa0", " ")
    return " ".join(raw.split()).strip().casefold()


def _coerce_non_negative_int(value, default):
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return max(0, int(default))


def _coerce_positive_int(value, default):
    try:
        return max(1, int(value))
    except (TypeError, ValueError):
        return max(1, int(default))


def _coerce_positive_float(value, default):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    if parsed <= 0:
        parsed = float(default)
    return parsed


def _normalize_friend_list_scan_config(active_config):
    raw = active_config.get("friendListScan", {}) or {}
    return {
        "maxScanSeconds": _coerce_positive_int(raw.get("maxScanSeconds", 600), 600),
        "idleScanSeconds": _coerce_positive_int(raw.get("idleScanSeconds", 45), 45),
        "maxRecoveries": _coerce_non_negative_int(raw.get("maxRecoveries", 2), 2),
        "scrollStepPx": _coerce_positive_int(raw.get("scrollStepPx", 400), 400),
        "scrollDelaySeconds": _coerce_positive_float(raw.get("scrollDelaySeconds", 1.5), 1.5),
    }


def _normalize_send_strategy(active_config):
    raw = active_config.get("sendStrategy", {}) or {}
    start_min = _coerce_non_negative_int(raw.get("accountStartDelaySecondsMin", 0), 0)
    start_max = _coerce_non_negative_int(raw.get("accountStartDelaySecondsMax", start_min), start_min)
    if start_max < start_min:
        start_max = start_min

    message_min = _coerce_non_negative_int(raw.get("messageIntervalSecondsMin", 0), 0)
    message_max = _coerce_non_negative_int(raw.get("messageIntervalSecondsMax", message_min), message_min)
    if message_max < message_min:
        message_max = message_min

    return {
        "shuffleTargets": bool(raw.get("shuffleTargets", True)),
        "accountStartDelaySecondsMin": start_min,
        "accountStartDelaySecondsMax": start_max,
        "messageIntervalSecondsMin": message_min,
        "messageIntervalSecondsMax": message_max,
    }


def _ordered_targets_for_run(targets, send_strategy):
    ordered = list(targets or [])
    if send_strategy.get("shuffleTargets", True):
        random.shuffle(ordered)
    return ordered


def _normalize_persistent_profile_config(active_config):
    raw = active_config.get("persistentBrowserProfiles", {}) or {}
    return {
        "enabled": bool(raw.get("enabled", False)),
        "root": str(
            os.getenv("SPARKFLOW_BROWSER_PROFILE_ROOT")
            or raw.get("root")
            or "/opt/douyin-sparkflow/state/browser-profiles"
        ),
        "seedCookiesWhenEmpty": bool(raw.get("seedCookiesWhenEmpty", True)),
        "syncStoredCookiesBeforeRun": bool(raw.get("syncStoredCookiesBeforeRun", True)),
        "refreshStoredCookiesAfterLogin": bool(raw.get("refreshStoredCookiesAfterLogin", True)),
    }


def _account_profile_name(user):
    unique_id = normalize_unique_id(user.get("unique_id"))
    username = str(user.get("username") or "").strip()
    if unique_id:
        return f"uid-{unique_id}"
    if username:
        return f"user-{sanitize_profile_name(username)}"
    return "unknown"


def _random_delay_seconds(send_strategy, min_key, max_key):
    return random.randint(send_strategy[min_key], send_strategy[max_key])


async def _sleep_with_log(seconds, reason, account_name):
    if seconds <= 0:
        return
    logger.info("%s for %s by %ss", reason, account_name, seconds)
    await asyncio.sleep(seconds)


LOGIN_REQUIRED_TEXTS = (
    "扫码登录",
    "验证码登录",
    "密码登录",
    "登录/注册",
    "登录后免费",
    "请登录",
    "身份验证",
    "安全验证",
    "风险提示",
    "环境异常",
)
LOGIN_REQUIRED_SELECTORS = (
    ".login-mask",
    ".login-guide-container",
    ".login-img-code-wrapper",
    ".pc-login-verification-modal",
)
LOGIN_REQUIRED_DIALOG_SELECTORS = (
    ".semi-modal-content",
    'div[role="dialog"]',
)
NON_LOGIN_DIALOG_DISMISS_TEXTS = (
    "我知道了",
    "知道了",
    "好的",
    "确定",
    "确认",
    "稍后再说",
    "关闭",
)
NON_LOGIN_DIALOG_CLOSE_SELECTORS = (
    ".semi-modal-close",
    'button[aria-label="Close"]',
    'button[aria-label="关闭"]',
    '[aria-label="Close"]',
    '[aria-label="关闭"]',
)
AUTH_COOKIE_NAMES = {
    "sessionid",
    "sessionid_ss",
    "sid_guard",
    "sid_tt",
    "uid_tt",
    "uid_tt_ss",
}


async def _body_text_sample(page, limit=1200):
    try:
        return (await page.locator("body").inner_text(timeout=3000)).replace("\n", " ")[:limit]
    except Exception as exc:
        return f"BODY_TEXT_ERROR={exc!r}"


def _find_login_required_text(text):
    sample = text or ""
    for marker in LOGIN_REQUIRED_TEXTS:
        if marker in sample:
            return marker
    return None


async def _page_has_login_required_prompt(page):
    for selector in LOGIN_REQUIRED_SELECTORS:
        locator = page.locator(selector).first
        try:
            if await locator.count() > 0 and await locator.is_visible(timeout=500):
                return True, f"visible selector {selector}"
        except Exception:
            continue

    for selector in LOGIN_REQUIRED_DIALOG_SELECTORS:
        locator = page.locator(selector)
        try:
            count = min(await locator.count(), 5)
        except Exception:
            continue

        for index in range(count):
            item = locator.nth(index)
            try:
                if not await item.is_visible(timeout=500):
                    continue
                dialog_text = (await item.inner_text(timeout=1000)).replace("\n", " ")
            except Exception:
                continue

            matched_text = _find_login_required_text(dialog_text)
            if matched_text:
                return True, (
                    f"visible dialog {selector} contains {matched_text!r}: "
                    f"{dialog_text[:300]}"
                )
            logger.debug(
                "Ignoring visible non-login dialog for selector %s: %s",
                selector,
                dialog_text[:160],
            )

    sample = await _body_text_sample(page)
    matched_text = _find_login_required_text(sample)
    if matched_text:
        return True, f"body contains {matched_text!r}: {sample[:300]}"
    return False, ""


async def ensure_not_login_required(page, account_name, stage):
    is_required, detail = await _page_has_login_required_prompt(page)
    if is_required:
        raise RuntimeError(f"login_required at {stage} for {account_name}: {detail}")


async def _dismiss_non_login_dialogs(page, account_name, stage):
    dismissed = 0
    for selector in LOGIN_REQUIRED_DIALOG_SELECTORS:
        locator = page.locator(selector)
        try:
            count = min(await locator.count(), 5)
        except Exception:
            continue

        for index in range(count):
            item = locator.nth(index)
            try:
                if not await item.is_visible(timeout=500):
                    continue
                dialog_text = (await item.inner_text(timeout=1000)).replace("\n", " ")
            except Exception:
                continue

            if _find_login_required_text(dialog_text):
                continue

            clicked = False
            for text in NON_LOGIN_DIALOG_DISMISS_TEXTS:
                button = item.get_by_text(text, exact=True).first
                try:
                    if await button.count() > 0 and await button.is_visible(timeout=500):
                        await button.click(timeout=2000)
                        clicked = True
                        break
                except Exception:
                    continue

            if not clicked:
                for close_selector in NON_LOGIN_DIALOG_CLOSE_SELECTORS:
                    close_button = item.locator(close_selector).first
                    try:
                        if await close_button.count() > 0 and await close_button.is_visible(timeout=500):
                            await close_button.click(timeout=2000)
                            clicked = True
                            break
                    except Exception:
                        continue

            if clicked:
                dismissed += 1
                logger.info(
                    "Dismissed non-login dialog for %s at %s: %s",
                    account_name,
                    stage,
                    dialog_text[:160],
                )
                await asyncio.sleep(0.5)
            else:
                logger.debug(
                    "Visible non-login dialog for %s at %s was not dismissed: %s",
                    account_name,
                    stage,
                    dialog_text[:160],
                )
    return dismissed


async def apply_stored_cookies_to_profile(context, cookies, account_name, only_when_empty=False):
    if not cookies:
        return
    if only_when_empty:
        try:
            current_cookies = await context.cookies(
                ["https://creator.douyin.com/", "https://www.douyin.com/"]
            )
        except Exception:
            current_cookies = []

        if any(cookie.get("name") in AUTH_COOKIE_NAMES for cookie in current_cookies):
            logger.info(
                "Persistent profile for %s already has auth cookies; not seeding stored cookies",
                account_name,
            )
            return

    await context.add_cookies(cookies)
    logger.info("Applied %s stored cookies to persistent profile for %s", len(cookies), account_name)


def _merge_cookies_preserving_auth(stored_cookies, fresh_cookies):
    """以浏览器运行时 cookie 为基础合并，但保留已存储的关键登录态 cookie。

    浏览器运行期间平台可能下发短期或权限较低的 cookie；整体覆盖可能破坏有效登录态。
    """
    auth_by_key = {
        (cookie.get("name"), cookie.get("domain")): cookie
        for cookie in (stored_cookies or [])
        if cookie.get("name") in AUTH_COOKIE_NAMES
    }
    merged = []
    reused_auth = set()
    for cookie in fresh_cookies or []:
        key = (cookie.get("name"), cookie.get("domain"))
        if key in auth_by_key:
            merged.append(auth_by_key[key])
            reused_auth.add(key)
        else:
            merged.append(cookie)

    # 运行中被清掉的关键登录态 cookie 也要补回来，避免下一轮彻底失联。
    for key, cookie in auth_by_key.items():
        if key not in reused_auth:
            merged.append(cookie)
    return merged


async def merged_cookies_from_profile(context, stored_cookies):
    """把 profile 当前 cookie 与已存 cookie 合并，返回新 cookie 列表（保留 auth 组）。"""
    try:
        fresh_cookies = await context.cookies()
    except Exception as exc:
        logger.warning("Could not read persistent profile cookies: %s", exc)
        return list(stored_cookies or [])

    if not fresh_cookies:
        return list(stored_cookies or [])

    merged = _merge_cookies_preserving_auth(stored_cookies, fresh_cookies)
    logger.info(
        "Merged profile cookies with stored cookies count=%s (auth cookies preserved)",
        len(merged),
    )
    return merged


def _is_stop_account_category(category):
    return _is_account_level_failure_category(category)


async def save_debug_artifacts(page, account_name, target_name, stage):
    if not config.load_config().get("debug", {}).get("saveArtifacts", False):
        return

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    stem = f"{timestamp}-{_safe_name(account_name)}-{_safe_name(target_name)}-{stage}"
    screenshot_path = debug_artifacts_dir / f"{stem}.png"
    html_path = debug_artifacts_dir / f"{stem}.html"

    await page.screenshot(path=str(screenshot_path), full_page=True)
    html_path.write_text(await page.content(), encoding="utf-8")
    logger.info("Saved debug artifacts at stage=%s for %s/%s", stage, account_name, target_name)


async def dump_friend_list_failure(page, account_name, stage="friend-list-unavailable"):
    """好友列表相关失败时无条件落盘当前页面 DOM + 结构摘要 + 登录态，便于事后定位根因。

    stage 区分失败阶段：
      - ``friend-list-unavailable``：等不到好友行；
      - ``friends-tab-not-found``：找不到「朋友私信」tab。

    落盘同时判断登录态，用于区分「会话失效」与「页面结构/加载问题」——
    这两类故障的处置方式完全不同（前者需重新登录，后者改选择器）。
    """
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    stem = f"{timestamp}-{_safe_name(account_name)}-{stage}"
    screenshot_path = debug_artifacts_dir / f"{stem}.png"
    html_path = debug_artifacts_dir / f"{stem}.html"
    summary_path = debug_artifacts_dir / f"{stem}.json"
    try:
        await page.screenshot(path=str(screenshot_path), full_page=True)
    except Exception:
        pass
    try:
        html_path.write_text(await page.content(), encoding="utf-8")
    except Exception:
        pass
    try:
        summary = await page.evaluate(
            """() => {
                const sub = document.querySelector('#sub-app');
                const body = document.body ? (document.body.innerText || '') : '';
                const markers = ['扫码登录', '验证码登录', '密码登录', '请登录',
                                 '登录后', '安全验证', '身份验证', '环境异常'];
                const loginPrompt = markers.some(function (m) { return body.indexOf(m) !== -1; });
                const base = { url: location.href, loginPrompt: loginPrompt };
                if (!sub) {
                    base.hasSubApp = false;
                    base.bodyText = body.slice(0, 600);
                    return base;
                }
                base.hasSubApp = true;
                base.ulCount = sub.querySelectorAll('ul').length;
                base.liCount = sub.querySelectorAll('li').length;
                base.roleListItem = sub.querySelectorAll('li[role="list-item"]').length;
                base.semiListItem = sub.querySelectorAll('.semi-list-item').length;
                base.listItemClass = sub.querySelectorAll('[class*="list-item"]').length;
                base.nameSpanCount = sub.querySelectorAll('[class*="item-header-name"]').length;
                base.tabCount = sub.querySelectorAll('[role="tab"]').length;
                base.text = (sub.innerText || '').split(String.fromCharCode(10)).join(' ').slice(0, 800);
                return base;
            }"""
        )
        summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.warning("friend list failure stage=%s dom summary: %s", stage, summary)
    except Exception as exc:
        logger.warning("failed to summarize friend list dom: %s", exc)


async def locate_chat_input(page):
    selectors = [
        "xpath=//div[contains(@class, 'chat-input-dccKiL')]//div[@contenteditable='true']",
        "xpath=//div[@contenteditable='true' and @role='textbox']",
        "xpath=(//div[@contenteditable='true'])[last()]",
    ]

    last_error = None
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            await locator.wait_for(state="visible", timeout=10000)
            await locator.click(timeout=5000)
            return locator, selector
        except Exception as exc:
            last_error = exc

    raise RuntimeError(f"Unable to locate chat input, last error: {last_error}")


async def read_chat_input_text(chat_input):
    try:
        return await chat_input.evaluate(
            """(node) => {
                const raw = node.innerText ?? node.textContent ?? "";
                return raw.trim();
            }"""
        )
    except Exception:
        return ""


def _message_probe_text(message):
    for line in str(message or "").splitlines():
        probe = line.strip()
        if probe:
            return probe
    return str(message or "").strip()


def _normalize_message_text(value):
    return "\n".join(
        line.strip()
        for line in str(value or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
        if line.strip()
    ).strip()


async def _detect_send_failure_indicator(page):
    """Return a short string describing a visible send-failure UI element, or ""."""
    try:
        return await page.evaluate(
            """() => {
                // Red exclamation icon commonly used by chat UIs for failed sends.
                const failIcons = document.querySelectorAll(
                    "svg[class*='fail'], svg[class*='Fail'], svg[class*='error'], svg[class*='Error'],"
                    + " [class*='exclamation'], [class*='Exclamation'],"
                    + " [class*='retry'], [class*='Retry'], [class*='resend'], [class*='Resend']"
                );
                for (const el of failIcons) {
                    const r = el.getBoundingClientRect();
                    if (r.width > 0 && r.height > 0) {
                        // Climb up to find the associated message row text.
                        let row = el.closest("li, [role='listitem'], [class*='message'], [class*='Message'], [class*='item']");
                        let snippet = String(row?.innerText || "").slice(0, 40);
                        return "fail_icon(" + (el.className || "").slice(0, 60) + ") near=" + snippet;
                    }
                }
                // Explicit failure / retry text.
                const bodyText = String(document.body?.innerText || "");
                const failKeywords = ["发送失败", "重试", "重新发送", "send failed", "retry", "resend"];
                for (const kw of failKeywords) {
                    if (bodyText.includes(kw)) {
                        return "fail_text=" + kw;
                    }
                }
                return "";
            }"""
        )
    except Exception:
        return ""



async def start_im_send_observer(page, account_name, target_name):
    """Observe creator IM service calls for one browser send attempt."""
    state = {
        "enabled": False,
        "send_request_seen": False,
        "send_response_seen": False,
        "send_receipt": {},
        "mark_read_calls": [],
        "identity_security_token_calls": [],
        "events": [],
        "error": "",
    }
    pending = {}
    session = None

    try:
        session = await page.context.new_cdp_session(page)
        await session.send("Network.enable")
        state["enabled"] = True
    except Exception as exc:
        state["error"] = f"cdp_unavailable: {exc}"
        logger.warning("IM observer unavailable for %s/%s: %s", account_name, target_name, exc)

        async def disabled_summary(extra_wait_seconds=0):
            if extra_wait_seconds:
                await asyncio.sleep(extra_wait_seconds)
            return dict(state)

        return disabled_summary

    def _trim(value, limit=220):
        return str(value or "")[:limit]

    def _call_kind(url):
        value = str(url or "")
        if "/v1/message/send" in value:
            return "message_send"
        if "mark_read" in value:
            return "mark_read"
        if "identity_security_token" in value:
            return "identity_security_token"
        if "imapi.douyin.com" in value:
            return "imapi_other"
        return ""

    def _safe_url(url):
        return str(url or "").split("?", 1)[0]

    def _header_value(headers, name):
        target = name.lower()
        for key, value in (headers or {}).items():
            if str(key).lower() == target:
                return str(value)
        return ""

    def _decode_body(body):
        raw = body.get("body") or ""
        if body.get("base64Encoded"):
            try:
                return base64.b64decode(raw).decode("utf-8", errors="replace")
            except Exception:
                return ""
        return raw

    def _parse_json_body(text):
        try:
            return json.loads(text)
        except Exception:
            return None

    def _extract_error_text(data, body_text):
        if isinstance(data, dict):
            for key in ("status_msg", "message", "msg", "err_msg", "error", "reason"):
                value = data.get(key)
                if value:
                    return _trim(value, 300)
            nested = data.get("data")
            if isinstance(nested, dict):
                for key in ("status_msg", "message", "msg", "err_msg", "error", "reason"):
                    value = nested.get(key)
                    if value:
                        return _trim(value, 300)
        return _trim(body_text, 300)

    def _receipt_body_meta(data, body_text):
        meta = {"bodyLen": len(body_text or "")}
        if body_text:
            meta["bodySha256"] = hashlib.sha256(body_text.encode("utf-8", errors="replace")).hexdigest()
        if isinstance(data, dict):
            meta["jsonKeys"] = sorted(str(key) for key in data.keys())[:20]
            nested = data.get("data")
            if isinstance(nested, dict):
                meta["dataKeys"] = sorted(str(key) for key in nested.keys())[:20]
                for source_key, dest_key in (
                    ("server_message_id", "serverMessageId"),
                    ("message_id", "messageId"),
                    ("msg_id", "messageId"),
                    ("conversation_id", "conversationId"),
                    ("conversation_short_id", "conversationShortId"),
                ):
                    value = nested.get(source_key)
                    if value:
                        meta[dest_key] = _trim(value, 120)
            for source_key, dest_key in (
                ("server_message_id", "serverMessageId"),
                ("message_id", "messageId"),
                ("msg_id", "messageId"),
                ("conversation_id", "conversationId"),
                ("conversation_short_id", "conversationShortId"),
            ):
                value = data.get(source_key)
                if value:
                    meta[dest_key] = _trim(value, 120)
        return meta

    def _record(event):
        state["events"].append(event)
        if len(state["events"]) > 50:
            del state["events"][:-50]

    def on_request(params):
        request = params.get("request") or {}
        url = request.get("url") or ""
        kind = _call_kind(url)
        if not kind:
            return
        request_id = params.get("requestId")
        post_data = request.get("postData") or ""
        method = request.get("method") or ""
        is_send_post = kind == "message_send" and method.upper() == "POST"
        pending[request_id] = {"url": url, "kind": kind, "request": request, "is_send_post": is_send_post}
        _record({"kind": "request", "call": kind, "url": _safe_url(url), "method": method, "postLen": len(post_data)})
        if is_send_post:
            state["send_request_seen"] = True
            logger.info("IM observer saw message send request for %s/%s url=%s postLen=%s", account_name, target_name, _trim(_safe_url(url), 160), len(post_data))

    def on_response(params):
        request_id = params.get("requestId")
        item = pending.get(request_id)
        if not item:
            return
        response = params.get("response") or {}
        item["response"] = response
        kind = item.get("kind")
        headers = response.get("headers") or {}
        status = response.get("status")
        logid = _header_value(headers, "x-tt-logid") or _header_value(headers, "x-tt-trace-log") or _header_value(headers, "x-tt-trace-id")
        event = {"kind": "response", "call": kind, "url": _safe_url(item.get("url")), "status": status}
        if logid:
            event["logid"] = _trim(logid, 120)
        _record(event)
        if item.get("is_send_post"):
            state["send_response_seen"] = True
            state["send_receipt"] = {"call": kind, "httpStatus": status, "ok": False, "pendingBody": True, "logid": _trim(logid, 120), "url": _safe_url(item.get("url"))}
            logger.info("IM observer saw message send response for %s/%s status=%s logid=%s", account_name, target_name, status, _trim(logid, 80))

    async def fetch_body(request_id):
        item = pending.get(request_id) or {}
        kind = item.get("kind") or ""
        url = item.get("url") or ""
        response = item.get("response") or {}
        status = response.get("status")
        headers = response.get("headers") or {}
        logid = _header_value(headers, "x-tt-logid") or _header_value(headers, "x-tt-trace-log") or _header_value(headers, "x-tt-trace-id")
        try:
            body = await session.send("Network.getResponseBody", {"requestId": request_id})
            body_text = _decode_body(body)
            data = _parse_json_body(body_text)
            http_ok = isinstance(status, (int, float)) and 200 <= status < 300
            json_ok = _json_body_success(data)
            binary_ok = bool(
                item.get("is_send_post")
                and not isinstance(data, dict)
                and binary_receipt_ok(body_text)
            )
            if item.get("is_send_post"):
                receipt_ok = bool(http_ok and (json_ok is True or binary_ok))
            else:
                receipt_ok = bool(http_ok and json_ok is not False)
            receipt = {"call": kind, "httpStatus": status, "ok": receipt_ok, "jsonParsed": isinstance(data, dict), "logid": _trim(logid, 120), "url": _safe_url(url)}
            receipt.update(_receipt_body_meta(data, body_text))
            if json_ok is not None:
                receipt["jsonOk"] = bool(json_ok)
            if binary_ok:
                receipt["protobufOk"] = True
            if not receipt["ok"]:
                receipt["reason"] = _extract_error_text(data, body_text)
            if item.get("is_send_post"):
                state["send_receipt"] = receipt
                _record({"kind": "body", "call": kind, "status": status, "ok": receipt["ok"], "jsonParsed": receipt["jsonParsed"], "logid": receipt.get("logid", ""), "reason": receipt.get("reason", "")})
            elif kind == "mark_read":
                state["mark_read_calls"].append(receipt)
                _record({"kind": "body", "call": kind, "status": status, "ok": receipt["ok"]})
            elif kind == "identity_security_token":
                has_token = isinstance(data, dict) and bool((data.get("data") or {}).get("identity_security_token"))
                receipt["hasToken"] = has_token
                receipt.pop("reason", None)
                state["identity_security_token_calls"].append(receipt)
                _record({"kind": "body", "call": kind, "status": status, "ok": receipt["ok"], "hasToken": has_token})
            else:
                _record({"kind": "body", "call": kind, "status": status, "ok": receipt["ok"]})
        except Exception as exc:
            error = _trim(exc, 160)
            _record({"kind": "body_error", "call": kind, "url": _safe_url(url), "error": error})
            if item.get("is_send_post"):
                state["send_receipt"] = {"call": kind, "httpStatus": status, "ok": False, "logid": _trim(logid, 120), "url": _safe_url(url), "reason": f"response_body_unavailable: {error}"}
        finally:
            pending.pop(request_id, None)

    def on_loading_finished(params):
        request_id = params.get("requestId")
        if request_id in pending:
            asyncio.create_task(fetch_body(request_id))

    session.on("Network.requestWillBeSent", on_request)
    session.on("Network.responseReceived", on_response)
    session.on("Network.loadingFinished", on_loading_finished)

    async def summary(extra_wait_seconds=3, response_grace_seconds=8):
        if extra_wait_seconds:
            await asyncio.sleep(extra_wait_seconds)
        # 慢响应兜底：已观察到发送请求但回执未齐时，再多等一小段。
        # 平台响应可能略晚于初始观察窗口；这里以 0.25s 粒度
        # 轮询，拿到最终回执立即返回；真正无响应的发送也只在多等 grace 秒后
        # 按原逻辑判定，不影响正常快响应路径（首轮条件即不满足，立即返回）。
        deadline = asyncio.get_running_loop().time() + max(
            0.0, float(response_grace_seconds)
        )
        while True:
            if not state.get("send_request_seen"):
                break
            receipt = state.get("send_receipt") or {}
            if (
                state.get("send_response_seen")
                and receipt
                and not receipt.get("pendingBody")
            ):
                break
            if asyncio.get_running_loop().time() >= deadline:
                break
            await asyncio.sleep(0.25)
        try:
            await session.detach()
        except Exception:
            pass
        return dict(state)

    return summary

async def snapshot_last_own_message(page, chat_input=None):
    try:
        return await page.evaluate(
            r"""() => {
                // ---- DOM structure (douyin creator private message) ----
                // Each row is <div class="box-item-W0TV01 ...">...</div>.
                //   - time separator:   class contains "time-" (e.g. time-Tl7Z4j)
                //   - own message:       class contains "is-me"   (e.g. is-me-cb9NAa)
                //   - peer message:      no "is-me" and no "time-"
                // Text is inside <pre class="text-...">.

                const allRows = Array.from(
                    document.querySelectorAll("[class*='box-item-']")
                ).filter(el => {
                    const r = el.getBoundingClientRect();
                    return r.width > 0 && r.height > 0;
                });

                // Walk backwards to find the boundary: first time separator that
                // indicates yesterday or earlier. Everything after it is "today".
                let todayStart = 0;
                for (let i = allRows.length - 1; i >= 0; i--) {
                    const cls = String(allRows[i].className || "");
                    if (!cls.includes("time-")) continue;
                    const label = String(allRows[i].innerText || "").trim();
                    // "今天" / "刚刚" / "N分钟前" / pure time like "11:01" => today
                    // "昨天" / "星期X" / "2026-..." => not today
                    if (/^(昨天|星期|20\d{2}-)/.test(label)) {
                        todayStart = i + 1;
                        break;
                    }
                }

                // From bottom, find the last own-message row within today's section.
                for (let i = allRows.length - 1; i >= todayStart; i--) {
                    const cls = String(allRows[i].className || "");
                    if (!cls.includes("is-me")) continue;
                    if (cls.includes("time-")) continue;
                    const pre = allRows[i].querySelector("pre, [class*='text-']");
                    const text = String(pre?.innerText || allRows[i].innerText || "").trim();
                    if (!text) continue;
                    const rect = allRows[i].getBoundingClientRect();
                    return {
                        text,
                        centerX: rect.left + rect.width / 2,
                        centerY: rect.top + rect.height / 2,
                        top: rect.top,
                        bottom: rect.bottom,
                        right: rect.right,
                        className: cls.slice(0, 240),
                    };
                }
                return null;
            }""",
        )
    except Exception as exc:
        logger.debug("Unable to snapshot last own message: %s", exc)
        return None


async def count_today_own_message_matches(page, target_text):
    """Count how many of *target_text* appear in today's own-message bubbles."""
    try:
        return await page.evaluate(
            r"""(targetText) => {
                const allRows = Array.from(
                    document.querySelectorAll("[class*='box-item-']")
                ).filter(el => {
                    const r = el.getBoundingClientRect();
                    return r.width > 0 && r.height > 0;
                });
                let todayStart = 0;
                for (let i = allRows.length - 1; i >= 0; i--) {
                    const cls = String(allRows[i].className || "");
                    if (!cls.includes("time-")) continue;
                    const label = String(allRows[i].innerText || "").trim();
                    if (/^(昨天|星期|20\d{2}-)/.test(label)) {
                        todayStart = i + 1;
                        break;
                    }
                }
                let count = 0;
                for (let i = todayStart; i < allRows.length; i++) {
                    const cls = String(allRows[i].className || "");
                    if (!cls.includes("is-me")) continue;
                    if (cls.includes("time-")) continue;
                    const pre = allRows[i].querySelector("pre, [class*='text-']");
                    const text = String(pre?.innerText || allRows[i].innerText || "").trim();
                    if (text === targetText) count++;
                }
                return count;
            }""",
            _normalize_message_text(target_text),
        )
    except Exception:
        return 0


async def count_visible_message_matches(page, message, chat_input=None):
    probe = _message_probe_text(message)
    if not probe:
        return 0

    locator = page.locator(f"text={probe}")
    try:
        count = await locator.count()
    except Exception:
        return 0

    input_box = None
    if chat_input is not None:
        try:
            input_box = await chat_input.bounding_box(timeout=1000)
        except Exception:
            input_box = None

    visible_count = 0
    for index in range(min(count, 200)):
        try:
            item = locator.nth(index)
            if not await item.is_visible(timeout=500):
                continue
            if await item.evaluate("(node) => Boolean(node.closest('[contenteditable=\"true\"]'))"):
                continue
            if input_box:
                box = await item.bounding_box(timeout=500)
                if not box:
                    continue
                center_x = box["x"] + box["width"] / 2
                center_y = box["y"] + box["height"] / 2
                in_chat_column = (
                    input_box["x"] - 80
                    <= center_x
                    <= input_box["x"] + input_box["width"] + 80
                )
                above_input = center_y < input_box["y"] + input_box["height"] + 80
                if not (in_chat_column and above_input):
                    continue
            visible_count += 1
        except Exception:
            continue
    return visible_count


async def confirm_message_sent(page, chat_input, message, before_snapshot=None):
    expected_message = _normalize_message_text(message)
    before_signature = None
    if before_snapshot:
        before_signature = (
            before_snapshot.get("text"),
            round(float(before_snapshot.get("centerY") or 0), 1),
            round(float(before_snapshot.get("right") or 0), 1),
        )
    # Record how many times the target text already appears today so we can
    # detect a *new* occurrence even if the last-own-message slot was taken by
    # a manual send from the user's phone.
    before_count = await count_today_own_message_matches(page, expected_message)

    last_input_text = ""
    last_snapshot = None
    deadline = asyncio.get_running_loop().time() + 8
    while True:
        await asyncio.sleep(1)
        last_input_text = await read_chat_input_text(chat_input)
        # Fail fast if the page shows a send-failure indicator (red exclamation,
        # retry button, or explicit failure text) before we even check the bubble.
        failure_indicator = await _detect_send_failure_indicator(page)
        if failure_indicator:
            return False, f"send failure indicator detected: {failure_indicator}"
        last_snapshot = await snapshot_last_own_message(page, chat_input=chat_input)
        last_signature = None
        if last_snapshot:
            last_signature = (
                last_snapshot.get("text"),
                round(float(last_snapshot.get("centerY") or 0), 1),
                round(float(last_snapshot.get("right") or 0), 1),
            )
        if (
            not last_input_text
            and last_snapshot
            and _normalize_message_text(last_snapshot.get("text")) == expected_message
            and last_signature != before_signature
        ):
            return (
                True,
                "last own message matches sent text; chat input cleared",
            )
        # Count fallback: even if the last own bubble is not our message (e.g.
        # the user sent something else from their phone), a new occurrence of
        # the target text today means our send went through.
        if (
            not last_input_text
            and not last_snapshot
        ):
            after_count = await count_today_own_message_matches(page, expected_message)
            if after_count > before_count:
                return (
                    True,
                    "count fallback: new own message with target text appeared today",
                )
        if asyncio.get_running_loop().time() >= deadline:
            break

    if last_input_text:
        return False, f"chat input still contains: {last_input_text!r}"
    # Final count fallback before giving up.
    after_count = await count_today_own_message_matches(page, expected_message)
    if after_count > before_count:
        return (
            True,
            "count fallback at deadline: new own message with target text appeared today",
        )
    if last_snapshot:
        return False, (
            "last own message did not match sent text: "
            f"expected={expected_message!r} actual={last_snapshot.get('text')!r}"
        )
    return (
        False,
        "chat input cleared but no new own message bubble was confirmed",
    )


async def find_visible_spark_message_candidate(page, chat_input=None, candidates=None):
    messages = []
    for candidate in candidates or ():
        probe = _message_probe_text(candidate)
        if probe and probe not in messages:
            messages.append(probe)

    for candidate in messages:
        try:
            if await count_visible_message_matches(page, candidate, chat_input=chat_input) > 0:
                return candidate
        except Exception:
            continue
    return ""


def _json_body_success(data):
    if not isinstance(data, dict):
        return None

    candidates = []
    pending = [data]
    while pending:
        candidate = pending.pop(0)
        if isinstance(candidate, dict):
            candidates.append(candidate)
            for value in candidate.values():
                if isinstance(value, (dict, list)):
                    pending.append(value)
        elif isinstance(candidate, list):
            pending.extend(
                value
                for value in candidate
                if isinstance(value, (dict, list))
            )

    code_seen = False
    code_success = True
    error_seen = False
    for candidate in candidates:
        for key in ("status_code", "err_no", "errno", "error_code", "code"):
            if key in candidate:
                code_seen = True
                if candidate.get(key) not in (0, "0"):
                    code_success = False
        message = str(
            candidate.get("message")
            or candidate.get("status_msg")
            or ""
        ).lower()
        if message and message not in ("success", "ok"):
            error_seen = True
        for key in ("error", "error_msg", "err_msg"):
            if candidate.get(key):
                error_seen = True

    if code_seen:
        return code_success and not error_seen
    if error_seen:
        return False
    return None


# 抖音在风控命中时返回给客户端的拒绝标志
_SERVER_REJECT_TOKENS = (
    "KICK",
    "CHECKMESSAGENOTPASS",
    "CHECK_MESSAGE_NOT_PASS",
    "NOTPASS",
    "RISKREJECT",
    # 内容审核未通过。响应体可能包含 status_code=8610 等拒绝标志，
    # 即使 HTTP response 为 200，也不代表消息已被接受。
    "8610",
)


def _server_accepted_send(im_summary, send_receipt):
    """依据 CDP 观察到的服务端响应判断消息是否真的被接受，返回 ``(accepted, reason)``。

    以下两种情况一律视为**未送达**：

    - 服务端对 ``/message/send`` 完全没有响应（平台静默丢弃，常见于风控）；
    - 响应体里出现 ``decision=KICK`` / ``CheckMessageNotPass`` 等拒绝标志。

    ``reason`` 以 ``response_body_unavailable`` 开头表示响应体没抓到
    （CDP ``Network.getResponseBody`` 失败），但服务端已返回 2xx，判定为接受。

    为什么必须这样判：重发场景下页面上的相同消息可能是**历史消息**，
    仅依赖页面状态会产生假阳性，因此同时检查服务端回执。
    """
    if not dict(im_summary or {}).get("send_response_seen"):
        return False, "server never responded to message/send"
    reason = str(dict(send_receipt or {}).get("reason") or "")
    if "response_body_unavailable" in reason:
        return True, ""
    upper = reason.upper().replace(" ", "")
    if any(token in upper for token in _SERVER_REJECT_TOKENS):
        return False, f"server rejected the send: {reason[:160]}"
    return True, ""


def _should_persist_recovered_browser_evidence(sent_ok, error):
    del error
    return bool(sent_ok)


async def detect_message_already_sent(page, chat_input, message, before_snapshot=None):
    try:
        if chat_input is not None:
            sent_ok, detail = await confirm_message_sent(
                page,
                chat_input,
                message,
                before_snapshot=before_snapshot,
            )
            if sent_ok:
                return True, detail
    except Exception:
        pass

    return False, ""


def classify_browser_failure(stage, exc):
    detail = str(exc or "")
    lowered = detail.lower()

    if "friend_list_unavailable" in lowered:
        return "friend_list_unavailable"
    if "friend_list_incomplete" in lowered:
        return "friend_list_incomplete"
    if "login_required" in lowered:
        return "login_required"
    if any(token in detail for token in LOGIN_REQUIRED_TEXTS):
        return "login_required"
    if "page crashed" in lowered or "target page, context or browser has been closed" in lowered:
        return "page_crashed"
    if "timeout" in lowered:
        if stage in {"open_creator_home", "open_chat_page"}:
            return "navigation_timeout"
        if stage == "locate_chat_input":
            return "chat_input_timeout"
        if stage == "friend_list":
            return "friend_list_timeout"
        return "timeout"
    if "unable to locate chat input" in lowered:
        return "chat_input_not_found"
    if "could not find the friend list scroll container" in lowered:
        return "friend_list_container_missing"
    if "chat input still contains" in lowered or "visible message count did not increase" in lowered:
        return "send_unconfirmed"
    if "missing targets" in lowered:
        return "friend_not_found"
    if stage in {"open_creator_home", "open_chat_page"}:
        return "navigation_failed"
    return "unknown"


class FriendListUnavailableError(RuntimeError):
    pass


class FriendListIncompleteError(RuntimeError):
    pass


ACCOUNT_LEVEL_FAILURE_CATEGORIES = {
    "login_required",
    "friend_list_unavailable",
    "friend_list_timeout",
    "friend_list_incomplete",
    "friend_list_container_missing",
    "navigation_timeout",
    "navigation_failed",
    "page_crashed",
}


TEMPORARY_ACCOUNT_FAILURE_CATEGORIES = {
    "friend_list_unavailable",
    "friend_list_timeout",
    "friend_list_incomplete",
    "friend_list_container_missing",
    "navigation_timeout",
    "navigation_failed",
    "page_crashed",
    "protocol_sender_failed",
    "timeout",
}

# 结构性/瞬时失败：页面没渲染出来、选择器没命中、导航超时等。
# 这类故障常能自愈（页面加载抖动、抖音临时改版），用短冷却让它下一轮自动重试；
# 过长的冷却可能耗尽当前发送窗口内的重试机会。
STRUCTURAL_ACCOUNT_FAILURE_CATEGORIES = {
    "friend_list_unavailable",
    "friend_list_timeout",
    "friend_list_incomplete",
    "friend_list_container_missing",
    "navigation_timeout",
    "navigation_failed",
    "timeout",
}
# 必须短于 launchd 触发间隔（20min），否则无法在窗口内自愈重试。
STRUCTURAL_FAILURE_COOLDOWN_MINUTES = 15


def _is_account_level_failure_category(category):
    return category in ACCOUNT_LEVEL_FAILURE_CATEGORIES


def _target_display_name(target):
    if isinstance(target, dict):
        for key in ("name", "displayName", "nickname", "username", "unique_id", "uid"):
            value = str(target.get(key) or "").strip()
            if value:
                return value
        return ""
    return str(target or "").strip()


def _build_normalized_target_map(targets):
    normalized_targets = {}
    for target in targets or []:
        display_name = _target_display_name(target)
        normalized_name = _normalize_target_name(display_name)
        if normalized_name:
            normalized_targets[normalized_name] = display_name
    return normalized_targets


def _normalized_target_order(targets):
    ordered = []
    seen = set()
    for target in targets or []:
        normalized_name = _normalize_target_name(_target_display_name(target))
        if normalized_name and normalized_name not in seen:
            seen.add(normalized_name)
            ordered.append(normalized_name)
    return ordered


async def _locator_count_with_timeout(locator, account_name, stage, label, timeout_seconds=3):
    try:
        return await asyncio.wait_for(locator.count(), timeout=timeout_seconds)
    except asyncio.TimeoutError:
        logger.warning(
            "Account %s locator count timed out at %s label=%s after %ss",
            account_name,
            stage,
            label,
            timeout_seconds,
        )
        return None


async def _first_non_empty_locator(page, selectors):
    for selector in selectors:
        locator = page.locator(selector)
        count = await _locator_count_with_timeout(
            locator,
            "unknown",
            "first_non_empty_locator",
            selector,
        )
        if count is None:
            continue
        for index in range(min(count, 5)):
            item = locator.nth(index)
            try:
                if await item.is_visible(timeout=500):
                    return selector, locator
            except Exception:
                continue
    return "", None


async def _selector_visible(page, selectors):
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            count = await _locator_count_with_timeout(
                locator,
                "unknown",
                "selector_visible",
                selector,
            )
            if count and await locator.is_visible(timeout=500):
                return selector
        except Exception:
            continue
    return ""


async def _click_first_visible_locator(candidates, account_name, stage):
    last_error = None
    for label, locator in candidates:
        try:
            count = await _locator_count_with_timeout(
                locator,
                account_name,
                stage,
                label,
            )
            count = min(count or 0, 5)
        except Exception as exc:
            last_error = exc
            continue

        for index in range(count):
            item = locator.nth(index)
            try:
                if not await item.is_visible(timeout=800):
                    continue
                await item.click(timeout=3000)
                logger.info("Account %s clicked %s at %s", account_name, label, stage)
                return True
            except Exception as exc:
                last_error = exc
                continue
    if last_error:
        logger.debug("Account %s could not click a visible locator at %s: %s", account_name, stage, last_error)
    return False

async def _open_friends_tab(page, account_name, fallback_selector, target_selectors):
    await _dismiss_non_login_dialogs(page, account_name, "before_open_friends_tab")

    _, existing_locator = await _first_non_empty_locator(page, target_selectors)
    if existing_locator:
        logger.info("Account %s friend list is already visible before tab click", account_name)
        return "already_visible"

    sub_app = page.locator('xpath=//*[@id="sub-app"]')
    text_selector = (
        'xpath=//*[@id="sub-app"]//*[self::div or self::button or self::span or @role="tab"]'
        '[contains(normalize-space(.), "朋友") or contains(normalize-space(.), "好友") '
        'or contains(normalize-space(.), "互关")]'
    )
    page_text_selector = (
        'xpath=//*[self::div or self::button or self::span or @role="tab" or @role="button"]'
        '[contains(normalize-space(.), "朋友私信") or contains(normalize-space(.), "好友私信") '
        'or contains(normalize-space(.), "朋友") or contains(normalize-space(.), "好友") '
        'or contains(normalize-space(.), "互关")]'
    )
    candidates = [
        ("page tab text 朋友私信", page.get_by_text("朋友私信", exact=True)),
        ("page tab text 好友私信", page.get_by_text("好友私信", exact=True)),
        ("tab text 朋友", sub_app.get_by_text("朋友", exact=True)),
        ("tab text 好友", sub_app.get_by_text("好友", exact=True)),
        ("tab text 互关", sub_app.get_by_text("互关", exact=True)),
        ("tab text 朋友私信", sub_app.get_by_text("朋友私信", exact=True)),
        ("tab text 好友私信", sub_app.get_by_text("好友私信", exact=True)),
        ("role=tab 私信", page.locator('xpath=//*[@role="tab"][contains(normalize-space(.), "私信")]')),
        ("role=tab 消息", page.locator('xpath=//*[@role="tab"][contains(normalize-space(.), "消息")]')),
        ("role=tab 朋友", page.locator('xpath=//*[@role="tab"][contains(normalize-space(.), "朋友")]')),
        ("page text 私信(contains)", page.get_by_text("私信")),
        ("page text 聊天(contains)", page.get_by_text("聊天")),
        ("page text 互动管理(contains)", page.get_by_text("互动管理")),
        ("sub-app friend tab text", page.locator(text_selector)),
        ("page friend tab text", page.locator(page_text_selector)),
        ("fallback friends tab xpath", page.locator(fallback_selector)),
    ]
    if await _click_first_visible_locator(candidates, account_name, "open_friends_tab"):
        await asyncio.sleep(2)
        return "clicked"

    # 所有定位方式都落空。若只对硬编码绝对路径 XPath 做长时间硬等待，
    # 选择器变化就可能让整轮任务超时。
    # 改为：先落盘现场（含登录态）→ 重新导航到私信页 → 再试一次点击 → 软失败返回。
    # 「好友行是否真的出现」才是唯一可靠判据，交由上层 _wait_for_friend_list_ready 判定。
    logger.warning(
        "Account %s friends tab not found by any locator; dumping page state and reopening chat page",
        account_name,
    )
    try:
        await dump_friend_list_failure(page, account_name, stage="friends-tab-not-found")
    except Exception:
        pass

    try:
        await page.goto(CREATOR_CHAT_URL, wait_until="domcontentloaded")
        await page.wait_for_selector("#sub-app", timeout=20000)
        await asyncio.sleep(3)
    except Exception as exc:
        logger.warning(
            "Account %s reopen chat page before friend list failed: %s", account_name, exc
        )

    if await _click_first_visible_locator(candidates, account_name, "open_friends_tab_after_reopen"):
        await asyncio.sleep(2)
        return "clicked_after_reopen"

    # 页面此时已重新加载完成，补一次登录判定。
    # 登录失效时页面根本不会渲染 #sub-app，
    # 若不在这一步判定，就会被上层笼统归类成 friend_list_timeout，
    # 把「需要重新登录」误报成「页面超时」，掩盖真正原因。
    try:
        await ensure_not_login_required(page, account_name, "open_friends_tab")
    except Exception as exc:
        logger.error(
            "Account %s login required while opening friends tab: %s", account_name, exc
        )
        raise

    logger.warning(
        "Account %s friends tab still not found; continuing and waiting for friend rows",
        account_name,
    )
    return "not_found"


async def _reopen_friend_chat_page(page, account_name):
    logger.warning(
        "Account %s friend list did not become visible within 60s; reopening chat page once",
        account_name,
    )
    await _dismiss_non_login_dialogs(page, account_name, "before_reopen_chat_page")
    try:
        # 抖音是重型 SPA，load 事件常因子资源挂起而不触发，统一用 domcontentloaded。
        await page.reload(wait_until="domcontentloaded", timeout=120000)
        await page.wait_for_selector("#sub-app", timeout=30000)
        await asyncio.sleep(2)
    except Exception as exc:
        logger.warning("Account %s reload before reopen chat page failed: %s", account_name, exc)

    await retry_operation(
        "reopen chat page after friend list wait",
        page.goto,
        retries=2,
        delay=5,
        url=CREATOR_CHAT_URL,
        wait_until="domcontentloaded",
    )
    await asyncio.sleep(3)
    await ensure_not_login_required(page, account_name, "reopen_chat_page")
    await _dismiss_non_login_dialogs(page, account_name, "after_reopen_chat_page")


async def _wait_for_friend_list_ready(
    page,
    account_name,
    target_selectors,
    no_more_selectors,
    loading_selectors,
    timeout_seconds=120,
    empty_grace_seconds=45,
):
    started_at = asyncio.get_running_loop().time()
    last_loading_seen_at = None
    last_dialog_dismissed_at = None
    while True:
        dismissed_count = await _dismiss_non_login_dialogs(page, account_name, "friend_list_ready")
        if dismissed_count:
            last_dialog_dismissed_at = asyncio.get_running_loop().time()

        selector, locator = await _first_non_empty_locator(page, target_selectors)
        if locator:
            logger.debug("Account %s friend list ready via selector %s", account_name, selector)
            return selector, locator

        is_login_required, login_detail = await _page_has_login_required_prompt(page)
        if is_login_required:
            raise RuntimeError(f"login_required at friend_list for {account_name}: {login_detail}")

        no_more_selector = await _selector_visible(page, no_more_selectors)
        if no_more_selector:
            elapsed = asyncio.get_running_loop().time() - started_at
            if elapsed > empty_grace_seconds:
                raise FriendListUnavailableError(
                    f"friend_list_unavailable for {account_name}: no friend entries visible; "
                    f"empty/end selector {no_more_selector} is visible after {empty_grace_seconds}s"
                )
            logger.debug(
                "Account %s saw empty/end selector %s after %.1fs; waiting within grace window",
                account_name,
                no_more_selector,
                elapsed,
            )

        loading_selector = await _selector_visible(page, loading_selectors)
        if loading_selector:
            last_loading_seen_at = asyncio.get_running_loop().time()
            logger.debug("Account %s waiting for friend list loading selector %s", account_name, loading_selector)

        elapsed = asyncio.get_running_loop().time() - started_at
        if elapsed > timeout_seconds:
            loading_note = ""
            if last_loading_seen_at:
                loading_note = f"; loading was visible {elapsed - last_loading_seen_at:.1f}s ago"
            if last_dialog_dismissed_at:
                loading_note += f"; dialog was dismissed {elapsed - (last_dialog_dismissed_at - started_at):.1f}s ago"
            raise FriendListUnavailableError(
                f"friend_list_unavailable for {account_name}: no friend entries became visible "
                f"within {timeout_seconds}s{loading_note}"
            )

        await asyncio.sleep(1)


async def _extract_friend_display_name(element):
    name_selectors = (
        """xpath=.//span[contains(@class, "item-header-name-")]""",
        """xpath=.//*[contains(@class, "item-header-name-")]""",
    )
    for selector in name_selectors:
        locator = element.locator(selector).first
        try:
            if await locator.count() > 0:
                text = (await locator.inner_text(timeout=1000)).strip()
                if text:
                    return text
        except Exception:
            continue

    try:
        text = (await element.inner_text(timeout=1000)).strip()
    except Exception:
        return ""
    for line in text.splitlines():
        line = line.strip()
        if line:
            return line
    return ""


async def _extract_friend_stable_keys(element):
    stable_keys = set()
    attrs = (
        "data-e2e",
        "data-id",
        "data-user-id",
        "data-conversation-id",
        "data-sec-uid",
        "href",
        "aria-label",
        "title",
    )
    for attr in attrs:
        try:
            value = await element.get_attribute(attr)
        except Exception:
            value = None
        if value:
            stable_keys.add(f"{attr}:{str(value).strip()[:240]}")

    try:
        links = await element.locator("xpath=.//a[@href]").all()
    except Exception:
        links = []
    for link in links[:3]:
        try:
            href = await link.get_attribute("href")
        except Exception:
            href = None
        if href:
            stable_keys.add(f"href:{str(href).strip()[:240]}")

    return sorted(stable_keys)


async def _extract_friend_record(element):
    display_name = await _extract_friend_display_name(element)
    normalized_name = _normalize_target_name(display_name)
    if not normalized_name:
        return None
    return {
        "visibleName": display_name,
        "normalizedName": normalized_name,
        "stableKeys": await _extract_friend_stable_keys(element),
    }


async def _first_scrollable_friends_element(page, selectors):
    # 委托给 friends.find_scrollable_friends_element：必须校验容器真的可滚，
    # 否则静止 wrapper 会让 scrollTop 写入失效，扫描/选人只看得见首屏。
    from spark.engine.friends import find_scrollable_friends_element

    return await find_scrollable_friends_element(page, selectors)


async def verify_chat_target(page, friend, timeout_seconds=10):
    """Require the conversation header to match before typing into any input."""
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    expected = _normalize_target_name(friend)
    last = ""
    while asyncio.get_running_loop().time() < deadline:
        header = page.locator('#sub-app [class*="box-header-name-"]')
        if await header.count() == 1:
            last = (await header.inner_text(timeout=1000)).strip()
            if _normalize_target_name(last) == expected:
                return
        await asyncio.sleep(0.25)
    raise RuntimeError(f"chat_target_mismatch: expected={friend!r}, visible={last!r}")


async def scroll_and_select_user(page, user, account_name, targets, friend_scan_config=None, index_targets=None):
    friends_tab_selector = 'xpath=//*[@id="sub-app"]/div/div/div[1]/div[2]'
    target_selectors = (
        '#sub-app li[role="list-item"]:has([class*="item-header-name-"])',
        '#sub-app li.semi-list-item:has([class*="item-header-name-"])',
        # 以下两项不依赖抖音的哈希类名，仅按"好友会话列表行"的结构锚定，
        # 避免页面 class 改版后整列检测失效。
        '#sub-app li[role="list-item"]',
        '#sub-app li.semi-list-item',
        'xpath=//*[@id="sub-app"]//li[@role="list-item"]',
        'xpath=//*[@id="sub-app"]/div/div[1]/div[2]/div[2]'
        '//div[contains(@class, "semi-list-item-body")]',
        'xpath=//*[@id="sub-app"]//div[contains(@class, "semi-list-item-body") and .//span[contains(@class, "item-header-name-")]]',
        'xpath=//*[@id="sub-app"]//li[.//span[contains(@class, "item-header-name-")]]',
    )
    scrollable_friends_selectors = (
        '#sub-app [role="grid"]',
        '#sub-app .ReactVirtualized__Grid',
        'xpath=//*[@id="sub-app"]/div/div[1]/div[2]/div[2]/div/div/div[3]/div/div/div/ul/div',
        'xpath=//*[@id="sub-app"]//ul/div',
        'xpath=//*[@id="sub-app"]//div[contains(@class, "semi-list")]//ul/..',
    )
    no_more_selectors = (
        'xpath=//div[contains(@class, "no-more-tip-ftdJnu")]',
        'xpath=//*[@id="sub-app"]//*[contains(text(), "没有更多") or contains(text(), "暂无")]',
    )
    loading_selectors = (
        'xpath=//div[contains(@class, "semi-spin")]',
        'xpath=//*[@id="sub-app"]//*[contains(@class, "loading") or contains(@class, "Loading")]',
    )
    friend_scan_config = friend_scan_config or _normalize_friend_list_scan_config({})
    max_scan_seconds = friend_scan_config["maxScanSeconds"]
    idle_scan_seconds = friend_scan_config["idleScanSeconds"]
    scroll_step_px = friend_scan_config["scrollStepPx"]
    scroll_delay_seconds = friend_scan_config["scrollDelaySeconds"]
    ready_timeout_seconds = 60
    empty_grace_seconds = 30

    ready_errors = []
    active_target_selector = ""
    for ready_attempt in range(2):
        logger.debug("Account %s is opening the friends tab attempt=%s", account_name, ready_attempt + 1)
        await _open_friends_tab(page, account_name, friends_tab_selector, target_selectors)

        try:
            active_target_selector, _ = await _wait_for_friend_list_ready(
                page,
                account_name,
                target_selectors,
                no_more_selectors,
                loading_selectors,
                timeout_seconds=ready_timeout_seconds,
                empty_grace_seconds=empty_grace_seconds,
            )
            break
        except FriendListUnavailableError as exc:
            ready_errors.append(str(exc))
            if ready_attempt == 0:
                await _reopen_friend_chat_page(page, account_name)
                continue
            # 列表加载失败时无条件落盘当前 DOM，便于事后定位是抖音改结构还是真的没加载。
            try:
                await dump_friend_list_failure(page, account_name)
            except Exception:
                pass
            raise FriendListUnavailableError(
                f"friend_list_unavailable for {account_name}: no friend entries after "
                f"two {ready_timeout_seconds}s waits; first={ready_errors[0]}; second={ready_errors[-1]}"
            ) from exc

    await asyncio.sleep(2)

    normalized_targets = _build_normalized_target_map(targets)
    remaining = set(normalized_targets)
    found = set()
    loop = asyncio.get_running_loop()
    started = last_progress = loop.time()
    recoveries = 0
    stuck_rounds = 0
    max_recoveries = friend_scan_config.get("maxRecoveries", 2)

    async def reset_scroll():
        _, element = await _first_scrollable_friends_element(page, scrollable_friends_selectors)
        if element:
            await page.evaluate("(e) => { e.scrollTop = 0; }", element)
            await asyncio.sleep(scroll_delay_seconds)

    await reset_scroll()
    while remaining:
        if loop.time() - started > max_scan_seconds:
            break
        await _dismiss_non_login_dialogs(page, account_name, "friend_list_scan")
        _, locator = await _first_non_empty_locator(page, (active_target_selector,) + target_selectors)
        delivered = False
        if locator:
            for element in await locator.all():
                record = await _extract_friend_record(element)
                if not record:
                    continue
                key = record["normalizedName"]
                if key not in found:
                    found.add(key)
                    last_progress = loop.time()
                if key not in remaining:
                    continue
                # Virtual rows can change after any send; re-read on the next iteration.
                await element.click(timeout=10000)
                friend = normalized_targets[key]
                await verify_chat_target(page, friend)
                logger.info("Account %s selected target friend %s", account_name, friend)
                paused = loop.time()
                yield friend
                pause_seconds = loop.time() - paused
                started += pause_seconds
                remaining.discard(key)
                last_progress = loop.time()
                stuck_rounds = 0
                delivered = True
                break
        if delivered:
            await reset_scroll()
            continue
        if not remaining:
            return
        selector, element = await _first_scrollable_friends_element(page, scrollable_friends_selectors)
        moved = False
        if element:
            before = await page.evaluate("(e) => e.scrollTop", element)
            await page.evaluate("(e) => { e.scrollTop += " + str(scroll_step_px) + "; }", element)
            await asyncio.sleep(scroll_delay_seconds)
            after = await page.evaluate("(e) => e.scrollTop", element)
            moved = after > before
        stuck_rounds = 0 if moved else stuck_rounds + 1
        loading = await _selector_visible(page, loading_selectors)
        stalled = loop.time() - last_progress >= idle_scan_seconds
        at_end = stuck_rounds >= 4 and not loading
        if not (stalled or at_end or element is None):
            continue
        missing = sorted(normalized_targets[key] for key in remaining)
        logger.warning("Friend scan stalled container=%s moved=%s scanned=%s missing=%s recovery=%s",
                       selector, moved, len(found), missing, recoveries)
        if recoveries >= max_recoveries:
            break
        recoveries += 1
        # Reopen instead of repeating a scan of the same frozen list.
        await _reopen_friend_chat_page(page, account_name)
        await _open_friends_tab(page, account_name, friends_tab_selector, target_selectors)
        active_target_selector, _ = await _wait_for_friend_list_ready(
            page, account_name, target_selectors, no_more_selectors, loading_selectors,
            timeout_seconds=ready_timeout_seconds, empty_grace_seconds=empty_grace_seconds,
        )
        await reset_scroll()
        last_progress = loop.time()
        stuck_rounds = 0
    if remaining:
        missing = sorted(normalized_targets[key] for key in remaining)
        await dump_friend_list_failure(page, account_name)
        raise FriendListIncompleteError(
            f"friend_list_incomplete for {account_name}: missingTargets={missing}; "
            f"scannedFriends={len(found)}; recoveries={recoveries}; budget={max_scan_seconds}s"
        )
