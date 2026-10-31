import asyncio
import logging
import re

from spark.engine.browser import get_persistent_browser_context
from spark.util import normalize_unique_id


logger = logging.getLogger(__name__)

READY_SELECTOR = (
    'xpath=//*[contains(@id, "garfish_app_for_douyin_creator_pc_home")]'
    '/div/div[2]/div/div[2]/div[1]'
)
_CONTAINER_XPATH = (
    '//*[contains(@id, "garfish_app_for_douyin_creator_pc_home")]'
    '/div/div[2]/div/div[2]/div[1]'
)
XPATHS = {
    "unique_id": (
        'xpath=//*[contains(@id, "garfish_app_for_douyin_creator_pc_home")]'
        '/div/div[2]/div/div[2]/div[1]/div[2]/div[1]/div[3]'
    ),
    "name": (
        'xpath=//*[contains(@id, "garfish_app_for_douyin_creator_pc_home")]'
        '/div/div[2]/div/div[2]/div[1]/div[2]/div[1]/div[1]/div[1]'
    ),
}

# The creator centre is a client-rendered SPA: on a cold cookie-only context the
# identity card routinely needs 25-30 seconds to appear, so the card wait is the
# slow part. The two text nodes render together with the card and only need a
# short trailing budget, and the card's own text is a fallback for the nickname so
# that a renamed CSS class cannot turn a valid login into a failure.
IDENTITY_TRAILING_BUDGET_MS = 8000
IDENTITY_POLL_SECONDS = 0.5
DOUYIN_ID_MARKERS = ("抖音号：", "抖音号:")
NICKNAME_FALLBACK_SELECTORS = (
    f'xpath={_CONTAINER_XPATH}//*[contains(@class, "name-")]',
)
LOGIN_FORM_SELECTORS = (
    'text=扫码登录',
    'text=验证码登录',
)
DOUYIN_ID_PATTERN = re.compile(r"[0-9A-Za-z_.\-]+")

# Messages are also the category carriers: ``core.friends.classify_refresh_error``
# maps a raw error by its text, so a page/DOM problem must read like one and a
# logged-out session like a login problem.
LOGIN_REQUIRED_MESSAGE = "账号登录已失效，请重新扫码登录"
CARD_NOT_READY_MESSAGE = "creator identity card did not become ready within timeout"
IDENTITY_UNREADABLE_MESSAGE = (
    "creator identity card was visible but the account identity could not be read; "
    "dom=identity-text-missing"
)


def identity_from_card_text(text):
    """Split the creator identity card's text into (nickname, 抖音号).

    The card renders ``<nickname>抖音号：<handle><signature>...`` as one text blob,
    so the nickname stays readable even when its dedicated node is missing or
    hidden. Returns an empty nickname when the marker is absent, because guessing
    from unrelated text would rename accounts.
    """
    raw = " ".join(str(text or "").split())
    for marker in DOUYIN_ID_MARKERS:
        index = raw.find(marker)
        if index > 0:
            nickname = raw[:index].strip()
            tail = raw[index + len(marker):].strip()
            match = DOUYIN_ID_PATTERN.match(tail)
            return nickname, (match.group(0) if match else tail)
    return "", ""


async def _first_visible_text(page, selectors):
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0 and await locator.is_visible():
                text = str(await locator.inner_text() or "").strip()
                if text:
                    return text
        except Exception:
            continue
    return ""


async def _login_form_visible(page):
    for selector in LOGIN_FORM_SELECTORS:
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0 and await locator.is_visible():
                return True
        except Exception:
            continue
    return False


async def _wait_for_identity_card(page, timeout_ms):
    deadline = asyncio.get_running_loop().time() + max(0.5, timeout_ms / 1000)
    while True:
        try:
            locator = page.locator(READY_SELECTOR).first
            if await locator.count() > 0 and await locator.is_visible():
                return
        except Exception:
            pass
        # A visible login form is a conclusive answer: waiting out the whole
        # budget on a page that is asking for a QR scan only delays it.
        if await _login_form_visible(page):
            raise RuntimeError(LOGIN_REQUIRED_MESSAGE)
        if asyncio.get_running_loop().time() >= deadline:
            raise RuntimeError(CARD_NOT_READY_MESSAGE)
        await asyncio.sleep(IDENTITY_POLL_SECONDS)


async def _wait_for_visible_text(page, selectors, budget_ms):
    deadline = asyncio.get_running_loop().time() + max(0.2, budget_ms / 1000)
    while True:
        text = await _first_visible_text(page, selectors)
        if text:
            return text
        if asyncio.get_running_loop().time() >= deadline:
            return ""
        await asyncio.sleep(IDENTITY_POLL_SECONDS)


async def wait_for_logged_in_identity(page, timeout_ms=300000):
    """Read the logged-in identity from the creator home page.

    Returns ``(unique_id, nickname)``. Raises when the card never renders, when a
    login form is shown instead, or when the card renders without a readable
    identity; the message decides the category the caller reports.
    """
    await _wait_for_identity_card(page, timeout_ms)

    trailing_ms = max(1000, min(int(timeout_ms), IDENTITY_TRAILING_BUDGET_MS))

    unique_id_text = await _wait_for_visible_text(page, (XPATHS["unique_id"],), trailing_ms)
    unique_id = normalize_unique_id(unique_id_text)
    if not unique_id:
        raise RuntimeError(IDENTITY_UNREADABLE_MESSAGE)

    nickname = await _wait_for_visible_text(
        page,
        (XPATHS["name"],) + NICKNAME_FALLBACK_SELECTORS,
        trailing_ms,
    )
    if not nickname:
        nickname, _handle = identity_from_card_text(await _first_visible_text(page, (READY_SELECTOR,)))
    if not nickname:
        raise RuntimeError(IDENTITY_UNREADABLE_MESSAGE)
    return unique_id, nickname


async def collect_login_result(page, context, timeout_ms=300000):
    unique_id, username = await wait_for_logged_in_identity(page, timeout_ms=timeout_ms)
    cookies = await context.cookies()
    return {
        "unique_id": unique_id,
        "username": username,
        "cookies": cookies,
    }


async def wait_for_scan_success(page, timeout_ms=420000):
    """轮询等待身份卡出现（扫码成功）。

    和旧逻辑的关键区别：扫码页可见时**不报错**——扫码登录的第一步
    本来就该停在扫码页，等手机确认后身份卡才渲染出来。
    """
    deadline = asyncio.get_running_loop().time() + max(0.5, timeout_ms / 1000)
    while True:
        locator = page.locator(READY_SELECTOR).first
        try:
            if await locator.count() > 0 and await locator.is_visible():
                return True
        except Exception:
            pass
        if asyncio.get_running_loop().time() >= deadline:
            raise RuntimeError(
                "扫码登录超时：未检测到登录成功（最长等待 7 分钟）"
            )
        await asyncio.sleep(1)


async def scan_login(profile_root=None, timeout_ms=420000):
    """打开有头持久化 Chromium 等待扫码，成功后返回登录结果字典。

    返回 ``{"unique_id": str, "username": str, "cookies": [...]}``。
    profile 复用以延续设备指纹；扫码页停留期间不会误报登录失效。
    """
    playwright, context, _profile_dir = await get_persistent_browser_context(
        "account",
        GUI=True,
        root=profile_root,
    )
    try:
        page = context.pages[0] if context.pages else await context.new_page()
        await page.goto(
            "https://creator.douyin.com/",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        print("已打开抖音创作者中心，请用手机抖音扫码登录……")
        await wait_for_scan_success(page, timeout_ms=timeout_ms)
        return await collect_login_result(page, context, timeout_ms=60000)
    finally:
        try:
            await playwright.stop()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(scan_login())
