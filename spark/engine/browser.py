import os
import re
import subprocess
import sys
import traceback
from pathlib import Path

from playwright.async_api import async_playwright

from spark import config


DEFAULT_PROFILE_ROOT = "data/profiles"


def configure_playwright_environment():
    if os.getenv("PLAYWRIGHT_BROWSERS_PATH"):
        return
    # 本地部署使用 playwright 默认浏览器缓存（~/Library/Caches/ms-playwright）。
    return


def _headless_for(GUI=False):
    headful_env = str(os.getenv("SPARKFLOW_BROWSER_HEADFUL") or "").strip().lower()
    if headful_env in {"1", "true", "yes", "on"}:
        return False
    return not GUI


def _browser_args():
    return [
        "--disable-dev-shm-usage",
        "--no-sandbox",
    ]


def _douyin_network_mode():
    return str(
        os.getenv("SPARKFLOW_DOUYIN_NETWORK_MODE")
        or config.load_config().get("network", {}).get("mode", "direct")
    ).strip().lower()


def douyin_network_modes():
    # Direct is the default; Mihomo is the fallback unless explicitly selected.
    mode = _douyin_network_mode()
    if mode == "mihomo":
        return ("mihomo",)
    return ("direct", "mihomo")


def _douyin_browser_proxy(network_mode=None):
    # Return an explicit proxy URL for Douyin traffic, or None for direct.
    mode = str(network_mode or _douyin_network_mode()).strip().lower()
    if mode != "mihomo":
        return None
    return str(
        os.getenv("SPARKFLOW_DOUYIN_PROXY_URL")
        or config.load_config().get("network", {}).get("proxyUrl", "http://127.0.0.1:7890")
    ).strip() or None


def _browser_launch_options(GUI=False, network_mode=None):
    args = _browser_args()
    proxy = _douyin_browser_proxy(network_mode=network_mode)
    if proxy:
        return {
            "headless": _headless_for(GUI),
            "args": args,
            "proxy": {"server": proxy},
        }
    args.append("--no-proxy-server")
    return {
        "headless": _headless_for(GUI),
        "args": args,
    }


async def select_douyin_network_mode(target_url):
    # Select the first route that can load the target before a task starts.
    failures = []
    for network_mode in douyin_network_modes():
        playwright = browser = page = None
        try:
            playwright, browser = await get_browser(network_mode=network_mode)
            page = await browser.new_page()
            response = await page.goto(target_url, wait_until="commit", timeout=30000)
            status = response.status if response is not None else None
            if status is not None and status < 500:
                return network_mode
            failures.append(f"{network_mode}: HTTP {status}")
        except Exception as exc:
            failures.append(f"{network_mode}: {exc}")
        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass
            if playwright:
                try:
                    await playwright.stop()
                except Exception:
                    pass
    raise RuntimeError(f"Douyin network preflight failed: {'; '.join(failures)}")


def sanitize_profile_name(value):
    raw = str(value or "").strip()
    if not raw:
        raw = "unknown"
    safe = re.sub(r"[^0-9A-Za-z._-]+", "_", raw)
    safe = safe.strip("._-") or "unknown"
    return safe[:80]


def browser_profile_root(root=None):
    configured = (
        root
        or os.getenv("SPARKFLOW_BROWSER_PROFILE_ROOT")
        or DEFAULT_PROFILE_ROOT
    )
    path = Path(configured)
    if not path.is_absolute():
        path = config.ROOT / path
    return path


async def install_browser():
    try:
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium"], check=True)
        print("Browser install completed. Please run the command again.")
    except subprocess.CalledProcessError as exc:
        print(f"Browser install failed: {exc}")


async def get_browser(GUI=False, network_mode=None):
    configure_playwright_environment()
    playwright = None

    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(**_browser_launch_options(GUI, network_mode=network_mode))
        return playwright, browser
    except Exception as exc:
        if playwright is not None:
            try:
                await playwright.stop()
            except Exception:
                pass
        if "Executable doesn't exist" in str(exc):
            print("Playwright browser is missing.")
            await install_browser()
            raise RuntimeError(
                "Playwright browser is missing; install Chromium before retrying"
            ) from exc
        traceback.print_exc()
        raise


async def get_persistent_browser_context(profile_name, GUI=False, root=None, network_mode=None):
    configure_playwright_environment()

    profile_dir = browser_profile_root(root) / sanitize_profile_name(profile_name)
    profile_dir.mkdir(parents=True, exist_ok=True)

    playwright = None
    try:
        playwright = await async_playwright().start()
        launch_options = _browser_launch_options(GUI, network_mode=network_mode)
        launch_options["viewport"] = {"width": 1600, "height": 1000}
        context = await playwright.chromium.launch_persistent_context(
            str(profile_dir),
            **launch_options,
        )
        return playwright, context, profile_dir
    except Exception as exc:
        if playwright is not None:
            try:
                await playwright.stop()
            except Exception:
                pass
        if "Executable doesn't exist" in str(exc):
            print("Playwright browser is missing.")
            await install_browser()
            raise RuntimeError(
                "Playwright browser is missing; install Chromium before retrying"
            ) from exc
        traceback.print_exc()
        raise
