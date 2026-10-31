import asyncio

import pytest

from spark.engine import login


class FakeLocator:
    def __init__(self, page):
        self.page = page

    @property
    def first(self):
        return self

    async def count(self):
        return 1 if self.page.card_visible else 0

    async def is_visible(self):
        return bool(self.page.card_visible)


class FakePage:
    def __init__(self):
        self.card_visible = False

    def locator(self, selector):
        return FakeLocator(self)


def test_wait_for_scan_does_not_raise_when_card_absent_then_times_out():
    page = FakePage()
    with pytest.raises(RuntimeError, match="扫码登录超时"):
        asyncio.run(login.wait_for_scan_success(page, timeout_ms=600))


def test_wait_for_scan_returns_when_card_appears():
    page = FakePage()
    page.card_visible = True
    assert asyncio.run(login.wait_for_scan_success(page, timeout_ms=600)) is True
