import asyncio
import datetime
from unittest.mock import AsyncMock

import pytest

from spark import config, service, state, server, scheduler
from spark.engine import send as engine


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'CONFIG_PATH', tmp_path / 'config.json')
    state.connect(tmp_path / 'state.sqlite3')
    state.ensure_schema()
    return tmp_path


def test_visible_targets_are_not_blocked_by_missing_first(monkeypatch):
    class Row:
        def __init__(self, name): self.name = name
        async def click(self, **kwargs): pass
    class Rows:
        async def all(self): return [Row('Test Friend A'), Row('Test Friend B')]
    class Page:
        async def evaluate(self, script, element): return 0
    async def record(row):
        return {'normalizedName': row.name, 'visibleName': row.name}
    monkeypatch.setattr(engine, '_open_friends_tab', AsyncMock())
    monkeypatch.setattr(engine, '_wait_for_friend_list_ready', AsyncMock(return_value=('rows', Rows())))
    monkeypatch.setattr(engine, '_first_non_empty_locator', AsyncMock(return_value=('rows', Rows())))
    monkeypatch.setattr(engine, '_first_scrollable_friends_element', AsyncMock(return_value=('scroll', object())))
    monkeypatch.setattr(engine, '_extract_friend_record', record)
    monkeypatch.setattr(engine, '_dismiss_non_login_dialogs', AsyncMock())
    monkeypatch.setattr(engine, '_selector_visible', AsyncMock(return_value=None))
    monkeypatch.setattr(engine, 'verify_chat_target', AsyncMock())
    monkeypatch.setattr(engine, 'dump_friend_list_failure', AsyncMock())
    reopen = AsyncMock()
    monkeypatch.setattr(engine, '_reopen_friend_chat_page', reopen)
    async def run():
        sent = []
        with pytest.raises(engine.FriendListIncompleteError):
            async for friend in engine.scroll_and_select_user(
                Page(), {}, 'account', ['Test Friend C', 'Test Friend A', 'Test Friend B'],
                {'maxScanSeconds': 60, 'idleScanSeconds': 30, 'scrollStepPx': 400,
                 'scrollDelaySeconds': 0.001, 'maxRecoveries': 1},
            ): sent.append(friend)
        assert sent == ['Test Friend A', 'Test Friend B']
        assert reopen.await_count == 1
    asyncio.run(run())


def test_wrong_conversation_never_types(monkeypatch):
    monkeypatch.setattr(engine, 'verify_chat_target', AsyncMock(side_effect=RuntimeError('chat_target_mismatch')))
    locate = AsyncMock()
    monkeypatch.setattr(engine, 'locate_chat_input', locate)
    with pytest.raises(RuntimeError, match='chat_target_mismatch'):
        asyncio.run(service._send_one(object(), 'account', 'Test Friend A', 'hello'))
    locate.assert_not_awaited()


def test_summary_includes_friends_with_no_records(isolated):
    state.upsert_friend('Test Friend A')
    state.upsert_friend('Test Friend B')
    state.upsert_record('2026-10-17', 'Test Friend A', 'confirmed')
    summary = service.build_summary('2026-10-17')
    assert '1/2' in summary['text']
    assert any('Test Friend B' in item for item in summary['problems'])


def test_uncertain_and_rejected_are_not_automatically_retried():
    records = {'甲': {'status': 'unconfirmed', 'attempts': 1},
               '乙': {'status': 'rejected', 'attempts': 1},
               '丙': {'status': 'failed', 'attempts': 1}}
    assert service.select_pending(['甲', '乙', '丙', '丁'], records, 2) == ['丙', '丁']


def test_catch_up_is_persisted_and_only_sends_pending(isolated, monkeypatch):
    fixed = datetime.datetime(2026, 10, 17, 20, 35, tzinfo=scheduler.TZ)
    class FixedDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None): return fixed
    monkeypatch.setattr(server.datetime, 'datetime', FixedDateTime)
    monkeypatch.setattr(server, '_summary_done_days', set())
    cfg = config.load_config()
    cfg['schedule']['slots'] = []
    config.save_config(cfg)
    state.upsert_friend('Test Friend A')
    state.upsert_friend('Test Friend B')
    state.upsert_record('2026-10-17', 'Test Friend A', 'confirmed')
    run = AsyncMock(return_value={'status': 'failed'})
    monkeypatch.setattr(service, 'run_round', run)
    asyncio.run(server._tick())
    catch = state.get_slot_runs('2026-10-17')['catch_up']
    assert catch['attempts'] == 1
    asyncio.run(server._tick())
    assert run.await_count == 1
    run.assert_awaited_once_with(trigger='catch_up', catch_up=True)


def test_no_pending_round_does_not_falsely_complete(isolated, monkeypatch):
    state.upsert_friend('Test Friend A')
    state.upsert_record(state.today(), 'Test Friend A', 'failed')
    state.increment_attempts(state.today(), 'Test Friend A')
    state.increment_attempts(state.today(), 'Test Friend A')
    monkeypatch.setattr(service, '_open_session', AsyncMock(return_value=(None, None, object())))
    monkeypatch.setattr(service, '_open_chat_page', AsyncMock())
    monkeypatch.setattr(service, '_sync_cookies', AsyncMock())
    result = asyncio.run(service.run_round())
    assert result['status'] == 'incomplete'


def test_uncertain_delivery_is_not_reported_as_done(isolated):
    state.upsert_friend('Test Friend A')
    state.upsert_record('2026-10-17', 'Test Friend A', 'needs_confirmation')
    records = state.get_day_records('2026-10-17')
    assert service.select_pending(['Test Friend A'], records, 2) == []
    assert not service.slot_members_all_sent(['Test Friend A'], records)
    assert '待人工核实' in service.build_summary('2026-10-17')['text']


def test_chat_header_mismatch_is_rejected():
    class Header:
        async def count(self): return 1
        async def inner_text(self, **kwargs): return 'Test Friend B'
    class Page:
        def locator(self, selector): return Header()
    with pytest.raises(RuntimeError, match='chat_target_mismatch'):
        asyncio.run(engine.verify_chat_target(Page(), 'Test Friend A', timeout_seconds=0.001))
    asyncio.run(engine.verify_chat_target(Page(), 'Test Friend B'))


def test_start_time_reserves_recovery_and_never_goes_backwards():
    start = datetime.datetime(2026, 10, 17, 14, tzinfo=scheduler.TZ)
    end = start.replace(hour=18)
    now = end - datetime.timedelta(minutes=2)
    assert scheduler.compute_slot_run_at(start, end, 3, now) == now
    run = scheduler.compute_slot_run_at(start, end, 3, start)
    assert run <= end - datetime.timedelta(seconds=2 * (360 + 600 + 180) + 600)


def test_catch_up_stops_after_persisted_limit(isolated, monkeypatch):
    fixed = datetime.datetime(2026, 10, 17, 20, 35, tzinfo=scheduler.TZ)
    class FixedDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None): return fixed
    monkeypatch.setattr(server.datetime, 'datetime', FixedDateTime)
    cfg = config.load_config()
    cfg['schedule']['slots'] = []
    config.save_config(cfg)
    state.upsert_friend('Test Friend A')
    state.mark_slot_run('2026-10-17', 'catch_up', 'pending', run_at=fixed.isoformat())
    for _ in range(3): state.bump_slot_attempts('2026-10-17', 'catch_up')
    run = AsyncMock()
    monkeypatch.setattr(service, 'run_round', run)
    asyncio.run(server._tick())
    run.assert_not_awaited()


def test_round_preserves_partial_progress_and_passes_scan_config(isolated, monkeypatch):
    fixed = datetime.datetime(2026, 10, 17, 15, tzinfo=scheduler.TZ)
    monkeypatch.setattr(service, '_now', lambda: fixed)
    monkeypatch.setattr(state, 'today', lambda: '2026-10-17')
    for name in ['Test Friend C', 'Test Friend A', 'Test Friend B']: state.upsert_friend(name)
    state.upsert_record('2026-10-17', 'Test Friend B', 'confirmed', message='旧文案')
    monkeypatch.setattr(service, '_open_session', AsyncMock(return_value=(None, None, object())))
    monkeypatch.setattr(service, '_open_chat_page', AsyncMock())
    monkeypatch.setattr(service, '_sync_cookies', AsyncMock())
    monkeypatch.setattr(service.messages, 'load_pool', lambda: [{'text': '续火花', 'when': 'any'}])
    monkeypatch.setattr(engine, 'verify_chat_target', AsyncMock())
    monkeypatch.setattr(service.notify, 'notify', lambda *args: None)
    monkeypatch.setattr(service.asyncio, 'sleep', AsyncMock())
    captured = {}
    async def scan(page, user, account, targets, scan_config, **kwargs):
        captured.update(scan_config)
        assert targets == ['Test Friend C', 'Test Friend A']
        yield 'Test Friend A'
        raise engine.FriendListIncompleteError('Test Friend C missing')
    monkeypatch.setattr(engine, 'scroll_and_select_user', scan)
    send = AsyncMock(return_value={'status': 'confirmed'})
    monkeypatch.setattr(service, '_send_one', send)
    result = asyncio.run(service.run_round())
    assert result['status'] == 'failed'
    assert captured['maxScanSeconds'] == 600
    rows = state.get_day_records('2026-10-17')
    assert rows['Test Friend A']['status'] == 'confirmed'
    assert rows['Test Friend B']['attempts'] == 0
    assert 'Test Friend C' not in rows
    assert send.await_count == 1


def test_missing_response_requires_review_not_resend(monkeypatch):
    chat = type('Chat', (), {'type': AsyncMock(), 'press': AsyncMock()})()
    monkeypatch.setattr(engine, 'verify_chat_target', AsyncMock())
    monkeypatch.setattr(engine, 'locate_chat_input', AsyncMock(return_value=(chat, 'input')))
    monkeypatch.setattr(engine, 'snapshot_last_own_message', AsyncMock(return_value={}))
    monkeypatch.setattr(engine, 'save_debug_artifacts', AsyncMock())
    monkeypatch.setattr(engine, 'start_im_send_observer', AsyncMock(return_value=AsyncMock(return_value={
        'enabled': True, 'send_request_seen': True, 'send_response_seen': False,
    })))
    monkeypatch.setattr(engine, 'confirm_message_sent', AsyncMock(return_value=(False, 'timeout')))
    monkeypatch.setattr(engine, 'detect_message_already_sent', AsyncMock(return_value=(False, '')))
    monkeypatch.setattr(engine, '_page_has_login_required_prompt', AsyncMock(return_value=(False, '')))
    outcome = asyncio.run(service._send_one(object(), 'account', 'Test Friend A', '续火花'))
    assert outcome['status'] == 'needs_confirmation'


def test_catch_up_counter_exists_before_send(isolated, monkeypatch):
    fixed = datetime.datetime(2026, 10, 17, 20, 35, tzinfo=scheduler.TZ)
    class FixedDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None): return fixed
    monkeypatch.setattr(server.datetime, 'datetime', FixedDateTime)
    cfg = config.load_config()
    cfg['schedule']['slots'] = []
    config.save_config(cfg)
    state.upsert_friend('Test Friend A')
    async def run(**kwargs):
        assert state.get_slot_runs('2026-10-17')['catch_up']['attempts'] == 1
        return {'status': 'failed'}
    monkeypatch.setattr(service, 'run_round', run)
    asyncio.run(server._tick())


def test_overview_uses_actual_scheduled_time(isolated, monkeypatch):
    fixed = datetime.datetime(2026, 10, 17, 14, 30, tzinfo=scheduler.TZ)
    class FixedDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None): return fixed
    monkeypatch.setattr(server.datetime, 'datetime', FixedDateTime)
    monkeypatch.setattr(state, 'today', lambda: '2026-10-17')
    state.upsert_friend('Test Friend A')
    state.ensure_slot_plan('2026-10-17', ['Test Friend A'], ['afternoon'])
    state.mark_slot_run('2026-10-17', 'afternoon', 'pending', run_at=fixed.replace(hour=15, minute=40).isoformat())
    assert asyncio.run(server.overview())['nextRunAt'] == '15:40'
    state.upsert_record('2026-10-17', 'Test Friend A', 'confirmed')
    assert asyncio.run(server.overview())['nextRunAt'] is None


def test_scroll_container_skips_missing_and_checks_multiple_candidates():
    from spark.engine.friends import find_scrollable_friends_element
    class Locator:
        def __init__(self, handles): self.handles = handles
        async def count(self): return len(self.handles)
        def nth(self, index):
            handle = self.handles[index]
            return type('Item', (), {'element_handle': AsyncMock(return_value=handle)})()
    class Page:
        def locator(self, selector):
            return Locator([] if selector == 'missing' else ['wrapper', 'scroller'])
        async def evaluate(self, script, handle):
            return {'ch': 400, 'sh': 400 if handle == 'wrapper' else 1200}
    selector, handle = asyncio.run(find_scrollable_friends_element(Page(), ['missing', 'container']))
    assert (selector, handle) == ('container', 'scroller')
