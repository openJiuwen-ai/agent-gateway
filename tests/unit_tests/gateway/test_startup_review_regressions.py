# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Failed optional startup must preserve Cron availability and RPC responses."""

from __future__ import annotations

import asyncio
import builtins
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.application import cron_startup
from agent_gateway.channel_manager.web import app_web_handlers
from agent_gateway.common import model_migration, remote_model_catalog
from agent_gateway.cron.controller import CronController
from agent_gateway.cron.scheduler import CronSchedulerService
from agent_gateway.lifecycle_errors import LifecycleError
from tests.unit_tests.gateway.test_app_web_handlers import _bind

pytestmark = pytest.mark.asyncio


def fast_startup(monkeypatch):
    for name, value in [('_MIGRATION_ATTEMPTS', 3), ('_RETRY_DELAY_SECONDS', .001),
                        ('_CATALOG_TIMEOUT_SECONDS', .01), ('_READY_TIMEOUT_SECONDS', .1)]:
        monkeypatch.setattr(cron_startup, name, value, raising=False)


@pytest.mark.parametrize('failure', ['missing_catalog', 'catalog_hangs', 'store_unavailable'])
async def test_failed_migration_preserves_legacy_jobs_and_starts_once(monkeypatch, failure):
    fast_startup(monkeypatch)
    job = SimpleNamespace(model_selection=None, model_name='existing-model')
    store = SimpleNamespace(list_jobs=AsyncMock(return_value=[job]), update_job=AsyncMock())

    async def fetch(**kwargs):
        if failure == 'catalog_hangs':
            await asyncio.Event().wait()
        return None

    fetch_mock = AsyncMock(side_effect=fetch)
    monkeypatch.setattr(remote_model_catalog, 'fetch_agent_model_rows', fetch_mock)
    if failure == 'store_unavailable':
        store.list_jobs.side_effect = OSError('store temporarily unavailable')
    scheduler = SimpleNamespace(start=AsyncMock())
    startup = cron_startup.CronStartup(store, scheduler, SimpleNamespace(mutation_lock=asyncio.Lock()),
                                       object(), migrate_file=True)
    try:
        await asyncio.wait_for(startup.wait_ready(), .5)
        await startup.wait_ready()
        scheduler.start.assert_awaited_once()
        assert (store.list_jobs.await_count if failure == 'store_unavailable' else fetch_mock.await_count) == 3
        store.update_job.assert_not_awaited()
        assert job.model_name == 'existing-model' and job.model_selection is None
    finally:
        await startup.stop()


async def test_request_timeout_does_not_cancel_shared_migration_write(monkeypatch):
    fast_startup(monkeypatch)
    monkeypatch.setattr(cron_startup, '_READY_TIMEOUT_SECONDS', .02)
    entered, release = asyncio.Event(), asyncio.Event()
    controller = SimpleNamespace(mutation_lock=asyncio.Lock())
    scheduler = SimpleNamespace(start=AsyncMock())
    job = SimpleNamespace(model_selection=None, model_name='old')
    monkeypatch.setattr(remote_model_catalog, 'fetch_agent_model_rows', AsyncMock(return_value=[]))

    async def migrate(*args, **kwargs):
        entered.set()
        await release.wait()
        assert controller.mutation_lock.locked()

    monkeypatch.setattr(model_migration, 'migrate_legacy_cron_selections', migrate)
    startup = cron_startup.CronStartup(SimpleNamespace(list_jobs=AsyncMock(return_value=[job])),
                                       scheduler, controller, object(), migrate_file=True)
    try:
        with pytest.raises(LifecycleError, match='still initializing'):
            await asyncio.wait_for(startup.wait_ready(), .5)
        assert entered.is_set() and controller.mutation_lock.locked()
        scheduler.start.assert_not_awaited()
        release.set()
        await startup.wait_ready()
        scheduler.start.assert_awaited_once()
    finally:
        release.set()
        await startup.stop()


async def test_cancelled_waiter_keeps_other_caller_startup_alive(monkeypatch):
    fast_startup(monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()

    async def start():
        entered.set()
        await release.wait()

    scheduler = SimpleNamespace(start=AsyncMock(side_effect=start))
    startup = cron_startup.CronStartup(object(), scheduler, object(), object(), migrate_file=False)
    first = asyncio.create_task(startup.wait_ready())
    second = asyncio.create_task(startup.wait_ready())
    try:
        await entered.wait()
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        release.set()
        await second
        scheduler.start.assert_awaited_once()
    finally:
        release.set()
        await asyncio.gather(first, second, return_exceptions=True)
        await startup.stop()


async def test_scheduler_failure_is_reported_without_restarting_it(monkeypatch):
    fast_startup(monkeypatch)
    scheduler = SimpleNamespace(start=AsyncMock(side_effect=OSError('partial start failed')))
    startup = cron_startup.CronStartup(object(), scheduler, object(), object(), migrate_file=False)
    try:
        for _ in range(2):
            with pytest.raises(LifecycleError, match='startup failed'):
                await asyncio.wait_for(startup.wait_ready(), .5)
        scheduler.start.assert_awaited_once()
    finally:
        await startup.stop()


async def test_closed_startup_returns_business_error_to_controller(monkeypatch):
    fast_startup(monkeypatch)
    store = SimpleNamespace(get_job=AsyncMock())
    scheduler = SimpleNamespace(start=AsyncMock(), trigger_run_now_info=AsyncMock())
    controller = CronController(store=store, scheduler=scheduler)
    startup = cron_startup.CronStartup(store, scheduler, controller, object(), migrate_file=False)
    controller.bind_startup_waiter(startup.wait_ready)
    await startup.stop()
    with pytest.raises(LifecycleError) as error:
        await controller.run_now_info('job')
    assert error.value.code == 'SERVICE_UNAVAILABLE'
    scheduler.trigger_run_now_info.assert_not_awaited()


class SchedulerForStartupTest(CronSchedulerService):
    @property
    def loop_task(self):
        return self._task

    @property
    def watch_task(self):
        return self._watch_task


async def test_partial_scheduler_start_cleans_created_loop(tmp_path):
    class Store:
        path = tmp_path/'jobs.json'
        captured = None

        async def list_jobs(self):
            return []

        @property
        def supports_watch(self):
            self.captured = scheduler.loop_task
            raise OSError('watch setup failed')

    store = Store()
    scheduler = SchedulerForStartupTest(store=store, agent_client=object(), message_handler=object())
    try:
        with pytest.raises(OSError, match='watch setup failed'):
            await scheduler.start()
        assert not scheduler.is_running()
        assert scheduler.loop_task is None and scheduler.watch_task is None
        assert store.captured is not None and store.captured.done()
    finally:
        await scheduler.stop()


@pytest.mark.parametrize('method', [
    'openai_account.auth.status', 'openai_account.auth.start_login',
    'openai_account.auth.pending_login', 'openai_account.auth.poll_login',
    'openai_account.auth.logout', 'openai_account.models.list',
])
@pytest.mark.parametrize('failure_type', [ImportError, OSError, RuntimeError])
async def test_account_sdk_failure_responds_without_sdk_error_types(monkeypatch, method, failure_type):
    from agent_gateway.common import openjiuwen_logging

    monkeypatch.setattr(openjiuwen_logging, 'ensure_openjiuwen_core_logging', lambda: None)
    for name in app_web_handlers._OPENAI_ACCOUNT_SDK_NAMES:
        monkeypatch.delitem(vars(app_web_handlers), name, raising=False)
    original = builtins.__import__

    def reject(name, *args, **kwargs):
        if name.startswith('openjiuwen.extensions.external_provider.openai_auth'):
            raise failure_type('injected SDK initialization failure')
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, '__import__', reject)
    channel = _bind()
    await channel.methods[method](object(), 'sdk-unavailable', {'login_id':'test'}, 'session')
    assert len(channel.responses) == 1
    assert channel.responses[0]['ok'] is False
    assert channel.responses[0]['code'] == 'SERVICE_UNAVAILABLE'


@pytest.mark.parametrize('method,params', [
    ('cron.job.create', {}), ('cron.job.update', {'id':'job', 'patch':{}}),
    ('cron.job.delete', {'id':'job'}), ('cron.job.toggle', {'id':'job','enabled':True}),
    ('cron.job.run_now', {'id':'job'}),
])
async def test_cron_readiness_error_is_a_response_not_a_hanging_request(method, params):
    unavailable = LifecycleError('SERVICE_UNAVAILABLE', 'Cron is still initializing')
    controller = SimpleNamespace(get_job=AsyncMock(return_value={'id':'job','user_id':'','group_id':'','bot_id':''}),
                                 create_job=AsyncMock(side_effect=unavailable), update_job=AsyncMock(side_effect=unavailable),
                                 delete_job=AsyncMock(side_effect=unavailable), toggle_job=AsyncMock(side_effect=unavailable),
                                 run_now_info=AsyncMock(side_effect=unavailable))
    channel = _bind(cron_controller=controller)
    await channel.methods[method](SimpleNamespace(), 'cron-unavailable', params, None)
    assert len(channel.responses)==1
    assert channel.responses[0]['code']=='SERVICE_UNAVAILABLE'
