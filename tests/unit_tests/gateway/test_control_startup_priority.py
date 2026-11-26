# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Control readiness must not wait for optional SDK or legacy Cron resources."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


def test_web_control_imports_without_sdk(tmp_path: Path) -> None:
    env = dict(os.environ, JIUWENSWARM_DATA_DIR=str(tmp_path / "data"))
    result = subprocess.run([sys.executable, "-c", """
import importlib.abc, importlib.machinery, sys
class RejectLoader(importlib.abc.Loader):
    def create_module(self, spec):
        return None
    def exec_module(self, module):
        raise AssertionError('Control startup executed SDK: ' + module.__name__)
class RejectSDK(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'openjiuwen' or fullname.startswith('openjiuwen.'):
            spec = importlib.machinery.PathFinder.find_spec(fullname, path)
            if spec is not None:
                spec.loader = RejectLoader()
            return spec
sys.meta_path.insert(0, RejectSDK())
from agent_gateway.application.preparation import preload_web_modules
from agent_gateway.extensions.registry import ExtensionRegistry
from agent_gateway.startup.bootstrap import configure_process_logging
configure_process_logging(lambda stage: None)
preload_web_modules()
from agent_gateway import app_gateway
"""], env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr


async def test_callback_single_flight_keeps_owner_loop_and_registration_order() -> None:
    from agent_gateway.extensions.registry import ExtensionRegistry

    loop_thread = threading.get_ident()
    entered, release = threading.Event(), threading.Event()
    imports, constructions, events = [], [], []

    def preload():
        imports.append(threading.get_ident())
        entered.set()
        assert release.wait(10)

    class Framework:
        def register_sync(self, event, handler, **kwargs):
            assert threading.get_ident() == loop_thread
            events.append(("register", handler))

        def unregister_sync(self, event, handler):
            events.append(("unregister", handler))

        async def trigger(self, event, *args, **kwargs):
            events.append(("trigger", event))

    framework = Framework()

    def factory():
        constructions.append(threading.get_ident())
        return framework

    registry = ExtensionRegistry(None, {}, Mock(), factory, preload)
    first, second = lambda: None, lambda: None
    registry.register("event", first)
    registry.unregister("event", first)
    registry.register("event", second)
    cancelled = asyncio.create_task(registry.prepare_callback_framework())
    other = asyncio.create_task(registry.trigger("event"))
    while not entered.is_set():
        await asyncio.sleep(0.001)
    cancelled.cancel()
    await asyncio.gather(cancelled, return_exceptions=True)
    # The loop remains responsive while the synchronous import is blocked.
    assert not other.done()
    registry.unregister("event", second)
    release.set()
    await other
    assert imports and imports[0] != loop_thread and len(imports) == 1
    assert constructions == [loop_thread]
    assert events == [("register", first), ("unregister", first),
                      ("register", second), ("unregister", second), ("trigger", "event")]
    await registry.aclose()
    with pytest.raises(RuntimeError, match="closed"):
        await registry.trigger("event")


async def test_registry_close_does_not_publish_after_cancelled_import() -> None:
    from agent_gateway.extensions.registry import ExtensionRegistry

    entered, release = threading.Event(), threading.Event()
    factory = Mock()

    def preload():
        entered.set()
        assert release.wait(10)

    registry = ExtensionRegistry(None, {}, Mock(), factory, preload)
    preparation = asyncio.create_task(registry.prepare_callback_framework())
    while not entered.is_set():
        await asyncio.sleep(0.001)
    closing = asyncio.create_task(registry.aclose())
    await asyncio.sleep(0)
    release.set()
    await closing
    with pytest.raises(RuntimeError, match="closed"):
        await preparation
    factory.assert_not_called()


async def test_empty_cron_does_not_query_worker(monkeypatch) -> None:
    from agent_gateway.application.cron_startup import CronStartup
    from agent_gateway.common import remote_model_catalog

    fetch = AsyncMock()
    monkeypatch.setattr(remote_model_catalog, "fetch_agent_model_rows", fetch)
    scheduler = SimpleNamespace(start=AsyncMock())
    startup = CronStartup(SimpleNamespace(list_jobs=AsyncMock(return_value=[])), scheduler,
                          SimpleNamespace(mutation_lock=asyncio.Lock()), object(), migrate_file=True)
    await startup.wait_ready()
    fetch.assert_not_awaited()
    scheduler.start.assert_awaited_once()
    await startup.stop()


async def test_legacy_cron_migrates_before_dispatch_without_locking_catalog_request(monkeypatch) -> None:
    from agent_gateway.application.cron_startup import CronStartup
    from agent_gateway.common import remote_model_catalog, model_migration

    release = asyncio.Event()
    controller = SimpleNamespace(mutation_lock=asyncio.Lock())
    scheduler = SimpleNamespace(start=AsyncMock())
    job = SimpleNamespace(model_selection=None, model_name="legacy")
    store = SimpleNamespace(list_jobs=AsyncMock(return_value=[job]))

    async def fetch(**kwargs):
        assert not controller.mutation_lock.locked()
        await release.wait()
        return []

    async def migrate(*args, **kwargs):
        assert controller.mutation_lock.locked()
        scheduler.start.assert_not_awaited()

    monkeypatch.setattr(remote_model_catalog, "fetch_agent_model_rows", fetch)
    migration = AsyncMock(side_effect=migrate)
    monkeypatch.setattr(model_migration, "migrate_legacy_cron_selections", migration)
    startup = CronStartup(store, scheduler, controller, object(), migrate_file=True)
    startup.start()
    await asyncio.sleep(0.01)
    scheduler.start.assert_not_awaited()
    # Unrelated Web control work can still be scheduled.
    await asyncio.wait_for(asyncio.sleep(0), timeout=0.1)
    release.set()
    await startup.wait_ready()
    assert migration.await_args.kwargs["remote_validated"] is True
    scheduler.start.assert_awaited_once()
    await startup.stop()


async def test_legacy_cron_shutdown_never_starts_unmigrated_jobs(monkeypatch) -> None:
    from agent_gateway.application.cron_startup import CronStartup
    from agent_gateway.common import remote_model_catalog

    started = asyncio.Event()

    async def fetch(**kwargs):
        started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(remote_model_catalog, "fetch_agent_model_rows", fetch)
    job = SimpleNamespace(model_selection=None, model_name="legacy")
    scheduler = SimpleNamespace(start=AsyncMock())
    startup = CronStartup(SimpleNamespace(list_jobs=AsyncMock(return_value=[job])), scheduler,
                          SimpleNamespace(mutation_lock=asyncio.Lock()), object(), migrate_file=True)
    startup.start()
    await started.wait()
    await startup.stop()
    scheduler.start.assert_not_awaited()
