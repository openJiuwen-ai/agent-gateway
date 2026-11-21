# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Exercise Gateway Web proxy startup and listener reconciliation."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.app_gateway import _apply_web_proxy_config
from agent_gateway.channel_manager.protocol.web_proxy import web_proxy_listen
from agent_gateway.common import config
from agent_gateway.extensions.agentos.registry_watcher import service
from agent_gateway.runtime.cron import factory


@pytest.mark.asyncio
@pytest.mark.parametrize('router,enabled,resolver,starts', [
    ('adx_router', True, True, False),
    ('agentos_router', True, True, True),
    ('websocket', True, True, False),
    ('agentos_router', False, True, False),
    ('agentos_router', True, False, False),
])
async def test_web_proxy_starts_registry_driven_listeners(monkeypatch, router, enabled, resolver, starts):
    managers = []
    watchers = []
    previous_watcher = SimpleNamespace(stop=AsyncMock())
    previous_manager = SimpleNamespace(close_all=AsyncMock())
    channel = SimpleNamespace(web_registry_watcher=previous_watcher, web_port_manager=previous_manager)
    extension = SimpleNamespace(resolve_web_endpoint=AsyncMock() if resolver else None,
                                release_web_endpoint=AsyncMock(), acquire_web_endpoint=AsyncMock())

    class Manager:
        def __init__(self, web_channel, config):
            assert web_channel is channel
            self.started = False
            self.desired = {}
            managers.append(self)

        def start(self):
            self.started = True

        async def reconcile(self, desired):
            self.desired = desired

    class Watcher:
        def __init__(self, *, etcd_endpoints, on_desired):
            assert etcd_endpoints == ['http://etcd.test:2379']
            self.on_desired = on_desired
            self.enabled = True
            self.started = False
            watchers.append(self)

        async def start(self):
            self.started = True
            await self.on_desired({'registered-agent': 19107})

    monkeypatch.setattr(service, 'RegistryWatcherService', Watcher)
    monkeypatch.setattr(factory, 'load_cron_store_settings',
                        lambda config: SimpleNamespace(endpoints=['http://etcd.test:2379']))
    monkeypatch.setattr(web_proxy_listen, 'WebPortManager', Manager)
    monkeypatch.setattr(config, 'get_config', lambda: {'gateway': {'agent_client': {'type': router}}})
    await _apply_web_proxy_config(channel, extension, {'enabled': enabled})
    previous_watcher.stop.assert_awaited_once()
    previous_manager.close_all.assert_awaited_once()
    assert channel.web_proxy_enabled is starts
    assert len(managers) == len(watchers) == int(starts)
    if starts:
        assert managers[0].started and watchers[0].started
        assert managers[0].desired == {'registered-agent': 19107}
        assert channel.web_registry_watcher is watchers[0]
        await watchers[0].on_desired({})
        assert managers[0].desired == {}
