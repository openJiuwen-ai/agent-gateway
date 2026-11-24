# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Behavior contracts for the single-process optional-channel owner."""

from __future__ import annotations

import asyncio
import importlib
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from agent_gateway.channel_manager.channel_manager import ChannelManager


def _owner(manager=None, homepage=None, initial=None):
    from agent_gateway.application.optional_channels import OptionalChannels

    manager = manager if manager is not None else ChannelManager(SimpleNamespace())
    if not hasattr(manager, 'pop_channels_by_id'):
        manager.pop_channels_by_id = Mock(return_value=[])
    owner = OptionalChannels(
        channel_manager=manager,
        message_handler=SimpleNamespace(handle_persistent_welink_event=AsyncMock()),
        im_inbound=object(),
        im_outbound=object(),
        agent_server_ext=SimpleNamespace(set_key_issuer=Mock()),
        web_channel=SimpleNamespace(),
        initial_channels_conf=initial or {},
        homepage_request_dispatched=homepage or asyncio.Event(),
        schedule_agent_prewarm_sync=Mock(),
        log_stage=Mock(),
    )
    owner.install()
    return owner, manager


async def _close(owner):
    await owner.stop_configuration()
    await owner.stop_a2a()
    await owner.stop_channels()


def _telegram(monkeypatch):
    module = importlib.import_module('agent_gateway.channel_manager.im_platforms.telegram.telegram_connect')
    instances = []

    class Channel:
        channel_id = 'telegram'

        def __init__(self, config, bus, *, im_platform_adapter=None):
            self.config = config
            self.started = asyncio.Event()
            self.cancelled = asyncio.Event()
            self.stop = AsyncMock()
            self.on_message = Mock()
            instances.append(self)

        async def start(self):
            self.started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled.set()

    monkeypatch.setattr(module, 'TelegramChannel', Channel)
    return instances


def test_import_keeps_optional_sdks_and_entrypoint_unloaded(tmp_path):
    env = os.environ.copy()
    for key in ('JIUWENSWARM_HOME', 'JIUWENSWARM_DATA_DIR', 'AGENTOS_GATEWAY_LOG_DIR'):
        env[key] = str(tmp_path / key)
    result = subprocess.run(
        [sys.executable, '-c', '''
import importlib, os, sys
from pathlib import Path
importlib.import_module('agent_gateway.application.optional_channels')
for module in ('agent_gateway.app_gateway', 'telegram', 'discord', 'slack_bolt', 'asyncssh', 'lark_oapi', 'dingtalk_stream'):
    assert module not in sys.modules, module
for key in ('JIUWENSWARM_HOME', 'JIUWENSWARM_DATA_DIR', 'AGENTOS_GATEWAY_LOG_DIR'):
    assert not Path(os.environ[key]).exists(), key
'''], env=env, capture_output=True, text=True, timeout=25,
    )
    assert result.returncode == 0, result.stderr


async def test_configuration_enable_unchanged_update_disable_owns_one_instance(monkeypatch):
    instances = _telegram(monkeypatch)
    owner, manager = _owner()
    try:
        first = {'enabled': True, 'bot_token': 'test-token-one'}
        await manager.set_conf('telegram', first)
        await instances[0].started.wait()
        await manager.set_conf('telegram', first)
        assert len(instances) == 1
        await manager.set_conf('telegram', {'enabled': True, 'bot_token': 'test-token-two'})
        await instances[1].started.wait()
        assert len(instances) == 2
        instances[0].stop.assert_awaited_once()
        assert instances[0].cancelled.is_set()
        await manager.set_conf('telegram', {'enabled': False})
        instances[1].stop.assert_awaited_once()
        assert instances[1].cancelled.is_set()
        assert manager.get_channel('telegram') is None
    finally:
        await _close(owner)


async def test_initial_configuration_waits_for_homepage_and_preserves_delay(monkeypatch):
    from agent_gateway.application import optional_channels as module

    gate = asyncio.Event()
    manager = SimpleNamespace(set_config_callback=Mock(), set_conf=AsyncMock())
    owner, _ = _owner(manager, gate, {'telegram': {'enabled': False}})
    original_sleep = asyncio.sleep
    delays = []

    async def sleep(delay):
        delays.append(delay)
        await original_sleep(0)

    monkeypatch.setattr(module, 'asyncio', SimpleNamespace(
        create_task=asyncio.create_task, wait_for=asyncio.wait_for,
        sleep=sleep, TimeoutError=asyncio.TimeoutError,
    ))
    task = owner.schedule_initial_configuration()
    assert owner.schedule_initial_configuration() is task
    await original_sleep(0)
    manager.set_conf.assert_not_awaited()
    gate.set()
    await task
    manager.set_conf.assert_awaited_once_with('telegram', {'enabled': False})
    assert delays == [0.75]
    owner._log_stage.assert_any_call('channel_configuration_applied')
    # Only the lightweight fake was patched; restore the real module for cleanup.
    monkeypatch.setattr(module, 'asyncio', asyncio)
    await _close(owner)


async def test_initial_configuration_headless_timeout_still_applies(monkeypatch):
    from agent_gateway.application import optional_channels as module

    manager = SimpleNamespace(set_config_callback=Mock(), set_conf=AsyncMock())
    owner, _ = _owner(manager, initial={'telegram': {'enabled': False}})
    timeouts = []

    async def timeout(awaitable, timeout):
        timeouts.append(timeout)
        awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(module, 'asyncio', SimpleNamespace(
        create_task=asyncio.create_task, wait_for=timeout,
        sleep=AsyncMock(), TimeoutError=asyncio.TimeoutError,
    ))
    await owner.schedule_initial_configuration()
    assert timeouts == [5.0]
    manager.set_conf.assert_awaited_once()
    monkeypatch.setattr(module, 'asyncio', asyncio)
    await _close(owner)


async def test_initial_failure_resets_snapshot_and_does_not_skip_other_channels(monkeypatch):
    from agent_gateway.application import optional_channels as module

    calls = []

    async def set_conf(name, conf):
        calls.append((name, conf))
        if name == 'broken' and conf:
            raise RuntimeError('SDK unavailable')

    manager = SimpleNamespace(set_config_callback=Mock(), set_conf=set_conf, get_conf_revision=lambda name: 2)
    gate = asyncio.Event()
    gate.set()
    owner, _ = _owner(manager, gate, {'broken': {'enabled': True}, 'healthy': {'enabled': False}})
    retries = Mock()
    monkeypatch.setattr(owner, '_schedule_channel_config_retry', retries)
    real_sleep = asyncio.sleep

    async def sleep(delay):
        await real_sleep(0)

    monkeypatch.setattr(module, 'asyncio', SimpleNamespace(
        create_task=asyncio.create_task, wait_for=asyncio.wait_for,
        sleep=sleep, TimeoutError=asyncio.TimeoutError,
    ))
    await owner.schedule_initial_configuration()
    assert calls == [('broken', {'enabled': True}), ('broken', {}), ('healthy', {'enabled': False})]
    retries.assert_called_once_with('broken', {'enabled': True}, expected_revision=2)
    monkeypatch.setattr(module, 'asyncio', asyncio)
    await _close(owner)


async def test_old_retry_cannot_resurrect_a_disabled_channel(monkeypatch):
    from agent_gateway.application import optional_channels as module

    instances = _telegram(monkeypatch)
    owner, manager = _owner()
    sleeping = asyncio.Event()
    resume = asyncio.Event()

    async def sleep(delay):
        assert delay == 2.0
        sleeping.set()
        await resume.wait()

    monkeypatch.setattr(module, 'asyncio', SimpleNamespace(create_task=asyncio.create_task, sleep=sleep))
    owner._schedule_channel_config_retry('telegram', {'enabled': True, 'bot_token': 'test-token'}, expected_revision=0)
    await sleeping.wait()
    await manager.set_conf('telegram', {'enabled': False})
    resume.set()
    await asyncio.gather(*tuple(owner._channel_retry_tasks))
    assert instances == []
    assert manager.get_conf('telegram') == {'enabled': False}
    monkeypatch.setattr(module, 'asyncio', asyncio)
    await _close(owner)


async def test_retry_failure_resets_then_backs_off_until_success(monkeypatch):
    from agent_gateway.application import optional_channels as module

    manager = SimpleNamespace(set_config_callback=Mock(), get_conf_revision=lambda name: 2)
    calls = []

    async def set_conf(name, conf):
        calls.append((name, conf))
        if len(calls) == 1:
            raise RuntimeError('transient SDK error')

    manager.set_conf = set_conf
    owner, _ = _owner(manager)
    delays = []
    real_sleep = asyncio.sleep

    async def sleep(delay):
        delays.append(delay)
        await real_sleep(0)

    monkeypatch.setattr(module, 'asyncio', SimpleNamespace(create_task=asyncio.create_task, sleep=sleep))
    owner._schedule_channel_config_retry('example', {'enabled': True}, expected_revision=2)
    await asyncio.gather(*tuple(owner._channel_retry_tasks))
    assert calls == [('example', {'enabled': True}), ('example', {}), ('example', {'enabled': True})]
    assert delays == [2.0, 4.0]
    monkeypatch.setattr(module, 'asyncio', asyncio)
    await _close(owner)


async def test_shutdown_cancels_configuration_and_joins_background_stop(monkeypatch):
    owner, manager = _owner()
    task = owner.schedule_initial_configuration()
    owner._schedule_channel_config_retry('example', {'enabled': True}, expected_revision=0)
    started = asyncio.Event()
    finished = asyncio.Event()

    async def run():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finished.set()

    channel_task = asyncio.create_task(run())
    await started.wait()
    channel = SimpleNamespace(channel_id='temporary', stop=AsyncMock())
    await owner._stop_channel(channel, channel_task, 'temporary', background_wait=True)
    await owner.stop_configuration()
    await owner.stop_a2a()
    await owner.stop_channels()
    assert task.done()
    assert not owner._channel_retry_tasks
    assert not owner._background_stop_tasks
    assert finished.is_set()
    channel.stop.assert_awaited_once()


async def test_shutdown_stops_owned_channel_once_and_preserves_web_resources(monkeypatch):
    instances = _telegram(monkeypatch)
    owner, manager = _owner()
    watcher = SimpleNamespace(stop=AsyncMock())
    ports = SimpleNamespace(close_all=AsyncMock())
    owner._web_channel.web_registry_watcher = watcher
    owner._web_channel.web_port_manager = ports
    await manager.set_conf('telegram', {'enabled': True, 'bot_token': 'test-token'})
    await instances[0].started.wait()
    await _close(owner)
    await _close(owner)
    instances[0].stop.assert_awaited_once()
    assert instances[0].cancelled.is_set()
    watcher.stop.assert_not_awaited()
    ports.close_all.assert_not_awaited()


async def test_a2a_is_lazy_single_owned_and_stopped_before_other_channels(monkeypatch):
    from agent_gateway.application.optional_channels import OptionalChannels
    module = importlib.import_module('agent_gateway.channel_manager.protocol.a2a.a2a_connect')
    instances = []

    class Channel:
        channel_id = 'a2a'

        def __init__(self, config, bus, *, im_platform_adapter=None):
            self.config = config
            self.started = asyncio.Event()
            self.stop = AsyncMock()
            self.on_message = Mock()
            instances.append(self)

        async def start(self):
            self.started.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(module, 'A2AChannel', Channel)
    monkeypatch.delenv('A2A_SERVER_ENABLED', raising=False)
    owner, manager = _owner()
    owner.start_a2a()
    assert not instances
    monkeypatch.setenv('A2A_SERVER_ENABLED', 'true')
    monkeypatch.setenv('A2A_SERVER_PORT', '0')
    owner.start_a2a()
    owner.start_a2a()
    await instances[0].started.wait()
    assert len(instances) == 1
    assert instances[0].config.port == 0
    await owner.stop_configuration()
    await owner.stop_a2a()
    instances[0].stop.assert_awaited_once()
    assert manager.get_channel('a2a') is None
    await owner.stop_channels()


@pytest.mark.parametrize('platform,class_name,config', [
    ('discord', 'DiscordChannel', {'enabled': True, 'bot_token': 'test-token'}),
    ('slack', 'SlackChannel', {'enabled': True, 'bot_token': 'test-token', 'app_token': 'test-app-token'}),
    ('whatsapp', 'WhatsAppChannel', {'enabled': True, 'bridge_url': 'ws://127.0.0.1:1'}),
    ('wecom', 'WecomChannel', {'enabled': True, 'bot_id': 'test-bot', 'secret': 'test-secret'}),
    ('wechat', 'WechatChannel', {'enabled': True}),
])
async def test_other_single_channels_keep_one_task_and_disable(monkeypatch, platform, class_name, config):
    module = importlib.import_module(f'agent_gateway.channel_manager.im_platforms.{platform}.{platform}_connect')
    instances = []

    class Channel:
        channel_id = platform

        def __init__(self, config, bus, *, im_platform_adapter=None):
            self.config = config
            self.on_message = Mock()
            self.stop = AsyncMock()
            self.started = asyncio.Event()
            instances.append(self)

        async def start(self):
            self.started.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(module, class_name, Channel)
    owner, manager = _owner()
    try:
        await manager.set_conf(platform, config)
        assert len(instances) == 1
        await instances[0].started.wait()
        await manager.set_conf(platform, config)
        assert len(instances) == 1
        await manager.set_conf(platform, {'enabled': False})
        instances[0].stop.assert_awaited_once()
        assert manager.get_channel(platform) is None
    finally:
        await _close(owner)


async def test_ssh_key_issuer_injection_and_removal_are_preserved(monkeypatch):
    module = importlib.import_module('agent_gateway.channel_manager.protocol.ssh.ssh_connect')
    from agent_gateway.common import config as config_module

    monkeypatch.setattr(config_module, 'get_config', lambda: {'gateway': {'agent_client': {'type': 'agentos_router'}}})
    instances = []

    class Channel:
        channel_id = 'ssh'

        def __init__(self, config, bus, *, key_registry):
            self.config = config
            self.key_registry = key_registry
            self.key_issuer = object()
            self.on_message = Mock()
            self.stop = AsyncMock()
            self.started = asyncio.Event()
            instances.append(self)

        async def start(self):
            self.started.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(module, 'SshChannel', Channel)
    owner, manager = _owner()
    try:
        await manager.set_conf('ssh', {'enabled': True, 'listen_port': 2222, 'auth': {'enabled': True}})
        await instances[0].started.wait()
        owner._agent_server_ext.set_key_issuer.assert_called_with(instances[0].key_issuer, ephemeral_key_ttl_sec=300.0)
        await manager.set_conf('ssh', {'enabled': False})
        instances[0].stop.assert_awaited_once()
        owner._agent_server_ext.set_key_issuer.assert_called_with(None, ephemeral_key_ttl_sec=300.0)
        assert manager.get_channel('ssh') is None
    finally:
        await _close(owner)


async def test_dingtalk_multi_app_uses_existing_lifecycle_and_cleans_every_task(monkeypatch):
    module = importlib.import_module('agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect')
    instances = []

    class Channel:
        channel_id = 'dingtalk'

        def __init__(self, config, bus, *, im_platform_adapter=None):
            self.config = config
            self.app_id = config.client_id
            self.on_message = Mock()
            self.stop = AsyncMock()
            self.started = asyncio.Event()
            instances.append(self)

        async def start(self):
            self.started.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(module, 'DingTalkChannel', Channel)
    owner, manager = _owner()
    try:
        conf = {'apps': [
            {'enabled': True, 'client_id': 'ordinary', 'client_secret': 'test-secret'},
            {'enabled': True, 'client_id': 'persistent', 'client_secret': 'test-secret', 'persistent_agent_id': 'agent-1'},
        ]}
        await manager.set_conf('dingtalk', conf)
        await asyncio.gather(*(channel.started.wait() for channel in instances))
        assert len(instances) == 2
        await manager.set_conf('dingtalk', conf)
        assert len(instances) == 2
        assert len(manager.get_channels_by_id('dingtalk')) == 2
        await _close(owner)
        assert not manager.get_channels_by_id('dingtalk')
        for channel in instances:
            channel.stop.assert_awaited_once()
            assert channel.start_task.done()
    finally:
        await _close(owner)


@pytest.mark.parametrize('platform', ['feishu', 'xiaoyi'])
async def test_multi_app_channels_keep_registration_and_cleanup(monkeypatch, platform):
    class_name = 'FeishuChannel' if platform == 'feishu' else 'XiaoyiChannel'
    module = importlib.import_module(f'agent_gateway.channel_manager.im_platforms.{platform}.{platform}_connect')
    instances = []

    class Channel:
        channel_id = platform

        def __init__(self, config, bus, *, im_platform_adapter=None):
            self.config = config
            self.app_id = config.app_id if platform == 'feishu' else config.api_id
            self.on_message = Mock()
            self.stop = AsyncMock()
            self.started = asyncio.Event()
            instances.append(self)

        async def start(self):
            self.started.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(module, class_name, Channel)
    owner, manager = _owner()
    owner._im_inbound = SimpleNamespace(unregister_adapter=Mock())
    owner._im_outbound = SimpleNamespace(unregister_adapter=Mock())
    try:
        apps = [
            {'enabled': True, 'app_id': name, 'app_secret': 'test-secret', 'ak': 'test-ak', 'sk': 'test-sk', 'agent_id': 'agent-1', 'api_id': name}
            for name in ('first', 'second')
        ]
        await manager.set_conf(platform, {'apps': apps})
        assert len(instances) == 2
        await asyncio.gather(*(channel.started.wait() for channel in instances))
        await _close(owner)
        for channel in instances:
            channel.stop.assert_awaited_once()
            assert channel.start_task.done()
        assert not manager.get_channels_by_id(platform)
    finally:
        await _close(owner)


async def test_welink_failure_keeps_restart_pending_and_reuses_runtime(monkeypatch):
    module = importlib.import_module('agent_gateway.channel_manager.im_platforms.welink.welink_runtime')
    runtime = SimpleNamespace(apply=AsyncMock(side_effect=[RuntimeError('CLI unavailable'), None]), stop=AsyncMock())
    factory = Mock(return_value=runtime)
    monkeypatch.setattr(module, 'WelinkRuntime', factory)
    owner, manager = _owner()
    try:
        conf = {'enabled': False}
        with pytest.raises(RuntimeError, match='CLI unavailable'):
            await manager.set_conf('welink', conf)
        assert manager.get_conf('welink') == {}
        await manager.set_conf('welink', conf)
        factory.assert_called_once()
        assert runtime.apply.await_count == 2
        await _close(owner)
        runtime.stop.assert_awaited_once()
    finally:
        await _close(owner)


async def test_enterprise_feishu_tasks_and_adapters_keep_original_ownership(monkeypatch):
    module = importlib.import_module('agent_gateway.channel_manager.im_platforms.feishu.feishu_connect')
    instances = []

    class Channel:
        def __init__(self, config, bus, *, im_platform_adapter=None):
            self.config = config
            self.app_id = config.app_id
            self.channel_id = config.channel_id
            self.on_message = Mock()
            self.stop = AsyncMock()
            self.started = asyncio.Event()
            instances.append(self)

        async def start(self):
            self.started.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(module, 'FeishuChannel', Channel)
    owner, manager = _owner()
    owner._im_inbound = SimpleNamespace(unregister_adapter=Mock())
    owner._im_outbound = SimpleNamespace(unregister_adapter=Mock())
    try:
        await manager.set_conf('feishu_enterprise', {'bot-1': {'enabled': True, 'app_id': 'app-1', 'app_secret': 'test-secret'}})
        await instances[0].started.wait()
        task = owner._feishu_enterprise_tasks['bot-1']
        assert manager.get_channel('feishu_enterprise:app-1') is instances[0]
        await manager.set_conf('feishu_enterprise', {})
        instances[0].stop.assert_awaited_once()
        assert task.done()
        owner._im_inbound.unregister_adapter.assert_not_called()
        assert manager.get_channel('feishu_enterprise:app-1') is None
    finally:
        await _close(owner)


async def test_single_retains_optional_startup_and_shutdown_stage_boundaries(monkeypatch):
    from agent_gateway.application import single
    from agent_gateway.common import config as config_module
    from agent_gateway.cron import proactive_cron_sync

    events = []
    shutdown = asyncio.Event()
    services = SimpleNamespace(
        credential_access=object(), auth_service_provider=lambda: None, credential_update_sink=object(),
        client=object(), third_agent=object(), agent_server_ext=object(), using_adx_router=True, extension_registry=object(),
        message_handler=object(), im_inbound=object(), im_outbound=object(),
        cron_scheduler=object(), cron_controller=object(), heartbeat_controller=object(),
        heartbeat_service=object(), config_updater=None, channel_manager=object(),
        updater_service=object(), initial_channels_conf={}, full_cfg={},
        stop=AsyncMock(side_effect=lambda: events.append('services.stop')),
    )
    monkeypatch.setattr(single, 'assemble_services', AsyncMock(return_value=services))
    web = SimpleNamespace(stop=AsyncMock(side_effect=lambda: events.append('web.stop')))
    homepage = asyncio.Event()
    monkeypatch.setattr(single, 'assemble_web', Mock(return_value=(web, homepage)))

    async def start_web(*args):
        events.append('web.ready')
        return asyncio.create_task(asyncio.Event().wait())

    monkeypatch.setattr(single, 'start_web_core', start_web)
    runtime = SimpleNamespace(start=AsyncMock(side_effect=lambda: events.append('protocol.ready')), wait_until_closed=asyncio.Event().wait)
    tui = SimpleNamespace(protocol_runtime=runtime, stop=AsyncMock(side_effect=lambda: events.append('tui.stop')))
    monkeypatch.setattr(single, 'assemble_tui_acp', AsyncMock(return_value=tui))
    monkeypatch.setattr(config_module, 'get_config', lambda: {})
    monkeypatch.setattr(proactive_cron_sync, 'sync_proactive_tick_job', AsyncMock())
    monkeypatch.setattr(single, 'shutdown_gateway_telemetry', AsyncMock(side_effect=lambda: events.append('telemetry.stop')))

    class Channels:
        def __init__(self, *, channel_manager, message_handler, im_inbound, im_outbound,
                     agent_server_ext, web_channel, initial_channels_conf,
                     homepage_request_dispatched, schedule_agent_prewarm_sync, log_stage):
            assert channel_manager is services.channel_manager
            assert web_channel is web
            assert homepage_request_dispatched is homepage
            events.append('optional.create')

        def start_a2a(self):
            events.append('optional.a2a')

        def install(self):
            events.append('optional.install')

        def schedule_initial_configuration(self):
            events.append('optional.schedule')
            return asyncio.create_task(asyncio.sleep(0))

        async def stop_configuration(self):
            events.append('optional.config.stop')

        async def stop_a2a(self):
            events.append('optional.a2a.stop')

        async def stop_channels(self):
            events.append('optional.channels.stop')

    monkeypatch.setattr(single, 'OptionalChannels', Channels)
    shutdown.set()
    await single.run_single(
        'ws://example/ws', '127.0.0.1', 0, '/ws', shutdown,
        config_saved_factory=Mock(), restart_gateway=Mock(), process_start_t0=0.0,
        startup_import_phases=[], preprocessor=SimpleNamespace(with_credential_access=lambda access: object()), connect_with_retry=AsyncMock(),
    )
    assert events == [
        'web.ready', 'optional.create', 'optional.a2a', 'optional.install',
        'protocol.ready', 'optional.schedule', 'optional.config.stop',
        'optional.a2a.stop', 'tui.stop', 'web.stop', 'optional.channels.stop',
        'services.stop', 'telemetry.stop',
    ]


async def test_web_proxy_reconfiguration_closes_old_resources_and_preserves_web_shutdown(monkeypatch):
    from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
    from agent_gateway.channel_manager.web.app_web_handlers import _DummyBus
    from agent_gateway.channel_manager.protocol.web_proxy import web_proxy_listen
    from agent_gateway.extensions.agentos.registry_watcher import service
    from agent_gateway.common import config as config_module

    monkeypatch.setattr(config_module, 'get_config', lambda: {'gateway': {'agent_client': {'type': 'agentos_router'}}})
    manager_factory = Mock(side_effect=lambda channel, config: SimpleNamespace(
        start=Mock(), close_all=AsyncMock(), reconcile=AsyncMock(),
    ))
    watcher_factory = Mock(side_effect=lambda **values: SimpleNamespace(start=AsyncMock(), stop=AsyncMock(), enabled=True))
    monkeypatch.setattr(web_proxy_listen, 'WebPortManager', manager_factory)
    monkeypatch.setattr(service, 'RegistryWatcherService', watcher_factory)
    owner, manager = _owner()
    web = WebChannel(WebChannelConfig(host='127.0.0.1', port=0), _DummyBus())
    owner._web_channel = web
    owner._agent_server_ext.resolve_web_endpoint = AsyncMock(return_value='http://127.0.0.1:1')
    owner._agent_server_ext.release_web_endpoint = AsyncMock()
    try:
        await manager.set_conf('web_proxy', {'enabled': True})
        first_ports, first_watcher = web.web_port_manager, web.web_registry_watcher
        first_ports.start.assert_called_once()
        first_watcher.start.assert_awaited_once()
        await manager.set_conf('web_proxy', {'enabled': True, 'auth_enabled': False})
        first_ports.close_all.assert_awaited_once()
        first_watcher.stop.assert_awaited_once()
        assert manager_factory.call_count == 2
        second_ports, second_watcher = web.web_port_manager, web.web_registry_watcher
        ws_session = SimpleNamespace(closed=False, close=AsyncMock())
        http_session = SimpleNamespace(closed=False, close=AsyncMock())
        web.web_proxy_session = ws_session
        web.web_proxy_http_session = http_session
        await owner.stop_configuration()
        await owner.stop_a2a()
        second_watcher.stop.assert_not_awaited()
        await web.stop()
        await owner.stop_channels()
        second_ports.close_all.assert_awaited_once()
        second_watcher.stop.assert_awaited_once()
        ws_session.close.assert_awaited_once()
        http_session.close.assert_awaited_once()
    finally:
        await _close(owner)


@pytest.mark.parametrize('replacement', [
    {'enabled': False}, {'enabled': True, 'bot_token': 'test-new-token'},
])
async def test_real_revision_edit_invalidates_retry_without_overwriting_user_choice(monkeypatch, replacement):
    from agent_gateway.application import optional_channels as module

    instances = _telegram(monkeypatch)
    owner, manager = _owner()
    sleeping = asyncio.Event()
    resume = asyncio.Event()

    async def sleep(delay):
        sleeping.set()
        await resume.wait()

    monkeypatch.setattr(module, 'asyncio', SimpleNamespace(create_task=asyncio.create_task, sleep=sleep))
    owner._schedule_channel_config_retry('telegram', {'enabled': True, 'bot_token': 'test-old-token'}, expected_revision=0)
    await sleeping.wait()
    await manager.set_conf('telegram', replacement)
    resume.set()
    await asyncio.gather(*tuple(owner._channel_retry_tasks))
    assert manager.get_conf('telegram') == replacement
    assert len(instances) == (1 if replacement['enabled'] else 0)
    monkeypatch.setattr(module, 'asyncio', asyncio)
    await _close(owner)
