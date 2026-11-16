# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Boundaries and lifecycle contracts for the single-process composition root."""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.mark.parametrize('module', ['services', 'web', 'tui_acp', 'single', 'config_saved_effects'])
def test_application_import_does_not_initialize_entrypoint(tmp_path: Path, module: str) -> None:
    env = os.environ.copy()
    for key, suffix in [('JIUWENSWARM_HOME', 'home'), ('JIUWENSWARM_DATA_DIR', 'data'), ('AGENTOS_GATEWAY_LOG_DIR', 'logs')]:
        env[key] = str(tmp_path / suffix)
    result = subprocess.run(
        [sys.executable, '-c', f'''
import importlib, os, sys
from pathlib import Path
importlib.import_module('agent_gateway.application.{module}')
assert 'agent_gateway.app_gateway' not in sys.modules
assert 'agent_gateway.startup.bootstrap' not in sys.modules
assert 'agent_gateway.channel_manager.protocol.acp.acp_connect' not in sys.modules
for key in ('JIUWENSWARM_HOME', 'JIUWENSWARM_DATA_DIR', 'AGENTOS_GATEWAY_LOG_DIR'):
    assert not Path(os.environ[key]).exists(), key
'''], env=env, capture_output=True, text=True, timeout=25,
    )
    assert result.returncode == 0, result.stderr


async def test_legacy_run_delegates_explicit_inputs_without_creating_services(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_gateway import app_gateway
    from agent_gateway.application import single

    run = AsyncMock()
    monkeypatch.setattr(single, 'run_single', run)
    shutdown = asyncio.Event()
    await app_gateway._run('ws://example/ws', '127.0.0.1', 0, '/ws', shutdown, True)
    run.assert_awaited_once()
    inputs = run.await_args.kwargs
    assert inputs['agent_server_url'] == 'ws://example/ws'
    assert inputs['web_port'] == 0
    assert inputs['web_ws_deflate'] is True
    assert inputs['shutdown_event'] is shutdown
    assert inputs['config_saved_factory'] is app_gateway._build_config_saved_callback
    assert inputs['restart_gateway'] is app_gateway._exec_gateway_restart


async def test_single_web_start_retains_original_stage_order() -> None:
    from agent_gateway.application.single import start_web_core

    events = []
    running = asyncio.Event()
    stopped = asyncio.Event()

    async def serve():
        events.append('web.start')
        running.set()
        await stopped.wait()

    async def ready():
        await running.wait()
        events.append('web.ready')

    async def dispatch():
        events.append('dispatch')

    async def cron():
        events.append('cron')

    async def update():
        events.append('updater')

    channel = SimpleNamespace(start=serve, wait_ready=ready, config=SimpleNamespace(host='127.0.0.1', port=0, path='/ws'))
    task = await start_web_core(channel, SimpleNamespace(start_dispatch=dispatch), SimpleNamespace(start=cron), SimpleNamespace(start=update), events.append)
    try:
        assert events == ['web.start', 'web.ready', 'web_channel_listening', 'dispatch', 'message_dispatch_started', 'cron', 'updater', 'cron_scheduler_started', 'web_core_ready']
    finally:
        stopped.set()
        await task


async def test_single_web_start_propagates_bind_failure_without_starting_services() -> None:
    from agent_gateway.application.single import start_web_core

    async def fail():
        raise OSError('bind failed')

    manager = SimpleNamespace(start_dispatch=AsyncMock())
    scheduler = SimpleNamespace(set_credential_access=Mock(), start=AsyncMock())
    channel = SimpleNamespace(start=fail, wait_ready=asyncio.Event().wait)
    with pytest.raises(OSError, match='bind failed'):
        await start_web_core(channel, manager, scheduler, None, Mock())
    manager.start_dispatch.assert_not_awaited()
    scheduler.start.assert_not_awaited()


async def test_tui_assembly_registers_channels_before_client_subscription(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_gateway.application import tui_acp
    from agent_gateway.application.inbound import MessagePreprocessor
    from agent_gateway.channel_manager.base import RobotMessageRouter

    events = []
    channels = {}
    manager = SimpleNamespace(
        register_channel_with_inbound=lambda channel, callback: (channels.update({channel.channel_id: channel}), events.append('tui.register')),
        register_external_channel=lambda channel_id, channel: channels.update({channel_id: channel}),
        deliver_to_message_handler=AsyncMock(),
    )
    def subscribe(value):
        assert value is manager and 'web' in channels and 'tui' in channels
        events.append('client.subscribe')
    channels['web'] = object()
    client = SimpleNamespace(set_channel_manager=subscribe)
    from agent_gateway.channel_manager.tui import tui_connect
    monkeypatch.setattr(tui_connect, 'build_cli_route_binding', lambda params: tui_acp.GatewayRouteBinding(path='/tui', channel_id='tui', ws_channel=params.ws_channel))
    processor = MessagePreprocessor(AsyncMock(), lambda msg: msg, Mock())
    assembly = await tui_acp.assemble_tui_acp(
        agent_client=client, message_handler=object(), channel_manager=manager, third_agent=object(),
        on_config_saved=AsyncMock(), cron_controller=object(), heartbeat_controller=object(),
        preprocessor=processor, log_stage=events.append,
    )
    try:
        assert events[:2] == ['tui.register', 'client.subscribe']
        assert channels['tui'] is assembly.tui_channel
        assert channels['acp'] is assembly.protocol_runtime
        assert assembly.protocol_runtime.message_handler_ref is not None
        assert assembly.protocol_runtime._listener is None
        assert assembly.protocol_runtime.config.routes['/tui'].ws_channel is assembly.tui_channel
    finally:
        await assembly.stop()


async def test_preprocessor_keeps_refresh_normalize_work_mode_and_local_dispatch_order() -> None:
    from agent_gateway.application.inbound import MessagePreprocessor
    from agent_gateway.common.protocol_methods import ReqMethod

    events = []
    msg = SimpleNamespace(req_method=ReqMethod.SESSION_CREATE, id='r', channel_id='web')
    async def refresh(value):
        assert value is msg
        events.append('refresh')
    def normalize(value):
        assert value is msg
        events.append('normalize')
        return msg
    async def deliver(value):
        assert value is msg
        events.append('deliver')
    processor = MessagePreprocessor(refresh, normalize, lambda value: events.append('work_mode'))
    manager = SimpleNamespace(deliver_to_message_handler=deliver)
    handler = processor.for_channel(manager, frozenset({'session.create'}), frozenset(), 'Web')
    assert await handler(msg) is False
    assert events == ['refresh', 'normalize', 'work_mode', 'deliver']
    events.clear()
    assert await processor.for_channel(manager, frozenset(), frozenset(), 'Web')(msg) is False
    assert events == []


@pytest.mark.parametrize('enterprise', [False, True])
@pytest.mark.parametrize('extension_client', [False, True])
async def test_service_assembly_preserves_selection_storage_order_and_single_instances(
    monkeypatch: pytest.MonkeyPatch, enterprise: bool, extension_client: bool,
) -> None:
    from agent_gateway.application import services
    from agent_gateway.common import config, updater, external_providers
    from agent_gateway import cron, enterprise as enterprise_module, health_check, heartbeat, cleanup
    from agent_gateway.extensions import registry, manager
    from agent_gateway.extensions import redis
    from agent_gateway.storage_assembly import setup
    from agent_gateway.routing import agent_client, third_agent, session_metadata_remote, mcp_remote
    from agent_gateway.message_handler import message_handler
    from agent_gateway.channel_manager import channel_manager
    from agent_gateway.channel_manager.persistent_output import resolve_output_file
    from agent_gateway.im_pipeline import im_inbound, im_outbound
    from agent_gateway.common import model_migration
    from agent_gateway.enterprise.team import abandoned_hold

    events = []
    snapshots = {'channels': {'telegram': {'enabled': False}}, 'health_check': {'every': 60}}
    monkeypatch.setattr(config, 'get_config', lambda: snapshots)
    monkeypatch.setattr(enterprise_module, 'is_enterprise', lambda: enterprise)
    monkeypatch.setattr(services, 'configure_gateway_telemetry', lambda *args, **kwargs: events.append('telemetry'))
    storage = object()
    async def initialize_redis(value):
        events.append('redis')
    async def start_storage(value):
        events.append('storage')
        return storage
    monkeypatch.setattr(redis, 'init_gateway_redis_from_config', initialize_redis)
    monkeypatch.setattr(setup, 'start_gateway_storage', start_storage)

    class Client:
        def __init__(self, *, ping_interval=20.0, ping_timeout=600.0):
            events.append('client.created')
            self.connect = AsyncMock(side_effect=lambda uri: events.append('client.connected'))
            self.set_or_update_server_config = Mock()
    monkeypatch.setattr(agent_client, 'WebSocketAgentServerClient', Client)
    selected = SimpleNamespace(connect=AsyncMock(side_effect=lambda uri: events.append('extension.connected')), set_or_update_server_config=Mock())
    extension = SimpleNamespace(metadata=SimpleNamespace(id='external', name='external'), get_client=lambda: selected) if extension_client else None
    third = object()
    monkeypatch.setattr(third_agent, 'get_unsupported_third_agent', lambda: third)
    extensions = SimpleNamespace(get_agent_server_client_extension=lambda: extension, get_third_agent_extension=lambda: None)
    monkeypatch.setattr(registry.ExtensionRegistry, 'create_instance', Mock(return_value=extensions))
    extension_manager = SimpleNamespace(load_all_extensions=AsyncMock(side_effect=lambda: events.append('extensions')), list_extensions=lambda: [])
    monkeypatch.setattr(manager, 'ExtensionManager', Mock(return_value=extension_manager))

    handler = SimpleNamespace(set_session_login_bindings=Mock(side_effect=lambda value: events.append('bindings.bound')), set_credential_access=Mock(side_effect=lambda value: events.append('credentials.bound')), start_forwarding=AsyncMock(side_effect=lambda: events.append('forwarding')), set_inbound_pipeline=Mock(), set_outbound_pipeline=Mock(), set_cron_controller=Mock(), update_evolution_auto_save=Mock(), set_channel_manager=Mock())
    def create_handler(client):
        events.append('handler.created')
        if enterprise:
            assert events.index('storage') < events.index('handler.created')
        return handler
    handler_factory = Mock(side_effect=create_handler)
    monkeypatch.setattr(message_handler, 'MessageHandler', handler_factory)
    inbound, outbound = object(), object()
    monkeypatch.setattr(im_inbound, 'IMInboundPipeline', Mock(return_value=inbound))
    monkeypatch.setattr(im_outbound, 'IMOutboundPipeline', Mock(return_value=outbound))
    store = object()
    monkeypatch.setattr(cron, 'create_gateway_cron_store', AsyncMock(return_value=store))
    monkeypatch.setattr(model_migration, 'migrate_legacy_cron_selections', AsyncMock())
    scheduler = SimpleNamespace(set_credential_access=Mock(), start=AsyncMock())
    scheduler_factory = Mock(return_value=scheduler)
    monkeypatch.setattr(cron, 'CronSchedulerService', scheduler_factory)
    controller = SimpleNamespace(set_credential_access=Mock(), set_on_channels_changed=Mock(), bind_agent_client=Mock())
    monkeypatch.setattr(cron.CronController, 'get_instance', Mock(return_value=controller))
    monkeypatch.setattr(external_providers, 'register', Mock())
    session_bind = Mock()
    mcp_bind = Mock()
    monkeypatch.setattr(session_metadata_remote, 'bind_agent_client', session_bind)
    monkeypatch.setattr(mcp_remote, 'bind_agent_client', mcp_bind)
    monkeypatch.setattr(heartbeat, 'HeartbeatControllerProxy', Mock(return_value=object()))
    probe = SimpleNamespace(start=AsyncMock(side_effect=lambda: events.append('health')))
    monkeypatch.setattr(health_check, 'GatewayHealthCheckService', Mock(return_value=probe))
    async def background():
        await asyncio.Event().wait()
    monkeypatch.setattr(abandoned_hold, 'run_abandoned_hold_release_loop', background)
    monkeypatch.setattr(cleanup, 'start_background_cleanup', lambda: asyncio.create_task(background()))
    cm = SimpleNamespace(set_persistent_output_file_resolver=Mock(), start_dispatch=AsyncMock())
    cm_factory = Mock(return_value=cm)
    monkeypatch.setattr(channel_manager, 'ChannelManager', cm_factory)
    monkeypatch.setattr(updater, 'UpdaterService', Mock(return_value=object()))
    retry = AsyncMock(side_effect=lambda *args, **kwargs: events.append('client.connected'))
    value = await services.assemble_services('ws://example/ws', log_stage=events.append, startup_t0=0.0, connect_with_retry=retry)
    try:
        assert value.client is selected if extension_client else isinstance(value.client, Client)
        assert value.third_agent is third
        assert value.storage_ctx is storage if enterprise else value.storage_ctx is None
        assert value.message_handler is handler and value.channel_manager is cm
        handler_factory.assert_called_once_with(value.client)
        cm_factory.assert_called_once_with(handler, config={})
        handler.set_channel_manager.assert_called_once_with(cm)
        handler.set_inbound_pipeline.assert_called_once_with(inbound)
        handler.set_outbound_pipeline.assert_called_once_with(outbound)
        handler.set_cron_controller.assert_called_once_with(controller)
        controller.bind_agent_client.assert_called_once_with(value.client)
        cm.set_persistent_output_file_resolver.assert_called_once_with(resolve_output_file)
        scheduler_factory.assert_called_once_with(store=store, agent_client=value.client, message_handler=handler)
        session_bind.assert_called_once_with(value.client)
        mcp_bind.assert_called_once_with(value.client)
        assert events.index('credentials.bound') < events.index('forwarding')
        assert events.index('bindings.bound') < events.index('forwarding')
        handler.set_session_login_bindings.assert_called_once_with(value.session_login_bindings)
        assert value.session_login_bindings._auth_service_provider is value.auth_service_provider
        scheduler.set_credential_access.assert_called_once_with(value.credential_access)
        controller.set_credential_access.assert_called_once_with(value.credential_access)
        handler.set_credential_access.assert_called_once_with(value.credential_access)
        assert value.credential_update_sink._message_handler is handler
        scheduler.start.assert_not_awaited()
        cm.start_dispatch.assert_not_awaited()
        assert events.index('telemetry') < events.index('handler.created')
        if extension_client:
            retry.assert_not_awaited()
            selected.connect.assert_awaited_once_with('')
        else:
            retry.assert_awaited_once()
            value.client.connect.assert_not_awaited()
    finally:
        for task in (value._hold_release_task, value._cleanup_task):
            task.cancel()
        await asyncio.gather(value._hold_release_task, value._cleanup_task, return_exceptions=True)


async def test_service_shutdown_retains_original_order_and_tolerates_storage_teardown_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_gateway.application.services import GatewayServices
    from agent_gateway.storage_assembly import setup
    from dataclasses import fields

    events = []
    def stop(name):
        async def invoke():
            events.append(name)
        return invoke
    async def wait(name):
        try:
            await asyncio.Event().wait()
        finally:
            events.append(name)
    hold, cleanup = asyncio.create_task(wait('hold')), asyncio.create_task(wait('cleanup'))
    await asyncio.sleep(0)
    services = GatewayServices(*[None for _ in fields(GatewayServices)])
    services.cron_scheduler = SimpleNamespace(stop=stop('cron'))
    services.channel_manager = SimpleNamespace(stop_dispatch=stop('dispatch'))
    services.heartbeat_service = SimpleNamespace(stop=stop('health'))
    services.message_handler = SimpleNamespace(stop_forwarding=stop('forwarding'))
    services.client = SimpleNamespace(disconnect=stop('client'))
    services.storage_ctx = object()
    services._hold_release_task, services._cleanup_task = hold, cleanup
    async def fail_storage(value):
        events.append('storage')
        raise RuntimeError('unavailable')
    monkeypatch.setattr(setup, 'teardown_gateway_storage_repositories', fail_storage)
    await services.stop()
    assert events == ['cron', 'dispatch', 'health', 'forwarding', 'client', 'storage', 'hold', 'cleanup']


async def test_web_assembly_keeps_original_instances_and_first_request_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_gateway.application.web import assemble_web
    from agent_gateway.application.inbound import MessagePreprocessor
    from agent_gateway.channel_manager.web import app_web_handlers, web_connect, git_diff_watcher
    from agent_gateway.channel_manager.protocol.web_proxy import web_proxy_listen
    from e2a.message import PROMPT_SUGGESTION_READY_EVENT, TOPIC_STARTERS_READY_EVENT

    handler, client = SimpleNamespace(), object()
    channel = SimpleNamespace(send=AsyncMock(), broadcast_event=AsyncMock(), channel_id='web', wrap_message_callback=Mock())
    constructor = Mock(return_value=channel)
    monkeypatch.setattr(web_connect, 'WebChannel', constructor)
    monkeypatch.setattr(web_proxy_listen, 'register_3rdagent_web_method', Mock())
    watchers = SimpleNamespace(set_channel=Mock(), set_diff_status_fetcher=Mock())
    monkeypatch.setattr(git_diff_watcher, 'get_git_diff_watcher_registry', lambda: watchers)
    bind = Mock()
    monkeypatch.setattr(app_web_handlers, '_register_web_handlers', bind)
    plugins = SimpleNamespace(bind_application_plugins=Mock())
    manager = SimpleNamespace(register_channel_with_inbound=Mock(), deliver_to_message_handler=AsyncMock())
    save, health, cron, heartbeat, updater = AsyncMock(), object(), object(), object(), object()
    processor = MessagePreprocessor(AsyncMock(), lambda msg: msg, Mock())
    actual, gate = assemble_web(agent_client=client, message_handler=handler, channel_manager=manager, extension_registry=plugins, on_config_saved=save, heartbeat_service=health, cron_controller=cron, heartbeat_controller=heartbeat, updater_service=updater, host='127.0.0.1', port=0, path='/ws', ws_permessage_deflate=True, preprocessor=processor, log_stage=Mock())
    assert actual is channel and not gate.is_set()
    constructor.assert_called_once()
    assert constructor.call_args.kwargs['agent_client'] is client
    assert constructor.call_args.args[0].ws_permessage_deflate is True
    params = bind.call_args.args[0]
    assert params.channel is channel and params.message_handler is handler and params.channel_manager is manager
    assert params.on_config_saved is save and params.cron_controller is cron and params.heartbeat_service is health
    assert params.heartbeat_controller is heartbeat and params.updater_service is updater
    watchers.set_channel.assert_called_once_with(channel)
    assert channel.git_watcher_registry is watchers
    plugins.bind_application_plugins.assert_called_once()
    await handler.on_prompt_suggestion('s', 'r', {'event_type': PROMPT_SUGGESTION_READY_EVENT, 'suggestion_id': 'p'})
    assert channel.send.await_args.args[0].session_id == 's'
    await handler.on_recommendation_event(TOPIC_STARTERS_READY_EVENT, {'ok': True})
    channel.broadcast_event.assert_awaited_once_with(TOPIC_STARTERS_READY_EVENT, {'ok': True})
    callback = channel.wrap_message_callback.call_args.args[0]
    original = AsyncMock(return_value=True)
    assert await callback(original, object()) is True
    assert gate.is_set()
