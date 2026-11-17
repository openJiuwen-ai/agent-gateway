# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Preserve saved-config effects without writing configuration or routing twice."""
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

from e2a.message import ReqMethod


@pytest.fixture
def effects():
    module = importlib.import_module('agent_gateway.application.config_saved_effects')
    events = []
    background = []

    class Client:
        def __init__(self):
            self.requests = []
            self.responses = []

        def set_or_update_server_config(self, *, config, env):
            events.append(('snapshot', config, env))

        async def send_request(self, envelope):
            self.requests.append(envelope)
            events.append(('request', envelope.method))
            response = self.responses.pop(0) if self.responses else SimpleNamespace(ok=True)
            if isinstance(response, BaseException):
                raise response
            return response

    def evolution(payload):
        events.append(('evolution', payload))

    def prewarm(name, *, delay_seconds=1.0):
        events.append(('prewarm', name, delay_seconds))

    async def proactive(payload):
        events.append(('proactive', payload))

    def restart():
        events.append(('restart',))

    def spawn(coro, *, name):
        background.append((name, coro))
        events.append(('background', name))

    client = Client()
    coordinator = module.ConfigSavedEffectCoordinator(
        client=client, update_evolution_auto_save=evolution, schedule_prewarm=prewarm,
        sync_proactive_tick=proactive, schedule_restart=restart, spawn_background=spawn,
    )
    yield SimpleNamespace(
        module=module, client=client, coordinator=coordinator, events=events,
        background=background,
    )
    for _name, coro in background:
        coro.close()


def _rejected(error, *, code=None):
    payload = {'error': error}
    if code is not None:
        payload['code'] = code
    return SimpleNamespace(ok=False, payload=payload)


async def test_success_retains_snapshot_reload_effect_order_and_envelope(effects):
    config = {'models': {'defaults': []}, 'evolution': {'auto_save': False}}
    env = {'MODEL_NAME': 'new-model'}
    options = {'reload_scopes': ['model', None, ''], 'target_channel_id': 'web'}
    assert await effects.coordinator(
        {'MODEL_NAME', 'proactive_recommendation_enabled'}, env_updates=env,
        config_payload=config, reload_options=options,
    ) is True
    assert effects.events == [
        ('snapshot', config, env), ('request', ReqMethod.AGENT_RELOAD_CONFIG.value),
        ('evolution', config), ('prewarm', 'agent-prewarm-sync-after-config', 3.0),
        ('request', ReqMethod.BROWSER_RUNTIME_RESTART.value), ('proactive', config),
        ('background', 'topic-recommendation-config-sync'),
    ]
    reload, browser = effects.client.requests
    assert reload.channel == '' and reload.session_id == 'sess_reload'
    assert reload.user_id == 'agentos_test'
    assert reload.request_id.startswith('agent-reload-')
    assert reload.params == {'config': config, 'env': env, **options}
    assert browser.request_id.startswith('browser-restart-')
    assert browser.user_id == 'agentos_test' and browser.session_id == 'sess_reload'
    assert config == {'models': {'defaults': []}, 'evolution': {'auto_save': False}}
    assert options == {'reload_scopes': ['model', None, ''], 'target_channel_id': 'web'}


@pytest.mark.parametrize('updated,scopes,want_proactive,want_background', [
    (set(), ['ui'], False, False),
    ({'MODEL_NAME'}, ['ui'], False, False),
    ({'proactive_recommendation_enabled'}, ['ui'], True, False),
    ({'topic_recommendation_enabled'}, ['ui'], False, True),
])
async def test_no_reload_scope_does_not_update_runtime_or_restart(
    effects, updated, scopes, want_proactive, want_background,
):
    assert await effects.coordinator(updated, reload_options={'reload_scopes': scopes}) is True
    assert effects.client.requests == []
    names = [event[0] for event in effects.events]
    assert names == ['snapshot'] + (['proactive'] if want_proactive else []) + (['background'] if want_background else [])


@pytest.mark.parametrize('error,code', [
    ('unpublished edits', 'PERSISTENT_MODELS_NOT_APPLIED'),
    ('ValidationError: bad settings', None),
    ('validation error in model', None),
    ('Field required', None),
])
async def test_nonretryable_failure_never_runs_success_effects(effects, monkeypatch, error, code):
    sleep = AsyncMock()
    monkeypatch.setattr(effects.module, 'asyncio', SimpleNamespace(sleep=sleep))
    effects.client.responses = [_rejected(error, code=code)]
    assert await effects.coordinator({'MODEL_NAME'}, config_payload={'models': {}}) is False
    assert len(effects.client.requests) == 1
    assert [event[0] for event in effects.events] == ['snapshot', 'request']
    sleep.assert_not_awaited()


async def test_generic_failure_retries_then_only_schedules_restart(effects, monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(effects.module, 'asyncio', SimpleNamespace(sleep=sleep))
    effects.client.responses = [OSError('offline')] * 4
    assert await effects.coordinator({'MODEL_NAME'}) is False
    assert len(effects.client.requests) == 4
    assert [call.args for call in sleep.await_args_list] == [(2.0,), (4.0,), (8.0,)]
    assert [event[0] for event in effects.events] == ['snapshot', 'request', 'request', 'request', 'request', 'restart']
    assert all(request is effects.client.requests[0] for request in effects.client.requests)


async def test_runtime_failed_waits_for_recovery_but_does_not_restart(effects, monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(effects.module, 'asyncio', SimpleNamespace(sleep=sleep))
    effects.client.responses = [_rejected('RUNTIME_FAILED')] * 6
    assert await effects.coordinator({'MODEL_NAME'}) is False
    assert len(effects.client.requests) == 6
    assert [call.args for call in sleep.await_args_list] == [(2.0,), (4.0,), (8.0,), (8.0,), (8.0,)]
    assert not any(event[0] in {'restart', 'evolution', 'prewarm', 'background'} for event in effects.events)


async def test_recovered_runtime_runs_success_effects_only_after_ack(effects, monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(effects.module, 'asyncio', SimpleNamespace(sleep=sleep))
    effects.client.responses = [_rejected('runtime failed')] * 4 + [SimpleNamespace(ok=True)]
    assert await effects.coordinator(reload_options={'reload_scopes': ['permissions']}) is True
    assert len(effects.client.requests) == 5
    assert [event[0] for event in effects.events][-2:] == ['evolution', 'prewarm']
    assert [call.args for call in sleep.await_args_list] == [(2.0,), (4.0,), (8.0,), (8.0,)]


async def test_success_after_transient_failure_keeps_original_retry_budget(effects, monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(effects.module, 'asyncio', SimpleNamespace(sleep=sleep))
    effects.client.responses = [OSError('once'), SimpleNamespace(ok=True)]
    assert await effects.coordinator(reload_options={'reload_scopes': ['permissions']}) is True
    assert len(effects.client.requests) == 2
    sleep.assert_awaited_once_with(2.0)
    assert [event[0] for event in effects.events][-2:] == ['evolution', 'prewarm']


async def test_browser_and_proactive_failure_remain_nonfatal(effects, monkeypatch):
    effects.client.responses = [SimpleNamespace(ok=True), OSError('browser unavailable')]

    async def fail_proactive(payload):
        raise OSError('cron unavailable')

    # Reconstruct with the failing external dependency, not a second algorithm.
    coordinator = effects.module.ConfigSavedEffectCoordinator(
        client=effects.client, update_evolution_auto_save=Mock(), schedule_prewarm=Mock(),
        sync_proactive_tick=fail_proactive, schedule_restart=Mock(side_effect=AssertionError),
        spawn_background=lambda coro, *, name: effects.background.append((name, coro)),
    )
    assert await coordinator(
        {'MODEL_NAME', 'proactive_recommendation_enabled'}, reload_options={'reload_scopes': ['permissions']},
    ) is True
    assert len(effects.client.requests) == 2


async def test_cancel_during_reload_propagates_without_restart(effects):
    effects.client.responses = [asyncio.CancelledError()]
    with pytest.raises(asyncio.CancelledError):
        await effects.coordinator()
    assert [event[0] for event in effects.events] == ['snapshot', 'request']


@pytest.mark.parametrize('failure', ['reject', 'exception'])
async def test_topic_sync_is_background_and_failure_does_not_change_save(effects, monkeypatch, caplog, failure):
    monkeypatch.setattr(effects.module.logger, 'propagate', True)
    fetch = AsyncMock(return_value=(False, {'code': 'OFFLINE', 'error': 'not available'}))
    if failure == 'exception':
        fetch.side_effect = OSError('offline')
    monkeypatch.setattr('agent_gateway.routing.e2a_proxy.fetch_agent_unary', fetch)
    config = {'recommendation': {'topic_enabled': False}}
    assert await effects.coordinator(
        {'topic_recommendation_enabled'}, config_payload=config, reload_options={'reload_scopes': ['ui']},
    ) is True
    fetch.assert_not_awaited()
    assert len(effects.background) == 1
    name, coro = effects.background[0]
    assert name == 'topic-recommendation-config-sync'
    await coro
    kwargs = fetch.await_args.kwargs
    assert kwargs['agent_client'] is effects.client
    assert kwargs['req_method'] == ReqMethod.RECOMMENDATION
    assert kwargs['params'] == {
        'action': 'apply_saved_config', 'updated_env_keys': ['topic_recommendation_enabled'],
        'reload_scopes': ['ui'], 'config': config,
    }
    assert kwargs['session_id'] == 'sess_reload' and kwargs['user_id'] == 'agentos_test'
    assert 'topic recommendation config sync failed' in caplog.text


async def test_factory_binds_existing_local_actions_without_constructing_services(monkeypatch):
    from agent_gateway import app_gateway
    from agent_gateway.application.config_saved_effects import ConfigSavedEffectCoordinator
    from agent_gateway.application.single import GatewayRestartRequest

    client = SimpleNamespace(set_or_update_server_config=Mock(), send_request=AsyncMock(return_value=SimpleNamespace(ok=True)))
    handler = SimpleNamespace(update_evolution_auto_save=Mock())
    cron = object()
    prewarm = Mock()
    restart_request = GatewayRestartRequest()
    restart = Mock()
    proactive = AsyncMock()
    monkeypatch.setattr(app_gateway, '_schedule_gateway_restart', restart)
    monkeypatch.setattr('agent_gateway.cron.proactive_cron_sync.sync_proactive_tick_job', proactive)
    result = app_gateway._build_config_saved_callback(
        client=client, message_handler=handler, cron_controller=cron,
        schedule_agent_prewarm_sync=prewarm, restart_request=restart_request,
    )
    assert isinstance(result, ConfigSavedEffectCoordinator)
    client.set_or_update_server_config.assert_not_called()
    client.send_request.assert_not_awaited()
    assert await result({'proactive_recommendation_enabled'}, config_payload={'proactive': {}}, reload_options={'reload_scopes': ['permissions']}) is True
    proactive.assert_awaited_once_with(cron, {'proactive': {}})
    handler.update_evolution_auto_save.assert_called_once_with({'proactive': {}})
    prewarm.assert_called_once_with('agent-prewarm-sync-after-config', delay_seconds=3.0)
    restart.assert_not_called()


def test_coordinator_cold_import_does_not_initialize_gateway_or_listeners(tmp_path):
    env = os.environ.copy()
    env['JIUWENSWARM_HOME'] = str(tmp_path / 'home')
    env['JIUWENSWARM_DATA_DIR'] = str(tmp_path / 'data')
    env['AGENTOS_GATEWAY_LOG_DIR'] = str(tmp_path / 'logs')
    result = subprocess.run([sys.executable, '-c', '''
import importlib, sys
from pathlib import Path
importlib.import_module('agent_gateway.application.config_saved_effects')
assert 'agent_gateway.app_gateway' not in sys.modules
assert 'agent_gateway.startup.bootstrap' not in sys.modules
assert 'agent_gateway.auth.service' not in sys.modules
assert 'agent_gateway.cron.controller' not in sys.modules
assert 'agent_gateway.channel_manager.web.app_web_handlers' not in sys.modules
'''], env=env, capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    assert not any((tmp_path / name).exists() for name in ('home', 'data', 'logs'))


@pytest.mark.parametrize('channel_id,method,params', [
    ('web', 'config.set', {'permissions_enabled': 'false'}),
    ('web', 'config.save_all', {'config': {'permissions_enabled': 'false'}}),
    ('tui', 'config.set', {'permissions_enabled': 'false'}),
])
async def test_personal_registered_config_routes_only_to_as(effects, monkeypatch, channel_id, method, params):
    from agent_gateway.channel_manager.tui.tui_connect import CliHandlersBindParams, register_cli_handlers
    from agent_gateway.channel_manager.web.app_web_handlers import WebHandlersBindParams, _register_web_handlers
    from agent_gateway.routing.agent_client import WebSocketAgentServerClient

    class Channel:
        def __init__(self):
            self.channel_id = channel_id
            self.methods = {}
            self.responses = []

        def register_method(self, name, handler, *, local_only=False):
            self.methods[name] = handler

        def register_local_handler(self, path, name, handler):
            self.methods[name] = handler

        def on_connect(self, handler):
            pass

        def on_disconnect(self, handler):
            pass

        async def send_response(self, ws, req_id, **response):
            self.responses.append(response)

    monkeypatch.setattr('agent_gateway.enterprise.is_enterprise', lambda: False)
    client = WebSocketAgentServerClient()
    client._apply_connection_ack({'payload': {'readiness': 'AGENT_READY'}})
    response = {'updated': ['permissions_enabled'], 'applied_without_restart': True}
    send = AsyncMock(return_value=SimpleNamespace(ok=True, payload=response))
    monkeypatch.setattr(client, 'send_request', send)
    channel = Channel()
    if channel_id == 'web':
        _register_web_handlers(WebHandlersBindParams(
            channel=channel, agent_client=client, on_config_saved=effects.coordinator,
        ))
    else:
        register_cli_handlers(CliHandlersBindParams(
            channel=channel, agent_client=client, on_config_saved=effects.coordinator, path='/tui',
        ))
    await channel.methods[method](object(), 'config-save', params, 'user-session', 'business-user')
    assert effects.events == []
    send.assert_awaited_once()
    envelope = send.await_args.args[0]
    assert envelope.method == method and envelope.channel == channel_id
    assert envelope.user_id == 'business-user' and envelope.session_id == 'user-session'
    assert envelope.params == params
    assert channel.responses == [{'ok': True, 'payload': response, 'error': None, 'code': None}]
