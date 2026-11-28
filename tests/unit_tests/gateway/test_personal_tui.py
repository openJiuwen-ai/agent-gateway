# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from websockets.datastructures import Headers

from agent_gateway.application import tui_acp


async def assemble(*, enabled=True):
    client = SimpleNamespace(server_ready=True, send_request=AsyncMock())
    handler = SimpleNamespace(cancel_scheduled_disconnect_cancel_for_session=lambda *_: None)
    manager = SimpleNamespace(register_channel_with_inbound=lambda *_: None, register_external_channel=lambda *_: None)
    assembly = await tui_acp.assemble_tui_acp(
        agent_client=client, message_handler=handler, channel_manager=manager,
        third_agent=None, on_config_saved=None, cron_controller=None, heartbeat_controller=None,
        preprocessor=SimpleNamespace(for_channel=lambda *_: None), log_stage=lambda *_: None,
        personal_tui=enabled,
    )
    return assembly, client


def window(*, host='127.0.0.1', headers=(), user=None):
    return SimpleNamespace(
        remote_address=(host, 40000), request_headers=Headers(headers), send=AsyncMock(),
        _gateway_user_id=user,
    )


async def request(runtime, ws, method, params, *, path='/tui'):
    await runtime._handle_raw_message(ws, json.dumps({
        'type': 'req', 'id': 'legacy-request', 'method': method, 'params': params,
    }), path, runtime.config.routes[path])
    return json.loads(ws.send.await_args.args[0])


@pytest.mark.asyncio
async def test_query_keeps_connection_and_configuration_authority(monkeypatch):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock(return_value=(True, {
        'preferred_language': 'zh', 'model_api_key': 'secret', 'model': 'visible',
        'schema': [{'default': 'secret'}],
    }))
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    runtime = assembly.protocol_runtime
    owner = window()
    viewer = window()
    route = runtime.config.routes['/tui']
    await runtime._bind_route_session_client(route, 's1', owner)
    try:
        response = await request(runtime, viewer, 'config.get', {
            'session_id': 's1', 'cwd': '/tmp', 'project_dir': '/tmp', 'trusted_dirs': ['/tmp'],
        })
        assert response == {'type': 'res', 'id': 'legacy-request', 'ok': True,
                            'payload': {'preferred_language': 'zh', 'model': 'visible'}}
        assert not owner.send.called
        assert runtime.is_session_bound_to_client('tui', 's1', owner)
        assert not runtime._pending_session_clients
        assert fetch.await_args.kwargs['params'] == {}
    finally:
        await assembly.stop()


@pytest.mark.asyncio
async def test_legacy_reads_require_native_loopback_and_do_not_open_management(monkeypatch):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock()
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    runtime = assembly.protocol_runtime
    try:
        for ws in (window(headers=[('Origin', '')]), window(host='192.0.2.1')):
            assert (await request(runtime, ws, 'models.list', {}))['code'] == 'FORBIDDEN'
        ws = window()
        for method, params in (
            ('command.model', {'model': 'other', 'session_id': 's1'}),
            ('command.model', {'action': 'add_model'}),
            ('command.mcp', {'action': 'enable', 'name': 'demo'}),
            ('config.get', {'key': 'model_api_key'}),
        ):
            assert (await request(runtime, ws, method, params))['code'] == 'UNSUPPORTED_REQUEST'
        assert not (await request(runtime, ws, 'models.list', {}, path='/acp'))['ok']
        fetch.assert_not_awaited()
    finally:
        await assembly.stop()


@pytest.mark.asyncio
async def test_default_assembly_keeps_the_regular_route_without_a_hook():
    assembly, _ = await assemble(enabled=False)
    try:
        assert assembly.protocol_runtime.config.routes['/tui'].unbound_request_handler is None
        assert assembly.protocol_runtime.config.routes['/acp'].unbound_request_handler is None
    finally:
        await assembly.stop()


@pytest.mark.asyncio
async def test_other_methods_and_unreadable_peers_stay_on_the_regular_path(monkeypatch):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock(return_value=(True, {}))
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    runtime = assembly.protocol_runtime
    try:
        # A method outside the compatibility set returns the hook's "not mine".
        assert await runtime.config.routes['/tui'].unbound_request_handler(
            window(), 'r', 'chat.send', {}, None, None,
        ) is False
        fetch.assert_not_awaited()
        for ws in (window(headers=[('Origin', 'http://127.0.0.1:5173')]), window(host='203.0.113.9'),
                   SimpleNamespace(send=AsyncMock()), window(headers=[('origin', 'null')])):
            assert (await request(runtime, ws, 'models.list', {}))['code'] == 'FORBIDDEN'
        assert (await request(runtime, window(), 'models.list', ['not', 'an', 'object']))['code'] == 'BAD_REQUEST'
        assert (await request(runtime, window(), 'skills.list', {'user_id': 'other'}))['ok'] is True
        assert fetch.await_args.kwargs['params'] == {} and fetch.await_args.kwargs['user_id'] is None
        fetch.assert_awaited_once()
    finally:
        await assembly.stop()


def test_worker_admits_compatibility_frames_only_for_the_personal_tui_route():
    from agent_gateway.application.worker_data import is_data_frame

    def frame(method, **extra):
        return json.dumps({'type': 'req', 'id': 'f', 'method': method, 'params': {}, **extra})

    for method in ('config.get', 'models.list', 'command.model', 'tui.disconnect', 'memory.status'):
        assert is_data_frame(frame(method)) is False
        assert is_data_frame(frame(method), personal=True) is False
        assert is_data_frame(frame(method), tui=True) is False
        assert is_data_frame(frame(method), tui=True, personal=True) is True
    assert is_data_frame(frame('chat.send'), tui=True, personal=True) is True
    assert is_data_frame(frame('session.create'), tui=True, personal=True) is False
    assert is_data_frame(frame('cron.job.list'), tui=True, personal=True) is False
    assert is_data_frame(frame('config.set'), tui=True, personal=True) is False


def test_personal_tui_gate_requires_a_personal_loopback_agent_server(monkeypatch):
    from agent_gateway.application import personal_tui

    monkeypatch.setattr(personal_tui, 'is_enterprise', lambda: False)
    monkeypatch.setattr(personal_tui, 'is_agentos_runtime', lambda: False)
    for url in ('ws://localhost:19010', 'ws://127.0.0.1:19010/agent/ws', 'ws://[::1]:19010'):
        assert personal_tui.enable_personal_tui(url) is True
    for url in ('ws://10.0.0.5:19010', 'ws://gateway.example.com:19010', '', None):
        assert personal_tui.enable_personal_tui(url) is False
    monkeypatch.setattr(personal_tui, 'is_enterprise', lambda: True)
    assert personal_tui.enable_personal_tui('ws://127.0.0.1:19010') is False
    monkeypatch.setattr(personal_tui, 'is_enterprise', lambda: False)
    monkeypatch.setattr(personal_tui, 'is_agentos_runtime', lambda: True)
    assert personal_tui.enable_personal_tui('ws://127.0.0.1:19010') is False
