# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from websockets.datastructures import Headers

from agent_gateway.application import tui_acp
from agent_gateway.application import personal_tui


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
            ('command.model', {'model': 'other', 'config': {'api_key': 'secret'}}),
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


@pytest.mark.asyncio
@pytest.mark.parametrize(('metadata', 'code'), [
    ({'user_id': 'another', 'runtime': {'is_processing': False}}, 'SESSION_IDENTITY_REQUIRED'),
    ({'user_id': 'owner', 'runtime': {'is_processing': True}}, 'SESSION_BUSY'),
    ({'user_id': 'owner', 'runtime': {'is_processing': False, 'recovery_pending': True}}, 'SERVICE_UNAVAILABLE'),
    ({'user_id': 'owner'}, 'SERVICE_UNAVAILABLE'),
])
async def test_mutations_fail_closed_before_the_write(monkeypatch, metadata, code):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock(return_value=(True, metadata))
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    try:
        result = await request(assembly.protocol_runtime, window(user='owner'), 'session.rewind', {
            'session_id': 's1', 'turn_index': 1, 'user_id': 'another',
        })
        assert result['code'] == code
        fetch.assert_awaited_once()
        assert fetch.await_args.kwargs['req_method'].value == 'session.get_metadata'
        assert fetch.await_args.kwargs['user_id'] == 'owner'
        assert fetch.await_args.kwargs['params'] == {'session_id': 's1', 'enable_writeback': False}
    finally:
        await assembly.stop()


@pytest.mark.asyncio
async def test_unavailable_metadata_fails_closed_without_echoing_it(monkeypatch):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock(return_value=(False, {'code': 'AGENT_SERVER_TIMEOUT', 'error': 'AgentServer request timed out'}))
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    try:
        result = await request(assembly.protocol_runtime, window(user='owner'), 'session.delete', {'session_id': 's1'})
        assert (result['ok'], result['code'], result['error']) == (
            False, 'AGENT_SERVER_TIMEOUT', 'AgentServer request timed out',
        )
        fetch.assert_awaited_once()
    finally:
        await assembly.stop()


@pytest.mark.asyncio
async def test_web_activity_in_the_same_worker_blocks_rewind(monkeypatch):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock(return_value=(True, {'user_id': '', 'runtime': {'is_processing': False}}))
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    checked = []
    assembly = await tui_acp.assemble_tui_acp(
        agent_client=SimpleNamespace(server_ready=True, send_request=AsyncMock()),
        message_handler=SimpleNamespace(cancel_scheduled_disconnect_cancel_for_session=lambda *_: None),
        channel_manager=SimpleNamespace(register_channel_with_inbound=lambda *_: None,
                                        register_external_channel=lambda *_: None),
        third_agent=None, on_config_saved=None, cron_controller=None, heartbeat_controller=None,
        preprocessor=SimpleNamespace(for_channel=lambda *_: None), log_stage=lambda *_: None,
        personal_tui=True, web_session_busy=lambda session_id: checked.append(session_id) or True,
    )
    try:
        result = await request(assembly.protocol_runtime, window(), 'session.restore_files', {
            'session_id': 's-busy', 'turn_index': 3,
        })
        assert (result['ok'], result['code'], result['error']) == (False, 'SESSION_BUSY', 'Session is running')
        assert checked == ['s-busy']
        fetch.assert_awaited_once()
    finally:
        await assembly.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(('summary', 'activity', 'expected'), [
    ((True, {'status': 'failed', 'summary': 'stale'}), None, 'SUMMARY_FAILED'),
    ((True, {'status': 'ok', 'summary': '', 'summarized_count': 1}), None, 'SUMMARY_FAILED'),
    ((True, {'status': 'ok', 'summary': 'only summary', 'summarized_count': 2}), True, 'SESSION_BUSY'),
    ((True, {'status': 'ok', 'summary': 'only summary', 'summarized_count': 2}), False, 'AGENT_SERVER_TIMEOUT'),
])
async def test_rewind_with_summary_rechecks_activity_and_never_replays_write(monkeypatch, summary, activity, expected):
    from agent_gateway.routing import e2a_proxy

    replies = [(True, {'user_id': '', 'runtime': {'is_processing': False}}), summary]
    methods = ['session.get_metadata', 'command.compact_partial']
    if activity is not None:
        replies.append((True, {'runtime': {'is_processing': activity}}))
        methods.append('session.get_metadata')
    if activity is False:
        replies.append((False, {'code': 'AGENT_SERVER_TIMEOUT', 'error': 'AgentServer request timed out'}))
        methods.append('session.rewind_compact')
    fetch = AsyncMock(side_effect=replies)
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    try:
        result = await request(assembly.protocol_runtime, window(), 'command.rewind_compact', {
            'session_id': 's1', 'turn_index': 2, 'direction': 'up_to', 'compact_summary': 'client text',
        })
        assert result['code'] == expected
        assert [call.kwargs['req_method'].value for call in fetch.await_args_list] == methods
        if activity is False:
            assert fetch.await_args.kwargs['params']['compact_summary'] == 'only summary'
    finally:
        await assembly.stop()


@pytest.mark.asyncio
async def test_rewind_with_summary_returns_the_server_summary(monkeypatch):
    from agent_gateway.routing import e2a_proxy

    idle = (True, {'user_id': '', 'runtime': {'is_processing': False}})
    fetch = AsyncMock(side_effect=[
        idle, (True, {'status': 'ok', 'summary': 'server summary', 'summarized_count': 2}), idle,
        (True, {'turns_removed': 2}),
    ])
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    try:
        result = await request(assembly.protocol_runtime, window(), 'command.rewind_compact', {
            'session_id': 's1', 'turn_index': 2, 'compact_summary': 'client text', 'summarized_count': 99,
        })
        assert result == {'type': 'res', 'id': 'legacy-request', 'ok': True,
                          'payload': {'turns_removed': 2, 'summary': 'server summary', 'summarized_messages': 2}}
        writes = [call.kwargs['params'] for call in fetch.await_args_list[3:]]
        assert writes == [{'session_id': 's1', 'turn_index': 2, 'direction': 'from',
                           'compact_summary': 'server summary', 'summarized_count': 2}]
    finally:
        await assembly.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(('method', 'params', 'expected'), [
    ('session.delete', {'session_id': 'owned', 'session_ids': ['other']}, 'BAD_REQUEST'),
    ('session.rewind', {'session_id': 'owned', 'turn_index': True}, 'BAD_REQUEST'),
    ('session.rewind_compact', {'session_id': 'owned', 'turn_index': 1}, None),
    ('session.color_set', {}, 'BAD_REQUEST'),
    ('session.rebind_project', {'session_id': 'owned', 'project_dir': '  '}, 'BAD_REQUEST'),
    ('command.rewind_compact', {'session_id': 'owned', 'turn_index': 2, 'direction': 'sideways'}, 'BAD_REQUEST'),
])
async def test_tui_write_selectors_cannot_bypass_session_checks(monkeypatch, method, params, expected):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock()
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    try:
        result = await request(assembly.protocol_runtime, window(user='owner'), method, params)
        assert not result['ok'] and result.get('code') == expected, result
        fetch.assert_not_awaited()
    finally:
        await assembly.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(('method', 'params', 'forwarded'), [
    ('command.add_dir', {'path': '/tmp/project', 'remember': True, 'user_id': 'another'},
     {'path': '/tmp/project', 'remember': True}),
    ('session.color_set', {'session_id': 's1', 'color': '#fff'}, {'session_id': 's1', 'color': '#fff'}),
    ('session.delete', {'session_id': 's1', 'extra': 1}, {'session_id': 's1'}),
    ('session.rebind_project', {'session_id': 's1', 'project_dir': '/tmp/project'},
     {'session_id': 's1', 'project_dir': '/tmp/project'}),
])
async def test_checked_writes_forward_only_their_own_fields(monkeypatch, method, params, forwarded):
    from agent_gateway.routing import e2a_proxy

    idle = (True, {'user_id': 'owner', 'runtime': {'is_processing': False}})
    checked = method != 'command.add_dir'
    fetch = AsyncMock(side_effect=[idle, (True, {'done': True})] if checked else [(True, {'done': True})])
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    try:
        result = await request(assembly.protocol_runtime, window(user='owner'), method, params)
        assert result['ok'] is True and result['payload'] == {'done': True}, result
        write = fetch.await_args_list[-1]
        assert write.kwargs['req_method'].value == method and write.kwargs['params'] == forwarded
        assert write.kwargs['user_id'] == 'owner'
    finally:
        await assembly.stop()


@pytest.mark.asyncio
async def test_tui_disconnect_uses_the_regular_local_handler(monkeypatch):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock()
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    runtime = assembly.protocol_runtime
    called = AsyncMock()

    async def disconnect(ws, req_id, params, session_id):
        await called((req_id, params, session_id))
        await runtime.send_response(ws, req_id, ok=True, payload={'disconnected': True})

    original = runtime.config.routes['/tui'].local_handlers['tui.disconnect']
    runtime.config.routes['/tui'].local_handlers['tui.disconnect'] = disconnect
    try:
        ws = window()
        assert (await request(runtime, ws, 'tui.disconnect', {'session_id': 's1', 'user_id': 'x'}))['ok'] is True
        called.assert_awaited_once()
        assert called.await_args.args[0] == ('legacy-request', {'session_id': 's1'}, 's1')
        assert (await request(runtime, ws, 'tui.disconnect', {'session_id': 'bad id'}))['code'] == 'BAD_REQUEST'
        assert called.await_count == 1
        runtime.config.routes['/tui'].local_handlers['tui.disconnect'] = original
        fetch.assert_not_awaited()
    finally:
        await assembly.stop()


@pytest.mark.asyncio
async def test_model_switch_requires_agent_capability_and_keeps_legacy_result(monkeypatch):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock(side_effect=[
        (False, {'code': 'UNSUPPORTED_DATA_PLANE', 'error': 'missing capability'}),
        (True, {'type': 'switched', 'applied': True, 'current': 'other', 'requested': 'other', 'api_key': 'excluded'}),
    ])
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    try:
        runtime = assembly.protocol_runtime
        ws = window(user='local-user')
        params = {'model': 'other', 'index': 1, 'session_id': 's1', 'cwd': '/tmp', 'user_id': 'ignored'}
        assert (await request(runtime, ws, 'command.model', params))['code'] == 'UNSUPPORTED_DATA_PLANE'
        response = await request(runtime, ws, 'command.model', params)
        assert response['ok'] and response['payload'] == {
            'type': 'switched', 'applied': True, 'current': 'other', 'requested': 'other',
        }
        for call in fetch.await_args_list:
            assert call.kwargs['required_capability'] == 'personal_tui_model_switch_v1'
            assert call.kwargs['params'] == {'model': 'other', 'index': 1, '_personal_tui_model_switch_v1': True}
            assert call.kwargs['user_id'] == 'local-user'
        assert not runtime._pending_session_clients
    finally:
        await assembly.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(('params', 'code'), [
    ({'model': 'other', 'index': 1, 'action': 'add_model'}, 'UNSUPPORTED_REQUEST'),
    ({'model': 'other', 'api_key': 'secret'}, 'UNSUPPORTED_REQUEST'),
    ({'model': '   '}, 'BAD_REQUEST'),
    ({'model': 'x' * 513}, 'BAD_REQUEST'),
    ({'model': 7}, 'BAD_REQUEST'),
    ({'model': 'other', 'index': True}, 'BAD_REQUEST'),
    ({'model': 'other', 'index': -1}, 'BAD_REQUEST'),
])
async def test_model_switch_rejects_anything_but_a_default_model_selection(monkeypatch, params, code):
    from agent_gateway.routing import e2a_proxy

    fetch = AsyncMock()
    monkeypatch.setattr(e2a_proxy, 'fetch_agent_unary', fetch)
    assembly, _ = await assemble()
    try:
        assert (await request(assembly.protocol_runtime, window(), 'command.model', params))['code'] == code
        fetch.assert_not_awaited()
    finally:
        await assembly.stop()


@pytest.mark.asyncio
async def test_model_switch_without_the_capability_never_sends_the_plain_model_command():
    """A miss must not fall back to the unmarked command.model the TUI read path uses."""
    from e2a.message import ReqMethod
    from websockets.datastructures import Headers

    from agent_gateway.routing import e2a_proxy
    from agent_gateway.routing.agent_client import WebSocketAgentServerClient

    class Socket:
        def __init__(self):
            self.sent = []

        async def send(self, raw):
            self.sent.append(raw)

        async def close(self):
            pass

    class Client(WebSocketAgentServerClient):
        socket = Socket()

        def _send_wire_payload(self, payload):
            return self.socket.send("written")

    client = Client()
    client._ws = client.socket
    client._apply_connection_ack({'payload': {'capabilities': ['personal_tui_summary_rewind_v1']}})
    try:
        result = await asyncio.wait_for(e2a_proxy.fetch_agent_unary(
            agent_client=client, req_method=ReqMethod.COMMAND_MODEL,
            params={'model': 'other', '_personal_tui_model_switch_v1': True}, session_id=None, user_id=None,
            channel_id='tui', required_capability=personal_tui.MODEL_SWITCH_CAPABILITY, timeout_seconds=5,
        ), 5)
    finally:
        await client.disconnect()
    assert result == (False, {'code': 'UNSUPPORTED_DATA_PLANE',
                              'error': 'This AgentServer does not support the requested operation'})
    assert client.socket.sent == [] and client._message_queues == {}
