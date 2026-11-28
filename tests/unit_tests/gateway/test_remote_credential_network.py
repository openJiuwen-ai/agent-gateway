"""Exercise explicit remote ports without enabling the cold data account gate."""

import asyncio
import json
import os
import socket
import time
from types import SimpleNamespace

import pytest
from websockets.asyncio.client import connect

from agent_gateway.auth import apig, login_credentials, model_catalog, session_owners
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.control_credentials import ControlCredentialServices
from agent_gateway.auth.remote_credential_access import RemoteCredentialAccess, RemoteSessionLoginBindings
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceAdmission, ControlServiceClient, ControlServiceIdentity, ControlServiceServer,
)
from testcases_gateway.stub_agent_server import StubAgentServer


class _NoopBus:
    async def publish_user_messages(self, message):
        pass

    async def route_incoming_message(self, message):
        pass


async def _terminal(websocket, request_id):
    async with asyncio.timeout(5):
        while True:
            frame = json.loads(await websocket.recv())
            if (frame.get('id') == request_id and frame.get('ok') is False) or (
                frame.get('event') in {'chat.final', 'chat.error'}
            ):
                return frame


async def _chat(websocket, request_id):
    await websocket.send(json.dumps({
        'type': 'req', 'id': request_id, 'method': 'chat.send', 'is_stream': True,
        'params': {
            'session_id': 'remote-chat', 'content': 'hello', 'model_name': 'login-model',
            '_model_auth': {'api_key': 'forged-client-canary', 'credential_ref': 'f' * 32},
        },
    }))
    return await _terminal(websocket, request_id)


async def test_web_and_nonweb_binding_use_control_ports_and_fail_closed(tmp_path, monkeypatch):
    from agent_gateway.application.inbound import MessagePreprocessor
    from agent_gateway.channel_manager.channel_manager import ChannelManager
    from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
    from agent_gateway.message_handler.message_handler import MessageHandler
    from agent_gateway.routing.agent_client import WebSocketAgentServerClient
    from e2a.gateway_normalize import e2a_from_agent_fields
    from e2a.message import ReqMethod

    auth_path = tmp_path / 'control-auth'
    auth_path.mkdir()
    monkeypatch.setattr('agent_gateway.auth.session_store.auth_dir', lambda: auth_path)
    monkeypatch.setattr(login_credentials, '_ref_key_cache', b'n' * 32)
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [model_catalog.LoginModel('login-model', 'Login')])
    monkeypatch.setattr(apig, 'resolve_apig_config', lambda **k: SimpleNamespace(invoke_base_url='https://test.invalid'))
    monkeypatch.setattr('agent_gateway.auth.service.get_auth_service', lambda: pytest.fail('global login service used'))
    monkeypatch.setattr(MessageHandler, '_instance', None)
    async def no_hook(self, message):
        pass
    monkeypatch.setattr(MessageHandler, '_trigger_before_chat_request_hook', no_hook)
    session_owners.reset_for_test()
    store = AuthSessionStore(persist=False)
    session = store.create(SimpleNamespace(
        user_id='model-account', user_name='A', credential=Credential(
            id_token='remote-network-canary', refresh_token='remote-refresh-canary', expires_at=time.time() + 3600,
        ),
    ))
    authority = AuthService(store=store)
    services = ControlCredentialServices(lambda: authority)
    left, right = socket.socketpair()
    identity = ControlServiceIdentity('network-group', 0, 1, os.getpid(), os.getpid())
    control_client = ControlServiceClient(ControlServiceChannel(left), identity)
    control_server = ControlServiceServer(
        ControlServiceChannel(right), identity, handlers=services.handlers, admission=ControlServiceAdmission(),
    )
    stub = StubAgentServer().start()
    as_client = WebSocketAgentServerClient(ping_interval=None, ping_timeout=None)
    handler = manager = web = web_task = None
    try:
        await asyncio.gather(control_client.start(), control_server.start())
        credentials = RemoteCredentialAccess(control_client)
        bindings = RemoteSessionLoginBindings(control_client)
        handler = MessageHandler(as_client, session_login_bindings=bindings)
        handler.set_credential_access(credentials)
        handler.set_session_login_bindings(bindings)
        manager = ChannelManager(handler)
        handler.set_channel_manager(manager)
        web = WebChannel(
            WebChannelConfig(enabled=True, host='127.0.0.1', port=0, path='/ws'), _NoopBus(),
            agent_client=as_client,
        )
        preprocessor = MessagePreprocessor(lambda message: None, lambda message: message, lambda message: None)
        manager.register_channel_with_inbound(
            web, preprocessor.with_credential_access(credentials).for_channel(
                manager, {'chat.send'}, {'chat.send'}, 'Web',
            ),
        )
        await as_client.connect(stub.url)
        await handler.start_forwarding()
        await manager.start_dispatch()
        web_task = asyncio.create_task(web.start())
        await asyncio.wait_for(web.wait_ready(), 5)
        port = web._listener.bound_addresses[0][1]
        async with connect(f'ws://127.0.0.1:{port}/ws', additional_headers={
            'X-Auth-Session': session.session_id, 'X-User-Id': 'business-user',
        }) as websocket:
            terminal = await _chat(websocket, 'remote-web')
            assert terminal['event'] == 'chat.final'
            sent = next(request for request in stub.requests if request.get('request_id') == 'remote-web')
            expected_auth = {
                'api_base': 'https://test.invalid', 'api_key': 'remote-network-canary',
                'credential_ref': login_credentials.credential_ref_for_user(session.user_id),
            }
            assert sent['params'].get('_model_auth') == expected_auth, sent
            assert sent['user_id'] == 'business-user'
            assert 'forged-client-canary' not in json.dumps(sent)
            for channel in ('tui', 'acp'):
                envelope = e2a_from_agent_fields(
                    request_id=f'remote-{channel}', req_method=ReqMethod.CHAT_SEND, channel_id=channel,
                    session_id='remote-chat', params={'query': 'bound reply', 'model_name': 'login-model'},
                    timestamp=time.time(), user_id='business-user',
                )
                assert (await handler._send_non_stream_agent_request(envelope)).ok
                sent = next(request for request in stub.requests if request.get('request_id') == f'remote-{channel}')
                assert sent['params']['_model_auth'] == expected_auth
            authority.logout(session.session_id)
            failed = await _chat(websocket, 'logged-out-web')
            assert failed['ok'] is False
            assert failed['code'] == 'CREDENTIAL_SERVICE_FAILED'
            assert not any(request.get('request_id') == 'logged-out-web' for request in stub.requests)
            await control_server.stop()
            failed = await _chat(websocket, 'offline-control')
            assert failed['ok'] is False
            assert failed['code'] == 'CREDENTIAL_SERVICE_FAILED'
            assert 'remote-network-canary' not in json.dumps(failed)
            assert not any(request.get('request_id') == 'offline-control' for request in stub.requests)
    finally:
        if web is not None:
            await web.stop()
        if web_task is not None:
            await asyncio.wait_for(web_task, 5)
        if manager is not None:
            await manager.stop_dispatch()
        if handler is not None:
            await handler.stop_forwarding()
        await as_client.disconnect()
        stub.stop()
        await control_client.stop()
        await control_server.stop()
        await services.stop(timeout=2)
        session_owners.reset_for_test()
