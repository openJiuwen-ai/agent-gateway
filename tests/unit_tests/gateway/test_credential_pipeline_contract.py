# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Explicit credential paths preserve the original channel and AS protocols."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import logging
import pytest
from e2a.message import Message, ReqMethod


def _message(channel='web', method=ReqMethod.CHAT_SEND):
    return Message(type='request', timestamp=1.0, ok=True, id='request-a', channel_id=channel, session_id='session-a', req_method=method,
                   user_id='business-a', params={'content': 'hello', 'model_name': 'login-model', '_model_auth': {'api_key': 'forged'}},
                   metadata={'auth_session': 'login-a'})


@pytest.mark.parametrize('channel', ['web', 'tui', 'acp'])
async def test_all_inbound_channels_use_only_explicit_credentials(monkeypatch, channel):
    from agent_gateway.application import inbound
    from agent_gateway.auth.credential_access import ModelCredential
    access = SimpleNamespace(request_model_auth=AsyncMock(return_value=ModelCredential('https://test.invalid', 'server-token', 'ref')))
    processor = inbound.MessagePreprocessor(AsyncMock(), lambda msg: pytest.fail('legacy normalization used'), lambda msg: None)
    processor = processor.with_credential_access(access)
    manager = SimpleNamespace(deliver_to_message_handler=AsyncMock())
    msg = _message(channel)
    assert await processor.for_channel(manager, {'chat.send'}, {'chat.send'}, channel)(msg) is True
    sent = manager.deliver_to_message_handler.await_args.args[0]
    assert '_model_auth' not in msg.params
    assert sent.params['query'] == 'hello'
    assert sent.params['_model_auth'] == {'api_base': 'https://test.invalid', 'api_key': 'server-token', 'credential_ref': 'ref'}
    assert sent.user_id == 'business-a' and sent.session_id == 'session-a'
    assert sent.metadata == {'auth_session': 'login-a'}
    access.request_model_auth.assert_awaited_once_with('login-a', 'login-model')


async def test_credential_failure_keeps_dispatch_but_never_client_credentials():
    from agent_gateway.application import inbound
    access = SimpleNamespace(request_model_auth=AsyncMock(side_effect=ConnectionError('offline')))
    processor = inbound.MessagePreprocessor(AsyncMock(), lambda msg: msg, lambda msg: None).with_credential_access(access)
    manager = SimpleNamespace(deliver_to_message_handler=AsyncMock())
    await processor.forward(_message(), manager)
    assert '_model_auth' not in manager.deliver_to_message_handler.await_args.args[0].params


async def test_client_credential_removal_is_audited(caplog):
    from agent_gateway.application import inbound
    from agent_gateway.auth.credential_access import ModelCredential

    access = SimpleNamespace(request_model_auth=AsyncMock(return_value=ModelCredential('https://test.invalid', 'server-token', 'ref')))
    processor = inbound.MessagePreprocessor(AsyncMock(), lambda msg: msg, lambda msg: None).with_credential_access(access)
    manager = SimpleNamespace(deliver_to_message_handler=AsyncMock())

    with caplog.at_level(logging.WARNING, logger='gateway'):
        await processor.forward(_message(), manager)

    assert any(
        record.getMessage() == "[ModelAuth] 客户端请求里带了 _model_auth，已丢弃（该键只允许服务端写）"
        for record in caplog.records
    )


async def test_new_credential_path_retains_resume_and_work_mode_semantics():
    from agent_gateway.application import inbound
    events = []
    access = SimpleNamespace(request_model_auth=AsyncMock(return_value=None))
    processor = inbound.MessagePreprocessor(AsyncMock(), lambda msg: msg, lambda msg: events.append('mode')).with_credential_access(access)
    manager = SimpleNamespace(deliver_to_message_handler=AsyncMock())
    await processor.forward(_message(method=ReqMethod.CHAT_RESUME), manager)
    sent = manager.deliver_to_message_handler.await_args.args[0]
    assert sent.req_method == ReqMethod.CHAT_CANCEL and sent.params['intent'] == 'resume'
    assert events == []
    await processor.forward(_message(method=ReqMethod.SESSION_CREATE), manager)
    assert events == ['mode']


async def test_handler_uses_explicit_access_and_original_as_transport(monkeypatch):
    from agent_gateway.auth.credential_access import CredentialUpdate
    from agent_gateway.message_handler.message_handler import MessageHandler
    handler = object.__new__(MessageHandler)
    handler._send_non_stream_agent_request = AsyncMock(return_value=SimpleNamespace(ok=True))
    access = SimpleNamespace(credential_update_for_ref=AsyncMock(return_value=CredentialUpdate('ref', api_key='new-token')))
    handler.set_credential_access(access)
    monkeypatch.setattr(
        'agent_gateway.auth.passthrough.refreshed_credential_for_ref',
        lambda ref, *, service=None: pytest.fail('global refresh used'),
    )
    assert await handler._handle_login_credential_refresh_push(SimpleNamespace(payload={'event_type': 'auth.credential.refresh', 'credential_ref': 'ref'})) is True
    access.credential_update_for_ref.assert_awaited_once_with('ref')
    env = handler._send_non_stream_agent_request.await_args.args[0]
    assert env.method == 'auth.credentials.update'
    assert env.params == {'credential_ref': 'ref', 'api_key': 'new-token'}


async def test_explicit_sink_does_not_look_up_singleton(monkeypatch):
    from agent_gateway.auth.credential_access import CredentialUpdate
    from agent_gateway.application.services import MessageHandlerCredentialUpdateSink
    from agent_gateway.message_handler.message_handler import MessageHandler
    handler = SimpleNamespace(push_login_credential_update=AsyncMock(return_value=True))
    monkeypatch.setattr(MessageHandler, 'try_get_instance', lambda: pytest.fail('singleton lookup'))
    assert await MessageHandlerCredentialUpdateSink(handler).publish(CredentialUpdate('ref', revoked=True)) is True
    handler.push_login_credential_update.assert_awaited_once_with({'credential_ref': 'ref', 'revoked': True})
