# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Preserve persisted session login bindings through the explicit local port."""
from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.auth import apig, login_credentials, model_catalog, session_owners
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore


@pytest.fixture(autouse=True)
def _isolated_bindings(monkeypatch, tmp_path):
    monkeypatch.setattr('agent_gateway.auth.session_store.auth_dir', lambda: tmp_path)
    session_owners.reset_for_test()
    yield
    session_owners.reset_for_test()


def _bindings(monkeypatch):
    from agent_gateway.auth.session_login_bindings import LocalSessionLoginBindings

    store = AuthSessionStore(persist=False)
    session = store.create(SimpleNamespace(user_id='model-account', user_name='A', credential=Credential(
        id_token='binding-server-token', refresh_token='binding-refresh', expires_at=time.time() + 3600,
    )))
    service = AuthService(store=store)
    monkeypatch.setattr(login_credentials, '_ref_key_cache', b'b' * 32)
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [model_catalog.LoginModel('login-model', 'Login')])
    monkeypatch.setattr(apig, 'resolve_apig_config', lambda **k: SimpleNamespace(invoke_base_url='https://test.invalid'))
    monkeypatch.setattr('agent_gateway.auth.service.get_auth_service', lambda: pytest.fail('global login service used'))
    return LocalSessionLoginBindings(lambda: service), service, session


async def test_local_bindings_reuse_old_disk_format_and_ttl(monkeypatch, tmp_path):
    from agent_gateway.auth.session_login_bindings import LocalSessionLoginBindings, SessionLoginBinding

    bindings = LocalSessionLoginBindings(lambda: pytest.fail('binding persistence initialized login'))
    monkeypatch.setattr(asyncio, 'to_thread', lambda *a, **k: pytest.fail('local binding lookup spawned a thread'))
    ref = 'a' * 32
    (tmp_path / 'session_owners.json').write_text(json.dumps({
        'legacy-session': {'ref': ref, 'model': 'login-model', 'at': time.time()},
        'expired-session': {'ref': ref, 'model': 'login-model', 'at': time.time() - session_owners._TTL_S - 1},
    }))
    value = await bindings.lookup('legacy-session')
    assert value == SessionLoginBinding(ref, 'login-model')
    with pytest.raises(FrozenInstanceError):
        value.model_name = 'changed'
    assert await bindings.lookup('expired-session') is None
    await bindings.remember('new-session', ref, 'login-model#3')
    assert await bindings.lookup('new-session') == value
    saved = json.loads((tmp_path / 'session_owners.json').read_text())
    assert set(saved['new-session']) == {'ref', 'model', 'at'}
    assert 'expired-session' not in saved
    session_owners.reset_for_test()
    assert await bindings.lookup('new-session') == value


async def test_binding_auth_preserves_catalog_check_and_same_account(monkeypatch):
    from agent_gateway.auth.session_login_bindings import SessionLoginBinding

    bindings, service, session = _bindings(monkeypatch)
    binding = SessionLoginBinding(login_credentials.credential_ref_for_user(session.user_id), 'login-model')
    auth = await bindings.model_auth_for_binding(binding)
    assert auth.api_key == 'binding-server-token'
    assert auth.credential_ref == binding.credential_ref
    assert 'binding-server-token' not in repr(auth)
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [])
    assert await bindings.model_auth_for_binding(binding) is None
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [model_catalog.LoginModel('login-model', 'Login')])
    service.logout(session.session_id)
    assert await bindings.model_auth_for_binding(binding) is None


def _handler(port, *, model='login-model'):
    from agent_gateway.message_handler.message_handler import MessageHandler

    handler = object.__new__(MessageHandler)
    handler.agent_client = None
    handler._effective_session_model = AsyncMock(return_value=model)
    handler._session_login_bindings = port
    return handler


def _env(channel='feishu', params=None):
    return SimpleNamespace(channel=channel, session_id='session-a', method='chat.send',
                           params=dict(params or {}), user_id='business-user')


async def test_handler_remembers_and_reuses_explicit_bindings_not_global(monkeypatch):
    from agent_gateway.auth.credential_access import ModelCredential
    from agent_gateway.auth.session_login_bindings import SessionLoginBinding

    for method in ('remember', 'lookup', 'login_auth_for_owner'):
        monkeypatch.setattr(session_owners, method, lambda *a: pytest.fail('legacy binding helper used'))
    ref = 'a' * 32
    binding = SessionLoginBinding(ref, 'login-model')
    port = SimpleNamespace(remember=AsyncMock(), lookup=AsyncMock(return_value=binding),
                           model_auth_for_binding=AsyncMock(return_value=ModelCredential('https://test.invalid', 'token', ref)))
    handler = _handler(port)
    web = _env('web', {'model_name': 'login-model#3', '_model_auth': {'credential_ref': ref}})
    await handler._apply_session_login_owner(web)
    port.remember.assert_awaited_once_with('session-a', ref, 'login-model#3')
    im = _env()
    await handler._apply_session_login_owner(im)
    assert im.params['_model_auth']['api_key'] == 'token'
    assert im.params['model_name'] == 'login-model'
    port.model_auth_for_binding.assert_awaited_once_with(binding)
    handler._effective_session_model.assert_awaited_once_with('session-a', im.params, user_id='business-user')
    # A Web request with no credential never borrows the previous login.
    port.lookup.reset_mock()
    await handler._apply_session_login_owner(_env('web', {'_login_required_hint': 'forged'}))
    port.lookup.assert_not_awaited()


def test_message_handler_requires_explicit_session_login_bindings(monkeypatch):
    from agent_gateway.message_handler.message_handler import MessageHandler

    monkeypatch.setattr(MessageHandler, "_instance", None)

    with pytest.raises(TypeError, match="session_login_bindings"):
        MessageHandler(SimpleNamespace())


@pytest.mark.parametrize('case', ['missing', 'mismatch', 'unknown', 'revoked', 'failure', 'timeout'])
async def test_handler_preserves_binding_hints_and_refresh_timeout(monkeypatch, case):
    from agent_gateway.auth.session_login_bindings import SessionLoginBinding
    from agent_gateway.message_handler import message_handler as module
    from e2a.constants import E2A_LOGIN_REQUIRED_HINT_PARAM_KEY

    binding = SessionLoginBinding('a' * 32, 'login-model')
    port = SimpleNamespace(remember=AsyncMock(), lookup=AsyncMock(return_value=None if case == 'missing' else binding),
                           model_auth_for_binding=AsyncMock(return_value=None))
    model = None if case == 'unknown' else 'self-model' if case == 'mismatch' else 'login-model'
    handler = _handler(port, model=model)
    if case == 'failure':
        port.model_auth_for_binding.side_effect = ConnectionError('offline')
    if case == 'timeout':
        monkeypatch.setattr(module, '_SESSION_OWNER_AUTH_WAIT_S', 0.01)
        async def slow(*args):
            await asyncio.sleep(0.05)
        port.model_auth_for_binding.side_effect = slow
    env = _env(params={E2A_LOGIN_REQUIRED_HINT_PARAM_KEY: 'forged'})
    await handler._apply_session_login_owner(env)
    if case == 'unknown':
        assert E2A_LOGIN_REQUIRED_HINT_PARAM_KEY not in env.params
    else:
        expected = module._LOGIN_HINT_NO_OWNER if case in ('missing', 'mismatch') else (
            module._LOGIN_HINT_RETRY if case in ('failure', 'timeout') else module._LOGIN_HINT_OWNER_LOGGED_OUT)
        assert env.params[E2A_LOGIN_REQUIRED_HINT_PARAM_KEY] == expected
    assert '_model_auth' not in env.params
    if case in ('missing', 'mismatch', 'unknown'):
        port.model_auth_for_binding.assert_not_awaited()


async def test_binding_late_refresh_cannot_revive_logged_out_session(monkeypatch):
    import threading

    from agent_gateway.auth.session_login_bindings import SessionLoginBinding

    bindings, service, session = _bindings(monkeypatch)
    original_resolve = service.resolve_session
    entered = threading.Event()
    release = threading.Event()
    # Force the original refresh path rather than mocking its storage guard.
    service._store.update_credential(session.session_id, Credential(
        id_token='expired', refresh_token='refresh', expires_at=time.time() - 30,
    ))
    def refresh_remote(credential):
        entered.set()
        assert release.wait(3)
        return Credential(id_token='too-late-token', refresh_token='refresh', expires_at=time.time() + 3600)
    monkeypatch.setattr(service._flow, 'refresh', refresh_remote)
    binding = SessionLoginBinding(login_credentials.credential_ref_for_user(session.user_id), 'login-model')
    task = asyncio.create_task(bindings.model_auth_for_binding(binding))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        service.logout(session.session_id)
    finally:
        release.set()
    result = await task
    assert result is None
    assert original_resolve(session.session_id) is None
