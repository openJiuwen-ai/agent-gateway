# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Behavior of the local credential boundary, including failure fallback."""
from __future__ import annotations

import asyncio
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
import time

import pytest

from agent_gateway.auth import model_catalog, apig, login_credentials
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore


def _access(monkeypatch, *, expired=False):
    from agent_gateway.auth.local_credential_access import LocalCredentialAccess
    store = AuthSessionStore(persist=False)
    session = store.create(SimpleNamespace(user_id='account-a', user_name='A', credential=Credential(
        id_token='test-token-a', refresh_token='test-refresh', expires_at=time.time() + (-60 if expired else 3600),
    )))
    service = AuthService(store=store)
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [model_catalog.LoginModel('login-model', 'Login')])
    monkeypatch.setattr(apig, 'resolve_apig_config', lambda **k: SimpleNamespace(invoke_base_url='https://test.invalid/invoke'))
    monkeypatch.setattr(login_credentials, '_ref_key_cache', b'k' * 32)
    return LocalCredentialAccess(lambda: service), service, session


def test_credential_values_are_immutable_and_hide_tokens():
    from agent_gateway.auth.credential_access import ModelCredential, CredentialUpdate
    model = ModelCredential('https://test.invalid', 'test-secret-token', 'ref')
    update = CredentialUpdate('ref', api_key='test-secret-token')
    assert 'test-secret-token' not in repr(model) + repr(update)
    with pytest.raises(FrozenInstanceError):
        model.api_key = 'changed'
    assert update.to_params() == {'credential_ref': 'ref', 'api_key': 'test-secret-token'}
    assert CredentialUpdate('ref', revoked=True).to_params() == {'credential_ref': 'ref', 'revoked': True}
    with pytest.raises(ValueError):
        CredentialUpdate('ref', api_key='key', revoked=True)
    with pytest.raises(ValueError):
        CredentialUpdate('ref')


async def test_request_uses_injected_service_without_global_fallback(monkeypatch):
    access, service, session = _access(monkeypatch)
    monkeypatch.setattr('agent_gateway.auth.service.get_auth_service', lambda: pytest.fail('global service used'))
    result = await access.request_model_auth(session.session_id, 'login-model#2')
    assert result.api_key == session.credential.id_token
    assert result.credential_ref == login_credentials.credential_ref_for_user(session.user_id)
    assert result.api_base == 'https://test.invalid/invoke'
    assert await access.request_model_auth(None, 'login-model') is None
    assert await access.request_model_auth('wrong-session', 'login-model') is None


async def test_self_configured_model_does_not_read_session_or_spawn_thread(monkeypatch):
    access, service, session = _access(monkeypatch)
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [])
    monkeypatch.setattr(asyncio, 'to_thread', lambda *a, **k: pytest.fail('ordinary request spawned thread'))
    monkeypatch.setattr(service._store, 'get', lambda *a, **k: pytest.fail('ordinary request read session'))
    assert await access.request_model_auth(session.session_id, 'configured-model') is None


async def test_model_classification_uses_the_injected_service(monkeypatch):
    access, service, session = _access(monkeypatch)
    services = []

    def models(session_id, allow_refresh=False, *, service=None):
        services.append(service)
        return [model_catalog.LoginModel('login-model', 'Login')] if service is expected_service else []

    expected_service = service
    monkeypatch.setattr(model_catalog, 'get_models', models)
    result = await access.request_model_auth(session.session_id, 'login-model')
    assert result is not None
    assert result.api_key == session.credential.id_token
    assert services and all(current is service for current in services)


async def test_expired_request_refreshes_off_loop_and_uses_updated_credential(monkeypatch):
    import threading
    access, service, session = _access(monkeypatch, expired=True)
    loop_thread = threading.get_ident()
    seen = []
    def refresh(current):
        seen.append(threading.get_ident())
        return service._store.update_credential(current.session_id, Credential(
            id_token='renewed-token', refresh_token='test-refresh', expires_at=time.time()+3600,
        ))
    monkeypatch.setattr(service, 'try_refresh', refresh)
    assert (await access.request_model_auth(session.session_id, 'login-model')).api_key == 'renewed-token'
    assert seen and seen[0] != loop_thread


async def test_expired_refresh_failure_forwards_without_credentials(monkeypatch):
    access, service, session = _access(monkeypatch, expired=True)
    def failed(*a, **k):
        raise ConnectionError('offline')
    monkeypatch.setattr(service, 'try_refresh', failed)
    monkeypatch.setattr(service, '_refresh_in_background', lambda s: None)
    assert await access.request_model_auth(session.session_id, 'login-model') is None


async def test_refresh_timeout_falls_back_to_now_usable_original_store(monkeypatch):
    from agent_gateway.auth import local_credential_access as local
    access, service, session = _access(monkeypatch, expired=True)
    monkeypatch.setattr(local, '_LOGIN_REFRESH_WAIT_S', 0.01)
    def refresh(current):
        # A concurrent refresh can complete while this waiter times out.
        service._store.update_credential(current.session_id, Credential(
            id_token='usable-token', refresh_token='test-refresh', expires_at=time.time()+3600,
        ))
        time.sleep(0.04)
    monkeypatch.setattr(service, 'try_refresh', refresh)
    assert (await access.request_model_auth(session.session_id, 'login-model')).api_key == 'usable-token'


async def test_credential_ref_refresh_and_revocation_use_injected_store(monkeypatch):
    access, service, session = _access(monkeypatch)
    monkeypatch.setattr('agent_gateway.auth.service.get_auth_service', lambda: pytest.fail('global service used'))
    ref = login_credentials.credential_ref_for_user(session.user_id)
    update = await access.credential_update_for_ref(ref)
    assert update.api_key == 'test-token-a'
    service.logout(session.session_id)
    assert (await access.credential_update_for_ref(ref)).revoked is True
    assert await access.credential_update_for_ref('not-a-ref') is None


def test_catalog_refresh_from_injected_service_never_resolves_global(monkeypatch):
    _, service, session = _access(monkeypatch)
    seen = []
    def refresh(session_id=None, *, service=None):
        seen.append((session_id, service))
        return [model_catalog.LoginModel('login-model', 'Login')]
    monkeypatch.setattr(model_catalog, 'refresh', refresh)
    assert service.refresh_model_catalog(session.session_id) == 1
    assert seen == [(session.session_id, service)]


@pytest.mark.parametrize("api_key,revoked", [("", True), ("", False), ("key", True), (None, False)])
def test_update_rejects_ambiguous_or_empty_credentials(api_key, revoked):
    from agent_gateway.auth.credential_access import CredentialUpdate

    with pytest.raises(ValueError):
        CredentialUpdate("ref", api_key=api_key, revoked=revoked)


def test_credential_modules_import_without_initializing_login(tmp_path):
    import os
    import subprocess
    import sys

    env = os.environ.copy()
    for key in ('JIUWENSWARM_HOME', 'JIUWENSWARM_DATA_DIR', 'AGENTOS_GATEWAY_LOG_DIR'):
        env[key] = str(tmp_path / key)
    result = subprocess.run(
        [sys.executable, '-c', "import importlib, os, sys\nfrom pathlib import Path\nimportlib.import_module('agent_gateway.auth.credential_access')\nimportlib.import_module('agent_gateway.auth.local_credential_access')\nimportlib.import_module('agent_gateway.auth.session_login_bindings')\nassert 'agent_gateway.auth.service' not in sys.modules\nassert 'agent_gateway.app_gateway' not in sys.modules\nfor key in ('JIUWENSWARM_HOME', 'JIUWENSWARM_DATA_DIR', 'AGENTOS_GATEWAY_LOG_DIR'):\n    assert not Path(os.environ[key]).exists(), key\n"],
        env=env, capture_output=True, text=True, timeout=15,
    )
    assert result.returncode == 0, result.stderr


async def test_login_session_binding_ref_is_strict_and_uses_explicit_service(monkeypatch):
    access, service, session = _access(monkeypatch)
    monkeypatch.setattr('agent_gateway.auth.service.get_auth_service', lambda: pytest.fail('global service used'))
    expected = login_credentials.credential_ref_for_user(session.user_id)
    assert await access.credential_ref_for_login_session(session.session_id) == expected
    assert await access.credential_ref_for_login_session('missing-session') is None
    assert await access.credential_ref_for_login_session(None) is None
    service.logout(session.session_id)
    assert await access.credential_ref_for_login_session(session.session_id) is None


async def test_login_session_binding_does_not_block_for_expired_refresh(monkeypatch):
    access, service, session = _access(monkeypatch, expired=True)
    monkeypatch.setattr(service, '_refresh_in_background', lambda current: None)
    monkeypatch.setattr(service, 'try_refresh', lambda *a, **k: pytest.fail('binding blocked for refresh'))
    assert await access.credential_ref_for_login_session(session.session_id) is None


async def test_model_auth_by_ref_reuses_only_bound_account_off_loop(monkeypatch):
    import threading

    access, service, session = _access(monkeypatch)
    loop_thread = threading.get_ident()
    threads = []
    original = service.try_refresh

    def refresh(current, **kwargs):
        threads.append(threading.get_ident())
        return original(current, **kwargs)

    monkeypatch.setattr(service, 'try_refresh', refresh)
    monkeypatch.setattr('agent_gateway.auth.service.get_auth_service', lambda: pytest.fail('global service used'))
    ref = login_credentials.credential_ref_for_user(session.user_id)
    auth = await access.model_auth_for_ref(ref)
    assert auth.to_params() == {
        'api_base': 'https://test.invalid/invoke', 'api_key': 'test-token-a', 'credential_ref': ref,
    }
    assert threads and all(value != loop_thread for value in threads)
    service.logout(session.session_id)
    assert await access.model_auth_for_ref(ref) is None


async def test_model_auth_invalid_ref_does_not_initialize_login(monkeypatch):
    from agent_gateway.auth.local_credential_access import LocalCredentialAccess

    access = LocalCredentialAccess(lambda: pytest.fail('invalid ref initialized login'))
    assert await access.model_auth_for_ref('') is None
    assert await access.model_auth_for_ref('invalid') is None
    assert await access.credential_ref_for_login_session(None) is None


async def test_bound_ref_uses_usable_old_token_on_refresh_failure(monkeypatch):
    access, service, session = _access(monkeypatch)
    from agent_gateway.auth.login_credentials import REFRESH_AHEAD_S

    service._store.update_credential(session.session_id, Credential(
        id_token='still-usable-token', refresh_token='refresh', expires_at=time.time() + REFRESH_AHEAD_S / 2,
    ))
    monkeypatch.setattr(service._flow, 'refresh', lambda credential: None)
    ref = login_credentials.credential_ref_for_user(session.user_id)
    assert (await access.model_auth_for_ref(ref)).api_key == 'still-usable-token'
    service._store.update_credential(session.session_id, Credential(
        id_token='expired', refresh_token='refresh', expires_at=time.time() - 1,
    ))
    assert await access.model_auth_for_ref(ref) is None


async def test_bound_ref_without_apig_does_not_return_credentials(monkeypatch):
    access, service, session = _access(monkeypatch)
    monkeypatch.setattr(apig, 'resolve_apig_config', lambda **kwargs: None)
    assert await access.model_auth_for_ref(login_credentials.credential_ref_for_user(session.user_id)) is None
