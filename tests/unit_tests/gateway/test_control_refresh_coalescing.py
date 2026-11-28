"""Race and coalescing behavior for explicit AS credential refresh."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import threading
import time
from types import SimpleNamespace

import pytest

from agent_gateway.auth import apig, login_credentials, model_catalog
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.control_credentials import ControlCredentialServices
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.control_service import ControlServiceIdentity


@pytest.fixture
async def refresh_services(monkeypatch):
    store = AuthSessionStore(persist=False)

    def login():
        return store.create(SimpleNamespace(
            user_id='refresh-account',
            user_name='Refresh Account',
            credential=Credential(
                id_token='refresh-token',
                refresh_token='refresh-secret',
                expires_at=time.time() + 3600,
            ),
        ))

    session = login()
    service = AuthService(store=store)
    monkeypatch.setattr(login_credentials, '_ref_key_cache', b'r' * 32)
    monkeypatch.setattr(model_catalog, 'get_models', lambda *args, **kwargs: [
        model_catalog.LoginModel('login-model', 'Login'),
    ])
    monkeypatch.setattr(apig, 'resolve_apig_config', lambda **kwargs: SimpleNamespace(
        invoke_base_url='https://test.invalid/invoke',
    ))
    services = ControlCredentialServices(lambda: service, server_id='as-one')
    identity = ControlServiceIdentity('group', 0, 1, 101, 100)
    handlers = services.handlers_for(identity)
    try:
        yield services, service, store, session, login, identity, handlers
    finally:
        await services.stop(timeout=2)


async def test_missing_session_refresh_keeps_preawait_revision_after_relogin(refresh_services):
    services, _service, _store, _session, login, _identity, handlers = refresh_services
    first = await handlers['credentials.request_model_auth']({
        'auth_session': _session.session_id,
        'model_name': 'login-model',
    })
    credential_ref = first['credential']['credential_ref']
    await services.logout(_session.session_id)

    original_session_for_ref = services._session_for_ref
    entered = asyncio.Event()
    release = asyncio.Event()

    async def no_session_after_relogin(value):
        entered.set()
        await release.wait()
        return None

    services._session_for_ref = no_session_after_relogin
    pending = asyncio.create_task(handlers['credentials.refresh_for_ref']({
        'server_id': 'as-one', 'credential_ref': credential_ref,
    }))
    try:
        await entered.wait()
        newer = login()
        second = await handlers['credentials.request_model_auth']({
            'auth_session': newer.session_id, 'model_name': 'login-model',
        })
        release.set()
        result = await pending
    finally:
        services._session_for_ref = original_session_for_ref
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
    assert result['status'] == 'ok'
    assert result['update'] == {'credential_ref': credential_ref, 'revoked': True}
    assert result['ref_revision'] == first['lease']['ref_revision']
    assert second['lease']['ref_revision'] > first['lease']['ref_revision']


async def test_same_ref_epoch_refreshes_share_one_pool_future(refresh_services, monkeypatch):
    services, service, store, session, _login, _identity, handlers = refresh_services
    issued = await handlers['credentials.request_model_auth']({
        'auth_session': session.session_id, 'model_name': 'login-model',
    })
    credential_ref = issued['credential']['credential_ref']
    store.update_credential(session.session_id, Credential(
        id_token='ten-minutes-remaining',
        refresh_token='refresh-secret',
        expires_at=time.time() + 600,
    ))

    entered = threading.Event()
    release = threading.Event()
    calls = []

    def refresh(credential):
        calls.append(credential.id_token)
        entered.set()
        assert release.wait(3)
        return Credential(
            id_token='renewed-shared-token',
            refresh_token=credential.refresh_token,
            expires_at=time.time() + 3600,
        )

    monkeypatch.setattr(service._flow, 'refresh', refresh)
    requests = [asyncio.create_task(handlers['credentials.refresh_for_ref']({
        'server_id': 'as-one', 'credential_ref': credential_ref,
    })) for _ in range(4)]
    try:
        await asyncio.to_thread(entered.wait, 3)
        await asyncio.sleep(0.05)
        assert services.pending_work_count == 1
        assert len(calls) == 1
        other = store.create(SimpleNamespace(
            user_id='other-refresh-account',
            user_name='Other Refresh Account',
            credential=Credential(
                id_token='other-account-token',
                refresh_token='other-refresh-secret',
                expires_at=time.time() + 3600,
            ),
        ))
        other_result = await handlers['credentials.request_model_auth']({
            'auth_session': other.session_id, 'model_name': 'login-model',
        })
        assert other_result['status'] == 'ok'
    finally:
        release.set()
        results = await asyncio.gather(*requests)
    assert {result['update']['api_key'] for result in results} == {'renewed-shared-token'}
    assert len(calls) == 1
    assert services.pending_work_count == 0


async def test_refresh_timeout_keeps_actual_pool_work_until_completion(refresh_services, monkeypatch):
    services, service, store, session, _login, _identity, handlers = refresh_services
    issued = await handlers['credentials.request_model_auth']({
        'auth_session': session.session_id, 'model_name': 'login-model',
    })
    credential_ref = issued['credential']['credential_ref']
    store.update_credential(session.session_id, Credential(
        id_token='ten-minutes-remaining',
        refresh_token='refresh-secret',
        expires_at=time.time() + 600,
    ))

    entered = threading.Event()
    release = threading.Event()

    def refresh(credential):
        entered.set()
        assert release.wait(3)
        return Credential(
            id_token='renewed-after-timeout',
            refresh_token=credential.refresh_token,
            expires_at=time.time() + 3600,
        )

    monkeypatch.setattr(service._flow, 'refresh', refresh)

    @asynccontextmanager
    async def immediate_timeout(_timeout):
        raise TimeoutError
        yield

    original_timeout = asyncio.timeout
    monkeypatch.setattr(asyncio, 'timeout', immediate_timeout)
    timed_out = asyncio.create_task(handlers['credentials.refresh_for_ref']({
        'server_id': 'as-one', 'credential_ref': credential_ref,
    }))
    try:
        await timed_out
        await asyncio.to_thread(entered.wait, 3)
        assert timed_out.result() == {'status': 'invalid'}
        assert services.pending_work_count == 1
    finally:
        monkeypatch.setattr(asyncio, 'timeout', original_timeout)
        release.set()

    joined = await handlers['credentials.refresh_for_ref']({
        'server_id': 'as-one', 'credential_ref': credential_ref,
    })
    assert joined['update']['api_key'] == 'renewed-after-timeout'
    assert services.pending_work_count == 0


async def test_refresh_cancel_keeps_actual_pool_work_until_completion(refresh_services, monkeypatch):
    services, service, store, session, _login, _identity, handlers = refresh_services
    issued = await handlers['credentials.request_model_auth']({
        'auth_session': session.session_id, 'model_name': 'login-model',
    })
    credential_ref = issued['credential']['credential_ref']
    store.update_credential(session.session_id, Credential(
        id_token='ten-minutes-remaining',
        refresh_token='refresh-secret',
        expires_at=time.time() + 600,
    ))

    entered = threading.Event()
    release = threading.Event()

    def refresh(credential):
        entered.set()
        assert release.wait(3)
        return Credential(
            id_token='renewed-after-cancel',
            refresh_token=credential.refresh_token,
            expires_at=time.time() + 3600,
        )

    monkeypatch.setattr(service._flow, 'refresh', refresh)
    cancelled = asyncio.create_task(handlers['credentials.refresh_for_ref']({
        'server_id': 'as-one', 'credential_ref': credential_ref,
    }))
    try:
        await asyncio.to_thread(entered.wait, 3)
        cancelled.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled
        assert services.pending_work_count == 1
    finally:
        release.set()
        await asyncio.gather(cancelled, return_exceptions=True)

    release.set()
    joined = await handlers['credentials.refresh_for_ref']({
        'server_id': 'as-one', 'credential_ref': credential_ref,
    })
    assert joined['update']['api_key'] == 'renewed-after-cancel'
    assert services.pending_work_count == 0


async def test_relogin_refresh_does_not_share_old_epoch_future(refresh_services, monkeypatch):
    services, service, store, session, login, _identity, handlers = refresh_services
    first = await handlers['credentials.request_model_auth']({
        'auth_session': session.session_id, 'model_name': 'login-model',
    })
    credential_ref = first['credential']['credential_ref']
    store.update_credential(session.session_id, Credential(
        id_token='old-epoch-token',
        refresh_token='refresh-secret',
        expires_at=time.time() + 600,
    ))

    entered = threading.Event()
    release = threading.Event()
    refreshed_tokens = []

    def refresh(credential):
        refreshed_tokens.append(credential.id_token)
        entered.set()
        assert release.wait(3)
        return Credential(
            id_token='renewed-' + credential.id_token,
            refresh_token=credential.refresh_token,
            expires_at=time.time() + 3600,
        )

    monkeypatch.setattr(service._flow, 'refresh', refresh)
    old_refresh = asyncio.create_task(handlers['credentials.refresh_for_ref']({
        'server_id': 'as-one', 'credential_ref': credential_ref,
    }))
    new_refresh: asyncio.Task | None = None
    try:
        await asyncio.to_thread(entered.wait, 3)
        newer = login()
        store.update_credential(newer.session_id, Credential(
            id_token='new-epoch-token',
            refresh_token='refresh-secret',
            expires_at=time.time() + 600,
        ))
        second = await handlers['credentials.request_model_auth']({
            'auth_session': newer.session_id, 'model_name': 'login-model',
        })
        assert second['lease']['ref_revision'] > first['lease']['ref_revision']
        new_refresh = asyncio.create_task(handlers['credentials.refresh_for_ref']({
            'server_id': 'as-one', 'credential_ref': credential_ref,
        }))
        await asyncio.sleep(0.05)
        assert services.pending_work_count == 2
    finally:
        release.set()
        tasks = [old_refresh] if new_refresh is None else [old_refresh, new_refresh]
        await asyncio.gather(*tasks, return_exceptions=True)

    release.set()
    old_result, new_result = await asyncio.gather(old_refresh, new_refresh)
    assert old_result == {'status': 'invalid'}
    assert new_result['update']['api_key'] == 'renewed-new-epoch-token'
    assert refreshed_tokens == ['old-epoch-token', 'new-epoch-token']
    assert services.pending_work_count == 0
