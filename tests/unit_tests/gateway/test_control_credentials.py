"""Control-owned credential and binding behavior over the private service."""

import asyncio
import json
import os
import socket
import time
import threading
from types import SimpleNamespace

import pytest

from agent_gateway.auth import apig, login_credentials, model_catalog, session_owners
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceAdmission,
    ControlServiceClient,
    ControlServiceIdentity,
    ControlServiceOperationError,
    ControlServiceServer,
)


@pytest.fixture
async def authority(monkeypatch, tmp_path):
    from agent_gateway.auth.control_credentials import ControlCredentialServices

    monkeypatch.setattr('agent_gateway.auth.session_store.auth_dir', lambda: tmp_path)
    session_owners.reset_for_test()
    store = AuthSessionStore(persist=False)
    sessions = [store.create(SimpleNamespace(
        user_id=f'account-{index}', user_name=f'Account {index}',
        credential=Credential(
            id_token=f'control-canary-token-{index}', refresh_token=f'control-canary-refresh-{index}',
            expires_at=time.time() + 3600,
        ),
    )) for index in range(2)]
    service = AuthService(store=store)
    monkeypatch.setattr(login_credentials, '_ref_key_cache', b'c' * 32)
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [model_catalog.LoginModel('login-model', 'Login')])
    monkeypatch.setattr(apig, 'resolve_apig_config', lambda **k: SimpleNamespace(invoke_base_url='https://test.invalid/invoke'))
    monkeypatch.setattr('agent_gateway.auth.service.get_auth_service', lambda: pytest.fail('global authority used'))
    services = ControlCredentialServices(lambda: service)
    try:
        yield services, service, sessions, tmp_path
    finally:
        await services.stop(timeout=2)
        session_owners.reset_for_test()


async def test_requests_are_account_scoped_and_not_required_is_not_invalid(authority):
    services, _service, sessions, _path = authority
    request = services.handlers['credentials.request_model_auth']
    for index, session in enumerate(sessions):
        result = await request({'auth_session': session.session_id, 'model_name': 'login-model#2'})
        assert result == {'status': 'ok', 'credential': {
            'api_base': 'https://test.invalid/invoke', 'api_key': f'control-canary-token-{index}',
            'credential_ref': login_credentials.credential_ref_for_user(f'account-{index}'),
        }}
    assert await request({'auth_session': None, 'model_name': 'login-model'}) == {'status': 'not_required'}
    assert await request({'auth_session': 'missing', 'model_name': 'configured'}) == {'status': 'not_required'}
    assert await request({'auth_session': 'missing', 'model_name': 'login-model'}) == {'status': 'invalid'}


async def test_bindings_preserve_disk_schema_and_verify_current_binding(authority):
    services, service, sessions, path = authority
    ref = login_credentials.credential_ref_for_user(sessions[0].user_id)
    remember = services.handlers['bindings.remember']
    lookup = services.handlers['bindings.lookup']
    resolve = services.handlers['bindings.resolve_model_auth']
    assert await remember({'session_id': 'chat', 'credential_ref': ref, 'model_name': 'login-model#2'}) == {'status': 'ok'}
    assert await lookup({'session_id': 'chat'}) == {
        'status': 'ok', 'binding': {'credential_ref': ref, 'model_name': 'login-model'},
    }
    saved = json.loads((path / 'session_owners.json').read_text())
    assert set(saved['chat']) == {'ref', 'model', 'at'}
    result = await resolve({'session_id': 'chat', 'credential_ref': ref, 'model_name': 'login-model'})
    assert result['credential']['api_key'] == 'control-canary-token-0'
    other_ref = login_credentials.credential_ref_for_user(sessions[1].user_id)
    assert await resolve({'session_id': 'chat', 'credential_ref': other_ref, 'model_name': 'login-model'}) == {'status': 'invalid'}
    assert await resolve({'session_id': 'missing', 'credential_ref': ref, 'model_name': 'login-model'}) == {'status': 'invalid'}
    service.logout(sessions[0].session_id)
    assert await resolve({'session_id': 'chat', 'credential_ref': ref, 'model_name': 'login-model'}) == {'status': 'invalid'}


async def test_expired_binding_is_not_resolved(authority):
    services, _service, sessions, path = authority
    ref = login_credentials.credential_ref_for_user(sessions[0].user_id)
    (path / 'session_owners.json').write_text(json.dumps({
        'expired': {'ref': ref, 'model': 'login-model', 'at': time.time() - session_owners._TTL_S - 1},
    }))
    assert await services.handlers['bindings.lookup']({'session_id': 'expired'}) == {'status': 'not_found'}
    assert await services.handlers['bindings.resolve_model_auth']({
        'session_id': 'expired', 'credential_ref': ref, 'model_name': 'login-model',
    }) == {'status': 'invalid'}


async def test_missing_binding_and_model_change_do_not_borrow_credentials(authority, monkeypatch):
    services, _service, sessions, _path = authority
    assert await services.handlers['bindings.lookup']({'session_id': 'missing'}) == {'status': 'not_found'}
    ref = login_credentials.credential_ref_for_user(sessions[0].user_id)
    await services.handlers['bindings.remember']({
        'session_id': 'chat', 'credential_ref': ref, 'model_name': 'login-model',
    })
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [])
    assert await services.handlers['bindings.resolve_model_auth']({
        'session_id': 'chat', 'credential_ref': ref, 'model_name': 'login-model',
    }) == {'status': 'invalid'}


@pytest.mark.parametrize('size', [4096, 4097])
async def test_tokens_at_wire_limit_are_not_truncated(authority, size):
    services, service, sessions, _path = authority
    service._store.update_credential(sessions[0].session_id, Credential(
        id_token='t' * size, refresh_token='control-canary-refresh', expires_at=time.time() + 3600,
    ))
    if size == 4096:
        value = await services.handlers['credentials.request_model_auth']({
            'auth_session': sessions[0].session_id, 'model_name': 'login-model',
        })
        assert len(value['credential']['api_key']) == 4096
    else:
        with pytest.raises(ValueError, match='invalid credential service value') as caught:
            await services.handlers['credentials.request_model_auth']({
                'auth_session': sessions[0].session_id, 'model_name': 'login-model',
            })
        assert 't' * size not in str(caught.value)


async def test_catalog_failure_does_not_become_not_required(authority, monkeypatch):
    services, _service, sessions, _path = authority
    def failed(*args, **kwargs):
        raise RuntimeError('catalog-canary-secret')
    monkeypatch.setattr(model_catalog, 'get_models', failed)
    left, right = socket.socketpair()
    identity = ControlServiceIdentity('catalog-group', 0, 1, os.getpid(), os.getpid())
    client = ControlServiceClient(ControlServiceChannel(left), identity)
    server = ControlServiceServer(
        ControlServiceChannel(right), identity, handlers=services.handlers, admission=ControlServiceAdmission(),
    )
    try:
        await asyncio.gather(server.start(), client.start())
        with pytest.raises(ControlServiceOperationError) as caught:
            await client.call('credentials.request_model_auth', {
                'auth_session': sessions[0].session_id, 'model_name': 'login-model',
            }, timeout=2)
        assert caught.value.error_code == 'CONTROL_SERVICE_FAILED'
        assert 'catalog-canary-secret' not in str(caught.value)
    finally:
        await client.stop()
        await server.stop()


@pytest.mark.parametrize('arguments', [
    {}, {'auth_session': 's', 'model_name': 'm', 'credential_ref': 'forged'},
    {'auth_session': 7, 'model_name': 'm'}, {'auth_session': 'x' * 257, 'model_name': 'm'},
    {'auth_session': 's', 'model_name': 'm' * 257},
])
async def test_bad_request_arguments_fail_without_echo(authority, arguments):
    services, _service, _sessions, _path = authority
    with pytest.raises(ValueError, match='invalid credential service value') as caught:
        await services.handlers['credentials.request_model_auth'](arguments)
    assert 'forged' not in str(caught.value)


async def test_real_service_keeps_unfinished_operations_disabled(authority):
    services, _service, sessions, _path = authority
    left, right = socket.socketpair()
    identity = ControlServiceIdentity('credential-group', 0, 1, os.getpid(), os.getpid())
    client = ControlServiceClient(ControlServiceChannel(left), identity)
    server = ControlServiceServer(
        ControlServiceChannel(right), identity, handlers=services.handlers, admission=ControlServiceAdmission(),
    )
    try:
        await asyncio.gather(server.start(), client.start())
        result = await client.call('credentials.request_model_auth', {
            'auth_session': sessions[0].session_id, 'model_name': 'login-model',
        }, timeout=2)
        assert result['credential']['api_key'] == 'control-canary-token-0'
        for operation in ('credentials.refresh_for_ref', 'catalog.list_login_models'):
            with pytest.raises(ControlServiceOperationError) as caught:
                await client.call(operation, {}, timeout=2)
            assert caught.value.error_code == 'OPERATION_NOT_ENABLED'
    finally:
        await client.stop()
        await server.stop()


def test_constructing_services_is_lazy_and_only_enables_four_operations():
    from agent_gateway.auth.control_credentials import ControlCredentialServices

    services = ControlCredentialServices(lambda: pytest.fail('construction initialized login'))
    assert set(services.handlers) == {
        'credentials.request_model_auth', 'bindings.remember', 'bindings.lookup', 'bindings.resolve_model_auth',
    }


async def test_timed_out_refresh_keeps_actual_work_budget(authority, monkeypatch):
    from agent_gateway.auth import local_credential_access
    from agent_gateway.auth.control_credentials import ControlCredentialServices
    from agent_gateway.processes.control_service import ControlServiceBusy, ControlServiceError

    _services, service, sessions, _path = authority
    started = threading.Event()
    release = threading.Event()
    def blocked(session):
        started.set()
        release.wait(3)
        return None
    monkeypatch.setattr(service, 'try_refresh', blocked)
    monkeypatch.setattr(service, '_refresh_in_background', lambda session: pytest.fail('unbudgeted background refresh'))
    monkeypatch.setattr(local_credential_access, '_LOGIN_REFRESH_WAIT_S', 0.03)
    for session in sessions:
        service._store.update_credential(session.session_id, Credential(
            id_token='expired', refresh_token='test-refresh', expires_at=time.time() - 10,
        ))
    services = ControlCredentialServices(lambda: service, max_blocking_work=1)
    arguments = {'auth_session': sessions[0].session_id, 'model_name': 'login-model'}
    try:
        assert await services.request_model_auth(arguments) == {'status': 'invalid'}
        assert started.is_set()
        assert services.pending_work_count == 1
        with pytest.raises(ControlServiceBusy):
            await services.request_model_auth({'auth_session': sessions[1].session_id, 'model_name': 'login-model'})
        # Real work uses a private executor, not the shared default executor.
        assert await asyncio.wait_for(asyncio.to_thread(lambda: 'available'), 0.2) == 'available'
        with pytest.raises(ControlServiceError):
            await services.stop(timeout=0.01)
        assert services.pending_work_count == 1
    finally:
        release.set()
        async with asyncio.timeout(2):
            while services.pending_work_count:
                await asyncio.sleep(0.01)


async def test_near_expiry_refresh_is_bounded_and_account_scoped(authority, monkeypatch):
    from agent_gateway.auth.control_credentials import ControlCredentialServices
    from agent_gateway.processes.control_service import ControlServiceBusy

    _services, service, sessions, _path = authority
    release = threading.Event()
    calls = []
    def blocked(session):
        calls.append(session.session_id)
        release.wait(3)
        return session
    monkeypatch.setattr(service, 'try_refresh', blocked)
    monkeypatch.setattr(service, '_refresh_in_background', lambda session: pytest.fail('unbudgeted background refresh'))
    for session in sessions:
        service._store.update_credential(session.session_id, Credential(
            id_token='still-valid-token', refresh_token='test-refresh', expires_at=time.time() + 60,
        ))
    services = ControlCredentialServices(lambda: service, max_blocking_work=1)
    try:
        for _index in range(4):
            result = await services.request_model_auth({'auth_session': sessions[0].session_id, 'model_name': 'login-model'})
            assert result['credential']['api_key'] == 'still-valid-token'
        assert services.pending_work_count == 1
        with pytest.raises(ControlServiceBusy):
            await services.request_model_auth({'auth_session': sessions[1].session_id, 'model_name': 'login-model'})
        assert calls == [sessions[0].session_id]
    finally:
        release.set()
        await services.stop(timeout=2)


async def test_binding_cancel_keeps_work_visible_until_thread_finishes(authority, monkeypatch):
    from agent_gateway.auth.control_credentials import ControlCredentialServices

    _services, service, sessions, _path = authority
    release = threading.Event()
    started = threading.Event()
    def blocked(session, **kwargs):
        started.set()
        release.wait(3)
        return session
    monkeypatch.setattr(service, 'try_refresh', blocked)
    services = ControlCredentialServices(lambda: service, max_blocking_work=1)
    ref = login_credentials.credential_ref_for_user(sessions[0].user_id)
    arguments = {'session_id': 'chat', 'credential_ref': ref, 'model_name': 'login-model'}
    await services.remember(arguments)
    task = asyncio.create_task(services.resolve_model_auth(arguments))
    try:
        async with asyncio.timeout(1):
            while not started.is_set():
                await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert services.pending_work_count == 1
    finally:
        release.set()
        await services.stop(timeout=2)


async def test_actual_work_busy_has_fixed_wire_code():
    from agent_gateway.processes.control_service import ControlServiceBusy

    async def busy(arguments):
        raise ControlServiceBusy()
    left, right = socket.socketpair()
    identity = ControlServiceIdentity('busy-group', 0, 1, os.getpid(), os.getpid())
    client = ControlServiceClient(ControlServiceChannel(left), identity)
    server = ControlServiceServer(
        ControlServiceChannel(right), identity, handlers={'bindings.lookup': busy}, admission=ControlServiceAdmission(),
    )
    try:
        await asyncio.gather(server.start(), client.start())
        with pytest.raises(ControlServiceBusy):
            await client.call('bindings.lookup', {'session_id': 'chat'}, timeout=1)
    finally:
        await client.stop()
        await server.stop()


async def test_late_refresh_failure_after_timeout_is_consumed(authority, monkeypatch):
    import gc

    from agent_gateway.auth import local_credential_access
    from agent_gateway.auth.control_credentials import ControlCredentialServices

    _services, service, sessions, _path = authority
    release = threading.Event()
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    abandoned = []
    loop.set_exception_handler(lambda _loop, context: abandoned.append(context))

    def fail_late(session):
        release.wait(3)
        raise RuntimeError('late-refresh-secret-canary')

    monkeypatch.setattr(service, 'try_refresh', fail_late)
    monkeypatch.setattr(local_credential_access, '_LOGIN_REFRESH_WAIT_S', 0.01)
    service._store.update_credential(sessions[0].session_id, Credential(
        id_token='expired', refresh_token='test-refresh', expires_at=time.time() - 10,
    ))
    services = ControlCredentialServices(lambda: service, max_blocking_work=1)
    try:
        assert await services.request_model_auth({
            'auth_session': sessions[0].session_id, 'model_name': 'login-model',
        }) == {'status': 'invalid'}
        assert services.pending_work_count == 1
        release.set()
        await services.stop(timeout=2)
        for _index in range(3):
            gc.collect()
            await asyncio.sleep(0)
        assert abandoned == []
    finally:
        release.set()
        await services.stop(timeout=2)
        loop.set_exception_handler(previous_handler)
