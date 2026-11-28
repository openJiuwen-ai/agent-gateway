"""Controlled credential lifecycle uses real stores and bound endpoint identities."""

import asyncio
from types import SimpleNamespace
import time

import pytest

from agent_gateway.auth import apig, login_credentials, model_catalog
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.control_credentials import ControlCredentialServices
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.control_service import ControlServiceIdentity


@pytest.fixture
async def lifecycle(monkeypatch):
    store = AuthSessionStore(persist=False)
    def login():
        return store.create(SimpleNamespace(user_id='account', user_name='Account',
            credential=Credential(id_token='canary', refresh_token='refresh-canary',
                                  expires_at=time.time() + 3600)))
    session = login()
    service = AuthService(store=store)
    monkeypatch.setattr(login_credentials, '_ref_key_cache', b'c' * 32)
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [model_catalog.LoginModel('login', 'Login')])
    monkeypatch.setattr(apig, 'resolve_apig_config', lambda **k: SimpleNamespace(invoke_base_url='https://test.invalid'))
    services = ControlCredentialServices(lambda: service, server_id='as-one')
    identity = ControlServiceIdentity('group', 0, 1, 101, 100)
    handlers = services.handlers_for(identity)
    try:
        yield services, service, session, login, identity, handlers
    finally:
        await services.stop(timeout=2)


async def test_response_records_real_target_before_return_and_relogin_advances(lifecycle):
    services, _service, session, login, identity, handlers = lifecycle
    request = handlers['credentials.request_model_auth']
    first = await request({'auth_session': session.session_id, 'model_name': 'login'})
    ref = first['credential']['credential_ref']
    assert first['lease'] == {'group_id': 'group', 'slot_id': 0, 'generation': 1,
        'server_id': 'as-one', 'credential_ref': ref, 'ref_revision': 1}
    assert services.registry.registered(identity, 'as-one', ref)
    new = login()
    second = await request({'auth_session': new.session_id, 'model_name': 'login'})
    assert second['credential']['credential_ref'] == ref
    assert second['lease']['ref_revision'] > first['lease']['ref_revision']
    with pytest.raises(ValueError):
        await request({'auth_session': new.session_id, 'model_name': 'login', 'slot_id': 8})


async def test_inflight_issuance_cannot_cross_logout_or_relogin(lifecycle, monkeypatch):
    services, _service, session, login, _identity, handlers = lifecycle
    entered, release = asyncio.Event(), asyncio.Event()
    original = services._credentials.request_model_auth_with_snapshot
    async def paused(*args):
        value = await original(*args)
        entered.set()
        await release.wait()
        return value
    monkeypatch.setattr(services._credentials, 'request_model_auth_with_snapshot', paused)
    pending = asyncio.create_task(handlers['credentials.request_model_auth'](
        {'auth_session': session.session_id, 'model_name': 'login'}))
    await entered.wait()
    await services.logout(session.session_id)
    login()
    release.set()
    assert await pending == {'status': 'invalid'}
    assert services.registry.target_count == 0


async def test_refresh_registered_only_and_logout_ack_is_not_remote_applied(lifecycle):
    services, service, session, _login, identity, handlers = lifecycle
    ref = login_credentials.credential_ref_for_user(session.user_id)
    refresh = handlers['credentials.refresh_for_ref']
    assert await refresh({'server_id': 'as-one', 'credential_ref': ref}) == {'status': 'invalid'}
    await handlers['credentials.request_model_auth']({'auth_session': session.session_id, 'model_name': 'login'})
    result = await refresh({'server_id': 'as-one', 'credential_ref': ref})
    assert result['update']['api_key'] == 'canary'
    assert result['ref_revision'] == 1
    events = []
    class Endpoint:
        async def notify(self, event, arguments, *, timeout):
            assert service.resolve_session(session.session_id) is None
            events.append((event, arguments))
        identity = None
    endpoint = Endpoint()
    endpoint.identity = identity
    services.endpoint_connected(endpoint)
    await services.logout(session.session_id)
    assert len(events) == 1
    event, arguments = events[0]
    assert event == 'credentials.invalidate'
    assert arguments['cutoff_revision'] == 1
    operation = services.registry.operations[arguments['operation_id']]
    assert operation.local_ack == 'ok' and operation.outcome is None
    assert services.registry.target_count == 1
    await handlers['credentials.update_result']({
        'operation_id': arguments['operation_id'], 'server_id': 'as-one',
        'credential_ref': ref, 'outcome': 'applied', 'written': True,
        'response_received': True, 'as_ok': True, 'as_applied': True})
    assert services.registry.target_count == 0


async def test_as_requested_refresh_uses_original_fifteen_minute_window(lifecycle, monkeypatch):
    services, service, session, _login, _identity, handlers = lifecycle
    issued = await handlers['credentials.request_model_auth']({
        'auth_session': session.session_id, 'model_name': 'login',
    })
    service._store.update_credential(session.session_id, Credential(
        id_token='ten-minutes-remaining', refresh_token='refresh-canary',
        expires_at=time.time() + 600,
    ))

    def refresh_credential(credential):
        return Credential(id_token='renewed-for-as', refresh_token=credential.refresh_token,
                          expires_at=time.time() + 3600)

    monkeypatch.setattr(service._flow, 'refresh', refresh_credential)
    refreshed = await handlers['credentials.refresh_for_ref']({
        'server_id': 'as-one', 'credential_ref': issued['credential']['credential_ref'],
    })
    assert refreshed['update']['api_key'] == 'renewed-for-as'
    assert service.resolve_session(session.session_id).credential.id_token == 'renewed-for-as'


async def test_stale_logout_does_not_revoke_new_login(lifecycle):
    services, _service, session, login, identity, handlers = lifecycle
    current = login()
    await handlers['credentials.request_model_auth']({'auth_session': current.session_id, 'model_name': 'login'})
    await services.logout(session.session_id)
    assert not services.registry.operations
    result = await handlers['credentials.request_model_auth']({'auth_session': current.session_id, 'model_name': 'login'})
    assert result['status'] == 'ok'
    services.endpoint_disconnected(identity)
    assert services.registry.target_count == 0


async def test_disconnect_rejects_late_issuance(lifecycle, monkeypatch):
    services, _service, session, _login, identity, handlers = lifecycle
    entered, release = asyncio.Event(), asyncio.Event()
    original = services._credentials.request_model_auth_with_snapshot
    async def paused(*args):
        result = await original(*args)
        entered.set()
        await release.wait()
        return result
    monkeypatch.setattr(services._credentials, 'request_model_auth_with_snapshot', paused)
    pending = asyncio.create_task(handlers['credentials.request_model_auth'](
        {'auth_session': session.session_id, 'model_name': 'login'}))
    await entered.wait()
    services.endpoint_disconnected(identity)
    release.set()
    assert await pending == {'status': 'invalid'}
    assert services.registry.target_count == 0


async def test_refresh_completion_after_relogin_is_rejected_and_threads_remain_bounded(lifecycle, monkeypatch):
    import threading

    services, service, session, login, _identity, handlers = lifecycle
    issued = await handlers['credentials.request_model_auth'](
        {'auth_session': session.session_id, 'model_name': 'login'})
    entered, release = threading.Event(), threading.Event()
    def refresh(old, *, ahead_s=None):
        entered.set()
        assert release.wait(3)
        return old
    monkeypatch.setattr(service, 'try_refresh', refresh)
    pending = asyncio.create_task(handlers['credentials.refresh_for_ref']({
        'credential_ref': issued['credential']['credential_ref'], 'server_id': 'as-one'}))
    try:
        async with asyncio.timeout(2):
            while not entered.is_set():
                await asyncio.sleep(0)
        login()
        assert services.pending_work_count == 1
    finally:
        release.set()
    assert await pending == {'status': 'invalid'}


async def test_logout_relogin_interleave_does_not_revoke_new_revision(lifecycle, monkeypatch):
    import threading

    services, service, session, login, _identity, handlers = lifecycle
    first = await handlers['credentials.request_model_auth'](
        {'auth_session': session.session_id, 'model_name': 'login'})
    entered, release = threading.Event(), threading.Event()
    original = service.logout
    def logout(session_id):
        entered.set()
        assert release.wait(3)
        original(session_id)
    monkeypatch.setattr(service, 'logout', logout)
    pending = asyncio.create_task(services.logout(session.session_id))
    try:
        async with asyncio.timeout(2):
            while not entered.is_set():
                await asyncio.sleep(0)
        newer = login()
        second = await handlers['credentials.request_model_auth'](
            {'auth_session': newer.session_id, 'model_name': 'login'})
        assert second['lease']['ref_revision'] > first['lease']['ref_revision']
    finally:
        release.set()
    await pending
    assert service.resolve_session(newer.session_id) is not None
    third = await handlers['credentials.request_model_auth'](
        {'auth_session': newer.session_id, 'model_name': 'login'})
    assert third['lease']['ref_revision'] == second['lease']['ref_revision']


async def test_real_http_logout_keeps_success_when_worker_notification_fails(lifecycle, monkeypatch):
    import httpx
    from fastapi import FastAPI
    from agent_gateway.channel_manager.web.web_http_auth import register_auth_routes

    services, service, session, _login, identity, handlers = lifecycle
    monkeypatch.setattr('agent_gateway.auth.remote_config.warm_up_in_background', lambda: None)
    await handlers['credentials.request_model_auth'](
        {'auth_session': session.session_id, 'model_name': 'login'})
    class Endpoint:
        async def notify(self, event, arguments, *, timeout):
            assert service.resolve_session(session.session_id) is None
            raise ConnectionError('synthetic closed endpoint')
    endpoint = Endpoint()
    endpoint.identity = identity
    services.endpoint_connected(endpoint)
    app = FastAPI()
    register_auth_routes(app, auth_service_provider=lambda: service, logout_callback=services.logout)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        response = await client.post('/api/v1/auth/logout',
            headers={'x-jiuwen-auth': '1', 'x-auth-session': session.session_id})
    assert response.status_code == 200
    assert response.json() == {'ok': True, 'islogin': False}
    assert 'Max-Age=0' in response.headers['set-cookie']
    assert services.registry.target_count == 1
    operation = next(iter(services.registry.operations.values()))
    assert operation.local_ack == 'unknown'
    assert operation.outcome is None


async def test_logout_records_busy_ack_without_claiming_remote_result(lifecycle):
    from agent_gateway.processes.control_service import ControlServiceBusy

    services, _service, session, _login, identity, handlers = lifecycle
    await handlers['credentials.request_model_auth'](
        {'auth_session': session.session_id, 'model_name': 'login'})
    class Endpoint:
        async def notify(self, event, arguments, *, timeout):
            raise ControlServiceBusy()
    endpoint = Endpoint()
    endpoint.identity = identity
    services.endpoint_connected(endpoint)
    await services.logout(session.session_id)
    operation = next(iter(services.registry.operations.values()))
    assert operation.local_ack == 'busy'
    assert operation.outcome is None
    assert services.registry.target_count == 1


async def test_logout_notifies_when_refresh_operation_budget_is_full(lifecycle):
    from agent_gateway.auth.credential_issuance import CredentialIssuanceRegistry

    services, service, session, _login, identity, handlers = lifecycle
    services.registry = CredentialIssuanceRegistry(max_operations=1)
    issued = await handlers['credentials.request_model_auth'](
        {'auth_session': session.session_id, 'model_name': 'login'})
    ref = issued['credential']['credential_ref']
    target = services.registry.targets(ref, issued['lease']['ref_revision'])[0]
    refresh_id = services.registry.operation(target, 1, revoked=False)
    events = []
    class Endpoint:
        async def notify(self, event, arguments, *, timeout):
            events.append((event, arguments))
    endpoint = Endpoint()
    endpoint.identity = identity
    services.endpoint_connected(endpoint)
    await services.logout(session.session_id)
    assert service.resolve_session(session.session_id) is None
    assert len(events) == 1
    assert events[0][0] == 'credentials.invalidate'
    assert events[0][1]['credential_ref'] == ref
    assert events[0][1]['operation_id'] != refresh_id
    assert services.registry.operations[refresh_id].outcome is None
    assert services.registry.operations[events[0][1]['operation_id']].local_ack == 'ok'


async def test_logout_reservation_is_not_reused_by_concurrent_relogin(lifecycle, monkeypatch):
    import threading
    from agent_gateway.auth.credential_issuance import CredentialIssuanceRegistry
    from agent_gateway.processes.control_service import ControlServiceBusy

    services, service, session, login, identity, handlers = lifecycle
    services.registry = CredentialIssuanceRegistry(max_revocations=1)
    await handlers['credentials.request_model_auth'](
        {'auth_session': session.session_id, 'model_name': 'login'})
    entered, release = threading.Event(), threading.Event()
    original = service.logout
    def logout(session_id):
        entered.set()
        assert release.wait(3)
        original(session_id)
    monkeypatch.setattr(service, 'logout', logout)
    events = []
    class Endpoint:
        async def notify(self, event, arguments, *, timeout):
            events.append(arguments)
    endpoint = Endpoint()
    endpoint.identity = identity
    services.endpoint_connected(endpoint)
    pending = asyncio.create_task(services.logout(session.session_id))
    try:
        async with asyncio.timeout(2):
            while not entered.is_set():
                await asyncio.sleep(0)
        newer = login()
        with pytest.raises(ControlServiceBusy):
            await handlers['credentials.request_model_auth'](
                {'auth_session': newer.session_id, 'model_name': 'login'})
    finally:
        release.set()
        await pending
    assert len(events) == 1
    assert events[0]['cutoff_revision'] == 1
    assert services.registry.latest_revision(events[0]['credential_ref']) == 1
