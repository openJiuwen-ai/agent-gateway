"""Control-owned public discovery shares actual credential work ownership."""

import asyncio
from dataclasses import replace
import threading
import time
from types import SimpleNamespace

import pytest

from agent_gateway.auth import model_catalog
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.control_credentials import ControlCredentialServices
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.control_service import ControlServiceBusy, ControlServiceError

_REAL_GET_MODELS = model_catalog.get_models


@pytest.fixture
async def catalog_authority(monkeypatch, tmp_path):
    monkeypatch.setattr('agent_gateway.auth.session_store.auth_dir', lambda: tmp_path)
    store = AuthSessionStore(persist=False)
    sessions = [store.create(SimpleNamespace(
        user_id=f'account-{i}', user_name=f'Account {i}',
        credential=Credential(id_token=f'private-token-{i}', refresh_token=f'private-refresh-{i}',
                              expires_at=time.time() + 3600),
    )) for i in range(2)]
    service = AuthService(store=store)
    calls = []

    def discover(session_id, *, service):
        calls.append((session_id, service))
        return [model_catalog.LoginModel('shared-model', 'Shared', raw={'secret': 'must-not-leave'})]

    monkeypatch.setattr(model_catalog, 'get_models', discover)
    monkeypatch.setattr('agent_gateway.auth.service.get_auth_service',
                        lambda: pytest.fail('global login authority used'))
    services = ControlCredentialServices(lambda: service, enable_public_catalog=True)
    try:
        yield services, service, sessions, calls
    finally:
        await services.stop(timeout=2)


async def test_catalog_is_explicit_and_construction_is_lazy():
    def unexpected():
        pytest.fail('construction must not resolve accounts')

    services = ControlCredentialServices(unexpected)
    assert 'catalog.list_login_models' not in services.handlers
    enabled = ControlCredentialServices(unexpected, enable_public_catalog=True)
    assert 'catalog.list_login_models' in enabled.handlers
    await services.stop()
    await enabled.stop()


async def test_catalog_validates_explicit_session_even_for_shared_cache(catalog_authority):
    services, service, sessions, calls = catalog_authority
    handler = services.handlers['catalog.list_login_models']
    for session in sessions:
        assert await handler({'auth_session': session.session_id}) == {
            'status': 'ok', 'models': [{'model_name': 'shared-model',
                                     'display_name': 'Shared', 'description': ''}],
        }
    assert calls == [(s.session_id, service) for s in sessions]
    for session_id in [None, '', 'not-a-session']:
        assert await handler({'auth_session': session_id}) == {
            'status': 'unauthenticated', 'models': [],
        }
    assert len(calls) == 2
    assert services.registry.ref_count == 0
    assert services.registry.target_count == 0


@pytest.mark.parametrize('arguments', [{}, {'auth_session': 3}, {'auth_session': 'x' * 257},
                                       {'auth_session': 's', 'slot_id': 0}])
async def test_invalid_catalog_arguments_are_fixed_errors(catalog_authority, arguments):
    services, _service, _sessions, calls = catalog_authority
    with pytest.raises(ValueError):
        await services.handlers['catalog.list_login_models'](arguments)
    assert calls == []


@pytest.mark.parametrize('change', ['logout', 'relogin', 'replace'])
async def test_discovery_does_not_outlive_login_epoch(catalog_authority, monkeypatch, change):
    services, service, sessions, _calls = catalog_authority
    started, release = threading.Event(), threading.Event()

    def discover(*args, **kwargs):
        started.set()
        assert release.wait(3)
        return [model_catalog.LoginModel('stale', 'Stale')]

    monkeypatch.setattr(model_catalog, 'get_models', discover)
    task = asyncio.create_task(services.handlers['catalog.list_login_models']({
        'auth_session': sessions[0].session_id,
    }))
    try:
        async with asyncio.timeout(2):
            while not started.is_set():
                await asyncio.sleep(.001)
        if change == 'logout':
            service.logout(sessions[0].session_id)
        elif change == 'relogin':
            service._store.create(SimpleNamespace(user_id=sessions[0].user_id, user_name='Again',
                                                 credential=sessions[0].credential))
        else:
            service._store._by_session[sessions[0].session_id] = replace(
                sessions[0], user_id='other-account',
            )
    finally:
        release.set()
    assert await task == {'status': 'unauthenticated', 'models': []}


async def test_cancelled_catalog_keeps_real_slot_and_credentials_can_progress(catalog_authority, monkeypatch):
    services, _service, sessions, _calls = catalog_authority
    started, release = threading.Event(), threading.Event()

    def discover(*args, **kwargs):
        started.set()
        assert release.wait(3)
        return []

    monkeypatch.setattr(model_catalog, 'get_models', discover)
    handler = services.handlers['catalog.list_login_models']
    task = asyncio.create_task(handler({'auth_session': sessions[0].session_id}))
    try:
        async with asyncio.timeout(2):
            while not started.is_set():
                await asyncio.sleep(.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert services.pending_work_count == 1
        with pytest.raises(ControlServiceBusy):
            await handler({'auth_session': sessions[1].session_id})
        assert await services.handlers['bindings.lookup']({'session_id': 'missing'}) == {'status': 'not_found'}
        with pytest.raises(ControlServiceError):
            await services.stop(timeout=.01)
        assert services.pending_work_count == 1
    finally:
        release.set()
    async with asyncio.timeout(2):
        while services.pending_work_count:
            await asyncio.sleep(.001)
    await services.stop(timeout=2)


async def test_catalog_timeout_does_not_release_actual_work(catalog_authority, monkeypatch):
    from agent_gateway.auth import control_model_catalog

    services, _service, sessions, _calls = catalog_authority
    release = threading.Event()
    monkeypatch.setattr(control_model_catalog, '_HANDLER_TIMEOUT_S', .01)
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: release.wait(3) and [])
    try:
        assert await services.handlers['catalog.list_login_models']({
            'auth_session': sessions[0].session_id,
        }) == {'status': 'unavailable', 'models': []}
        assert services.pending_work_count == 1
        with pytest.raises(ControlServiceBusy):
            await services.handlers['catalog.list_login_models']({'auth_session': sessions[1].session_id})
    finally:
        release.set()


@pytest.mark.parametrize('failure', ['oversized', 'exception', 'bad_dto'])
async def test_discovery_failures_are_whole_fixed_results(catalog_authority, monkeypatch, failure):
    services, _service, sessions, _calls = catalog_authority

    def discover(*args, **kwargs):
        if failure == 'exception':
            raise RuntimeError('private-token-must-not-escape')
        if failure == 'bad_dto':
            return [model_catalog.LoginModel('m', None)]
        return [model_catalog.LoginModel(str(i), str(i)) for i in range(49)]

    monkeypatch.setattr(model_catalog, 'get_models', discover)
    assert await services.handlers['catalog.list_login_models']({
        'auth_session': sessions[0].session_id,
    }) == {'status': 'too_large' if failure == 'oversized' else 'unavailable', 'models': []}


async def test_actual_cache_still_requires_live_explicit_session(catalog_authority, monkeypatch):
    services, service, sessions, _calls = catalog_authority
    # Exercise the real cache-first producer, whose cache wrapper is global.
    module = model_catalog
    monkeypatch.setattr(module, 'get_models', _REAL_GET_MODELS)
    monkeypatch.setattr(module, 'get_remote_config', lambda *a, **k: SimpleNamespace(is_effective=True))
    monkeypatch.setattr(module, '_memo', {'models': [module.LoginModel('cached', 'Cached')],
                                       'mtime': None, 'fetched_at': time.time()})
    monkeypatch.setattr(module, 'refresh', lambda *a, **k: pytest.fail('fresh cache must not discover'))
    try:
        handler = services.handlers['catalog.list_login_models']
        assert (await handler({'auth_session': sessions[0].session_id}))['models'][0]['model_name'] == 'cached'
        assert (await handler({'auth_session': sessions[1].session_id}))['status'] == 'ok'
        service.logout(sessions[0].session_id)
        assert await handler({'auth_session': sessions[0].session_id}) == {
            'status': 'unauthenticated', 'models': [],
        }
    finally:
        module._memo = None


async def test_shared_pool_saturation_does_not_leak_catalog_admission(catalog_authority):
    services, _service, sessions, _calls = catalog_authority
    release = threading.Event()
    futures = [services._blocking_work.submit(release.wait, 3) for _ in range(4)]
    try:
        with pytest.raises(ControlServiceBusy):
            await services.handlers['catalog.list_login_models']({'auth_session': sessions[0].session_id})
        assert services.pending_work_count == 4
    finally:
        release.set()
    await asyncio.gather(*(asyncio.wrap_future(future) for future in futures))
    assert (await services.handlers['catalog.list_login_models']({
        'auth_session': sessions[0].session_id,
    }))['status'] == 'ok'


@pytest.mark.parametrize('change', ['logout', 'relogin', 'expired', 'refresh'])
async def test_catalog_commit_rechecks_after_thread_finishes(catalog_authority, monkeypatch, change):
    services, service, sessions, _calls = catalog_authority
    catalog = services._public_catalog
    original = catalog._load
    loop = asyncio.get_running_loop()

    def update():
        if change == 'logout':
            service.logout(sessions[0].session_id)
        elif change == 'relogin':
            service._store.create(SimpleNamespace(user_id=sessions[0].user_id, user_name='Again',
                                                 credential=sessions[0].credential))
        else:
            service._store.update_credential(sessions[0].session_id, Credential(
                id_token='updated-token', refresh_token='updated-refresh',
                expires_at=time.time() + (-1 if change == 'expired' else 3600),
            ))

    def load(session_id):
        result = original(session_id)
        # This callback is queued before concurrent-future completion delivery.
        loop.call_soon_threadsafe(update)
        return result

    monkeypatch.setattr(catalog, '_load', load)
    result = await catalog.list_login_models({'auth_session': sessions[0].session_id})
    assert result['status'] == ('ok' if change == 'refresh' else 'unauthenticated')


async def test_catalog_commit_never_reads_disk_or_waits_for_store_lock(catalog_authority, monkeypatch):
    services, service, sessions, _calls = catalog_authority
    catalog = services._public_catalog
    original = catalog._load
    locked, release = threading.Event(), threading.Event()

    def hold_lock():
        with service._store._lock:
            locked.set()
            assert release.wait(3)

    thread = threading.Thread(target=hold_lock)

    def load(session_id):
        result = original(session_id)
        thread.start()
        assert locked.wait(2)
        return result

    monkeypatch.setattr(catalog, '_load', load)
    try:
        async with asyncio.timeout(.5):
            assert await catalog.list_login_models({'auth_session': sessions[0].session_id}) == {
                'status': 'unavailable', 'models': [],
            }
    finally:
        release.set()
        thread.join(2)
