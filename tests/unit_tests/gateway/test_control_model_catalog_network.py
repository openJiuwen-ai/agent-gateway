"""Real Control discovery handlers and Remote port share the private transport."""

import asyncio
import os
import socket
import threading
import time
from types import SimpleNamespace

import pytest

from agent_gateway.auth import model_catalog
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.control_credentials import ControlCredentialServices
from agent_gateway.auth.public_model_catalog import CatalogError
from agent_gateway.auth.remote_model_catalog import RemoteLoginModelCatalog
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceAdmission, ControlServiceClient, ControlServiceIdentity, ControlServiceServer,
)


@pytest.fixture
async def catalog_socket(monkeypatch, tmp_path):
    monkeypatch.setattr('agent_gateway.auth.session_store.auth_dir', lambda: tmp_path)
    store = AuthSessionStore(persist=False)
    sessions = [store.create(SimpleNamespace(user_id=f'account-{i}', user_name='Account',
        credential=Credential(id_token=f'never-wire-token-{i}', refresh_token='never-wire-refresh',
                              expires_at=time.time() + 3600))) for i in range(2)]
    service = AuthService(store=store)
    services = ControlCredentialServices(lambda: service, enable_public_catalog=True)
    left, right = socket.socketpair()
    identity = ControlServiceIdentity('catalog-real', 0, 1, os.getpid(), os.getpid())
    client = ControlServiceClient(ControlServiceChannel(left), identity)
    server = ControlServiceServer(ControlServiceChannel(right), identity,
        handlers=services.handlers, admission=ControlServiceAdmission())
    try:
        await asyncio.gather(client.start(), server.start())
        yield RemoteLoginModelCatalog(client), services, sessions, client
    finally:
        await client.stop()
        await server.stop()
        await services.stop(timeout=3)


async def test_real_directory_with_two_accounts_and_whole_failure_recovery(catalog_socket, monkeypatch):
    remote, services, sessions, _client = catalog_socket
    calls = []

    def discover(session_id, *, service):
        calls.append(session_id)
        return [model_catalog.LoginModel('shared', 'Shared', raw={'api_key': 'private-raw'})]

    monkeypatch.setattr(model_catalog, 'get_models', discover)
    for session in sessions:
        value = await remote.list_models(session.session_id)
        assert value.to_dict() == {'status': 'ok', 'models': [
            {'model_name': 'shared', 'display_name': 'Shared', 'description': ''},
        ]}
    assert calls == [session.session_id for session in sessions]
    assert (await remote.list_models('missing')).status == 'unauthenticated'
    assert (await remote.list_models(None)).status == 'unauthenticated'
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [
        model_catalog.LoginModel(str(i), 'M') for i in range(49)
    ])
    with pytest.raises(CatalogError) as caught:
        await remote.list_models(sessions[0].session_id)
    assert caught.value.reason == 'too_large'
    monkeypatch.setattr(model_catalog, 'get_models', lambda *a, **k: [])
    assert (await remote.list_models(sessions[0].session_id)).to_dict() == {'status': 'ok', 'models': []}
    assert services.registry.target_count == 0


async def test_directory_busy_keeps_other_private_operations_available(catalog_socket, monkeypatch):
    remote, services, sessions, client = catalog_socket
    started, release = threading.Event(), threading.Event()

    def discover(*args, **kwargs):
        if kwargs.get('allow_refresh') is False:
            return [model_catalog.LoginModel('cached', 'Cached')]
        started.set()
        assert release.wait(3)
        return []

    monkeypatch.setattr(model_catalog, 'get_models', discover)
    monkeypatch.setattr('agent_gateway.auth.apig.resolve_apig_config', lambda **k: SimpleNamespace(
        invoke_base_url='https://test.invalid/invoke',
    ))
    first = asyncio.create_task(remote.list_models(sessions[0].session_id))
    try:
        async with asyncio.timeout(2):
            while not started.is_set():
                await asyncio.sleep(.001)
        with pytest.raises(CatalogError) as caught:
            await remote.list_models(sessions[1].session_id)
        assert caught.value.reason == 'busy'
        assert await client.call('bindings.lookup', {'session_id': 'missing'}, timeout=1) == {'status': 'not_found'}
        credential = await client.call('credentials.request_model_auth', {
            'auth_session': sessions[1].session_id, 'model_name': 'cached',
        }, timeout=1)
        assert credential['status'] == 'ok'
        assert credential['credential']['api_key'] == 'never-wire-token-1'
        assert services.pending_work_count == 1
    finally:
        release.set()
        await first
