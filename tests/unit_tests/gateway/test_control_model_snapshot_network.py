"""The snapshot operation crosses the real private Control transport."""

from __future__ import annotations

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
from agent_gateway.auth.remote_model_catalog import (
    RemoteLoginModelCatalog,
    RemoteLoginModelSnapshot,
)
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes import control_protocol
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceAdmission,
    ControlServiceClient,
    ControlServiceIdentity,
    ControlServiceServer,
)


@pytest.fixture
async def snapshot_socket(monkeypatch, tmp_path):
    monkeypatch.setattr("agent_gateway.auth.session_store.auth_dir", lambda: tmp_path)
    store = AuthSessionStore(persist=False)
    session = store.create(SimpleNamespace(
        user_id="account",
        user_name="Account",
        credential=Credential(
            id_token="never-wire-token",
            refresh_token="never-wire-refresh",
            expires_at=time.time() + 3600,
        ),
    ))
    service = AuthService(store=store)
    monkeypatch.setattr(model_catalog, "get_models", lambda *args, **kwargs: [
        model_catalog.LoginModel("shared", "Shared", raw={"secret": "never-return"}),
    ])
    monkeypatch.setattr("agent_gateway.auth.apig.resolve_apig_config", lambda **kwargs: SimpleNamespace(
        invoke_base_url="https://models.example.invalid/v1",
    ))
    services = ControlCredentialServices(lambda: service, enable_public_catalog=True)
    left, right = socket.socketpair()
    identity = ControlServiceIdentity("snapshot-real", 0, 1, os.getpid(), os.getpid())
    client = ControlServiceClient(ControlServiceChannel(left), identity)
    server = ControlServiceServer(
        ControlServiceChannel(right),
        identity,
        handlers=services.handlers,
        admission=ControlServiceAdmission(),
    )
    try:
        await asyncio.gather(client.start(), server.start())
        yield RemoteLoginModelSnapshot(client), services, session, client
    finally:
        await client.stop()
        await server.stop()
        await services.stop(timeout=3)


async def test_real_snapshot_returns_public_contract_without_secrets(
    snapshot_socket,
) -> None:
    remote, _services, session, _client = snapshot_socket
    old_remote = RemoteLoginModelCatalog(_client)

    result = await remote.get_snapshot(session.session_id)
    old_result = await old_remote.list_models(session.session_id)

    assert result.to_dict() == {
        "version": 1,
        "status": "ok",
        "api_base": "https://models.example.invalid/v1",
        "models": [{
            "model_name": "shared",
            "display_name": "Shared",
            "description": "",
        }],
    }
    assert "never-wire-token" not in str(result)
    assert "never-return" not in str(result)
    assert old_result.to_dict() == {
        "status": "ok",
        "models": [{
            "model_name": "shared",
            "display_name": "Shared",
            "description": "",
        }],
    }


async def test_real_socket_shows_snapshot_and_d1_share_single_slot(
    snapshot_socket, monkeypatch
) -> None:
    remote, services, session, _client = snapshot_socket
    started, release = threading.Event(), threading.Event()

    def discover(*args, **kwargs):
        started.set()
        assert release.wait(3)
        return [model_catalog.LoginModel("shared", "Shared")]

    monkeypatch.setattr(model_catalog, "get_models", discover)
    snapshot_task = asyncio.create_task(remote.get_snapshot(session.session_id))
    try:
        async with asyncio.timeout(2):
            while not started.is_set():
                await asyncio.sleep(0.001)
        with pytest.raises(CatalogError) as caught:
            await RemoteLoginModelCatalog(_client).list_models(session.session_id)
        assert caught.value.reason == "busy"
        assert services.pending_work_count == 1
    finally:
        release.set()
    assert (await snapshot_task).status == "ok"


async def test_unregistered_operation_maps_to_not_enabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("agent_gateway.auth.session_store.auth_dir", lambda: tmp_path)
    store = AuthSessionStore(persist=False)
    service = AuthService(store=store)
    services = ControlCredentialServices(lambda: service, enable_public_catalog=False)
    left, right = socket.socketpair()
    identity = ControlServiceIdentity("snapshot-disabled", 0, 1, os.getpid(), os.getpid())
    client = ControlServiceClient(ControlServiceChannel(left), identity)
    server = ControlServiceServer(
        ControlServiceChannel(right),
        identity,
        handlers=services.handlers,
        admission=ControlServiceAdmission(),
    )
    try:
        await asyncio.gather(client.start(), server.start())
        with pytest.raises(CatalogError) as caught:
            await RemoteLoginModelSnapshot(client).get_snapshot("session")
        assert caught.value.reason == "not_enabled"
    finally:
        await client.stop()
        await server.stop()
        await services.stop(timeout=2)


def test_old_codec_rejects_snapshot_operation_without_d1_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_operations = frozenset(control_protocol.OPERATIONS - {
        "catalog.get_login_model_snapshot",
    })
    request = {
        "protocol_version": 1,
        "group_id": "gateway-group",
        "slot_id": 0,
        "generation": 1,
        "type": "REQUEST",
        "request_id": "a" * 32,
        "operation": "catalog.get_login_model_snapshot",
        "arguments": {"auth_session": "session"},
    }

    monkeypatch.setattr(control_protocol, "OPERATIONS", old_operations)
    with pytest.raises(control_protocol.ControlProtocolError):
        control_protocol.validate_message(request)

    monkeypatch.setattr(control_protocol, "OPERATIONS", frozenset({
        *old_operations,
        "catalog.get_login_model_snapshot",
    }))
    control_protocol.encode_message(request)


async def test_old_codec_rejection_crosses_real_socket_without_retry(monkeypatch, tmp_path):
    from agent_gateway.processes import control_channel

    original_decode = control_channel.decode_message
    old_operations = frozenset(control_protocol.OPERATIONS - {"catalog.get_login_model_snapshot"})
    rejected = []

    def old_decode(payload):
        # Scope the old enum to synchronous decoding, not concurrent endpoint work.
        with monkeypatch.context() as scoped:
            scoped.setattr(control_protocol, "OPERATIONS", old_operations)
            try:
                return original_decode(payload)
            except control_protocol.ControlProtocolError:
                rejected.append(True)
                raise

    monkeypatch.setattr(control_channel, "decode_message", old_decode)
    monkeypatch.setattr("agent_gateway.auth.session_store.auth_dir", lambda: tmp_path)
    services = ControlCredentialServices(lambda: AuthService(store=AuthSessionStore(persist=False)))
    left, right = socket.socketpair()
    identity = ControlServiceIdentity("old-codec-wire", 0, 1, os.getpid(), os.getpid())
    client = ControlServiceClient(ControlServiceChannel(left), identity)
    server = ControlServiceServer(ControlServiceChannel(right), identity, handlers=services.handlers,
                                  admission=ControlServiceAdmission())
    try:
        await asyncio.gather(client.start(), server.start())
        with pytest.raises(CatalogError) as caught:
            async with asyncio.timeout(2):
                await RemoteLoginModelSnapshot(client).get_snapshot("session")
        assert caught.value.reason in {"protocol", "unavailable"}
        assert rejected == [True]
    finally:
        await client.stop()
        await server.stop()
        await services.stop(timeout=2)
