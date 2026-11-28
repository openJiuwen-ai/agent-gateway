"""Control snapshot shares D1's directory admission and blocking-work budget."""

from __future__ import annotations

import asyncio
import threading
import time
from types import SimpleNamespace

import pytest

from agent_gateway.auth import model_catalog
from agent_gateway.auth.account_kit import Credential
from agent_gateway.auth.control_credentials import ControlCredentialServices
from agent_gateway.auth.service import AuthService
from agent_gateway.auth.session_store import AuthSessionStore
from agent_gateway.processes.control_service import ControlServiceBusy


@pytest.fixture
async def snapshot_authority(monkeypatch, tmp_path):
    monkeypatch.setattr("agent_gateway.auth.session_store.auth_dir", lambda: tmp_path)
    store = AuthSessionStore(persist=False)
    sessions = [store.create(SimpleNamespace(
        user_id=f"account-{index}",
        user_name=f"Account {index}",
        credential=Credential(
            id_token=f"private-token-{index}",
            refresh_token=f"private-refresh-{index}",
            expires_at=time.time() + 3600,
        ),
    )) for index in range(2)]
    service = AuthService(store=store)
    calls = []

    def discover(session_id, *, service):
        calls.append((session_id, threading.get_ident(), service))
        return [model_catalog.LoginModel(
            "shared-model", "Shared", raw={"secret": "must-not-leave"},
        )]

    def config(**kwargs):
        calls.append(("config", threading.get_ident(), kwargs))
        return SimpleNamespace(invoke_base_url="https://models.example.invalid/v1")

    monkeypatch.setattr(model_catalog, "get_models", discover)
    monkeypatch.setattr("agent_gateway.auth.apig.resolve_apig_config", config)
    monkeypatch.setattr(
        "agent_gateway.auth.service.get_auth_service",
        lambda: pytest.fail("global login authority used"),
    )
    services = ControlCredentialServices(lambda: service, enable_public_catalog=True)
    try:
        yield services, service, sessions, calls
    finally:
        await services.stop(timeout=2)


async def test_snapshot_operation_is_explicit_and_shares_admission() -> None:
    def unexpected():
        pytest.fail("construction must not resolve accounts")

    disabled = ControlCredentialServices(unexpected)
    enabled = ControlCredentialServices(unexpected, enable_public_catalog=True)

    assert "catalog.get_login_model_snapshot" not in disabled.handlers
    assert "catalog.get_login_model_snapshot" in enabled.handlers
    await disabled.stop()
    await enabled.stop()


async def test_snapshot_returns_public_models_and_cached_apig_address(
    snapshot_authority,
) -> None:
    services, service, sessions, calls = snapshot_authority

    result = await services.handlers["catalog.get_login_model_snapshot"](
        {"auth_session": sessions[0].session_id}
    )

    assert result == {
        "version": 1,
        "status": "ok",
        "api_base": "https://models.example.invalid/v1",
        "models": [{
            "model_name": "shared-model",
            "display_name": "Shared",
            "description": "",
        }],
    }
    assert calls[0][0] == "config"
    assert calls[0][2] == {"allow_refresh": False}
    assert calls[1][0] == sessions[0].session_id
    assert calls[1][2] is service
    assert calls[0][1] == calls[1][1]
    assert calls[0][1] != threading.get_ident()
    assert "private-token-0" not in str(result)
    assert "must-not-leave" not in str(result)


@pytest.mark.parametrize("auth_session", [None, "", "missing-session"])
async def test_snapshot_requires_explicit_login_session(
    snapshot_authority, auth_session: str | None
) -> None:
    services, _service, _sessions, calls = snapshot_authority

    result = await services.handlers["catalog.get_login_model_snapshot"](
        {"auth_session": auth_session}
    )

    assert result == {
        "version": 1,
        "status": "unauthenticated",
        "api_base": "",
        "models": [],
    }
    assert calls == []


@pytest.mark.parametrize(
    "arguments",
    [{}, {"auth_session": 3}, {"auth_session": "x" * 257},
     {"auth_session": "s", "slot_id": 0}],
)
async def test_invalid_snapshot_arguments_are_fixed_errors(
    snapshot_authority, arguments: dict[str, object]
) -> None:
    services, _service, _sessions, calls = snapshot_authority

    with pytest.raises(ValueError):
        await services.handlers["catalog.get_login_model_snapshot"](arguments)
    assert calls == []


async def test_snapshot_uses_new_eight_second_budget_without_changing_d1(
    snapshot_authority, monkeypatch
) -> None:
    from agent_gateway.auth import control_model_catalog

    services, _service, sessions, _calls = snapshot_authority
    release = threading.Event()
    monkeypatch.setattr(control_model_catalog, "_SNAPSHOT_HANDLER_TIMEOUT_S", 0.01)
    monkeypatch.setattr(
        model_catalog,
        "get_models",
        lambda *args, **kwargs: release.wait(3) and [],
    )

    try:
        assert await services.handlers["catalog.get_login_model_snapshot"](
            {"auth_session": sessions[0].session_id}
        ) == {
            "version": 1,
            "status": "unavailable",
            "api_base": "",
            "models": [],
        }
        assert services.pending_work_count == 1
        with pytest.raises(ControlServiceBusy):
            await services.handlers["catalog.list_login_models"](
                {"auth_session": sessions[1].session_id}
            )
    finally:
        release.set()
    assert control_model_catalog._HANDLER_TIMEOUT_S == 15.0


async def test_old_catalog_uses_original_fifteen_second_budget(
    snapshot_authority, monkeypatch
) -> None:
    from agent_gateway.auth import control_model_catalog

    services, _service, sessions, _calls = snapshot_authority
    release = threading.Event()
    monkeypatch.setattr(control_model_catalog, "_HANDLER_TIMEOUT_S", 0.01)
    monkeypatch.setattr(
        model_catalog,
        "get_models",
        lambda *args, **kwargs: release.wait(3) and [],
    )

    try:
        assert await services.handlers["catalog.list_login_models"](
            {"auth_session": sessions[0].session_id}
        ) == {"status": "unavailable", "models": []}
        assert services.pending_work_count == 1
    finally:
        release.set()
    assert control_model_catalog._SNAPSHOT_HANDLER_TIMEOUT_S == 8.0


async def test_snapshot_cancellation_does_not_release_directory_slot(
    snapshot_authority, monkeypatch
) -> None:
    services, _service, sessions, _calls = snapshot_authority
    started, release = threading.Event(), threading.Event()

    def discover(*args, **kwargs):
        started.set()
        assert release.wait(3)
        return []

    monkeypatch.setattr(model_catalog, "get_models", discover)
    handler = services.handlers["catalog.get_login_model_snapshot"]
    task = asyncio.create_task(
        handler({"auth_session": sessions[0].session_id})
    )
    try:
        async with asyncio.timeout(2):
            while not started.is_set():
                await asyncio.sleep(0.001)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert services.pending_work_count == 1
        with pytest.raises(ControlServiceBusy):
            await handler({"auth_session": sessions[1].session_id})
        with pytest.raises(ControlServiceBusy):
            await services.handlers["catalog.list_login_models"](
                {"auth_session": sessions[1].session_id}
            )
    finally:
        release.set()
    async with asyncio.timeout(2):
        while services.pending_work_count:
            await asyncio.sleep(0.001)


async def test_no_apig_config_returns_ok_empty_snapshot(
    snapshot_authority, monkeypatch
) -> None:
    services, _service, sessions, calls = snapshot_authority
    config_calls = []

    def disabled_config(**kwargs):
        config_calls.append(kwargs)
        return None

    monkeypatch.setattr("agent_gateway.auth.apig.resolve_apig_config", disabled_config)

    result = await services.handlers["catalog.get_login_model_snapshot"](
        {"auth_session": sessions[0].session_id}
    )

    assert result == {
        "version": 1,
        "status": "ok",
        "api_base": "",
        "models": [],
    }
    assert calls == []
    assert config_calls == [{"allow_refresh": False}]


async def test_snapshot_failures_are_whole_fixed_results(
    snapshot_authority, monkeypatch
) -> None:
    services, _service, sessions, _calls = snapshot_authority
    monkeypatch.setattr(
        model_catalog,
        "get_models",
        lambda *args, **kwargs: [model_catalog.LoginModel(str(i), str(i)) for i in range(49)],
    )

    result = await services.handlers["catalog.get_login_model_snapshot"](
        {"auth_session": sessions[0].session_id}
    )

    assert result == {
        "version": 1,
        "status": "too_large",
        "api_base": "",
        "models": [],
    }


async def test_snapshot_rechecks_login_epoch_before_publishing(
    snapshot_authority, monkeypatch
) -> None:
    services, service, sessions, _calls = snapshot_authority
    catalog = services._public_catalog
    original = catalog._load_snapshot
    loop = asyncio.get_running_loop()

    def relogin():
        service._store.create(SimpleNamespace(
            user_id=sessions[0].user_id,
            user_name="Again",
            credential=sessions[0].credential,
        ))

    def load(session_id):
        result = original(session_id)
        loop.call_soon_threadsafe(relogin)
        return result

    monkeypatch.setattr(catalog, "_load_snapshot", load)

    result = await services.handlers["catalog.get_login_model_snapshot"](
        {"auth_session": sessions[0].session_id}
    )

    assert result == {
        "version": 1,
        "status": "unauthenticated",
        "api_base": "",
        "models": [],
    }
