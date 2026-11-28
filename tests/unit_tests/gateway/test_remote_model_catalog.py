from __future__ import annotations

import asyncio
import os
import socket
from pathlib import Path
from typing import Any

import pytest

from agent_gateway.auth.public_model_catalog import (
    CatalogError,
    CatalogResult,
    PublicLoginModel,
)
from agent_gateway.auth.remote_model_catalog import RemoteLoginModelCatalog
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_protocol import ControlProtocolError
from agent_gateway.processes.control_service import (
    ControlServiceAdmission,
    ControlServiceBusy,
    ControlServiceClient,
    ControlServiceError,
    ControlServiceIdentity,
    ControlServiceOperationError,
    ControlServiceServer,
)


@pytest.fixture(autouse=True)
def isolated_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    data = tmp_path / "data"
    logs = tmp_path / "logs"
    temporary = tmp_path / "tmp"
    for directory in (home, data, logs, temporary):
        directory.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_DATA_DIR", str(data))
    monkeypatch.setenv("JIUWENSWARM_LOG_DIR", str(logs))
    monkeypatch.setenv("AGENT_GATEWAY_LOG_DIR", str(logs))
    monkeypatch.setenv("AGENTOS_GATEWAY_LOG_DIR", str(logs))
    monkeypatch.setenv("TMPDIR", str(temporary))
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[3]))


class RecordingClient:
    def __init__(self, response: dict[str, Any] | BaseException) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, Any], float]] = []

    async def call(
        self, operation: str, arguments: dict[str, Any], *, timeout: float
    ) -> dict[str, Any]:
        self.calls.append((operation, arguments, timeout))
        if isinstance(self.response, BaseException):
            raise self.response
        return self.response


class SleepingClient:
    def __init__(self) -> None:
        self.calls = 0

    async def call(
        self, operation: str, arguments: dict[str, Any], *, timeout: float
    ) -> dict[str, Any]:
        self.calls += 1
        await asyncio.sleep(3600)
        raise AssertionError("unreachable")


class FailingClient:
    async def call(
        self, operation: str, arguments: dict[str, Any], *, timeout: float
    ) -> dict[str, Any]:
        pytest.fail("control client must not be called")


def identity(group_id: str = "gateway-group") -> ControlServiceIdentity:
    current_pid = os.getpid()
    return ControlServiceIdentity(
        group_id=group_id,
        slot_id=0,
        generation=1,
        worker_pid=current_pid,
        control_pid=current_pid,
    )


def channel_pair() -> tuple[ControlServiceChannel, ControlServiceChannel]:
    worker_socket, control_socket = socket.socketpair()
    return ControlServiceChannel(worker_socket), ControlServiceChannel(control_socket)


async def start_services(
    client: ControlServiceClient, server: ControlServiceServer
) -> None:
    await asyncio.gather(server.start(), client.start())


async def stop_services(
    client: ControlServiceClient, server: ControlServiceServer
) -> None:
    await client.stop()
    await server.stop()


def model_response(status: str = "ok") -> dict[str, Any]:
    return {
        "status": status,
        "models": [
            {
                "model_name": "login-model",
                "display_name": "Login Model",
                "description": "Public description",
            }
        ]
        if status == "ok"
        else [],
    }


async def test_remote_catalog_makes_one_call_with_sixteen_second_budget() -> None:
    client = RecordingClient(model_response("unauthenticated"))
    catalog = RemoteLoginModelCatalog(client)

    result = await catalog.list_models("")

    assert result == CatalogResult("unauthenticated", ())
    assert client.calls == [
        ("catalog.list_login_models", {"auth_session": None}, 16.0)
    ]


def test_remote_catalog_construction_has_no_side_effects() -> None:
    RemoteLoginModelCatalog(FailingClient())


async def test_remote_catalog_rejects_invalid_session_before_one_call() -> None:
    client = FailingClient()
    catalog = RemoteLoginModelCatalog(client)

    with pytest.raises(CatalogError) as caught:
        await catalog.list_models("x" * 257)

    assert caught.value.reason == "protocol"
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        (
            model_response("ok"),
            CatalogResult(
                "ok",
                (
                    PublicLoginModel(
                        model_name="login-model",
                        display_name="Login Model",
                        description="Public description",
                    ),
                ),
            ),
        ),
        (model_response("unauthenticated"), CatalogResult("unauthenticated", ())),
    ],
)
async def test_remote_catalog_returns_snapshot(
    response: dict[str, Any], expected: CatalogResult
) -> None:
    client = RecordingClient(response)
    catalog = RemoteLoginModelCatalog(client)

    result = await catalog.list_models(" session ")

    assert result.status == expected.status
    assert result.models == expected.models
    assert client.calls[0][1] == {"auth_session": "session"}


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (model_response("too_large"), "too_large"),
        (model_response("unavailable"), "unavailable"),
        ({"status": "ok", "models": [{"model_name": "m"}]}, "protocol"),
    ],
)
async def test_remote_catalog_maps_control_business_result(
    response: dict[str, Any], reason: str
) -> None:
    client = RecordingClient(response)
    catalog = RemoteLoginModelCatalog(client)

    with pytest.raises(CatalogError) as caught:
        await catalog.list_models("session")

    assert caught.value.reason == reason
    assert caught.value.__cause__ is None
    assert len(client.calls) == 1


@pytest.mark.parametrize(
    ("exception", "reason"),
    [
        (ControlServiceBusy(), "busy"),
        (
            ControlServiceOperationError("OPERATION_NOT_ENABLED"),
            "not_enabled",
        ),
        (ControlServiceOperationError("CONTROL_SERVICE_FAILED"), "unavailable"),
        (ControlServiceError(), "unavailable"),
        (TimeoutError(), "unavailable"),
        (ConnectionResetError(), "unavailable"),
        (ControlProtocolError("invalid response"), "protocol"),
        (RuntimeError("secret-value"), "unavailable"),
    ],
)
async def test_remote_catalog_maps_transport_failures_without_secret_echo(
    exception: BaseException, reason: str
) -> None:
    client = RecordingClient(exception)
    catalog = RemoteLoginModelCatalog(client)

    with pytest.raises(CatalogError) as caught:
        await catalog.list_models("session")

    assert caught.value.reason == reason
    assert caught.value.__cause__ is None
    assert "secret-value" not in str(caught.value)
    assert len(client.calls) == 1


async def test_remote_catalog_preserves_cancellation_without_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def blocked_open(*args: object, **kwargs: object) -> object:
        pytest.fail("remote catalog must not read local files")

    monkeypatch.setattr("builtins.open", blocked_open)
    client = SleepingClient()
    catalog = RemoteLoginModelCatalog(client)
    request = asyncio.create_task(catalog.list_models("session"))
    await asyncio.sleep(0)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert client.calls == 1


async def test_remote_catalog_uses_real_socket_with_maximum_multibyte_group() -> None:
    calls: list[dict[str, Any]] = []

    async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        calls.append(arguments)
        return model_response("ok")

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity("😀" * 256))
    server = ControlServiceServer(
        control_channel,
        identity("😀" * 256),
        handlers={"catalog.list_login_models": handler},
        admission=ControlServiceAdmission(),
    )
    catalog = RemoteLoginModelCatalog(client)
    await start_services(client, server)
    try:
        result = await catalog.list_models(" session ")

        assert result.status == "ok"
        assert result.models[0].model_name == "login-model"
        assert calls == [{"auth_session": "session"}]
    finally:
        await stop_services(client, server)


async def test_capacity_failure_keeps_control_channel_usable() -> None:
    calls = 0

    async def handler(arguments: dict[str, Any]) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        return model_response("too_large" if calls == 1 else "ok")

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"catalog.list_login_models": handler},
        admission=ControlServiceAdmission(),
    )
    catalog = RemoteLoginModelCatalog(client)
    await start_services(client, server)
    try:
        with pytest.raises(CatalogError) as caught:
            await catalog.list_models("session")
        assert caught.value.reason == "too_large"

        result = await catalog.list_models("session")
        assert result.status == "ok"
        assert calls == 2
    finally:
        await stop_services(client, server)
