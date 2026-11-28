from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agent_gateway.auth.public_model_catalog import (
    CatalogError,
    LoginModelSnapshot,
    PublicLoginModel,
)
from agent_gateway.auth.remote_model_catalog import RemoteLoginModelSnapshot
from agent_gateway.processes.control_protocol import ControlProtocolError
from agent_gateway.processes.control_service import (
    ControlServiceBusy,
    ControlServiceError,
    ControlServiceOperationError,
)


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


def snapshot_response(status: str = "ok") -> dict[str, Any]:
    return {
        "version": 1,
        "status": status,
        "api_base": "https://models.example.invalid/v1" if status == "ok" else "",
        "models": [{
            "model_name": "login-model",
            "display_name": "Login Model",
            "description": "Public description",
        }] if status == "ok" else [],
    }


async def test_remote_snapshot_makes_one_call_with_nine_second_budget() -> None:
    client = RecordingClient(snapshot_response("unauthenticated"))
    snapshot = RemoteLoginModelSnapshot(client)

    result = await snapshot.get_snapshot("  ")

    assert result == LoginModelSnapshot("unauthenticated", "", ())
    assert client.calls == [
        (
            "catalog.get_login_model_snapshot",
            {"auth_session": None},
            9.0,
        )
    ]


async def test_remote_snapshot_returns_validated_snapshot() -> None:
    client = RecordingClient(snapshot_response())
    snapshot = RemoteLoginModelSnapshot(client)

    result = await snapshot.get_snapshot(" session ")

    assert result == LoginModelSnapshot(
        "ok",
        "https://models.example.invalid/v1",
        (PublicLoginModel("login-model", "Login Model", "Public description"),),
    )
    assert client.calls == [
        (
            "catalog.get_login_model_snapshot",
            {"auth_session": "session"},
            9.0,
        )
    ]


async def test_remote_snapshot_rejects_invalid_session_before_call() -> None:
    client = FailingClient()
    snapshot = RemoteLoginModelSnapshot(client)

    with pytest.raises(CatalogError) as caught:
        await snapshot.get_snapshot("x" * 257)

    assert caught.value.reason == "protocol"
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    ("response", "reason"),
    [
        (snapshot_response("too_large"), "too_large"),
        (snapshot_response("unavailable"), "unavailable"),
        ({**snapshot_response(), "api_base": None}, "protocol"),
        ({**snapshot_response(), "api_base": ""}, "protocol"),
    ],
)
async def test_remote_snapshot_maps_control_business_result(
    response: dict[str, Any], reason: str
) -> None:
    client = RecordingClient(response)
    snapshot = RemoteLoginModelSnapshot(client)

    with pytest.raises(CatalogError) as caught:
        await snapshot.get_snapshot("session")

    assert caught.value.reason == reason
    assert caught.value.__cause__ is None
    assert len(client.calls) == 1


@pytest.mark.parametrize(
    ("exception", "reason"),
    [
        (ControlServiceBusy(), "busy"),
        (ControlServiceOperationError("OPERATION_NOT_ENABLED"), "not_enabled"),
        (ControlServiceOperationError("CONTROL_SERVICE_FAILED"), "unavailable"),
        (ControlServiceError(), "unavailable"),
        (TimeoutError(), "unavailable"),
        (ConnectionResetError(), "unavailable"),
        (ControlProtocolError("invalid response"), "protocol"),
        (RuntimeError("secret-value"), "unavailable"),
    ],
)
async def test_remote_snapshot_maps_transport_failures_without_secret_echo(
    exception: BaseException, reason: str
) -> None:
    client = RecordingClient(exception)
    snapshot = RemoteLoginModelSnapshot(client)

    with pytest.raises(CatalogError) as caught:
        await snapshot.get_snapshot("session")

    assert caught.value.reason == reason
    assert caught.value.__cause__ is None
    assert "secret-value" not in str(caught.value)
    assert len(client.calls) == 1


async def test_remote_snapshot_preserves_cancellation_without_d1_fallback() -> None:
    client = SleepingClient()
    snapshot = RemoteLoginModelSnapshot(client)
    request = asyncio.create_task(snapshot.get_snapshot("session"))
    await asyncio.sleep(0)

    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    assert client.calls == 1
