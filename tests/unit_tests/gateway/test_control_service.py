"""Behavioral tests for the private control-service association layer."""

import asyncio
import os
import socket
from dataclasses import FrozenInstanceError

import pytest

from agent_gateway.processes import control_service as control_service_module
from agent_gateway.processes.control_service import (
    ControlServiceAdmission,
    ControlServiceBusy,
    ControlServiceClient,
    ControlServiceError,
    ControlServiceIdentity,
    ControlServiceOperationError,
    ControlServiceServer,
    ControlServices,
)
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_protocol import ControlProtocolError


@pytest.fixture
def isolated_environment(tmp_path, monkeypatch):
    home = tmp_path / "home"
    data = tmp_path / "data"
    temporary = tmp_path / "tmp"
    for directory in (home, data, temporary):
        directory.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_DATA_DIR", str(data))
    monkeypatch.setenv("TMPDIR", str(temporary))


def identity() -> ControlServiceIdentity:
    current_pid = os.getpid()
    return ControlServiceIdentity(
        group_id="gateway-group",
        slot_id=0,
        generation=1,
        worker_pid=current_pid,
        control_pid=current_pid,
    )


def channel_pair(
    *, send_timeout: float = 1.0
) -> tuple[ControlServiceChannel, ControlServiceChannel]:
    worker_socket, control_socket = socket.socketpair()
    return (
        ControlServiceChannel(worker_socket, send_timeout=send_timeout),
        ControlServiceChannel(control_socket, send_timeout=send_timeout),
    )


async def wait_until(predicate, timeout: float = 1.0) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(poll(), timeout)


def ack_message() -> dict:
    """Build a valid client-to-control frame that the control side treats as late."""
    return {
        "protocol_version": 1,
        "group_id": "gateway-group",
        "slot_id": 0,
        "generation": 1,
        "type": "EVENT_ACK",
        "event_id": "0123456789abcdef0123456789abcdef",
        "status": "OK",
        "error_code": None,
    }


def response_message() -> dict:
    """Build a valid control-to-client frame that the client side treats as late."""
    return {
        "protocol_version": 1,
        "group_id": "gateway-group",
        "slot_id": 0,
        "generation": 1,
        "type": "RESPONSE",
        "request_id": "0123456789abcdef0123456789abcdef",
        "status": "ERROR",
        "result": None,
        "error_code": "CONTROL_SERVICE_FAILED",
    }


def oversized_object() -> dict:
    """Build an object that passes the JSON limits but not the 64 KiB frame limit."""
    return {"values": ["x" * 4096 for _ in range(16)]}


async def start_services(
    client: ControlServiceClient,
    server: ControlServiceServer,
) -> None:
    await asyncio.gather(server.start(), client.start())


async def stop_services(
    client: ControlServiceClient,
    server: ControlServiceServer,
) -> None:
    await client.stop()
    await server.stop()


class CancelFirstTaskSet(set):
    """Cancel the first added task before the event loop runs its first step."""

    def __init__(self) -> None:
        """Start with the first cancellation still armed."""
        super().__init__()
        self.cancel_first = True

    def add(self, task: asyncio.Task[None]) -> None:
        """Add the task, cancelling it first when the arm is still set."""
        super().add(task)
        if self.cancel_first:
            self.cancel_first = False
            task.cancel()


async def test_admission_is_synchronous_and_bounded(isolated_environment) -> None:
    admission = ControlServiceAdmission(limit=2)

    assert admission.acquire() is True
    assert admission.acquire() is True
    assert admission.acquire() is False
    admission.release()
    assert admission.acquire() is True
    admission.release()
    admission.release()


def test_control_service_failures_have_distinct_fixed_messages() -> None:
    assert str(ControlServiceError()) == "control service connection failed"
    assert str(ControlServiceBusy()) == "control service budget exhausted"

    error = ControlServiceOperationError("CONTROL_SERVICE_FAILED")

    assert str(error) == "control service operation failed"
    assert error.error_code == "CONTROL_SERVICE_FAILED"


def test_control_services_rejects_non_reserved_handler(isolated_environment) -> None:
    async def handler(arguments: dict) -> dict:
        return {}

    with pytest.raises(ValueError, match="reserved operation"):
        ControlServices(handlers={"debug.echo": handler})

    assert ControlServices().handlers == {}
    assert ControlServices().max_handlers == 8
    assert ControlServices().max_group_handlers == 16


def test_control_services_validates_and_owns_its_configuration() -> None:
    async def handler(arguments: dict) -> dict:
        return {}

    async def handlers_factory(identity: ControlServiceIdentity) -> dict:
        return {"bindings.lookup": handler}

    def on_connect(server: ControlServiceServer) -> None:
        return None

    def on_disconnect(identity: ControlServiceIdentity) -> None:
        return None

    handlers = {"bindings.lookup": handler}
    services = ControlServices(
        handlers=handlers,
        handlers_factory=handlers_factory,
        on_connect=on_connect,
        on_disconnect=on_disconnect,
        max_handlers=4,
        max_group_handlers=8,
    )
    assert set(services.handlers) == {"bindings.lookup"}
    assert services.handlers_factory is handlers_factory
    assert services.on_connect is on_connect
    assert services.on_disconnect is on_disconnect
    assert services.max_handlers == 4
    assert services.max_group_handlers == 8

    # The record keeps a read-only copy, not the caller's mapping.
    handlers.clear()
    assert set(services.handlers) == {"bindings.lookup"}
    with pytest.raises(TypeError):
        services.handlers["bindings.lookup"] = handler
    with pytest.raises(FrozenInstanceError):
        services.max_handlers = 9

    for field, value, message in (
        ("handlers", object(), "handlers must be a mapping of reserved operations"),
        (
            "handlers",
            {"bindings.lookup": "not-callable"},
            "control handlers must be callable",
        ),
        ("handlers_factory", "not-callable", "handlers_factory must be callable"),
        ("on_connect", "not-callable", "on_connect must be callable"),
        ("on_disconnect", "not-callable", "on_disconnect must be callable"),
        ("max_handlers", 0, "max_handlers must be a positive integer"),
        ("max_group_handlers", 0, "max_group_handlers must be a positive integer"),
    ):
        with pytest.raises(ValueError, match=message):
            ControlServices(**{field: value})


async def test_recent_identifiers_are_bounded_and_expire(
    isolated_environment, monkeypatch
) -> None:
    assert control_service_module._SEEN_IDENTIFIERS_LIMIT == 128
    assert control_service_module._SEEN_IDENTIFIERS_TTL == 30.0

    recent = control_service_module._RecentIdentifiers()
    identifiers = [f"{index:032x}" for index in range(129)]
    for identifier in identifiers:
        recent.remember(identifier)

    retained = recent.identifiers()
    assert len(retained) == 128
    assert identifiers[0] not in retained
    assert identifiers[-1] in retained

    active_identifier = "f" * 32
    recent.remember(active_identifier)
    assert recent.contains(active_identifier) is True

    monkeypatch.setattr(
        control_service_module,
        "_SEEN_IDENTIFIERS_TTL",
        0.01,
    )
    await asyncio.sleep(0.02)
    assert recent.contains(active_identifier) is False
    assert recent.identifiers() == set()


async def test_concurrent_calls_match_out_of_order_responses(isolated_environment) -> None:
    release = asyncio.Event()

    async def slow(arguments: dict) -> dict:
        await release.wait()
        return {"operation": "slow"}

    async def fast(arguments: dict) -> dict:
        return {"operation": "fast"}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={
            "credentials.request_model_auth": slow,
            "bindings.lookup": fast,
        },
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    slow_call = asyncio.create_task(
        client.call("credentials.request_model_auth", {}, timeout=1.0)
    )
    fast_call = asyncio.create_task(client.call("bindings.lookup", {}, timeout=1.0))
    assert await fast_call == {"operation": "fast"}
    release.set()
    assert await slow_call == {"operation": "slow"}

    await stop_services(client, server)


async def test_local_and_group_handler_limits_return_busy(isolated_environment) -> None:
    for server_kwargs, admission_limit in (({"max_handlers": 1}, 2), ({"max_handlers": 2}, 1)):
        handler_started = asyncio.Event()
        release = asyncio.Event()

        async def handler(arguments: dict) -> dict:
            handler_started.set()
            await release.wait()
            return {}

        worker_channel, control_channel = channel_pair()
        client = ControlServiceClient(worker_channel, identity())
        server = ControlServiceServer(
            control_channel,
            identity(),
            handlers={"bindings.lookup": handler},
            admission=ControlServiceAdmission(limit=admission_limit),
            **server_kwargs,
        )
        await start_services(client, server)

        first = asyncio.create_task(client.call("bindings.lookup", {}, timeout=1.0))
        await handler_started.wait()
        with pytest.raises(ControlServiceBusy):
            await client.call("bindings.lookup", {}, timeout=1.0)

        release.set()
        assert await first == {}
        await stop_services(client, server)
        release.clear()


async def test_handler_cancelled_before_start_keeps_admission_available(
    isolated_environment,
) -> None:
    async def handler(arguments: dict) -> dict:
        return {"value": "ok"}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(limit=1),
    )
    await start_services(client, server)
    server._handler_tasks = CancelFirstTaskSet()

    with pytest.raises(asyncio.TimeoutError):
        await client.call("bindings.lookup", {}, timeout=0.1)
    assert await client.call("bindings.lookup", {}, timeout=1.0) == {"value": "ok"}

    await stop_services(client, server)


async def test_event_handler_cancelled_before_start_keeps_admission_available(
    isolated_environment,
) -> None:
    handled: list[dict] = []

    async def event_handler(arguments: dict) -> None:
        handled.append(arguments)

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(
        worker_channel,
        identity(),
        max_pending=1,
        event_handlers={"credentials.invalidate": event_handler},
    )
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    client._event_tasks = CancelFirstTaskSet()

    with pytest.raises(asyncio.TimeoutError):
        await server.notify("credentials.invalidate", {"attempt": 1}, timeout=0.1)
    await server.notify("credentials.invalidate", {"attempt": 2}, timeout=1.0)

    assert handled == [{"attempt": 2}]
    await stop_services(client, server)


async def test_event_admission_limit_returns_busy(isolated_environment) -> None:
    event_started = asyncio.Event()
    release = asyncio.Event()

    async def event_handler(arguments: dict) -> None:
        if arguments.get("mode") == "fast":
            return
        event_started.set()
        await release.wait()

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(
        worker_channel,
        identity(),
        max_pending=1,
        event_handlers={"credentials.invalidate": event_handler},
    )
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    held = asyncio.create_task(
        server.notify("credentials.invalidate", {"mode": "held"}, timeout=2.0),
    )
    await asyncio.wait_for(event_started.wait(), timeout=1.0)
    with pytest.raises(ControlServiceBusy):
        await server.notify("credentials.invalidate", {"mode": "fast"}, timeout=1.0)

    release.set()
    await asyncio.wait_for(held, timeout=1.0)
    await stop_services(client, server)


async def test_oversized_handler_result_is_an_error_not_a_service_failure(
    isolated_environment,
) -> None:
    async def handler(arguments: dict) -> dict:
        if arguments.get("mode") == "ok":
            return {"value": "ok"}
        return oversized_object()

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"catalog.list_login_models": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    with pytest.raises(ControlServiceOperationError) as raised:
        await client.call("catalog.list_login_models", {"mode": "big"}, timeout=1.0)

    assert raised.value.error_code == "CONTROL_SERVICE_FAILED"
    assert control_channel.closed is False
    assert server._failure is None
    assert await client.call(
        "catalog.list_login_models", {"mode": "ok"}, timeout=1.0
    ) == {"value": "ok"}

    await stop_services(client, server)


async def test_oversized_arguments_are_rejected_without_failing_the_service(
    isolated_environment,
) -> None:
    async def handler(arguments: dict) -> dict:
        return {"value": "ok"}

    async def event_handler(arguments: dict) -> None:
        return None

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(
        worker_channel,
        identity(),
        event_handlers={"credentials.invalidate": event_handler},
    )
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    with pytest.raises(ControlProtocolError, match="frame limit"):
        await client.call("bindings.lookup", oversized_object(), timeout=1.0)
    assert client._pending == {}
    with pytest.raises(ControlProtocolError, match="frame limit"):
        await server.notify("credentials.invalidate", oversized_object(), timeout=1.0)
    assert server._event_pending == {}

    assert worker_channel.closed is False
    assert client._failure is None
    assert server._failure is None
    assert await client.call("bindings.lookup", {}, timeout=1.0) == {"value": "ok"}

    await stop_services(client, server)


async def test_full_send_queue_is_backpressure_not_a_service_failure(
    isolated_environment,
) -> None:
    async def handler(arguments: dict) -> dict:
        return {"value": "ok"}

    worker_channel, control_channel = channel_pair(send_timeout=5.0)
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    # Holding the write lock stands in for a peer that drains slowly.
    async with worker_channel._send_lock:
        queued = [asyncio.create_task(client._send(ack_message())) for _ in range(32)]
        await wait_until(lambda: len(worker_channel._send_tasks) == 32)

        with pytest.raises(ControlServiceBusy):
            await client._send(ack_message())

        assert worker_channel.closed is False
        assert client._failure is None
    await asyncio.gather(*queued)

    assert await client.call("bindings.lookup", {}, timeout=1.0) == {"value": "ok"}

    await stop_services(client, server)


async def test_full_send_queue_does_not_kill_reader_owned_replies(
    isolated_environment,
) -> None:
    async def handler(arguments: dict) -> dict:
        return {"value": "ok"}

    worker_channel, control_channel = channel_pair(send_timeout=5.0)
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    async with control_channel._send_lock:
        queued = [
            asyncio.create_task(server._send(response_message())) for _ in range(32)
        ]
        await wait_until(lambda: len(control_channel._send_tasks) == 32)

        # No handler is registered, so the reader itself has to answer.
        with pytest.raises(asyncio.TimeoutError):
            await client.call("catalog.list_login_models", {}, timeout=0.1)

        assert control_channel.closed is False
        assert server._failure is None
    await asyncio.gather(*queued)

    assert await client.call("bindings.lookup", {}, timeout=1.0) == {"value": "ok"}

    await stop_services(client, server)


async def test_full_send_queue_makes_outgoing_requests_busy(
    isolated_environment,
) -> None:
    async def handler(arguments: dict) -> dict:
        return {"value": "ok"}

    async def event_handler(arguments: dict) -> None:
        return None

    worker_channel, control_channel = channel_pair(send_timeout=5.0)
    client = ControlServiceClient(
        worker_channel,
        identity(),
        event_handlers={"credentials.invalidate": event_handler},
    )
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    async with worker_channel._send_lock:
        queued = [asyncio.create_task(client._send(ack_message())) for _ in range(32)]
        await wait_until(lambda: len(worker_channel._send_tasks) == 32)

        with pytest.raises(ControlServiceBusy):
            await client.call("bindings.lookup", {}, timeout=1.0)

        assert client._pending == {}
        assert client._failure is None
    await asyncio.gather(*queued)

    async with control_channel._send_lock:
        queued = [
            asyncio.create_task(server._send(response_message())) for _ in range(32)
        ]
        await wait_until(lambda: len(control_channel._send_tasks) == 32)

        with pytest.raises(ControlServiceBusy):
            await server.notify("credentials.invalidate", {}, timeout=1.0)

        assert server._event_pending == {}
        assert server._failure is None
    await asyncio.gather(*queued)

    assert await client.call("bindings.lookup", {}, timeout=1.0) == {"value": "ok"}
    await server.notify("credentials.invalidate", {}, timeout=1.0)

    await stop_services(client, server)


async def test_client_pending_limit_returns_busy_without_dispatch(isolated_environment) -> None:
    handler_started = asyncio.Event()
    release = asyncio.Event()
    dispatches = 0

    async def handler(arguments: dict) -> dict:
        nonlocal dispatches
        dispatches += 1
        handler_started.set()
        await release.wait()
        return {}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity(), max_pending=1)
    server = ControlServiceServer(
        control_channel,
        identity(),
            handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(limit=4),
        max_handlers=4,
    )
    await start_services(client, server)

    first = asyncio.create_task(client.call("bindings.lookup", {}, timeout=1.0))
    await handler_started.wait()
    with pytest.raises(ControlServiceBusy):
        await client.call("bindings.lookup", {}, timeout=1.0)

    assert dispatches == 1
    release.set()
    assert await first == {}
    await stop_services(client, server)


async def test_reserved_operation_without_handler_is_not_enabled(isolated_environment) -> None:
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    with pytest.raises(ControlServiceOperationError) as raised:
        await client.call("catalog.list_login_models", {}, timeout=1.0)

    assert raised.value.error_code == "OPERATION_NOT_ENABLED"

    await stop_services(client, server)


async def test_immediate_error_response_is_owned_by_reader(
    isolated_environment, monkeypatch
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def held_send_response(self, *args, **kwargs) -> None:
        entered.set()
        await release.wait()

    monkeypatch.setattr(ControlServiceServer, "_send_response", held_send_response)
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    pending = asyncio.create_task(
        client.call("catalog.list_login_models", {}, timeout=2.0)
    )
    await entered.wait()
    await server.stop()
    assert not any(
        task.get_coro().__qualname__ == held_send_response.__qualname__
        for task in asyncio.all_tasks()
    )
    release.set()
    with pytest.raises(ControlServiceError):
        await pending
    await client.stop()


async def test_handler_failure_is_sanitized(isolated_environment) -> None:
    async def handler(arguments: dict) -> dict:
        raise RuntimeError("credential-token-value")

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    with pytest.raises(ControlServiceOperationError) as raised:
        await client.call("bindings.lookup", {"token": "credential-token-value"}, timeout=1.0)

    assert raised.value.error_code == "CONTROL_SERVICE_FAILED"
    assert "credential-token-value" not in str(raised.value)
    assert raised.value.__cause__ is None
    assert raised.value.__context__ is None

    await stop_services(client, server)


async def test_event_rejection_ack_is_owned_by_client_reader(
    isolated_environment, monkeypatch
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def held_send_event_ack(self, *args, **kwargs) -> None:
        entered.set()
        await release.wait()

    monkeypatch.setattr(ControlServiceClient, "_send_event_ack", held_send_event_ack)
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    notify = asyncio.create_task(
        server.notify("credentials.invalidate", {}, timeout=2.0)
    )
    await entered.wait()
    await client.stop()
    assert not any(
        task.get_coro().__qualname__ == held_send_event_ack.__qualname__
        for task in asyncio.all_tasks()
    )
    release.set()
    with pytest.raises(ControlServiceError):
        await notify
    await server.stop()


async def test_call_timeout_keeps_channel_open_and_drops_late_response(isolated_environment) -> None:
    release = asyncio.Event()

    async def handler(arguments: dict) -> dict:
        if arguments.get("mode") == "fast":
            return {"value": "ok"}
        await release.wait()
        return {"value": "late"}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    timed_out = asyncio.create_task(client.call("bindings.lookup", {}, timeout=0.01))
    with pytest.raises(asyncio.TimeoutError):
        await timed_out
    assert worker_channel.closed is False

    release.set()
    await asyncio.sleep(0.02)
    assert await client.call("bindings.lookup", {"mode": "fast"}, timeout=1.0) == {"value": "ok"}

    await stop_services(client, server)


async def test_event_delivery_and_not_enabled_event(isolated_environment) -> None:
    async def event_handler(arguments: dict) -> None:
        assert arguments == {"reason": "logout"}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(
        worker_channel,
        identity(),
        event_handlers={"credentials.invalidate": event_handler},
    )
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    await server.notify("credentials.invalidate", {"reason": "logout"}, timeout=1.0)

    await stop_services(client, server)

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    with pytest.raises(ControlServiceOperationError) as raised:
        await server.notify("credentials.invalidate", {}, timeout=1.0)

    assert raised.value.error_code == "EVENT_NOT_ENABLED"
    await stop_services(client, server)


async def test_quiesce_stop_pending_eof_and_idempotence(isolated_environment) -> None:
    handler_started = asyncio.Event()
    release = asyncio.Event()

    async def handler(arguments: dict) -> dict:
        handler_started.set()
        await release.wait()
        return {}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    pending = asyncio.create_task(client.call("bindings.lookup", {}, timeout=2.0))
    await handler_started.wait()
    await server.quiesce()
    await server.stop()

    with pytest.raises(ControlServiceError):
        await pending

    await client.stop()
    await client.stop()
    await server.stop()
    await server.stop()
    assert worker_channel.closed is True
    assert control_channel.closed is True


@pytest.mark.parametrize("stopped_side", ["client", "server"])
async def test_local_stop_is_not_recorded_as_a_service_failure(
    isolated_environment, stopped_side: str,
) -> None:
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    stopped, peer = (client, server) if stopped_side == "client" else (server, client)

    await asyncio.wait_for(stopped.stop(), timeout=1.0)
    await asyncio.wait_for(stopped.wait(), timeout=1.0)

    await asyncio.wait_for(peer.stop(), timeout=1.0)


async def test_local_stop_during_reader_owned_send_is_not_a_failure(
    isolated_environment, monkeypatch,
) -> None:
    send_entered = asyncio.Event()

    async def held_send_frame(frame: bytes, *, deadline: float) -> None:
        send_entered.set()
        await asyncio.Future()

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    monkeypatch.setattr(control_channel, "_send_frame", held_send_frame)

    # No handler is registered, so the reader itself owns the error response.
    pending = asyncio.create_task(
        client.call("catalog.list_login_models", {}, timeout=2.0),
    )
    await asyncio.wait_for(send_entered.wait(), timeout=1.0)

    await asyncio.wait_for(server.stop(), timeout=1.0)
    await asyncio.wait_for(server.wait(), timeout=1.0)

    with pytest.raises(ControlServiceError):
        await asyncio.wait_for(pending, timeout=1.0)
    await asyncio.wait_for(client.stop(), timeout=1.0)


@pytest.mark.parametrize("failed_side", ["client", "server"])
async def test_peer_disconnect_is_still_recorded_as_a_service_failure(
    isolated_environment, failed_side: str,
) -> None:
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    failed, peer = (client, server) if failed_side == "client" else (server, client)

    peer.channel.close()

    with pytest.raises(ControlServiceError):
        await asyncio.wait_for(failed.wait(), timeout=1.0)
    await asyncio.wait_for(failed.stop(), timeout=1.0)
    await asyncio.wait_for(peer.stop(), timeout=1.0)


async def test_start_failure_closes_channel(isolated_environment) -> None:
    worker_channel, control_channel = channel_pair()
    control_channel.close()
    client = ControlServiceClient(worker_channel, identity())

    with pytest.raises(ControlServiceError):
        await client.start(timeout=0.1)

    assert worker_channel.closed is True


async def test_call_timeout_must_be_finite_positive(isolated_environment) -> None:
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    for timeout in (0, -1, float("nan"), float("inf")):
        with pytest.raises(ValueError, match="timeout"):
            await client.call("catalog.list_login_models", {}, timeout=timeout)

    await stop_services(client, server)


async def test_call_timeout_includes_frame_send(isolated_environment, monkeypatch) -> None:
    async def slow_send(message: dict) -> None:
        await asyncio.sleep(0.1)

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    monkeypatch.setattr(worker_channel, "send", slow_send)

    started = asyncio.get_running_loop().time()
    with pytest.raises(asyncio.TimeoutError):
        await client.call("catalog.list_login_models", {}, timeout=0.01)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.05
    await stop_services(client, server)


async def test_invalid_call_arguments_do_not_consume_pending(isolated_environment) -> None:
    async def handler(arguments: dict) -> dict:
        return {}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity(), max_pending=1)
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    with pytest.raises(ControlProtocolError):
        await client.call("bindings.lookup", {"value": object()}, timeout=1.0)
    assert await client.call("bindings.lookup", {}, timeout=1.0) == {}

    await stop_services(client, server)


async def test_invalid_notify_arguments_do_not_consume_pending(isolated_environment) -> None:
    async def event_handler(arguments: dict) -> None:
        return None

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(
        worker_channel,
        identity(),
        event_handlers={"credentials.invalidate": event_handler},
    )
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
        max_pending=1,
    )
    await start_services(client, server)

    with pytest.raises(ControlProtocolError):
        await server.notify("credentials.invalidate", {"value": object()}, timeout=1.0)
    await server.notify("credentials.invalidate", {}, timeout=1.0)

    await stop_services(client, server)


async def test_notify_timeout_includes_frame_send(isolated_environment, monkeypatch) -> None:
    async def slow_send(message: dict) -> None:
        await asyncio.sleep(0.1)

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    monkeypatch.setattr(control_channel, "send", slow_send)

    started = asyncio.get_running_loop().time()
    with pytest.raises(asyncio.TimeoutError):
        await server.notify("credentials.invalidate", {}, timeout=0.01)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.05
    await stop_services(client, server)


async def test_quiesce_and_stop_have_hard_owned_task_budget(isolated_environment) -> None:
    handler_started = asyncio.Event()
    release = asyncio.Event()

    async def handler(arguments: dict) -> dict:
        handler_started.set()
        while not release.is_set():
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                continue
        return {}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    pending = asyncio.create_task(client.call("bindings.lookup", {}, timeout=2.0))
    await handler_started.wait()
    with pytest.raises(ControlServiceError):
        await asyncio.wait_for(server.quiesce(), timeout=2.0)
    with pytest.raises(ControlServiceError):
        await asyncio.wait_for(server.stop(), timeout=2.0)
    with pytest.raises(ControlServiceError):
        await server.stop()
    residual_handlers = server.residual_handlers
    assert residual_handlers
    with pytest.raises(ControlServiceError):
        await pending
    await asyncio.wait_for(client.stop(), timeout=2.0)

    release.set()
    for residual_handler in residual_handlers:
        residual_handler.cancel()
    await asyncio.gather(*residual_handlers, return_exceptions=True)

    assert worker_channel.closed is True
    assert control_channel.closed is True


async def test_cancelled_stop_finishes_owned_cleanup_on_next_stop(
    isolated_environment, monkeypatch
) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def held_quiesce(self) -> None:
        entered.set()
        await release.wait()

    monkeypatch.setattr(ControlServiceServer, "_quiesce", held_quiesce)
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)

    first_stop = asyncio.create_task(server.stop())
    await entered.wait()
    first_stop.cancel()
    assert control_channel.closed is False
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await first_stop

    await server.stop()

    assert worker_channel.closed is True
    assert control_channel.closed is True
    await client.stop()


async def test_quiesce_cancelling_owned_send_does_not_close_channel(
    isolated_environment, monkeypatch
) -> None:
    send_entered = asyncio.Event()
    release_send = asyncio.Event()

    async def handler(arguments: dict) -> dict:
        return {}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    original_send_frame = control_channel._send_frame

    async def held_send_frame(frame: bytes, *, deadline: float) -> None:
        send_entered.set()
        await release_send.wait()
        await original_send_frame(frame, deadline=deadline)

    monkeypatch.setattr(control_channel, "_send_frame", held_send_frame)
    pending = asyncio.create_task(client.call("bindings.lookup", {}, timeout=2.0))
    await send_entered.wait()
    await server.quiesce()

    assert control_channel.closed is False
    release_send.set()
    assert await pending == {}
    assert control_channel._send_tasks == set()

    await stop_services(client, server)


async def test_call_completion_timeout_and_cancel_record_recent_ids(
    isolated_environment,
) -> None:
    handler_started = asyncio.Event()
    release = asyncio.Event()

    async def handler(arguments: dict) -> dict:
        if arguments.get("mode") == "complete":
            return {"value": "ok"}
        handler_started.set()
        await release.wait()
        return {}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    initial_count = len(client._seen_responses.identifiers())

    assert await client.call(
        "bindings.lookup", {"mode": "complete"}, timeout=1.0
    ) == {"value": "ok"}
    assert len(client._seen_responses.identifiers()) == initial_count + 1

    with pytest.raises(asyncio.TimeoutError):
        await client.call("bindings.lookup", {"mode": "timeout"}, timeout=0.01)
    assert len(client._seen_responses.identifiers()) == initial_count + 2

    handler_started.clear()
    cancelled_call = asyncio.create_task(
        client.call("bindings.lookup", {"mode": "cancel"}, timeout=5.0)
    )
    await handler_started.wait()
    cancelled_call.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_call
    assert len(client._seen_responses.identifiers()) == initial_count + 3

    release.set()
    await stop_services(client, server)


async def test_notify_completion_timeout_and_cancel_record_recent_ids(
    isolated_environment,
) -> None:
    event_started = asyncio.Event()
    release = asyncio.Event()

    async def event_handler(arguments: dict) -> None:
        if arguments.get("mode") == "complete":
            return
        event_started.set()
        await release.wait()

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(
        worker_channel,
        identity(),
        event_handlers={"credentials.invalidate": event_handler},
    )
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await start_services(client, server)
    initial_count = len(server._seen_event_acks.identifiers())

    await server.notify(
        "credentials.invalidate", {"mode": "complete"}, timeout=1.0
    )
    assert len(server._seen_event_acks.identifiers()) == initial_count + 1

    with pytest.raises(asyncio.TimeoutError):
        await server.notify(
            "credentials.invalidate", {"mode": "timeout"}, timeout=0.01
        )
    assert len(server._seen_event_acks.identifiers()) == initial_count + 2

    event_started.clear()
    cancelled_notify = asyncio.create_task(
        server.notify(
            "credentials.invalidate", {"mode": "cancel"}, timeout=5.0
        )
    )
    await event_started.wait()
    cancelled_notify.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_notify
    assert len(server._seen_event_acks.identifiers()) == initial_count + 3

    release.set()
    await stop_services(client, server)


async def test_stop_wakes_local_pending_immediately(
    isolated_environment, monkeypatch
) -> None:
    handler_started = asyncio.Event()
    stop_entered = asyncio.Event()
    release_stop = asyncio.Event()

    async def handler(arguments: dict) -> dict:
        handler_started.set()
        await asyncio.Future()

    async def held_client_quiesce(self) -> None:
        stop_entered.set()
        await release_stop.wait()

    monkeypatch.setattr(ControlServiceClient, "_quiesce", held_client_quiesce)
    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await asyncio.wait_for(start_services(client, server), timeout=1.0)

    pending_call = asyncio.create_task(
        client.call("bindings.lookup", {}, timeout=5.0)
    )
    await asyncio.wait_for(handler_started.wait(), timeout=0.2)
    client_stop = asyncio.create_task(client.stop())
    await asyncio.wait_for(stop_entered.wait(), timeout=0.2)
    with pytest.raises(ControlServiceError):
        await asyncio.wait_for(pending_call, timeout=0.1)

    release_stop.set()
    await asyncio.wait_for(client_stop, timeout=1.0)
    await asyncio.wait_for(stop_services(client, server), timeout=2.0)


async def test_quiesce_wakes_local_pending_immediately(isolated_environment) -> None:
    event_started = asyncio.Event()

    async def event_handler(arguments: dict) -> None:
        event_started.set()
        await asyncio.Future()

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(
        worker_channel,
        identity(),
        event_handlers={"credentials.invalidate": event_handler},
    )
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={},
        admission=ControlServiceAdmission(),
    )
    await asyncio.wait_for(start_services(client, server), timeout=1.0)

    pending_notify = asyncio.create_task(
        server.notify("credentials.invalidate", {}, timeout=5.0)
    )
    await asyncio.wait_for(event_started.wait(), timeout=0.2)
    await asyncio.wait_for(server.quiesce(), timeout=1.0)
    with pytest.raises(ControlServiceError):
        await asyncio.wait_for(pending_notify, timeout=0.1)

    await asyncio.wait_for(stop_services(client, server), timeout=2.0)


async def test_cancelling_wait_does_not_cancel_shared_reader(
    isolated_environment,
) -> None:
    handler_started = asyncio.Event()
    release = asyncio.Event()

    async def handler(arguments: dict) -> dict:
        handler_started.set()
        await release.wait()
        return {"value": "ok"}

    worker_channel, control_channel = channel_pair()
    client = ControlServiceClient(worker_channel, identity())
    server = ControlServiceServer(
        control_channel,
        identity(),
        handlers={"bindings.lookup": handler},
        admission=ControlServiceAdmission(),
    )
    await asyncio.wait_for(start_services(client, server), timeout=1.0)

    pending_call = asyncio.create_task(
        client.call("bindings.lookup", {}, timeout=2.0)
    )
    await asyncio.wait_for(handler_started.wait(), timeout=0.2)
    wait_task = asyncio.create_task(client.wait())
    await asyncio.sleep(0)
    wait_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await wait_task

    assert worker_channel.closed is False
    release.set()
    assert await asyncio.wait_for(pending_call, timeout=1.0) == {"value": "ok"}

    await asyncio.wait_for(stop_services(client, server), timeout=2.0)
