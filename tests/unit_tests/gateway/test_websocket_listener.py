# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

import asyncio
import http
import socket
import builtins
import websockets
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from websockets.legacy.exceptions import InvalidStatusCode
from websockets.legacy import server as websockets_server
from websockets.legacy.client import connect


Handler = Callable[..., Awaitable[None]]
ProcessRequest = Callable[..., Awaitable[Any]]


async def no_op_process_request(*args: Any) -> None:
    return None


async def ready_handler(ws: Any, path: str | None = None) -> None:
    await ws.send("ready")
    await ws.wait_closed()


def create_listener(
    handler: Handler,
    *,
    host: str,
    port: int,
    process_request: ProcessRequest,
) -> Any:
    from agent_gateway.transport.websocket_listener import WebSocketListener

    return WebSocketListener(
        handler,
        host=host,
        port=port,
        process_request=process_request,
    )


async def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def assert_port_is_free(host: str, port: int) -> None:
    rebound = await asyncio.start_server(lambda reader, writer: writer.close(), host, port)
    rebound.close()
    await rebound.wait_closed()


@pytest.mark.asyncio
async def test_listener_keeps_handler_process_request_and_releases_socket() -> None:
    requests: list[tuple[str, dict[str, str]]] = []

    async def process_request(path: str, headers: Any) -> None:
        requests.append((path, dict(headers)))
        return None

    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=process_request,
    )
    try:
        await asyncio.wait_for(listener.start(), timeout=5)
        assert listener.bound_addresses
        host, port = listener.bound_addresses[0]
        assert host == "127.0.0.1"
        assert port > 0
        async with connect(f"ws://{host}:{port}/tui") as ws:
            assert await ws.recv() == "ready"
    finally:
        await asyncio.wait_for(listener.stop(), timeout=5)

    await listener.wait_closed()
    assert requests[0][0] == "/tui"
    await assert_port_is_free(host, port)


@pytest.mark.asyncio
async def test_listener_preserves_original_serve_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    original_serve = websockets_server.serve

    async def serve_spy(*args: Any, **kwargs: Any) -> Any:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return await original_serve(*args, **kwargs)

    monkeypatch.setattr(websockets_server, "serve", serve_spy)
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    try:
        await asyncio.wait_for(listener.start(), timeout=5)
        host, port = listener.bound_addresses[0]
        async with connect(f"ws://{host}:{port}/health") as ws:
            assert await ws.recv() == "ready"
    finally:
        await asyncio.wait_for(listener.stop(), timeout=5)

    assert captured["args"][0] is ready_handler
    assert captured["args"][1] == "127.0.0.1"
    assert captured["args"][2] == 0
    assert captured["kwargs"]["process_request"] is no_op_process_request
    assert captured["kwargs"]["ping_interval"] == 20
    assert captured["kwargs"]["ping_timeout"] == 600
    assert captured["kwargs"]["max_size"] == 8 * 2**20


@pytest.mark.asyncio
async def test_wait_ready_can_wait_before_successful_start() -> None:
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    ready = asyncio.create_task(listener.wait_ready())
    await asyncio.sleep(0)
    try:
        await asyncio.wait_for(listener.start(), timeout=5)
        await asyncio.wait_for(ready, timeout=5)
        host, port = listener.bound_addresses[0]
        async with connect(f"ws://{host}:{port}/ready-before-start") as ws:
            assert await ws.recv() == "ready"
    finally:
        await asyncio.wait_for(listener.stop(), timeout=5)

    await listener.wait_closed()


@pytest.mark.asyncio
async def test_listener_falls_back_to_top_level_serve_import(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(websockets, "serve", websockets_server.serve)
    real_import = builtins.__import__

    def import_without_legacy_server(
        name: str,
        globals_object: Any,
        locals_object: Any,
        fromlist: Any = None,
        level: int = 0,
    ) -> Any:
        if name == "websockets.legacy.server":
            raise ImportError("legacy server is unavailable")
        return real_import(
            name,
            globals_object,
            locals_object,
            fromlist,
            level,
        )

    monkeypatch.setattr(builtins, "__import__", import_without_legacy_server)
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    try:
        await asyncio.wait_for(listener.start(), timeout=5)
        host, port = listener.bound_addresses[0]
        async with connect(f"ws://{host}:{port}/fallback") as ws:
            assert await ws.recv() == "ready"
    finally:
        await asyncio.wait_for(listener.stop(), timeout=5)

    await listener.wait_closed()


@pytest.mark.asyncio
async def test_process_request_rejects_handshake_with_http_response() -> None:
    async def process_request(path: str, headers: Any) -> Any:
        return http.HTTPStatus.FORBIDDEN, [], b"forbidden"

    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=process_request,
    )
    try:
        await asyncio.wait_for(listener.start(), timeout=5)
        host, port = listener.bound_addresses[0]
        with pytest.raises(InvalidStatusCode) as error:
            async with connect(f"ws://{host}:{port}/denied"):
                pass
        assert error.value.status_code == http.HTTPStatus.FORBIDDEN
    finally:
        await asyncio.wait_for(listener.stop(), timeout=5)


@pytest.mark.asyncio
async def test_port_occupation_propagates_to_start_and_wait_ready() -> None:
    occupied = await asyncio.start_server(
        lambda reader, writer: writer.close(),
        "127.0.0.1",
        0,
    )
    port = occupied.sockets[0].getsockname()[1]
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=port,
        process_request=no_op_process_request,
    )
    start = asyncio.create_task(listener.start())
    await asyncio.sleep(0)
    ready = asyncio.create_task(listener.wait_ready())

    with pytest.raises(OSError):
        await asyncio.wait_for(start, timeout=5)
    with pytest.raises(OSError):
        await asyncio.wait_for(ready, timeout=5)

    await listener.stop()
    with pytest.raises(OSError):
        await listener.wait_closed()
    assert listener.bound_addresses == ()
    occupied.close()
    await occupied.wait_closed()


@pytest.mark.asyncio
async def test_repeated_start_stop_and_start_after_close() -> None:
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    first_start = asyncio.create_task(listener.start())
    second_start = asyncio.create_task(listener.start())
    await asyncio.wait_for(asyncio.gather(first_start, second_start), timeout=5)
    addresses = listener.bound_addresses

    await asyncio.wait_for(listener.stop(), timeout=5)
    await asyncio.wait_for(listener.stop(), timeout=5)
    await listener.wait_closed()

    assert listener.bound_addresses == addresses
    with pytest.raises(RuntimeError, match="stopped"):
        await listener.start()


@pytest.mark.asyncio
async def test_stop_before_start_is_idempotent_and_fails_ready() -> None:
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    await listener.stop()
    await listener.stop()
    await listener.wait_closed()
    assert listener.bound_addresses == ()

    ready = asyncio.create_task(listener.wait_ready())
    with pytest.raises(RuntimeError, match="stopped"):
        await asyncio.wait_for(ready, timeout=5)
    with pytest.raises(RuntimeError, match="stopped"):
        await listener.start()


@pytest.mark.asyncio
async def test_stop_during_start_wakes_start_and_releases_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_serve = websockets_server.serve
    serve_entered = asyncio.Event()
    allow_bind = asyncio.Event()

    async def delayed_serve(*args: Any, **kwargs: Any) -> Any:
        serve_entered.set()
        await allow_bind.wait()
        return await original_serve(*args, **kwargs)

    monkeypatch.setattr(websockets_server, "serve", delayed_serve)
    port = await reserve_port()
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=port,
        process_request=no_op_process_request,
    )
    start = asyncio.create_task(listener.start())
    await serve_entered.wait()
    stop = asyncio.create_task(listener.stop())
    allow_bind.set()

    await asyncio.wait_for(stop, timeout=5)
    with pytest.raises(RuntimeError, match="stopped during startup"):
        await asyncio.wait_for(start, timeout=5)
    await listener.wait_closed()
    await assert_port_is_free("127.0.0.1", port)


@pytest.mark.asyncio
async def test_post_bind_address_failure_releases_real_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_serve = websockets_server.serve
    bound: list[tuple[str, int]] = []

    class FailingAddressSocket:
        def getsockname(self) -> tuple[str, int]:
            raise ValueError("post-bind address probe")

    class AddressFailureServer:
        def __init__(self, server: Any) -> None:
            self._server = server
            self.sockets = [FailingAddressSocket() for _ in server.sockets or ()]

        def close(self) -> None:
            self._server.close()

        async def wait_closed(self) -> None:
            await self._server.wait_closed()

    async def serve_with_address_failure(*args: Any, **kwargs: Any) -> Any:
        server = await original_serve(*args, **kwargs)
        bound.extend(
            (str(sock.getsockname()[0]), int(sock.getsockname()[1]))
            for sock in server.sockets or ()
        )
        return AddressFailureServer(server)

    monkeypatch.setattr(websockets_server, "serve", serve_with_address_failure)
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )

    with pytest.raises(ValueError, match="post-bind address probe"):
        await asyncio.wait_for(listener.start(), timeout=5)
    with pytest.raises(ValueError, match="post-bind address probe"):
        await listener.wait_closed()

    host, port = bound[0]
    await assert_port_is_free(host, port)
    await listener.stop()


@pytest.mark.asyncio
async def test_cancelling_start_cleans_up_bound_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_serve = websockets_server.serve
    serve_entered = asyncio.Event()
    allow_bind = asyncio.Event()

    async def delayed_serve(*args: Any, **kwargs: Any) -> Any:
        serve_entered.set()
        await allow_bind.wait()
        return await original_serve(*args, **kwargs)

    monkeypatch.setattr(websockets_server, "serve", delayed_serve)
    port = await reserve_port()
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=port,
        process_request=no_op_process_request,
    )
    start = asyncio.create_task(listener.start())
    await serve_entered.wait()
    start.cancel()
    allow_bind.set()

    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(start, timeout=5)
    await listener.wait_closed()
    await assert_port_is_free("127.0.0.1", port)


@pytest.mark.asyncio
async def test_cancelling_wait_closed_does_not_stop_running_listener() -> None:
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    try:
        await asyncio.wait_for(listener.start(), timeout=5)
        waiter = asyncio.create_task(listener.wait_closed())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        host, port = listener.bound_addresses[0]
        async with connect(f"ws://{host}:{port}/still-running") as ws:
            assert await ws.recv() == "ready"
    finally:
        await asyncio.wait_for(listener.stop(), timeout=5)

    await listener.wait_closed()


@pytest.mark.asyncio
async def test_stop_closes_active_connection_with_1001() -> None:
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    await asyncio.wait_for(listener.start(), timeout=5)
    host, port = listener.bound_addresses[0]
    async with connect(f"ws://{host}:{port}/active") as ws:
        assert await ws.recv() == "ready"
        await asyncio.wait_for(listener.stop(), timeout=5)
        await ws.wait_closed()
        assert ws.close_code == 1001

    await listener.wait_closed()


@pytest.mark.asyncio
async def test_same_port_can_be_rebound_after_close() -> None:
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    await asyncio.wait_for(listener.start(), timeout=5)
    host, port = listener.bound_addresses[0]
    await asyncio.wait_for(listener.stop(), timeout=5)
    await listener.wait_closed()

    rebound = await asyncio.start_server(
        lambda reader, writer: writer.close(),
        host,
        port,
    )
    try:
        assert rebound.sockets[0].getsockname()[1] == port
    finally:
        rebound.close()
        await rebound.wait_closed()


@pytest.mark.asyncio
async def test_listener_task_cancelled_before_ready_does_not_cancel_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    serve_entered = asyncio.Event()

    async def blocked_serve(*args: Any, **kwargs: Any) -> Any:
        serve_entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(websockets_server, "serve", blocked_serve)
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    start = asyncio.create_task(listener.start())
    ready = asyncio.create_task(listener.wait_ready())
    await asyncio.wait_for(serve_entered.wait(), timeout=5)
    run_task = listener._run_task
    run_task.cancel()
    outcomes = await asyncio.wait_for(
        asyncio.gather(start, ready, return_exceptions=True), timeout=5
    )

    assert run_task.cancelled()
    assert all(isinstance(outcome, RuntimeError) for outcome in outcomes)
    assert not start.cancelled()
    assert not ready.cancelled()
    with pytest.raises(RuntimeError):
        await listener.wait_closed()
    await listener.stop()


@pytest.mark.asyncio
async def test_listener_task_cancelled_after_ready_preserves_readiness_and_releases_port() -> None:
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    await asyncio.wait_for(listener.start(), timeout=5)
    host, port = listener.bound_addresses[0]
    run_task = listener._run_task
    run_task.cancel()
    await asyncio.wait_for(listener._closed.wait(), timeout=5)

    try:
        assert run_task.cancelled()
        await asyncio.wait_for(listener.wait_ready(), timeout=5)
        with pytest.raises(RuntimeError):
            await listener.wait_closed()
        with pytest.raises(RuntimeError, match="closed"):
            await listener.start()
    finally:
        await asyncio.wait_for(listener.stop(), timeout=5)

    await assert_port_is_free(host, port)


@pytest.mark.asyncio
async def test_runtime_failure_after_ready_is_reported_only_by_wait_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_serve = websockets_server.serve
    fail = asyncio.Event()
    runtime_error = OSError("listener failed after readiness")

    async def serve_with_runtime_failure(*args: Any, **kwargs: Any) -> Any:
        server = await original_serve(*args, **kwargs)
        original_wait_closed = server.wait_closed

        async def fail_once() -> None:
            server.wait_closed = original_wait_closed
            await fail.wait()
            raise runtime_error

        server.wait_closed = fail_once
        return server

    monkeypatch.setattr(websockets_server, "serve", serve_with_runtime_failure)
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )
    await asyncio.wait_for(listener.start(), timeout=5)
    host, port = listener.bound_addresses[0]
    fail.set()
    await asyncio.wait_for(listener._closed.wait(), timeout=5)

    try:
        await asyncio.wait_for(listener.wait_ready(), timeout=5)
        with pytest.raises(OSError, match="failed after readiness") as error:
            await listener.wait_closed()
        assert error.value is runtime_error
        with pytest.raises(RuntimeError, match="closed"):
            await listener.start()
    finally:
        await asyncio.wait_for(listener.stop(), timeout=5)

    await assert_port_is_free(host, port)


@pytest.mark.asyncio
async def test_non_exception_task_failure_does_not_escape_to_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ListenerTaskFailure(BaseException):
        pass

    failure = ListenerTaskFailure("listener task aborted")

    async def failing_serve(*args: Any, **kwargs: Any) -> Any:
        raise failure

    monkeypatch.setattr(websockets_server, "serve", failing_serve)
    listener = create_listener(
        ready_handler,
        host="127.0.0.1",
        port=0,
        process_request=no_op_process_request,
    )

    for operation in [listener.start, listener.wait_ready, listener.wait_closed]:
        with pytest.raises(RuntimeError) as error:
            await asyncio.wait_for(operation(), timeout=5)
        assert error.value.__cause__ is failure
    await listener.stop()
