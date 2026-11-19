# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

import asyncio
import builtins
from contextlib import asynccontextmanager
from pathlib import Path
import os
import subprocess
import sys
import time
from typing import Any

from fastapi import FastAPI, WebSocket
import httpx
import pytest
from websockets.legacy.client import connect


async def test_protocol_import_failure_reaches_start_and_readiness(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    original = builtins.__import__

    def fail_guard_import(
        name: str,
        globals: dict[str, Any] | None = None,
        locals: dict[str, Any] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> Any:
        if name == "agent_gateway.channel_manager.web.transport_stall_guard":
            raise ImportError("protocol dependency probe")
        return original(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fail_guard_import)
    listener = UvicornListener(FastAPI(), host="127.0.0.1", port=0, ws_per_message_deflate=False)
    try:
        with pytest.raises(ImportError, match="protocol dependency probe"):
            await asyncio.wait_for(listener.start(), 1)
        with pytest.raises(ImportError, match="protocol dependency probe"):
            await asyncio.wait_for(listener.wait_ready(), 1)
        with pytest.raises(ImportError, match="protocol dependency probe"):
            await listener.wait_closed()
        assert listener.bound_addresses == ()
    finally:
        await listener.stop()


async def test_listener_ready_serves_http_and_stop_releases_port() -> None:
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    app = FastAPI()

    @app.get("/probe")
    async def probe() -> dict[str, str]:
        return {"status": "ok"}

    listener = UvicornListener(app, host="127.0.0.1", port=0, ws_per_message_deflate=True)
    try:
        await asyncio.wait_for(listener.start(), 5)
        await listener.wait_ready()
        host, port = listener.bound_addresses[0]
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://{host}:{port}/probe")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}
    finally:
        await asyncio.wait_for(listener.stop(), 5)
    await listener.wait_closed()
    rebound = await asyncio.start_server(lambda reader, writer: writer.close(), host, port)
    rebound.close()
    await rebound.wait_closed()


async def test_listener_closes_active_websocket_and_application_once() -> None:
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    phases: list[str] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        phases.append("start")
        yield
        phases.append("stop")

    app = FastAPI(lifespan=lifespan)

    @app.websocket("/ws")
    async def websocket(ws: WebSocket) -> None:
        await ws.accept()
        await ws.send_text("ready")
        try:
            await ws.receive_text()
        except Exception:
            pass

    listener = UvicornListener(app, host="127.0.0.1", port=0, ws_per_message_deflate=False)
    try:
        await listener.start()
        await listener.start()
        host, port = listener.bound_addresses[0]
        async with connect(f"ws://{host}:{port}/ws") as ws:
            assert await ws.recv() == "ready"
            await asyncio.wait_for(listener.stop(), 5)
            await ws.wait_closed()
            assert ws.close_code == 1012
        await listener.stop()
        await listener.wait_closed()
        assert phases == ["start", "stop"]
        with pytest.raises(RuntimeError, match="closed|stopped"):
            await listener.start()
    finally:
        await listener.stop()


async def test_stop_before_start_wakes_ready_waiter() -> None:
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    listener = UvicornListener(FastAPI(), host="127.0.0.1", port=0, ws_per_message_deflate=False)
    waiter = asyncio.create_task(listener.wait_ready())
    await listener.wait_closed()
    await listener.stop()
    await listener.stop()
    with pytest.raises(RuntimeError, match="closed|stopped"):
        await asyncio.wait_for(waiter, 5)
    assert listener.bound_addresses == ()
    with pytest.raises(RuntimeError, match="closed|stopped"):
        await listener.start()


async def test_stop_consumes_cancelled_task_and_wakes_ready_waiter() -> None:
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    listener = UvicornListener(FastAPI(), host="127.0.0.1", port=0, ws_per_message_deflate=False)
    task = asyncio.create_task(asyncio.sleep(60))
    task.cancel()
    await asyncio.sleep(0)
    listener._task = task
    waiter = asyncio.create_task(listener.wait_ready())

    await asyncio.wait_for(listener.stop(), 5)

    assert listener._ready.is_set()
    with pytest.raises(RuntimeError, match="stopped before becoming ready"):
        await asyncio.wait_for(waiter, 5)


async def test_stop_still_propagates_caller_cancellation() -> None:
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    listener = UvicornListener(FastAPI(), host="127.0.0.1", port=0, ws_per_message_deflate=False)
    task = asyncio.create_task(asyncio.sleep(60))
    listener._task = task
    stopper = asyncio.create_task(listener.stop())
    try:
        await asyncio.sleep(0)
        stopper.cancel()
        with pytest.raises(asyncio.CancelledError):
            await stopper
        assert listener._ready.is_set()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_stop_bounds_shutdown_with_inflight_streaming_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from fastapi.responses import StreamingResponse

    from agent_gateway.transport import uvicorn_listener
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    monkeypatch.setattr(uvicorn_listener, "_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", 0.2)
    stalled = asyncio.Event()
    app = FastAPI()

    @app.get("/stream")
    async def stream() -> StreamingResponse:
        async def body() -> Any:
            yield b"tick\n"
            await stalled.wait()
            yield b"done\n"

        return StreamingResponse(body())

    listener = UvicornListener(app, host="127.0.0.1", port=0, ws_per_message_deflate=False)
    writer: asyncio.StreamWriter | None = None
    try:
        await asyncio.wait_for(listener.start(), 5)
        host, port = listener.bound_addresses[0]
        reader, writer = await asyncio.open_connection(host, port)
        writer.write(b"GET /stream HTTP/1.1\r\nHost: gateway\r\n\r\n")
        await writer.drain()
        received = b""
        while b"tick\n" not in received:
            chunk = await asyncio.wait_for(reader.read(1024), 5)
            assert chunk, "stream closed before the first chunk"
            received += chunk
        assert received.startswith(b"HTTP/1.1 200")

        started = time.monotonic()
        await asyncio.wait_for(listener.stop(), 5)
        assert time.monotonic() - started < 3
        await listener.wait_closed()

        rebound = await asyncio.start_server(lambda reader, writer: writer.close(), host, port)
        rebound.close()
        await rebound.wait_closed()
    finally:
        stalled.set()
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        await listener.stop()


async def test_cancelled_ready_and_closed_waiters_do_not_stop_listener() -> None:
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    listener = UvicornListener(FastAPI(), host="127.0.0.1", port=0, ws_per_message_deflate=False)
    ready = asyncio.create_task(listener.wait_ready())
    ready.cancel()
    with pytest.raises(asyncio.CancelledError):
        await ready
    try:
        await listener.start()
        closed = asyncio.create_task(listener.wait_closed())
        await asyncio.sleep(0)
        assert not closed.done()
        closed.cancel()
        with pytest.raises(asyncio.CancelledError):
            await closed
        host, port = listener.bound_addresses[0]
        reader, writer = await asyncio.open_connection(host, port)
        writer.close()
        await writer.wait_closed()
        await listener.wait_ready()
    finally:
        await listener.stop()


@pytest.mark.parametrize("cancel_start", [False, True])
async def test_stop_during_startup_cleans_up_without_false_ready(cancel_start: bool) -> None:
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    entered = asyncio.Event()
    release = asyncio.Event()
    phases: list[str] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        entered.set()
        await release.wait()
        phases.append("start")
        yield
        phases.append("stop")

    listener = UvicornListener(FastAPI(lifespan=lifespan), host="127.0.0.1", port=0, ws_per_message_deflate=False)
    start = asyncio.create_task(listener.start())
    stop: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if cancel_start:
            start.cancel()
        else:
            stop = asyncio.create_task(listener.stop())
        await asyncio.sleep(0)
        release.set()
        expected = asyncio.CancelledError if cancel_start else RuntimeError
        with pytest.raises(expected):
            await asyncio.wait_for(start, 5)
        if stop is not None:
            await asyncio.wait_for(stop, 5)
        with pytest.raises(RuntimeError, match="closed|stopped"):
            await listener.wait_ready()
        assert phases == ["start", "stop"]
        if listener.bound_addresses:
            host, port = listener.bound_addresses[0]
            rebound = await asyncio.start_server(lambda reader, writer: writer.close(), host, port)
            rebound.close()
            await rebound.wait_closed()
    finally:
        release.set()
        await listener.stop()
        await asyncio.gather(start, *([stop] if stop is not None else []), return_exceptions=True)


async def test_bind_failure_reaches_all_waiters_and_closes_lifespan() -> None:
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    phases: list[str] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        phases.append("start")
        yield
        phases.append("stop")

    occupied = await asyncio.start_server(lambda reader, writer: writer.close(), "127.0.0.1", 0)
    port = occupied.sockets[0].getsockname()[1]
    listener = UvicornListener(FastAPI(lifespan=lifespan), host="127.0.0.1", port=port, ws_per_message_deflate=False)
    try:
        with pytest.raises(RuntimeError, match="startup|start|bind") as error:
            await asyncio.wait_for(listener.start(), 5)
        assert isinstance(error.value.__cause__, SystemExit)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(listener.wait_ready(), 5)
        with pytest.raises(RuntimeError):
            await asyncio.wait_for(listener.wait_closed(), 5)
        assert listener.bound_addresses == ()
        assert phases == ["start", "stop"]
    finally:
        await listener.stop()
        occupied.close()
        await occupied.wait_closed()


def test_lifespan_failure_does_not_exit_host_process(tmp_path: Path) -> None:
    script = '''
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from agent_gateway.transport.uvicorn_listener import UvicornListener

@asynccontextmanager
async def lifespan(app):
    raise ValueError("startup probe")
    yield

async def main():
    listener = UvicornListener(FastAPI(lifespan=lifespan), host="127.0.0.1", port=0, ws_per_message_deflate=False)
    try:
        await listener.start()
    except RuntimeError as error:
        assert isinstance(error.__cause__, SystemExit)
    else:
        raise AssertionError("startup unexpectedly succeeded")
    finally:
        await listener.stop()
    print("HOST_ALIVE")

asyncio.run(main())
'''
    env = os.environ.copy()
    env["JIUWENSWARM_HOME"] = str(tmp_path)
    env["JIUWENSWARM_DATA_DIR"] = str(tmp_path / "data")
    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert "HOST_ALIVE" in result.stdout
    assert "Task exception was never retrieved" not in result.stderr


async def test_framework_configuration_keeps_existing_web_protections(monkeypatch: pytest.MonkeyPatch) -> None:
    import uvicorn

    from agent_gateway.channel_manager.web.transport_stall_guard import guarded_http_protocol, guarded_websockets_protocol
    from agent_gateway.common.ws_limits import WEB_WS_MAX_MESSAGE_BYTES
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    configs: list[uvicorn.Config] = []
    original = uvicorn.Config.__init__

    def capture(self, *args, **kwargs) -> None:
        original(self, *args, **kwargs)
        configs.append(self)

    monkeypatch.setattr(uvicorn.Config, "__init__", capture)
    listener = UvicornListener(FastAPI(), host="127.0.0.1", port=0, ws_per_message_deflate=False)
    try:
        await listener.start()
        assert len(configs) == 1
        config = configs[0]
        assert config.http is guarded_http_protocol()
        assert config.ws is guarded_websockets_protocol()
        assert config.ws_max_size == WEB_WS_MAX_MESSAGE_BYTES
        assert config.ws_ping_interval == 20.0
        assert config.ws_ping_timeout == 60.0
        assert config.ws_per_message_deflate is False
        assert config.access_log is False
    finally:
        await listener.stop()


async def test_partial_bind_failure_closes_socket_and_lifespan(monkeypatch: pytest.MonkeyPatch) -> None:
    import uvicorn

    from agent_gateway.transport.uvicorn_listener import UvicornListener

    phases: list[str] = []
    bound: list[tuple[str, int]] = []

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        phases.append("start")
        yield
        phases.append("stop")

    def fail_after_bind(self, listeners) -> None:
        for sock in listeners:
            address = sock.getsockname()
            bound.append((address[0], address[1]))
        raise ValueError("partial bind probe")

    monkeypatch.setattr(uvicorn.Server, "_log_started_message", fail_after_bind)
    listener = UvicornListener(FastAPI(lifespan=lifespan), host="127.0.0.1", port=0, ws_per_message_deflate=False)
    try:
        with pytest.raises(ValueError, match="partial bind probe"):
            await asyncio.wait_for(listener.start(), 5)
        assert phases == ["start", "stop"]
        host, port = bound[0]
        rebound = await asyncio.start_server(lambda reader, writer: writer.close(), host, port)
        rebound.close()
        await rebound.wait_closed()
    finally:
        await listener.stop()
