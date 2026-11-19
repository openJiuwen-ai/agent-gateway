from __future__ import annotations

import asyncio
import signal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_shutdown_request_is_idempotent_and_restores_handlers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway import app_gateway

    old = {signal.SIGINT: object(), signal.SIGTERM: object()}
    installed = dict(old)

    def replace_handler(signum: signal.Signals, handler: Any) -> Any:
        previous = installed[signum]
        installed[signum] = handler
        return previous

    async def run(
        *,
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        shutdown_event: asyncio.Event,
        web_ws_deflate: bool,
    ) -> None:
        assert agent_server_url == "ws://127.0.0.1:18092"
        assert web_host == "127.0.0.1"
        assert web_port == 19000
        assert web_path == "/ws"
        assert web_ws_deflate is False
        installed[signal.SIGTERM](signal.SIGTERM, None)
        installed[signal.SIGTERM](signal.SIGTERM, None)
        await asyncio.wait_for(shutdown_event.wait(), timeout=1.0)

    cleanup = AsyncMock()
    monkeypatch.setattr(app_gateway.signal, "signal", replace_handler)
    monkeypatch.setattr(app_gateway, "_run", run)
    monkeypatch.setattr(app_gateway, "shutdown_gateway_telemetry", cleanup)

    await app_gateway._run_with_shutdown_signals(
        agent_server_url="ws://127.0.0.1:18092",
        web_host="127.0.0.1",
        web_port=19000,
        web_path="/ws",
    )

    assert installed == old
    cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_failure_restores_handlers_and_awaits_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway import app_gateway

    old = {signal.SIGINT: object(), signal.SIGTERM: object()}
    installed = dict(old)

    def replace_handler(signum: signal.Signals, handler: Any) -> Any:
        previous = installed[signum]
        installed[signum] = handler
        return previous

    async def run(
        *,
        agent_server_url: str,
        web_host: str,
        web_port: int,
        web_path: str,
        shutdown_event: asyncio.Event,
        web_ws_deflate: bool,
    ) -> None:
        assert agent_server_url == "ws://127.0.0.1:18092"
        assert web_host == "127.0.0.1"
        assert web_port == 19000
        assert web_path == "/ws"
        assert web_ws_deflate is False
        raise RuntimeError("startup failed")

    cleanup = AsyncMock()
    monkeypatch.setattr(app_gateway.signal, "signal", replace_handler)
    monkeypatch.setattr(app_gateway, "_run", run)
    monkeypatch.setattr(app_gateway, "shutdown_gateway_telemetry", cleanup)

    with pytest.raises(RuntimeError, match="startup failed"):
        await app_gateway._run_with_shutdown_signals(
            agent_server_url="ws://127.0.0.1:18092",
            web_host="127.0.0.1",
            web_port=19000,
            web_path="/ws",
        )

    assert installed == old
    cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_web_runtime_cleanup_preserves_primary_error() -> None:
    from agent_gateway import app_gateway

    channel = SimpleNamespace(stop=AsyncMock())

    async def failed_start() -> None:
        raise RuntimeError("listener stopped before ready")

    web_task = asyncio.create_task(failed_start())
    await asyncio.sleep(0)

    with pytest.raises(TimeoutError, match="ready timeout"):
        try:
            raise TimeoutError("ready timeout")
        finally:
            await app_gateway._stop_web_runtime(channel, web_task)

    channel.stop.assert_awaited_once()
    assert web_task.done()


@pytest.mark.asyncio
async def test_web_runtime_cleanup_cancels_running_start() -> None:
    from agent_gateway import app_gateway

    channel = SimpleNamespace(stop=AsyncMock())
    started = asyncio.Event()
    keep_running = asyncio.Event()

    async def running_start() -> None:
        started.set()
        await keep_running.wait()

    web_task = asyncio.create_task(running_start())
    await started.wait()

    await app_gateway._stop_web_runtime(channel, web_task)

    channel.stop.assert_awaited_once()
    assert web_task.cancelled()
