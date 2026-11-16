import asyncio
from pathlib import Path
import signal

import pytest


def test_workspace_lock_released_after_failure(tmp_path: Path) -> None:
    from agent_gateway.gateway_lock import GatewayLock
    from agent_gateway.startup.lifecycle import gateway_workspace_lock

    with pytest.raises(RuntimeError, match="body failed"):
        with gateway_workspace_lock(tmp_path):
            raise RuntimeError("body failed")
    lock = GatewayLock(tmp_path)
    try:
        assert lock.acquire(timeout=0)
    finally:
        lock.release()


@pytest.mark.asyncio
@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
async def test_signals_request_shutdown_and_restore_handlers(signum: signal.Signals) -> None:
    from agent_gateway.startup.lifecycle import run_with_shutdown_signals

    original = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    cleaned = asyncio.Event()

    async def run(shutdown_event: asyncio.Event) -> None:
        handler = signal.getsignal(signum)
        handler(signum, None)
        handler(signum, None)
        await asyncio.wait_for(shutdown_event.wait(), 1)

    async def cleanup() -> None:
        cleaned.set()

    await run_with_shutdown_signals(run, cleanup)
    assert cleaned.is_set()
    assert {number: signal.getsignal(number) for number in original} == original


@pytest.mark.asyncio
async def test_run_failure_restores_signals_and_cleans_up() -> None:
    from agent_gateway.startup.lifecycle import run_with_shutdown_signals

    original = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    cleaned = asyncio.Event()

    async def run(shutdown_event: asyncio.Event) -> None:
        raise RuntimeError("startup failed")

    async def cleanup() -> None:
        cleaned.set()

    with pytest.raises(RuntimeError, match="startup failed"):
        await run_with_shutdown_signals(run, cleanup)
    assert cleaned.is_set()
    assert {number: signal.getsignal(number) for number in original} == original


@pytest.mark.asyncio
async def test_partial_signal_installation_is_restored(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_gateway.startup import lifecycle

    original = signal.getsignal(signal.SIGINT)
    real_signal = signal.signal
    cleaned = asyncio.Event()

    def install(number, handler):
        if number == signal.SIGTERM:
            raise OSError("registration failed")
        return real_signal(number, handler)

    async def run(shutdown_event: asyncio.Event) -> None:
        pytest.fail("runtime must not start after failed signal registration")

    async def cleanup() -> None:
        cleaned.set()

    monkeypatch.setattr(lifecycle.signal, "signal", install)
    try:
        with pytest.raises(OSError, match="registration failed"):
            await lifecycle.run_with_shutdown_signals(run, cleanup)
        assert signal.getsignal(signal.SIGINT) == original
        assert cleaned.is_set()
    finally:
        real_signal(signal.SIGINT, original)
