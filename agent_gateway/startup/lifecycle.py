"""Workspace ownership and graceful shutdown for Gateway entrypoints."""

import asyncio
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
import logging
from pathlib import Path
import signal
from types import FrameType
from typing import Any

from agent_gateway.gateway_lock import GatewayLock


@contextmanager
def gateway_workspace_lock(workspace: Path) -> Iterator[GatewayLock]:
    """Hold the existing workspace lock for one Gateway lifetime."""
    lock = GatewayLock(workspace)
    if not lock.acquire():
        logging.getLogger("gateway").error(
            "[App] Another Gateway is already serving this workspace; "
            "refusing duplicate instance (lock: %s)",
            lock.lock_path,
        )
        raise SystemExit(1)
    try:
        yield lock
    finally:
        lock.release()


async def run_with_shutdown_signals(
    run: Callable[[asyncio.Event], Awaitable[None]],
    cleanup: Callable[[], Awaitable[None]],
) -> None:
    """Run on the main thread with idempotent signal-driven shutdown."""
    loop = asyncio.get_running_loop()
    shutdown_event = asyncio.Event()
    previous_handlers: dict[signal.Signals, Any] = {}

    def request_shutdown(signum: int, frame: FrameType | None) -> None:
        loop.call_soon_threadsafe(shutdown_event.set)

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[signum] = signal.signal(signum, request_shutdown)
        await run(shutdown_event)
    finally:
        try:
            for signum, handler in previous_handlers.items():
                signal.signal(signum, handler)
        finally:
            await cleanup()
