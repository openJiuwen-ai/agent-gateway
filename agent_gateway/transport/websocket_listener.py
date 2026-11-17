"""Own the lifecycle of one embedded WebSocket server."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any


class WebSocketListener:
    """Start one WebSocket listener and await its complete framework shutdown."""

    def __init__(
        self,
        handler: Callable[..., Awaitable[None]],
        *,
        host: str,
        port: int,
        process_request: Callable[..., Awaitable[Any]],
    ) -> None:
        self._handler = handler
        self._host = host
        self._port = port
        self._process_request = process_request
        self._ready = asyncio.Event()
        self._start_done = asyncio.Event()
        self._closed = asyncio.Event()
        self._run_task: asyncio.Task[None] | None = None
        self._server: Any | None = None
        self._bound_addresses: tuple[tuple[str, int], ...] = ()
        self._stop_requested = False
        self._startup_error: Exception | None = None
        self._run_error: Exception | None = None
        self._stopped_during_startup: RuntimeError | None = None

    @property
    def bound_addresses(self) -> tuple[tuple[str, int], ...]:
        return self._bound_addresses

    async def start(self) -> None:
        if self._stop_requested:
            raise RuntimeError("WebSocket listener is stopped")
        if self._run_task is not None and self._run_task.done():
            self._raise_startup_result()
            raise RuntimeError("WebSocket listener is closed")
        if self._run_task is None:
            self._run_task = asyncio.create_task(self._run(), name="gateway-websocket-listener")
            self._run_task.add_done_callback(self._finish)
        try:
            await self.wait_ready()
        except asyncio.CancelledError:
            await self.stop()
            raise

    async def wait_ready(self) -> None:
        await self._start_done.wait()
        self._raise_startup_result()

    async def wait_closed(self) -> None:
        if self._run_task is None:
            return
        await self._closed.wait()
        if self._run_error is not None:
            raise self._run_error

    async def stop(self) -> None:
        self._stop_requested = True
        run_task = self._run_task
        if (
            run_task is not None
            and not run_task.done()
            and self._server is None
            and self._stopped_during_startup is None
        ):
            self._stopped_during_startup = RuntimeError(
                "WebSocket listener stopped during startup"
            )
        self._start_done.set()
        if run_task is None:
            self._closed.set()
            return
        if self._server is not None:
            self._server.close()
        await self._closed.wait()

    async def _run(self) -> None:
        try:
            try:
                from websockets.legacy.server import serve
            except Exception:
                from websockets import serve

            server = await serve(
                self._handler,
                self._host,
                self._port,
                process_request=self._process_request,
                ping_interval=20,
                ping_timeout=600,
                max_size=8 * 2**20,
            )
            self._server = server
            self._bound_addresses = tuple(
                (
                    str(socket.getsockname()[0]),
                    int(socket.getsockname()[1]),
                )
                for socket in server.sockets or ()
            )
            if self._stop_requested:
                server.close()
                await server.wait_closed()
                return
            self._ready.set()
            self._start_done.set()
            await server.wait_closed()
        except asyncio.CancelledError:
            await self._close_after_failure()
            raise
        except Exception as error:
            if not self._ready.is_set():
                self._startup_error = error
            await self._close_after_failure()
            raise
        except BaseException:
            await self._close_after_failure()
            raise

    async def _close_after_failure(self) -> None:
        server = self._server
        if server is not None:
            try:
                server.close()
                await server.wait_closed()
            except Exception:
                pass

    def _finish(self, run_task: asyncio.Task[None]) -> None:
        try:
            run_task.result()
        except asyncio.CancelledError as error:
            failure = RuntimeError("WebSocket listener task was cancelled")
            failure.__cause__ = error
            self._run_error = failure
        except Exception as error:
            self._run_error = error
        except BaseException as error:
            failure = RuntimeError("WebSocket listener task failed")
            failure.__cause__ = error
            self._run_error = failure
        finally:
            self._start_done.set()
            self._closed.set()

    def _raise_startup_result(self) -> None:
        if self._startup_error is not None:
            raise self._startup_error
        if self._stopped_during_startup is not None:
            raise self._stopped_during_startup
        if self._stop_requested:
            raise RuntimeError("WebSocket listener is stopped")
        if not self._ready.is_set():
            if self._run_error is not None:
                raise self._run_error
            raise RuntimeError("WebSocket listener stopped before becoming ready")
