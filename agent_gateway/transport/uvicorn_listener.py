"""Own the embedded Web listener without owning channel business state."""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from collections.abc import Iterator
import socket

from starlette.types import ASGIApp
import uvicorn


_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = 5.0


class _EmbeddedServer(uvicorn.Server):
    """Leave process signals with the Gateway and publish actual readiness."""

    def __init__(self, config: uvicorn.Config, ready: asyncio.Event) -> None:
        super().__init__(config)
        self.ready = ready
        self.shutdown_started = False
        self.bound_addresses: tuple[tuple[str, int], ...] = ()

    @contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield

    async def startup(self, sockets: list[socket.socket] | None = None) -> None:
        await super().startup(sockets=sockets)
        self.bound_addresses = tuple(
            (str(sock.getsockname()[0]), int(sock.getsockname()[1]))
            for server in self.servers
            for sock in server.sockets or ()
        )
        if self.started and not self.should_exit:
            self.ready.set()

    async def shutdown(self, sockets: list[socket.socket] | None = None) -> None:
        if self.shutdown_started:
            return
        self.shutdown_started = True
        await super().shutdown(sockets=sockets)


class UvicornListener:
    """Start one HTTP/WS listener and await its complete framework shutdown."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        host: str,
        port: int,
        ws_per_message_deflate: bool,
    ) -> None:
        self._app = app
        self._host = host
        self._port = port
        self._ws_per_message_deflate = ws_per_message_deflate
        self._ready = asyncio.Event()
        self._server: _EmbeddedServer | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_requested = False
        self._error: Exception | None = None

    @property
    def bound_addresses(self) -> tuple[tuple[str, int], ...]:
        return self._server.bound_addresses if self._server is not None else ()

    async def start(self) -> None:
        if self._stop_requested or (self._task is not None and self._task.done()):
            raise RuntimeError("Uvicorn listener is closed")
        if self._task is None:
            self._task = asyncio.create_task(self._serve(), name="gateway-web-listener")
        try:
            await self.wait_ready()
        except asyncio.CancelledError:
            await self.stop()
            raise

    async def wait_ready(self) -> None:
        await self._ready.wait()
        if self._error is not None:
            raise self._error
        if self._stop_requested or self._server is None or not self._server.started:
            raise RuntimeError("Uvicorn listener stopped before becoming ready")

    async def wait_closed(self) -> None:
        if self._task is not None:
            await asyncio.shield(self._task)
        if self._error is not None:
            raise self._error

    async def stop(self) -> None:
        self._stop_requested = True
        if self._server is not None:
            self._server.should_exit = True
        task = self._task
        try:
            if task is not None:
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    if not task.cancelled():
                        raise
                except BaseException:
                    pass
        finally:
            self._ready.set()

    async def _serve(self) -> None:
        try:
            from agent_gateway.channel_manager.web.transport_stall_guard import (
                guarded_http_protocol,
                guarded_websockets_protocol,
            )
            from agent_gateway.common.ws_limits import WEB_WS_MAX_MESSAGE_BYTES

            config = uvicorn.Config(
                self._app,
                host=self._host,
                port=self._port,
                log_level="info",
                access_log=False,
                ws_max_size=WEB_WS_MAX_MESSAGE_BYTES,
                ws_ping_interval=20.0,
                ws_ping_timeout=60.0,
                timeout_graceful_shutdown=_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS,
                http=guarded_http_protocol(),
                ws=guarded_websockets_protocol(),
                ws_per_message_deflate=self._ws_per_message_deflate,
            )
            self._server = _EmbeddedServer(config, self._ready)
            self._server.should_exit = self._stop_requested
            await self._server.serve()
        except SystemExit as error:
            failure = RuntimeError("Uvicorn listener startup failed")
            failure.__cause__ = error
            self._error = failure
        except Exception as error:
            self._error = error
        finally:
            server = self._server
            if server is not None and not server.shutdown_started:
                try:
                    if getattr(server, "servers", None):
                        await server.shutdown()
                    else:
                        lifespan = getattr(server, "lifespan", None)
                        if lifespan is not None:
                            await lifespan.shutdown()
                except Exception as error:
                    if self._error is None:
                        self._error = error
            self._ready.set()
