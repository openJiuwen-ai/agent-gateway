# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Run the existing Web ASGI protocols without creating a listener."""

from __future__ import annotations

import asyncio
import socket
from typing import Literal
import weakref

from starlette.types import ASGIApp
import uvicorn
from uvicorn.lifespan.on import LifespanOn
from websockets.protocol import State

from agent_gateway.transport.accepted_socket import adopt_connection
from agent_gateway.transport.uvicorn_listener import web_protocol_config


class _ConnectionSet(set):
    def __init__(self, shared: set) -> None:
        super().__init__()
        self.shared = shared
        self.seen = weakref.WeakSet()

    def add(self, item) -> None:
        super().add(item)
        self.shared.add(item)
        self.seen.add(item)

    def discard(self, item) -> None:
        super().discard(item)
        self.shared.discard(item)

    def remove(self, item) -> None:
        super().remove(item)
        self.shared.discard(item)


class _ConnectionState:
    def __init__(self, shared: uvicorn.server.ServerState) -> None:
        self.shared = shared
        self.connections = _ConnectionSet(shared.connections)
        self.tasks = _ConnectionSet(shared.tasks)

    @property
    def total_requests(self):
        return self.shared.total_requests

    @total_requests.setter
    def total_requests(self, value):
        self.shared.total_requests = value

    @property
    def default_headers(self):
        return self.shared.default_headers

    async def abort(self, timeout: float) -> None:
        tasks = set(self.tasks)
        for connection in tuple(self.connections.seen):
            transport = getattr(connection, "transport", None)
            if transport is not None:
                transport.abort()
            handler = getattr(connection, "handler_task", None)
            if handler is not None:
                tasks.add(handler)
        for task in tasks:
            task.cancel()
        if tasks:
            _done, pending = await asyncio.wait(tasks, timeout=timeout)
            if pending:
                raise RuntimeError("Adopted Web connection tasks did not stop")
        await asyncio.sleep(0)


class _TrackedLifespan(LifespanOn):
    main_task: asyncio.Task | None = None

    async def main(self) -> None:
        self.main_task = asyncio.current_task()
        await super().main()


def _single_request_http_protocol(protocol_class: type[asyncio.Protocol]) -> type[asyncio.Protocol]:
    def single_request_app(app: ASGIApp) -> ASGIApp:
        async def bounded_app(scope, receive, send) -> None:
            async def bounded_send(message) -> None:
                if message["type"] == "http.response.start":
                    headers = [
                        (name, value)
                        for name, value in message.get("headers", [])
                        if name.lower() != b"connection"
                    ]
                    message = {**message, "headers": headers + [(b"connection", b"close")]}
                await send(message)

            await app(scope, receive, bounded_send)

        return bounded_app

    class SingleRequestHTTPProtocol(protocol_class):
        def __init__(self, *, config, server_state, app_state, _loop=None) -> None:
            super().__init__(config=config, server_state=server_state, app_state=app_state, _loop=_loop)
            self.app = single_request_app(self.app)

        def on_response_complete(self) -> None:
            # Closing before the base callback prevents uvicorn from starting
            # an already-parsed pipelined request. asyncio still flushes writes.
            self.transport.close()
            super().on_response_complete()

    return SingleRequestHTTPProtocol


class WebProtocolRuntime:
    """Own adopted HTTP/WS connections and one bounded ASGI lifecycle."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        ws_per_message_deflate: bool,
        single_request_http: bool = False,
        lifecycle_timeout: float = 5.0,
    ) -> None:
        if lifecycle_timeout <= 0:
            raise ValueError("lifecycle_timeout must be positive")
        self._app = app
        self._deflate = ws_per_message_deflate
        self._single_request_http = single_request_http
        self._timeout = lifecycle_timeout
        self._ready = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._server: uvicorn.Server | None = None
        self._lifespan: _TrackedLifespan | None = None
        self._adoptions: set[asyncio.Task] = set()
        self._states: set[_ConnectionState] = set()
        self._close_tasks: set[asyncio.Task] = set()
        self._stopping = False
        self._error: BaseException | None = None
        self._shutdown_error: BaseException | None = None

    @property
    def bound_addresses(self) -> tuple[tuple[str, int], ...]:
        return ()

    async def start(self) -> None:
        if self._stopping:
            raise RuntimeError("Web protocol runtime is closed")
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="gateway-web-protocol-runtime")
        try:
            await self.wait_ready()
        except asyncio.CancelledError:
            await self.stop()
            raise

    async def wait_ready(self) -> None:
        await self._ready.wait()
        if self._error is not None:
            raise self._error
        if self._stopping or self._server is None or not self._server.started:
            raise RuntimeError("Web protocol runtime stopped before ready")

    async def adopt(
        self,
        sock: socket.socket,
        prefix: bytes,
        *,
        protocol: Literal["http", "websocket"] = "http",
    ) -> None:
        server = self._server
        if self._stopping or server is None or not server.started or self._error is not None:
            sock.close()
            raise RuntimeError("Web protocol runtime is not ready")
        if protocol not in ("http", "websocket"):
            sock.close()
            raise ValueError("unsupported Web protocol")
        task = asyncio.current_task()
        self._adoptions.add(task)
        state = _ConnectionState(server.server_state)
        try:
            protocol_class = (
                server.config.http_protocol_class if protocol == "http" else server.config.ws_protocol_class
            )
            await adopt_connection(
                sock,
                prefix,
                lambda: protocol_class(
                    config=server.config,
                    server_state=state,
                    app_state=self._lifespan.state,
                ),
            )
            self._states.add(state)
        except BaseException:
            await state.abort(self._timeout)
            raise
        finally:
            self._adoptions.discard(task)

    async def stop(self) -> None:
        self._stopping = True
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            await asyncio.shield(self._task)
        self._ready.set()
        if self._shutdown_error is not None:
            raise self._shutdown_error

    async def wait_closed(self) -> None:
        if self._task is not None:
            await asyncio.shield(self._task)
        if self._error is not None:
            raise self._error

    async def _run(self) -> None:
        try:
            config = web_protocol_config(self._app, ws_per_message_deflate=self._deflate)
            config.load()
            if self._single_request_http:
                config.http_protocol_class = _single_request_http_protocol(config.http_protocol_class)
            self._server = uvicorn.Server(config)
            self._server.servers = []
            self._lifespan = _TrackedLifespan(config)
            self._server.lifespan = self._lifespan
            await asyncio.wait_for(self._lifespan.startup(), self._timeout)
            if self._lifespan.should_exit:
                raise RuntimeError("Web ASGI lifespan startup failed")
            await self._server.on_tick(0)
            self._server.started = True
            self._server.should_exit = self._stopping
            self._ready.set()
            counter = 0
            while not self._server.should_exit:
                await asyncio.sleep(0.1)
                counter = (counter + 1) % 864000
                self._server.should_exit = await self._server.on_tick(counter)
                self._states = {
                    state for state in self._states
                    if state.connections or state.tasks or self._state_handlers(state)
                }
        except BaseException as error:
            self._error = error
        finally:
            self._stopping = True
            try:
                await self._shutdown()
            except BaseException as error:
                self._shutdown_error = error
                if self._error is None:
                    self._error = error
            self._ready.set()

    async def _shutdown(self) -> None:
        error = None
        try:
            await asyncio.wait_for(self._drain(), self._timeout)
        except BaseException as failure:
            error = failure
            try:
                await self._abort_connections()
            except BaseException as failure:
                error = failure
        lifespan = self._lifespan
        if lifespan is not None:
            try:
                if self._server is not None and self._server.started:
                    await asyncio.wait_for(lifespan.shutdown(), self._timeout)
                    if lifespan.shutdown_failed:
                        raise RuntimeError("Web ASGI lifespan shutdown failed")
            except BaseException as failure:
                if error is None:
                    error = failure
            finally:
                task = lifespan.main_task
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.wait({task}, timeout=self._timeout)
                    if not task.done() and error is None:
                        error = RuntimeError("Web ASGI lifespan did not stop")
        if error is not None:
            raise error

    async def _drain(self) -> None:
        while self._adoptions:
            await asyncio.sleep(0.01)
        server = self._server
        if server is None:
            return
        for connection in tuple(server.server_state.connections):
            if isinstance(connection, server.config.ws_protocol_class):
                if connection.state in (State.OPEN, State.CLOSING):
                    task = asyncio.create_task(connection.close(code=1001))
                    self._close_tasks.add(task)
                else:
                    connection.transport.abort()
            else:
                connection.shutdown()
        while (
            server.server_state.connections or server.server_state.tasks
            or self._handler_tasks() or any(not task.done() for task in self._close_tasks)
        ):
            await asyncio.sleep(0.01)
        for task in self._close_tasks:
            task.result()
        self._close_tasks.clear()
        self._states.clear()

    async def _abort_connections(self) -> None:
        tasks = set(self._adoptions)
        if self._server is not None:
            for connection in tuple(self._server.server_state.connections):
                connection.transport.abort()
            tasks.update(self._server.server_state.tasks)
        tasks.update(self._handler_tasks())
        tasks.update(self._close_tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            _done, pending = await asyncio.wait(tasks, timeout=self._timeout)
            if pending:
                raise RuntimeError("Web protocol tasks did not stop")
            for task in self._close_tasks:
                if not task.cancelled():
                    task.exception()
        self._close_tasks.clear()
        self._states.clear()
        await asyncio.sleep(0)

    def _handler_tasks(self) -> set[asyncio.Task]:
        return {
            task
            for state in self._states
            for task in self._state_handlers(state)
        }

    @staticmethod
    def _state_handlers(state: _ConnectionState) -> set[asyncio.Task]:
        return {
            connection.handler_task
            for connection in state.connections.seen
            if hasattr(connection, "handler_task") and not connection.handler_task.done()
        }
