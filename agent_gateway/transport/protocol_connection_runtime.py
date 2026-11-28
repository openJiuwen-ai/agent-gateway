# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
import socket
from typing import Any

from agent_gateway.routing.protocol_runtime import ProtocolRuntime
from websockets.extensions.permessage_deflate import enable_server_permessage_deflate
from websockets.legacy.server import WebSocketServer, WebSocketServerProtocol
from websockets.protocol import State


class _NoSocketServer:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._serving = False
        self._closed = asyncio.Event()

    @property
    def sockets(self) -> tuple[(), ...]:
        return ()

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def is_serving(self) -> bool:
        return self._serving

    def start_serving(self) -> None:
        self._serving = True

    def close(self) -> None:
        self._serving = False
        self._closed.set()

    async def wait_closed(self) -> None:
        await self._closed.wait()


class ProtocolConnectionRuntime:
    def __init__(
        self, runtime: ProtocolRuntime, *, frame_allowed: Callable[..., bool], stop_timeout: float = 10.0,
    ) -> None:
        self._runtime = runtime
        self._frame_allowed = frame_allowed
        self._stop_timeout = stop_timeout
        self._ready = asyncio.Event()
        self._closed = asyncio.Event()
        self._closed.set()
        self._server: WebSocketServer | None = None
        self._socket_server: _NoSocketServer | None = None
        self._extensions = tuple(enable_server_permessage_deflate(None))
        self._adoptions: set[asyncio.Task[WebSocketServerProtocol]] = set()
        self._stop_task: asyncio.Task[None] | None = None
        self._stop_requested = False
        self._startup_error: BaseException | None = None
        self._stop_error: BaseException | None = None

    @property
    def bound_addresses(self) -> tuple[(), ...]:
        return ()

    async def start(self) -> None:
        if self._stop_requested:
            self._startup_error = self._startup_error or RuntimeError(
                "Protocol connection runtime stopped before startup"
            )
            self._ready.set()
            raise RuntimeError("Protocol connection runtime is stopped")
        if self._server is not None:
            return
        try:
            loop = asyncio.get_running_loop()
            socket_server = _NoSocketServer(loop)
            server = WebSocketServer()
            server.wrap(socket_server)
            socket_server.start_serving()
        except BaseException as error:
            self._startup_error = error
            self._stop_error = error
            self._ready.set()
            self._closed.set()
            raise
        self._socket_server = socket_server
        self._server = server
        self._closed.clear()
        self._ready.set()
        self._runtime._running = True

    async def wait_ready(self) -> None:
        await self._ready.wait()
        if self._startup_error is not None:
            raise self._startup_error
        self._require_serving()

    async def adopt(self, sock: socket.socket, prefix: bytes) -> WebSocketServerProtocol:
        try:
            self._require_serving()
        except BaseException:
            _close_socket_quietly(sock)
            raise
        adoption = asyncio.create_task(self._adopt_connection(sock, prefix))
        self._adoptions.add(adoption)
        adoption.add_done_callback(self._finish_adoption)
        try:
            return await asyncio.shield(adoption)
        except asyncio.CancelledError:
            _close_socket_quietly(sock)
            _cancel_task(adoption)
            deadline = asyncio.get_running_loop().time() + self._stop_timeout * 2
            await _wait_tasks_until((adoption,), deadline, ignore_cancelled=True)
            if not adoption.cancelled():
                await self._discard_protocol(adoption.result())
            raise

    async def _adopt_connection(
        self,
        sock: socket.socket,
        prefix: bytes,
    ) -> WebSocketServerProtocol:
        protocol: WebSocketServerProtocol | None = None

        def protocol_factory() -> WebSocketServerProtocol:
            nonlocal protocol
            protocol = self._protocol_factory()
            return protocol

        try:
            from agent_gateway.transport.accepted_socket import adopt_connection

            self._require_serving()
            _transport, adopted_protocol = await adopt_connection(
                sock,
                prefix,
                protocol_factory,
            )
            self._require_serving()
        except BaseException:
            _close_socket_quietly(sock)
            if protocol is not None:
                await self._discard_protocol(protocol)
            raise
        return adopted_protocol

    def _finish_adoption(self, adoption: asyncio.Task[WebSocketServerProtocol]) -> None:
        self._adoptions.discard(adoption)
        _consume_task_result(adoption)

    async def stop(self) -> None:
        self._stop_requested = True
        self._ready.set()
        if self._stop_task is None:
            self._closed.clear()
            if self._socket_server is not None:
                self._socket_server.close()
            if self._server is None:
                self._startup_error = self._startup_error or RuntimeError(
                    "Protocol connection runtime stopped before startup"
                )
                self._stop_task = asyncio.create_task(self._stop_before_started())
            else:
                self._stop_task = asyncio.create_task(self._stop_running_server())
            self._stop_task.add_done_callback(_consume_task_result)
        await asyncio.shield(self._stop_task)

    async def wait_closed(self) -> None:
        await self._closed.wait()
        if self._stop_error is not None:
            raise self._stop_error

    def is_serving(self) -> bool:
        if self._server is None or self._stop_requested:
            return False
        return self._server.is_serving()

    def _require_serving(self) -> None:
        if not self.is_serving():
            raise RuntimeError("Protocol connection runtime is not serving or is stopped")

    async def _stop_before_started(self) -> None:
        try:
            await self._runtime.stop()
        except BaseException as error:
            self._stop_error = error
            raise
        finally:
            self._clear_runtime_state()
            self._closed.set()

    async def _stop_running_server(self) -> None:
        server = self._server
        if server is None:
            return
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._stop_timeout
        close_tasks: list[asyncio.Task[Any]] = []
        try:
            server.close(close_connections=False)
            adoptions = tuple(self._adoptions)
            for adoption in adoptions:
                _cancel_task(adoption)
            await _wait_tasks_until(adoptions, deadline, ignore_cancelled=True)
            for protocol in list(server.websockets):
                if protocol.state is State.OPEN:
                    close_task = asyncio.create_task(protocol.close(1001))
                    close_task.add_done_callback(_consume_task_result)
                    close_tasks.append(close_task)
            await _wait_tasks_until(close_tasks, deadline)
            if server.close_task is not None:
                await _wait_tasks_until((server.close_task,), deadline)
            await self._runtime.stop()
        except BaseException as error:
            self._stop_error = error
            try:
                await self._force_close_protocols(server, close_tasks)
            except BaseException as cleanup_error:
                error.add_note(f"Forced protocol cleanup failed: {cleanup_error!r}")
            raise
        finally:
            self._clear_runtime_state()
            self._closed.set()

    async def _discard_protocol(self, protocol: WebSocketServerProtocol) -> None:
        tasks = self._abort_protocol(protocol)
        try:
            deadline = asyncio.get_running_loop().time() + self._stop_timeout
            await _wait_tasks_until(tasks, deadline, ignore_cancelled=True)
        finally:
            self._clear_connection_state(protocol)
            handler = getattr(protocol, "handler_task", None)
            if (
                self._server is not None
                and protocol in self._server.websockets
                and (handler is None or handler.done())
            ):
                self._server.unregister(protocol)

    async def _force_close_protocols(
        self,
        server: WebSocketServer,
        close_tasks: Sequence[asyncio.Task[Any]],
    ) -> None:
        tasks = set(close_tasks)
        for protocol in list(server.websockets):
            tasks.update(self._abort_protocol(protocol))
        tasks.update(self._adoptions)
        if server.close_task is not None:
            tasks.add(server.close_task)
        for task in tasks:
            task.add_done_callback(_consume_task_result)
            _cancel_task(task)
        deadline = asyncio.get_running_loop().time() + self._stop_timeout
        await _wait_tasks_until(tuple(tasks), deadline, ignore_cancelled=True)

    def _abort_protocol(self, protocol: WebSocketServerProtocol) -> list[asyncio.Task[Any]]:
        transport = getattr(protocol, "transport", None)
        if transport is not None:
            transport.abort()
        tasks = []
        for name in ("handler_task", "transfer_data_task", "keepalive_ping_task", "close_connection_task"):
            task = getattr(protocol, name, None)
            if task is not None:
                task.add_done_callback(_consume_task_result)
                if name != "close_connection_task":
                    _cancel_task(task)
                tasks.append(task)
        return tasks

    def _clear_connection_state(self, protocol: WebSocketServerProtocol) -> None:
        self._runtime._clients.discard(protocol)
        for table in (
            self._runtime._request_to_client,
            self._runtime._session_to_client,
            self._runtime._pending_session_clients,
        ):
            for key, client in list(table.items()):
                if client is protocol:
                    table.pop(key, None)

    def _clear_runtime_state(self) -> None:
        self._runtime._running = False
        self._runtime._clients.clear()
        self._runtime._request_to_client.clear()
        self._runtime._session_to_client.clear()
        self._runtime._pending_session_clients.clear()

    def _protocol_factory(self) -> WebSocketServerProtocol:
        self._require_serving()
        frame_allowed = self._frame_allowed

        class DataProtocol(WebSocketServerProtocol):
            async def recv(self) -> str | bytes:
                raw = await super().recv()
                if not frame_allowed(raw, acp=self.path.partition("?")[0] == "/acp"):
                    await self.close(code=1008, reason="unsupported Worker request")
                    await self.ensure_open()  # Raise the normal ConnectionClosed error.
                return raw

        return DataProtocol(
            self._runtime._connection_handler,
            self._server,
            extensions=self._extensions,
            process_request=self._runtime._process_request,
            ping_interval=20,
            ping_timeout=600,
            max_size=8 * 2**20,
        )


def _close_socket_quietly(sock: object) -> None:
    close = getattr(sock, "close", None)
    if close is None:
        return
    try:
        close()
    except OSError:
        return


def _cancel_task(task: asyncio.Task[Any]) -> None:
    if not task.done() and not task.cancelling():
        task.cancel()


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if not task.cancelled():
        task.exception()


async def _wait_tasks_until(
    tasks: Sequence[asyncio.Task[Any]],
    deadline: float,
    *,
    ignore_cancelled: bool = False,
) -> None:
    if not tasks:
        return
    loop = asyncio.get_running_loop()
    remaining = max(0.0, deadline - loop.time())
    done, pending = await asyncio.wait(tasks, timeout=remaining)
    failure: BaseException | None = None
    for task in done:
        if ignore_cancelled and task.cancelled():
            continue
        try:
            task.result()
        except BaseException as error:
            if failure is None:
                failure = error
    if failure is not None:
        raise failure
    if pending:
        raise asyncio.TimeoutError(f"{len(pending)} protocol task(s) did not finish before the deadline")
