# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Read only the first request head and give one selected Worker the socket."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import logging
from http import HTTPStatus
import socket

from agent_gateway.common.ingress_identity import IngressIdentityError, resolve_local_identity
from agent_gateway.common.worker_affinity import FixedUserHash
from agent_gateway.transport.acceptor_http import HeadRejected, RequestHead, read_request_head
from agent_gateway.transport.handoff import HandoffRejected

logger = logging.getLogger("gateway")


class ConnectionAcceptor:
    def __init__(
        self, listener: socket.socket, *, protocol_port: bool, affinity: FixedUserHash,
        handoff: Callable[..., Awaitable[None]], handoff_timeout: float, head_timeout: float = 5,
        control_http: Callable[[socket.socket, bytes], Awaitable[None]] | None = None,
    ) -> None:
        self._listener = listener
        self._protocol_port = protocol_port
        self._affinity = affinity
        self._handoff = handoff
        self._handoff_timeout = handoff_timeout
        self._head_timeout = head_timeout
        self._control_http = control_http
        self._inflight = asyncio.Semaphore(128)
        self._clients: dict[asyncio.Task, socket.socket] = {}
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._accept(), name="gateway-accept-loop")

    async def wait(self) -> None:
        await asyncio.shield(self._task)

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._listener.close()
        for task in self._clients:
            task.cancel()
        await asyncio.gather(*self._clients, return_exceptions=True)

    async def _accept(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            await self._inflight.acquire()
            try:
                client, _ = await loop.sock_accept(self._listener)
            except BaseException:
                self._inflight.release()
                raise
            client.setblocking(False)
            task = asyncio.create_task(self._dispatch(client), name="gateway-classify-connection")
            self._clients[task] = client
            task.add_done_callback(self._finish)

    def _finish(self, task: asyncio.Task) -> None:
        # Also handles cancellation before the coroutine begins.
        self._clients.pop(task).close()
        self._inflight.release()
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error("Connection admission failed", exc_info=error)

    async def _dispatch(self, client: socket.socket) -> None:
        try:
            request = await read_request_head(client, timeout=self._head_timeout)
            if self._protocol_port:
                if request.protocol != "websocket" or request.path not in {"/tui", "/acp"}:
                    raise HeadRejected(404)
                entry = request.path[1:]
                runtime = "protocol"
            elif request.protocol == "websocket" and request.path == "/ws":
                entry, runtime = "web", "web-ws"
            elif request.protocol == "http":
                entry, runtime = "http", "web-http"
                if self._is_control_http(request):
                    duplicate = client.dup()
                    try:
                        await self._control_http(duplicate, request.prefix)
                    except BaseException:
                        duplicate.close()
                        raise
                    return
            else:
                raise HeadRejected(404)
            try:
                identity = resolve_local_identity(
                    request.query, request.headers, entry=entry, policy=self._affinity.identity_policy,
                )
            except IngressIdentityError:
                raise HeadRejected(400) from None
            selection = self._affinity.select(identity)
        except HeadRejected as error:
            status = HTTPStatus(error.status)
            try:
                async with asyncio.timeout(0.25):
                    await asyncio.get_running_loop().sock_sendall(
                        client, f"HTTP/1.1 {status.value} {status.phrase}\r\nConnection: close\r\n"
                                "Content-Length: 0\r\n\r\n".encode("ascii"),
                    )
            except (OSError, TimeoutError):
                pass
            return
        except OSError:
            return

        try:
            await self._handoff(selection, client, runtime, request.prefix, timeout=self._handoff_timeout)
        except (HandoffRejected, ConnectionError, OSError, TimeoutError):
            # No response or retry after offering the fd: the Worker may own it.
            return

    def _is_control_http(self, request: RequestHead) -> bool:
        return self._control_http is not None and request.path.startswith("/api/v1/auth/")
