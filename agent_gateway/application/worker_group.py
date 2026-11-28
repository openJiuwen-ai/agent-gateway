# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Supervise the fixed Worker pool, loopback listeners and optional account authority."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import math
import socket
from urllib.parse import urlsplit

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest
from agent_gateway.enterprise import is_enterprise
from agent_gateway.processes.pool import WorkerPool
from agent_gateway.transport.connection_acceptor import ConnectionAcceptor


class WorkerGroup:
    """The parent owns admission and supervises one fixed generation."""

    def __init__(
        self, agent_server_url: str, *, workers: int, web_port: int = 19000,
        protocol_port: int = 19001, handoff_timeout: float = 2,
        auth_service_provider: Callable | None = None,
    ) -> None:
        url = urlsplit(agent_server_url)
        if (is_enterprise() or url.scheme != "ws" or not url.port
                or url.netloc != f"127.0.0.1:{url.port}" or url.query or url.fragment):
            raise ValueError("the Worker group only supports personal loopback with a single local AgentServer")
        if not math.isfinite(handoff_timeout) or handoff_timeout <= 0:
            raise ValueError("handoff timeout must be finite and positive")
        self._credential_authority = self._control_web = None
        control_services = None
        if auth_service_provider is not None:
            from agent_gateway.auth.control_credentials import ControlCredentialServices
            from agent_gateway.processes.control_service import ControlServices

            authority = self._credential_authority = ControlCredentialServices(
                auth_service_provider, server_id=agent_server_url, enable_public_catalog=True,
            )
            control_services = ControlServices(
                handlers_factory=authority.handlers_for, on_connect=authority.endpoint_connected,
                on_disconnect=authority.endpoint_disconnected,
            )
        self.pool = WorkerPool(agent_server_url, workers, control_services=control_services)
        self._ports = (web_port, protocol_port)
        self._handoff_timeout = handoff_timeout
        self._acceptors: list[ConnectionAcceptor] = []
        self._monitor: asyncio.Task | None = None
        self._started = False
        self._close_task: asyncio.Task | None = None
        self.addresses: tuple[tuple[str, int], ...] = ()

    async def start(self) -> None:
        if self._close_task is not None or self._started:
            raise RuntimeError("Worker group is single-use")
        self._started = True
        try:
            if self._credential_authority is not None:
                await self._credential_authority.prepare()
                await self._start_control_web()
            await self.pool.start()
            if self._close_task is not None:
                raise RuntimeError("Worker group stopped during startup")
            affinity = FixedUserHash(WorkerManifest(self.pool.slots), LocalIdentityPolicy())
            addresses = []
            for index, port in enumerate(self._ports):
                listener = socket.socket()
                try:
                    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    listener.bind(("127.0.0.1", port))
                    listener.listen()
                    listener.setblocking(False)
                    addresses.append(listener.getsockname())
                    self._acceptors.append(ConnectionAcceptor(
                        listener, protocol_port=index == 1, affinity=affinity, handoff=self.pool.handoff,
                        handoff_timeout=self._handoff_timeout,
                        control_http=(self._control_web.adopt if index == 0 and self._control_web is not None else None),
                    ))
                except BaseException:
                    listener.close()
                    raise
            self.addresses = tuple(addresses)
            for acceptor in self._acceptors:
                acceptor.start()
            self._monitor = asyncio.create_task(self._watch(), name="gateway-watch-worker-group")
        except BaseException:
            await self.stop()
            raise

    async def _start_control_web(self) -> None:
        from fastapi import FastAPI
        from agent_gateway.channel_manager.web.web_http_auth import register_auth_routes
        from agent_gateway.transport.web_protocol_runtime import WebProtocolRuntime

        authority = self._credential_authority
        app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, redirect_slashes=False)
        register_auth_routes(app, auth_service_provider=lambda: authority.prepared_service,
                             logout_callback=authority.logout)
        self._control_web = WebProtocolRuntime(app, ws_per_message_deflate=False, single_request_http=True)
        await self._control_web.start()

    async def _watch(self) -> Exception | None:
        waiters = [self.pool.wait, *(acceptor.wait for acceptor in self._acceptors)]
        if self._control_web is not None:
            waiters.append(self._control_web.wait_closed)
        tasks = [asyncio.create_task(wait()) for wait in waiters]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if not task.cancelled():
                    task.result()
        except Exception as error:
            return error
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await self.stop()
        return None

    async def wait(self) -> None:
        if self._monitor is None:
            raise RuntimeError("Worker group was not started")
        error = await asyncio.shield(self._monitor)
        if error is not None:
            raise error

    async def stop(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close(), name="gateway-stop-worker-group")
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        try:
            await asyncio.gather(*(acceptor.stop() for acceptor in self._acceptors))
        finally:
            try:
                if self._control_web is not None:
                    await self._control_web.stop()
            finally:
                try:
                    await self.pool.stop()
                finally:
                    if self._credential_authority is not None:
                        await self._credential_authority.stop()
