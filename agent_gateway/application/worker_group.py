# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Supervise the fixed Worker pool, loopback listeners and optional authorities.

With ``cron=True`` the group also owns the sharded personal Cron runtime. The
parent runs the read-only preflight before any Worker starts, prepares every
slot, activates all shards together and only then opens the listeners.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import logging
import math
import socket
from urllib.parse import urlsplit

from agent_gateway.application.cron_preflight import require_controlled_cron
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest
from agent_gateway.enterprise import is_enterprise
from agent_gateway.processes.pool import WorkerPool
from agent_gateway.transport.connection_acceptor import ConnectionAcceptor

logger = logging.getLogger("gateway")


class WorkerGroup:
    """The parent owns admission and supervises one fixed generation."""

    def __init__(
        self, agent_server_url: str, *, workers: int, web_port: int = 19000,
        protocol_port: int = 19001, handoff_timeout: float = 2,
        auth_service_provider: Callable | None = None,
        cron: bool = False,
    ) -> None:
        url = urlsplit(agent_server_url)
        if (is_enterprise() or url.scheme != "ws" or not url.port
                or url.netloc != f"127.0.0.1:{url.port}" or url.query or url.fragment):
            raise ValueError("the Worker group only supports personal loopback with a single local AgentServer")
        if not math.isfinite(handoff_timeout) or handoff_timeout <= 0:
            raise ValueError("handoff timeout must be finite and positive")
        if type(cron) is not bool:
            raise ValueError("cron must be an explicit boolean")
        if type(workers) is not int or not 1 <= workers <= 32:
            raise ValueError("number of Workers must be between 1 and 32")
        self._credential_authority = self._control_web = None
        self._cron_ownership = self._cron_commands = None
        if cron:
            from agent_gateway.cron.control_commands import ControlCronCommands
            from agent_gateway.cron.ownership import CronOwnership

            # Ownership follows the configured slots, never the healthy subset.
            self._cron_ownership = CronOwnership(WorkerManifest(tuple(range(workers))), LocalIdentityPolicy())
            self._cron_commands = ControlCronCommands(self._cron_ownership, agent_server_url)
        control_services = None
        if auth_service_provider is not None or self._cron_commands is not None:
            from agent_gateway.processes.control_service import ControlServices

            factories, connects, disconnects = [], [], []
            if auth_service_provider is not None:
                from agent_gateway.auth.control_credentials import ControlCredentialServices

                authority = self._credential_authority = ControlCredentialServices(
                    auth_service_provider, server_id=agent_server_url, enable_public_catalog=True,
                )
                factories.append(authority.handlers_for)
                connects.append(authority.endpoint_connected)
                disconnects.append(authority.endpoint_disconnected)
            if self._cron_commands is not None:
                factories.append(self._cron_commands.handlers_for)
                connects.append(self._cron_commands.endpoint_connected)
                disconnects.append(self._cron_commands.endpoint_disconnected)

            def handlers_for(identity):
                merged: dict = {}
                for factory in factories:
                    handlers = dict(factory(identity))
                    reserved = merged.keys() & handlers.keys()
                    if reserved:
                        raise ValueError(f"control operations are reserved twice: {sorted(reserved)}")
                    merged.update(handlers)
                return merged

            def run_all(callbacks):
                def invoke(*arguments):
                    for callback in callbacks:
                        callback(*arguments)
                return invoke

            control_services = ControlServices(
                handlers_factory=handlers_for, on_connect=run_all(connects),
                on_disconnect=run_all(disconnects),
            )
        self.pool = WorkerPool(
            agent_server_url, workers, control_services=control_services,
            cron_manifest=self._cron_ownership.manifest if self._cron_ownership is not None else None,
            account_services=auth_service_provider is not None,
        )
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
            if self._cron_ownership is not None:
                # Read-only and parent-side: no Worker is spawned while an
                # enabled record still needs a capability this batch lacks.
                await require_controlled_cron(self._cron_ownership)
            if self._credential_authority is not None:
                await self._credential_authority.prepare()
                await self._start_control_web()
            await self.pool.start()
            if self._cron_ownership is not None:
                await self.pool.activate_cron()
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
            if self._cron_ownership is not None:
                logger.info("[App] multiprocess_cron_ready workers=%d", len(self.pool.slots))
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
