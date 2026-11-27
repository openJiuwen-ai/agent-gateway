# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Connection ownership for managed physical instances.

The caller supplies the already-selected target, validity guard and connector.
The pool owns the socket cache and concurrent connection attempts only.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from agent_gateway.common.retry import jittered_delay
from agent_gateway.extensions.agentos.agentos_router.logutil import log_agentos
from agent_gateway.southbound.data.websocket import WebSocketConnection
from agent_gateway.telemetry import TraceOperation, current_request_trace, record_cleanup_result, trace_operation

logger = logging.getLogger("agent_gateway.extensions.agentos.agentos_router.router_client")


class ManagedWebSocketPool:
    def __init__(self, factory: Callable[[], Any] = WebSocketConnection) -> None:
        self._factory = factory
        self._push_handler = None
        self.clients: dict[str, Any] = {}
        self.connecting: dict[str, asyncio.Future[Any]] = {}
        self._traces: dict[str, TraceOperation] = {}
        self._lock = asyncio.Lock()

    def is_connecting(self, instance_id: str) -> bool:
        return instance_id in self.connecting

    def set_push_handler(self, handler) -> None:
        self._push_handler = handler
        for client in self.clients.values():
            client.set_server_push_handler(handler)

    async def connect_until_ready(
        self, uri: str, *, headers: dict[str, str], timeout: float, interval: float,
        retryable: Callable[[BaseException], bool], on_connected: Callable[[Any], None],
        context: dict[str, Any], strict_headers: bool = False, jitter: bool = True,
    ) -> Any:
        deadline = asyncio.get_running_loop().time() + timeout
        attempt = 0
        while True:
            attempt += 1
            client = self._factory()
            if self._push_handler is not None:
                client.set_server_push_handler(self._push_handler)
            log_agentos(logger, logging.DEBUG, "agent.ws.connecting", attempt=attempt, **context)
            try:
                if strict_headers:
                    await client.connect(uri, extra_headers=headers)
                elif headers:
                    try:
                        await client.connect(uri, extra_headers=headers)
                    except TypeError:
                        await client.connect(uri)
                else:
                    await client.connect(uri)
                on_connected(client)
                log_agentos(logger, logging.INFO, "agent.ws.ready", attempt=attempt, **context)
                return client
            except asyncio.CancelledError:
                try:
                    await asyncio.shield(client.disconnect())
                except BaseException:
                    logger.warning("agent.ws.cleanup.cancelled", exc_info=True)
                raise
            except Exception as exc:
                try:
                    await client.disconnect()
                except Exception:
                    logger.warning("agent.ws.cleanup.fail", exc_info=True)
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0 or not retryable(exc):
                    log_agentos(logger, logging.WARNING, "agent.ws.retry", attempt=attempt,
                                error=type(exc).__name__, final="yes", **context)
                    raise
                delay = min(interval, remaining)
                if jitter:
                    delay = min(jittered_delay(delay), remaining)
                level = logging.WARNING if attempt == 1 or attempt % 10 == 0 else logging.DEBUG
                log_agentos(logger, level, "agent.ws.retry", attempt=attempt,
                            error=type(exc).__name__, sleep=f"{delay:.1f}s", **context)
                await asyncio.sleep(delay)

    @staticmethod
    def _consume(future: asyncio.Future[Any]) -> None:
        if not future.cancelled():
            future.exception()

    async def get(
        self, instance_id: str, *, validate: Callable[[], Awaitable[None]],
        connect: Callable[[Callable[[], Awaitable[None]]], Awaitable[Any]],
        retired_error: Callable[[], Exception], on_failure: Callable[[BaseException], None],
    ) -> Any:
        async with self._lock:
            await validate()
            cached = self.clients.get(instance_id)
            if cached is not None:
                return cached
            inflight = self.connecting.get(instance_id)
            leader = inflight is None
            if leader:
                inflight = asyncio.get_running_loop().create_future()
                inflight.add_done_callback(self._consume)
                self.connecting[instance_id] = inflight
                owner = current_request_trace()
                if owner is not None:
                    self._traces[instance_id] = owner
            else:
                owner = self._traces.get(instance_id)

        if not leader:
            with trace_operation("gw.agent.ws.wait", attributes={"instance_id": instance_id},
                                 links=[owner] if owner is not None else None):
                return await asyncio.shield(inflight)

        async def validate_attempt() -> None:
            if self.connecting.get(instance_id) is not inflight:
                raise retired_error()
            await validate()

        client = None
        try:
            client = await connect(validate_attempt)
            async with self._lock:
                await validate_attempt()
                self.clients[instance_id] = client
                self.connecting.pop(instance_id, None)
                self._traces.pop(instance_id, None)
                if not inflight.done():
                    inflight.set_result(client)
            return client
        except (Exception, asyncio.CancelledError) as exc:
            if client is not None:
                try:
                    await asyncio.shield(client.disconnect())
                except BaseException:
                    logger.warning("[AgentOS] agent.ws.leader.cleanup_fail sandbox_id=%s", instance_id, exc_info=True)
            async with self._lock:
                if self.connecting.get(instance_id) is inflight:
                    self.connecting.pop(instance_id, None)
                    self._traces.pop(instance_id, None)
                if not inflight.done():
                    inflight.set_exception(RuntimeError("ws connect leader cancelled")
                                           if isinstance(exc, asyncio.CancelledError) else exc)
            on_failure(exc)
            raise

    async def discard(self, instance_id: str, client: Any) -> None:
        async with self._lock:
            if self.clients.get(instance_id) is client:
                self.clients.pop(instance_id, None)

    async def close(self, instance_id: str | None) -> None:
        if not instance_id:
            record_cleanup_result("ws.close", "skipped", reason="no_instance")
            return
        key = str(instance_id)
        async with self._lock:
            client = self.clients.pop(key, None)
            inflight = self.connecting.pop(key, None)
            self._traces.pop(key, None)
        if inflight is not None and not inflight.done():
            inflight.cancel()
        if client is None:
            record_cleanup_result("ws.close", "skipped", reason="no_client")
            return
        with trace_operation("gw.agent.ws.close", attributes={"instance_id": key}) as operation:
            try:
                await client.disconnect()
                record_cleanup_result("ws.close", "success")
            except Exception as exc:
                outcome = "timeout" if isinstance(exc, TimeoutError) or "timeout" in type(exc).__name__.lower() else "failed"
                if operation is not None:
                    operation.record_outcome(outcome, reason=type(exc).__name__)
                record_cleanup_result("ws.close", outcome, reason=type(exc).__name__)
                logger.warning("[AgentOS] agent.ws.close.fail sandbox_id=%s", instance_id,
                               extra={"sandbox_id": key}, exc_info=True)

    async def close_all(self) -> None:
        async with self._lock:
            clients = list(self.clients.items())
            self.clients.clear()
            inflight = list(self.connecting.values())
            self.connecting.clear()
            self._traces.clear()
        for future in inflight:
            if not future.done():
                future.cancel()
        for instance_id, client in clients:
            with trace_operation("gw.agent.ws.close", attributes={"instance_id": instance_id, "source": "shutdown"},
                                 root=True) as operation:
                try:
                    await client.disconnect()
                except Exception as exc:
                    if operation is not None:
                        outcome = "timeout" if isinstance(exc, TimeoutError) or "timeout" in type(exc).__name__.lower() else "failed"
                        operation.record_outcome(outcome, reason=type(exc).__name__)
                    logger.warning("[AgentOSRouter] close agent ws failed", exc_info=True)
