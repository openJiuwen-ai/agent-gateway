# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Lazy owner-instance routing over the existing WebSocket connection engine.

One Worker holds the logical routes of its admitted users. A request leases
the current physical generation of its route and connects it on first use, so
an idle Worker opens no socket. Every generation has one socket, and the
client alone reconnects, fences and retires it.

The provider owns admission and activity holds; this client owns sockets and
request lifetimes. The ingress credential and the network admission of an
Environment are supplied by the caller for each dial and request, and never
become part of a route or a lease.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from contextlib import aclosing
from dataclasses import dataclass, field
from functools import partial
import logging
import math
from typing import Any, AsyncIterator

from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.models import E2AEnvelope

from agent_gateway.application.worker_instance_push import InstancePushGuard
from agent_gateway.southbound.data.websocket import (
    DuplicateRequestIdError, RequestNotSentError, WebSocketConnection,
)
from agent_gateway.southbound.orchestration.client import SouthboundAgentClient
from agent_gateway.southbound.orchestration.instance_provider import (
    AgentOSInstanceProvider, InstanceTarget, OwnedRoute, TargetLease,
)

logger = logging.getLogger(__name__)

# These methods address no existing session, or create one whose id the
# AgentServer returns. Every other method must name a session this generation owns.
_EXISTING_ONLY = frozenset({
    'chat.interrupt', 'chat.user_answer', 'chat.swarmflow_reply', 'acp.tool_response',
    'swarmflow.pause', 'swarmflow.resume', 'swarmflow.stop',
})
_NOT_SENT = (RequestNotSentError, DuplicateRequestIdError)


class _InstanceSocket(WebSocketConnection):
    """One generation's socket. Only the client reconnects it and every write is fenced."""

    def __init__(self) -> None:
        super().__init__(ping_interval=20.0, ping_timeout=600.0)
        self.write_gate: Callable[[Callable[[], Awaitable[None]]], Awaitable[None]] | None = None
        self.close_task: asyncio.Task[None] | None = None
        # The engine forgets a connection when its receiver fails, without
        # closing it. Whoever retires this socket still needs the handle.
        self.retired_raw: Any = None

    @property
    def live(self) -> bool:
        """The peer acknowledged the socket and its receiver still runs."""
        receiver = self._receiver_task
        return bool(
            self.server_ready and self._ws is not None
            and receiver is not None and not receiver.done() and not receiver.cancelling()
        )

    async def _ensure_connected_for_request(self) -> None:
        if not self.live:
            # The request was not written; the caller may release its activity hold.
            raise RequestNotSentError('instance transport is unavailable')

    async def _write_wire(self, payload: dict[str, Any]) -> None:
        await WebSocketConnection._send_wire_payload(self, payload)

    async def _send_wire_payload(self, payload: dict[str, Any]) -> None:
        if self.write_gate is None:  # pragma: no cover - the client always sets a gate
            await self._write_wire(payload)
            return
        await self.write_gate(partial(self._write_wire, payload))

    async def _stop_receiver_after_fatal_error(self, exc: BaseException) -> None:
        if self._ws is not None:
            self.retired_raw = self._ws
        await super()._stop_receiver_after_fatal_error(exc)

    async def notify_failure(self) -> None:
        """Fail waiting callers before the socket is closed."""
        await self._stop_receiver_after_fatal_error(RuntimeError('instance transport retired'))


@dataclass(eq=False)
class _TargetEntry:
    target: InstanceTarget
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    socket: _InstanceSocket | None = None
    pending: _InstanceSocket | None = None
    connect_task: asyncio.Task[_InstanceSocket] | None = None
    active_calls: int = 0


def _observe_task(task: asyncio.Task) -> None:
    if not task.cancelled():
        task.exception()


class WorkerInstanceRoutedClient(SouthboundAgentClient):
    """Admit logical ownership before creation; pin sessions to physical generations."""

    agent_client_mode = 'websocket'
    # Consumers register their push handler, then the Worker activates explicitly.
    requires_initial_connection = False
    # Each user has a private AgentServer directory; no gateway-local fallback
    # may read or write another user's files.
    shared_directory_layout = False
    managed_routing = False
    # A socket failure is isolated per generation; no supervisor task is owned.
    required_tasks: tuple[asyncio.Future, ...] = ()

    def __init__(
        self, provider: AgentOSInstanceProvider, *,
        dial_headers: Callable[[], Mapping[str, str]] | None = None,
        admit_network: Callable[[InstanceTarget], Awaitable[None]] | None = None,
        connect_timeout: float = 10.0, startup_timeout: float = 60.0, stop_timeout: float = 10.0,
    ) -> None:
        if type(provider) is not AgentOSInstanceProvider:
            raise ValueError('instance routed client requires the concrete owner provider')
        for name, value in (
            ('connect', connect_timeout), ('startup', startup_timeout), ('stop', stop_timeout),
        ):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f'instance routed {name} timeout must be finite and positive')
        for name, hook in (('dial headers', dial_headers), ('network admission', admit_network)):
            if hook is not None and not callable(hook):
                raise ValueError(f'instance routed {name} hook must be callable')
        self._provider = provider
        self._dial_headers = dial_headers
        self._admit_network = admit_network
        self._connect_timeout = connect_timeout
        self._startup_timeout = startup_timeout
        self._stop_timeout = stop_timeout
        self._connect_limit = asyncio.Semaphore(4)
        self._push_guard = InstancePushGuard()
        self._entries: dict[str, _TargetEntry] = {}
        self._sockets: set[_InstanceSocket] = set()
        self._close_errors: list[BaseException] = []
        self._push_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None
        self._activated = False
        self._closing = False
        self._stop_task: asyncio.Task[None] | None = None
        self._stop_deadline: float | None = None
        self._cleanup_budget: asyncio.Timeout | None = None

    # ------------------------------------------------------------------ #
    # Identity and admission
    # ------------------------------------------------------------------ #
    @property
    def identity_policy(self):
        """The immutable policy shared by the provider and the ingress adapters."""
        return self._provider.identity_policy

    @property
    def server_ready(self) -> bool:
        """Routing admission. It is not the health of any individual instance."""
        return self._activated and not self._closing

    @property
    def agent_ready(self) -> bool:
        """There is no single readiness promise for a set of generations."""
        return False

    def validate_bound_user(
        self, user_id: str | None, *, agent_type: str | None = None,
    ) -> OwnedRoute:
        """Validate ingress ownership without opening a connection."""
        return self._provider.validate_owner(user_id, agent_type=agent_type)

    def validate_bound_session(self, user_id: str | None, session_id: str | None) -> None:
        """Reserve a session owner before shared northbound state is changed."""
        self._push_guard.claim_session(self.validate_bound_user(user_id), session_id)

    def validate_bound_request(self, user_id: str | None, request_id: str | None) -> None:
        """Refuse a request id that is active or protected on any route."""
        self._push_guard.reserve_request(self.validate_bound_user(user_id), request_id)

    def _validate_envelope(self, envelope: E2AEnvelope) -> OwnedRoute:
        if not self.server_ready:
            raise RuntimeError('instance routed pool is not active')
        if type(envelope) is not E2AEnvelope:
            raise ValueError('routed requests require an explicit E2A envelope')
        for mapping in (envelope.params, envelope.channel_context):
            if mapping.get('agent_type') not in (None, 'jiuwenswarm'):
                raise ValueError('request has no owned instance route')
        route = self.validate_bound_user(envelope.user_id)
        self._push_guard.reserve_request(route, envelope.request_id)
        self._push_guard.claim_session(route, envelope.params.get('session_id') or envelope.session_id)
        return route

    # ------------------------------------------------------------------ #
    # AgentServerClient contract
    # ------------------------------------------------------------------ #
    def set_server_push_handler(
        self, handler: Callable[[dict[str, Any]], Awaitable[None]] | None,
    ) -> None:
        if handler is not None and not callable(handler):
            raise ValueError('instance push handler must be callable')
        if self._activated or self._closing:
            raise RuntimeError('instance push handler is fixed at activation')
        self._push_handler = handler

    async def connect(self, uri: str) -> None:
        raise ValueError('instance connections require a leased generation and activate()')

    def set_or_update_server_config(
        self, *, config: dict[str, Any], env: dict[str, str] | None = None,
    ) -> None:
        raise ValueError('instance server configuration is not supported')

    async def activate(self) -> None:
        """Open routing admission. Generations are connected by their first request."""
        if self._closing:
            raise RuntimeError('instance routed pool is stopped')
        if self._push_handler is None:
            raise RuntimeError('instance routed activation requires a push handler')
        self._activated = True

    async def disconnect(self, *, deadline: float | None = None) -> None:
        await asyncio.shield(self._start_stop(deadline))

    # ------------------------------------------------------------------ #
    # Requests
    # ------------------------------------------------------------------ #
    async def _acquire_entry(
        self, envelope: E2AEnvelope,
    ) -> tuple[_TargetEntry, TargetLease] | None:
        route = self._validate_envelope(envelope)
        lease = await self._provider.acquire(
            route, existing_only=envelope.method in _EXISTING_ONLY,
            deadline=asyncio.get_running_loop().time() + self._startup_timeout,
        )
        if lease is None:
            return None
        try:
            if not await self._provider.is_current(lease):
                raise RuntimeError('instance route became stale before connection')
            await self._retire_stale_entries()
            entry = self._entries.get(lease.target.generation)
            if entry is None:
                entry = _TargetEntry(lease.target)
                self._entries[lease.target.generation] = entry
            return entry, lease
        except BaseException:
            await self._provider.release(lease, outcome='not_sent')
            raise

    async def _prepare(
        self, entry: _TargetEntry, lease: TargetLease, rid: str,
    ) -> _InstanceSocket:
        """Admit the leased generation, then open or reuse exactly its socket."""
        if self._admit_network is not None:
            await self._admit_network(entry.target)
        socket = await self._ensure_connection(entry)
        if not await self._provider.is_current(lease):
            raise RuntimeError('instance route became stale before connection')
        self._push_guard.dispatched(rid)
        return socket

    @staticmethod
    def _confirmed_session(response: AgentResponse) -> str:
        """The session an AgentServer reported for the request that created it."""
        if not response.ok or not isinstance(response.payload, dict):
            return ''
        result = response.payload.get('result', response.payload)
        if not isinstance(result, dict):
            return ''
        session_id = result.get('session_id') or result.get('sessionId')
        return session_id.strip() if isinstance(session_id, str) else ''

    @staticmethod
    def _missing_control_response(envelope: E2AEnvelope) -> AgentResponse:
        cancel = envelope.method == 'chat.interrupt'
        return AgentResponse(
            request_id=str(envelope.request_id or ''), channel_id=str(envelope.channel or ''),
            ok=cancel, payload=(
                {'event_type': 'chat.interrupt_result', 'success': True, 'session_id': envelope.session_id or ''}
                if cancel else {'error': 'no existing instance for this control request'}
            ),
        )

    async def send_request(
        self, envelope: E2AEnvelope, *, timeout: float | None = None,
    ) -> AgentResponse:
        acquired = await self._acquire_entry(envelope)
        if acquired is None:
            return self._missing_control_response(envelope)
        entry, lease = acquired
        rid = None
        outcome = 'not_sent'
        try:
            rid = self._push_guard.begin(entry.target, envelope)
            entry.active_calls += 1
            socket = await self._prepare(entry, lease, rid)
            outcome = 'unknown'
            response = await socket.send_request(envelope, timeout=timeout)
            outcome = 'finished'
            if not await self._provider.is_current(lease):
                raise RuntimeError('instance response belongs to a stale generation')
            self._push_guard.pin(entry.target, self._confirmed_session(response))
            return response
        except _NOT_SENT:
            outcome = 'not_sent'
            raise
        finally:
            if rid is not None:
                self._push_guard.finish(rid)
                entry.active_calls -= 1
            await self._provider.release(lease, outcome=outcome)
            await self._retire_stale_entries()

    async def send_request_stream(self, envelope: E2AEnvelope) -> AsyncIterator[AgentResponseChunk]:
        acquired = await self._acquire_entry(envelope)
        if acquired is None:
            raise ValueError('stream requires an existing instance')
        entry, lease = acquired
        rid = None
        outcome = 'not_sent'
        complete = False
        try:
            rid = self._push_guard.begin(entry.target, envelope)
            entry.active_calls += 1
            socket = await self._prepare(entry, lease, rid)
            # Streaming reports no write; after entering its transport, a failure
            # conservatively retains the hold rather than guessing.
            outcome = 'unknown'
            async with aclosing(socket.send_request_stream(envelope)) as stream:
                async for chunk in stream:
                    if not await self._provider.is_current(lease):
                        raise RuntimeError('instance stream belongs to a stale generation')
                    complete = complete or chunk.is_complete
                    yield chunk
            if complete:
                outcome = 'finished'
        except _NOT_SENT:
            outcome = 'not_sent'
            raise
        finally:
            if rid is not None:
                self._push_guard.finish(rid)
                entry.active_calls -= 1
            await self._provider.release(lease, outcome=outcome)
            await self._retire_stale_entries()

    # ------------------------------------------------------------------ #
    # Sockets
    # ------------------------------------------------------------------ #
    async def _ensure_connection(self, entry: _TargetEntry) -> _InstanceSocket:
        async with entry.lock:
            if self._closing:
                raise RuntimeError('instance routed pool is stopped')
            current = entry.socket
            if current is not None:
                if current.live:
                    return current
                await self._retire_locked(entry, current)
            task = entry.connect_task
            if task is None or task.done():
                task = asyncio.create_task(self._connect(entry), name='gateway-instance-connect')
                task.add_done_callback(_observe_task)
                entry.connect_task = task
        return await asyncio.shield(task)

    async def _connect(self, entry: _TargetEntry) -> _InstanceSocket:
        socket = _InstanceSocket()
        entry.pending = socket
        self._sockets.add(socket)

        async def push(wire: dict[str, Any]) -> None:
            # A frame that arrives while the socket is still connecting is kept;
            # a retired socket no longer speaks for the generation.
            if self._closing or (entry.socket is not socket and entry.pending is not socket):
                return
            admitted = await self._admit_push(entry, wire)
            if admitted is not None and self._push_handler is not None:
                await self._push_handler(admitted)

        socket.set_server_push_handler(push)
        socket.write_gate = partial(self._provider.send_current_target, entry.target)
        try:
            async with self._connect_limit:
                async with asyncio.timeout(self._connect_timeout):
                    if self._closing:
                        raise RuntimeError('instance routed pool is stopped')
                    headers = None if self._dial_headers is None else dict(self._dial_headers())
                    await socket.connect(entry.target.ws_url, extra_headers=headers)
                    if not socket.live:
                        raise RuntimeError('instance transport did not acknowledge its connection')
            async with entry.lock:
                if self._closing:
                    raise RuntimeError('instance routed pool is stopped')
                entry.socket = socket
            return socket
        except BaseException:
            await asyncio.shield(self._close(socket))
            raise
        finally:
            if entry.pending is socket:
                entry.pending = None

    async def _admit_push(self, entry: _TargetEntry, wire: dict[str, Any]) -> dict[str, Any] | None:
        if not await self._provider.is_target_current(entry.target):
            return None
        return self._push_guard.admit(entry.target, wire)

    async def _retire_locked(self, entry: _TargetEntry, socket: _InstanceSocket) -> None:
        # Notify old callers before another connection may be opened for this generation.
        await socket.notify_failure()
        if entry.socket is socket:
            entry.socket = None
        self._close(socket)

    async def _retire_stale_entries(self) -> None:
        for key, entry in tuple(self._entries.items()):
            if entry.active_calls or await self._provider.is_target_current(entry.target):
                continue
            async with entry.lock:
                if entry.active_calls or (entry.connect_task is not None and not entry.connect_task.done()):
                    continue
                if await self._provider.is_target_current(entry.target):
                    continue
                socket = entry.socket
                if socket is not None:
                    await self._retire_locked(entry, socket)
                    await asyncio.shield(self._close(socket))
                if self._entries.get(key) is entry:
                    self._entries.pop(key, None)

    def _close(self, socket: _InstanceSocket) -> asyncio.Task[None]:
        """Close one socket once; later callers observe the same bounded task."""
        task = socket.close_task
        if task is None:
            task = socket.close_task = asyncio.create_task(
                self._close_socket(socket), name='gateway-instance-close',
            )
            task.add_done_callback(partial(self._socket_closed, socket))
        return task

    def _socket_closed(self, socket: _InstanceSocket, task: asyncio.Task[None]) -> None:
        self._sockets.discard(socket)
        if not task.cancelled() and task.exception() is not None:
            self._close_errors.append(task.exception())

    async def _close_socket(self, socket: _InstanceSocket) -> None:
        # The receiver of a retired socket has already forgotten its connection.
        raw = socket._ws if socket._ws is not None else socket.retired_raw
        # A socket closed before shutdown (a failed dial) still gets a bounded wait.
        deadline = self._stop_deadline
        if deadline is None:
            deadline = asyncio.get_running_loop().time() + self._stop_timeout
        try:
            async with asyncio.timeout_at(deadline):
                await socket.notify_failure()
                await socket.disconnect()
                # Nothing else closes a connection the receiver forgot.
                if raw is not None and raw.close_code is None:
                    await raw.close()
            return
        except TimeoutError:
            logger.warning('instance transport did not close before the deadline')
        finally:
            leftover = set(socket._server_push_tails.values()) | set(socket._cleanup_tasks)
            if socket._receiver_task is not None:
                leftover.add(socket._receiver_task)
            for task in leftover:
                if not task.done() and task is not asyncio.current_task():
                    task.cancel()
            if leftover:
                await asyncio.gather(*leftover, return_exceptions=True)
        # The deadline elapsed: drop the socket without waiting for its peer.
        socket._running = False
        socket._connection_generation += 1
        socket._ws = None
        socket._reconnect_enabled = False
        socket._server_ready = False
        socket._agent_ready = False
        try:
            if raw is not None and raw.close_code is None:
                raw.transport.abort()
        except Exception:  # noqa: BLE001 - the peer may already be gone
            pass

    # ------------------------------------------------------------------ #
    # Shutdown
    # ------------------------------------------------------------------ #
    def _start_stop(self, deadline: float | None) -> asyncio.Task[None]:
        loop = asyncio.get_running_loop()
        stop_deadline = loop.time() + self._stop_timeout
        if deadline is not None:
            stop_deadline = min(stop_deadline, deadline)
        if self._stop_task is None:
            self._closing = True
            self._activated = False
            self._stop_deadline = stop_deadline
            self._stop_task = asyncio.create_task(self._cleanup(), name='gateway-instance-routed-stop')
            self._stop_task.add_done_callback(_observe_task)
        elif not self._stop_task.done() and stop_deadline < self._stop_deadline:
            self._stop_deadline = stop_deadline
            if self._cleanup_budget is not None:
                self._cleanup_budget.reschedule(stop_deadline)
        return self._stop_task

    async def _cleanup(self) -> None:
        tasks = {
            entry.connect_task for entry in self._entries.values() if entry.connect_task is not None
        }
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        closes = [self._close(socket) for socket in tuple(self._sockets)]
        try:
            async with asyncio.timeout_at(self._stop_deadline) as budget:
                self._cleanup_budget = budget
                await asyncio.gather(*(asyncio.shield(task) for task in closes))
        finally:
            self._cleanup_budget = None
            self._push_guard.clear()
            for task in closes:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*closes, return_exceptions=True)
            for entry in self._entries.values():
                entry.socket = None
                entry.pending = None
            self._entries.clear()
            # Local acquisitions stop here. Nothing is inferred about the remote
            # Environments and none is deleted. Closing them is local manager work,
            # so it keeps a short budget even when the sockets used all of theirs.
            loop = asyncio.get_running_loop()
            await self._provider.close(deadline=max(self._stop_deadline, loop.time() + 1.0))
        if self._close_errors:
            raise RuntimeError('instance transport cleanup failed')


__all__ = ['WorkerInstanceRoutedClient']
