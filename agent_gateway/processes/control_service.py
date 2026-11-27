# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Private request/event association services for Gateway workers."""

from __future__ import annotations

import asyncio
import math
import secrets
from collections import deque
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from agent_gateway.processes.control_channel import (
    ControlChannelSendQueueFull,
    ControlServiceChannel,
)
from agent_gateway.processes.control_protocol import (
    EVENTS,
    OPERATIONS,
    ControlFrameTooLarge,
    ControlProtocolError,
    validate_message,
)


Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
EventHandler = Callable[[dict[str, Any]], Awaitable[None]]

_SEEN_IDENTIFIERS_LIMIT = 128
_SEEN_IDENTIFIERS_TTL = 30.0
_OWNED_TASK_STOP_TIMEOUT = 1.0


def _positive_finite_timeout(value: object, name: str = "timeout") -> float:
    if type(value) not in {int, float} or not math.isfinite(float(value)) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return float(value)


async def _finish_owned(task: asyncio.Task[None]) -> None:
    """Finish an owned task even when the caller is cancelled repeatedly."""
    cancelled = False
    while True:
        try:
            await asyncio.shield(task)
            break
        except asyncio.CancelledError:
            if task.cancelled():
                raise
            cancelled = True
    if cancelled:
        raise asyncio.CancelledError


def _consume_owned_result(task: asyncio.Task[None]) -> None:
    if task.cancelled():
        return
    try:
        task.exception()
    except asyncio.CancelledError:
        return


@dataclass(frozen=True)
class ControlServiceIdentity:
    """Process identity bound to a control-service socket pair."""

    group_id: str
    slot_id: int
    generation: int
    worker_pid: int
    control_pid: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.group_id, str)
            or not self.group_id
            or len(self.group_id) > 256
        ):
            raise ValueError("group_id must be a non-empty bounded string")
        if type(self.slot_id) is not int or self.slot_id < 0:
            raise ValueError("slot_id must be a non-negative integer")
        if type(self.generation) is not int or self.generation < 1:
            raise ValueError("generation must be a positive integer")
        if type(self.worker_pid) is not int or self.worker_pid <= 0:
            raise ValueError("worker_pid must be a positive integer")
        if type(self.control_pid) is not int or self.control_pid <= 0:
            raise ValueError("control_pid must be a positive integer")


class ControlServiceError(ConnectionError):
    """A safe, fixed-message control-service failure."""

    _message = "control service connection failed"

    def __init__(self) -> None:
        super().__init__(self._message)


class ControlServiceBusy(ControlServiceError):
    """The local or remote control-service budget is exhausted."""

    _message = "control service budget exhausted"


class ControlServiceOperationError(ControlServiceError):
    """A remote control operation failed with a protocol error code."""

    _message = "control service operation failed"

    def __init__(self, error_code: str) -> None:
        super().__init__()
        self.error_code = error_code


class ControlServiceAdmission:
    """Synchronous, same-loop admission for bounded shared handlers."""

    def __init__(self, limit: int = 16) -> None:
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be a positive integer")
        self.limit = limit
        self._loop: asyncio.AbstractEventLoop | None = None
        self._count = 0

    def acquire(self) -> bool:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise ControlServiceError()
        if self._count >= self.limit:
            return False
        self._count += 1
        return True

    def release(self) -> None:
        loop = asyncio.get_running_loop()
        if self._loop is None:
            self._loop = loop
        elif self._loop is not loop:
            raise ControlServiceError()
        self._count = max(0, self._count - 1)


class _RecentIdentifiers:
    """Bounded, time-limited identifiers without payloads or results."""

    def __init__(self) -> None:
        self._entries: deque[tuple[float, str]] = deque(
            maxlen=_SEEN_IDENTIFIERS_LIMIT
        )

    def remember(self, identifier: str) -> None:
        self._entries.append((asyncio.get_running_loop().time(), identifier))

    def contains(self, identifier: str) -> bool:
        now = asyncio.get_running_loop().time()
        while self._entries and now - self._entries[0][0] > _SEEN_IDENTIFIERS_TTL:
            self._entries.popleft()
        return any(entry[1] == identifier for entry in self._entries)

    def identifiers(self) -> set[str]:
        now = asyncio.get_running_loop().time()
        while self._entries and now - self._entries[0][0] > _SEEN_IDENTIFIERS_TTL:
            self._entries.popleft()
        return {entry[1] for entry in self._entries}


@dataclass(frozen=True)
class ControlServices:
    """Validated, immutable configuration for the control-service handlers.

    ``handlers`` names the operations served directly and is copied into a
    read-only mapping. ``handlers_factory``, ``on_connect``, ``on_disconnect``
    and ``max_group_handlers`` are only validated here: the process pool that
    creates the server endpoints reads them.
    """

    handlers: Mapping[str, Handler] = field(default_factory=dict)
    handlers_factory: Callable[[ControlServiceIdentity], Mapping[str, Handler]] | None = None
    on_connect: Callable[["ControlServiceServer"], None] | None = None
    on_disconnect: Callable[[ControlServiceIdentity], None] | None = None
    max_handlers: int = 8
    max_group_handlers: int = 16

    def __post_init__(self) -> None:
        if not isinstance(self.handlers, Mapping):
            raise ValueError("handlers must be a mapping of reserved operations")
        unknown = set(self.handlers) - set(OPERATIONS)
        if unknown:
            raise ValueError("handlers may only use reserved operations")
        for handler in self.handlers.values():
            if not callable(handler):
                raise ValueError("control handlers must be callable")
        if self.handlers_factory is not None and not callable(self.handlers_factory):
            raise ValueError("handlers_factory must be callable")
        if self.on_connect is not None and not callable(self.on_connect):
            raise ValueError("on_connect must be callable")
        if self.on_disconnect is not None and not callable(self.on_disconnect):
            raise ValueError("on_disconnect must be callable")
        if type(self.max_handlers) is not int or self.max_handlers <= 0:
            raise ValueError("max_handlers must be a positive integer")
        if type(self.max_group_handlers) is not int or self.max_group_handlers <= 0:
            raise ValueError("max_group_handlers must be a positive integer")
        object.__setattr__(self, "handlers", MappingProxyType(dict(self.handlers)))


class _ControlServiceEndpoint:
    """Shared reader, fixed-failure, and idempotent-stop behavior."""

    def __init__(
        self,
        channel: ControlServiceChannel,
        identity: ControlServiceIdentity,
    ) -> None:
        if not isinstance(channel, ControlServiceChannel):
            raise TypeError("channel must be a ControlServiceChannel")
        if not isinstance(identity, ControlServiceIdentity):
            raise TypeError("identity must be a ControlServiceIdentity")
        self.channel = channel
        self.identity = identity
        self._reader: asyncio.Task[None] | None = None
        self._failure: ControlServiceError | None = None
        self._stopped = False
        self._owned_reclaim_attempted = False
        self._cleanup_failure: ControlServiceError | None = None
        self._quiescing = False
        self._stop_task: asyncio.Task[None] | None = None

    async def wait(self) -> None:
        """Return after a local ``stop``, or raise when the service failed."""
        if self._reader is None:
            raise ControlServiceError()
        await asyncio.shield(self._reader)
        await self._finish_owned_tasks()
        if self._failure is not None:
            raise self._failure

    async def stop(self) -> None:
        if self._stop_task is None:
            self._stopped = True
            self._stop_task = asyncio.create_task(
                self._stop_once(), name="gateway-control-service-stop"
            )
            self._stop_task.add_done_callback(_consume_owned_result)
        await _finish_owned(self._stop_task)

    async def _stop_once(self) -> None:
        cleanup_error: ControlServiceError | None = None
        self._fail_pending(ControlServiceError())
        try:
            await self._quiesce()
        except ControlServiceError as error:
            cleanup_error = error
        await self.channel.stop()
        if self._reader is not None and not self._reader.done():
            self._reader.cancel()
        try:
            await self._finish_owned_tasks()
        except ControlServiceError as error:
            if cleanup_error is None:
                cleanup_error = error
        if self._reader is not None:
            await asyncio.gather(self._reader, return_exceptions=True)
        if cleanup_error is not None:
            raise cleanup_error

    async def _quiesce(self) -> None:
        return None

    async def _finish_owned_tasks(self) -> None:
        return None

    async def _reclaim_owned_tasks(self, tasks: set[asyncio.Task[None]]) -> None:
        if self._cleanup_failure is not None:
            raise self._cleanup_failure
        pending_tasks = {task for task in tasks if not task.done()}
        if not pending_tasks:
            return
        if self._owned_reclaim_attempted:
            self._cleanup_failure = ControlServiceError()
            raise self._cleanup_failure
        self._owned_reclaim_attempted = True
        done, still_pending = await asyncio.wait(
            pending_tasks,
            timeout=_OWNED_TASK_STOP_TIMEOUT,
        )
        for task in done:
            if not task.cancelled():
                task.exception()
        if still_pending:
            self._cleanup_failure = ControlServiceError()
            raise self._cleanup_failure

    def _fail_pending(self, error: ControlServiceError) -> None:
        return None

    def _fail_service(self) -> None:
        if self._failure is None:
            self._failure = ControlServiceError()
        self._fail_pending(ControlServiceError())
        self.channel.close()

    async def _run_reader(self) -> None:
        try:
            while not self.channel.closed:
                message = validate_message(
                    await self.channel.receive(),
                    expected=self.identity.__dict__,
                )
                try:
                    await self._dispatch(message)
                except ControlServiceBusy:
                    # A reply the reader sends itself met a full local send
                    # queue. Dropping that one reply lets the peer time out on
                    # it instead of losing the whole connection.
                    continue
        except asyncio.CancelledError:
            self._fail_pending(ControlServiceError())
            if not self._stopped:
                self._fail_service()
            return
        except Exception:
            if not self._stopped:
                self._fail_service()

    async def _dispatch(self, message: dict[str, Any]) -> None:
        raise ControlProtocolError("unexpected control message")

    async def _send(self, message: dict[str, Any]) -> None:
        try:
            await self.channel.send(message)
        except asyncio.CancelledError:
            if not self._stopped and not self._quiescing:
                self._fail_service()
            raise
        except ControlChannelSendQueueFull:
            raise ControlServiceBusy() from None
        except ControlFrameTooLarge:
            # Both refusals happen before anything reaches the socket, so the
            # stream is intact and the service has not failed.
            raise
        except Exception:
            if not self._stopped:
                self._fail_service()
            raise ControlServiceError() from None


class ControlServiceClient(_ControlServiceEndpoint):
    """Worker-side request and event endpoint."""

    def __init__(
        self,
        channel: ControlServiceChannel,
        identity: ControlServiceIdentity,
        *,
        max_pending: int = 32,
        event_handlers: Mapping[str, EventHandler] | None = None,
    ) -> None:
        super().__init__(channel, identity)
        if type(max_pending) is not int or max_pending <= 0:
            raise ValueError("max_pending must be a positive integer")
        selected_event_handlers = {} if event_handlers is None else event_handlers
        if not isinstance(selected_event_handlers, Mapping):
            raise ValueError("event_handlers must be a mapping of reserved events")
        unknown_events = set(selected_event_handlers) - set(EVENTS)
        if unknown_events:
            raise ValueError("event_handlers may only use reserved events")
        for event_handler in selected_event_handlers.values():
            if not callable(event_handler):
                raise ValueError("event handlers must be callable")

        self.max_pending = max_pending
        self._event_handlers = MappingProxyType(dict(selected_event_handlers))
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._seen_responses = _RecentIdentifiers()
        self._late_responses = 0
        self._event_tasks: set[asyncio.Task[None]] = set()
        self._event_admission = ControlServiceAdmission(limit=max_pending)

    @property
    def residual_event_handlers(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(task for task in self._event_tasks if not task.done())

    async def start(self, *, timeout: float = 5.0) -> None:
        if self._reader is not None:
            return
        _positive_finite_timeout(timeout)
        try:
            async with asyncio.timeout(timeout):
                await self.channel.send(
                    validate_message(
                        {
                            "protocol_version": 1,
                            "group_id": self.identity.group_id,
                            "slot_id": self.identity.slot_id,
                            "generation": self.identity.generation,
                            "type": "HELLO",
                            "pid": self.identity.worker_pid,
                        }
                    )
                )
                ready = validate_message(
                    await self.channel.receive(),
                    expected=self.identity.__dict__,
                )
                if (
                    ready["type"] != "SERVICE_READY"
                    or ready["pid"] != self.identity.control_pid
                    or ready["worker_pid"] != self.identity.worker_pid
                ):
                    raise ControlProtocolError("control handshake mismatch")
        except asyncio.CancelledError:
            self.channel.close()
            raise
        except Exception:
            self.channel.close()
            raise ControlServiceError() from None

        self._reader = asyncio.create_task(
            self._run_reader(), name="gateway-control-service-client"
        )

    async def call(
        self,
        operation: str,
        arguments: dict[str, Any],
        *,
        timeout: float,
    ) -> dict[str, Any]:
        _positive_finite_timeout(timeout)
        if not isinstance(operation, str) or operation not in OPERATIONS:
            raise ValueError("operation must be a reserved control operation")
        if not isinstance(arguments, dict):
            raise ValueError("arguments must be an object")
        if self._reader is None or self.channel.closed:
            raise ControlServiceError()
        if len(self._pending) >= self.max_pending:
            raise ControlServiceBusy()

        request_id = self._new_identifier(
            self._pending.keys() | self._seen_responses.identifiers()
        )
        loop = asyncio.get_running_loop()
        message = validate_message(
            {
                "protocol_version": 1,
                "group_id": self.identity.group_id,
                "slot_id": self.identity.slot_id,
                "generation": self.identity.generation,
                "type": "REQUEST",
                "request_id": request_id,
                "operation": operation,
                "arguments": arguments,
            }
        )
        pending_future: asyncio.Future[dict[str, Any]] = loop.create_future()
        self._pending[request_id] = pending_future
        deadline = loop.time() + timeout

        try:
            async with asyncio.timeout_at(deadline):
                await self.channel.send(message)
        except asyncio.CancelledError:
            self._seen_responses.remember(request_id)
            self._pending.pop(request_id, None)
            raise
        except TimeoutError:
            self._seen_responses.remember(request_id)
            self._pending.pop(request_id, None)
            raise
        except Exception as error:
            self._seen_responses.remember(request_id)
            self._pending.pop(request_id, None)
            if isinstance(error, ControlChannelSendQueueFull):
                raise ControlServiceBusy() from None
            if isinstance(error, ControlFrameTooLarge):
                raise
            raise ControlServiceError() from None

        try:
            async with asyncio.timeout_at(deadline):
                return await pending_future
        except asyncio.CancelledError:
            self._seen_responses.remember(request_id)
            if not pending_future.done():
                pending_future.cancel()
            self._pending.pop(request_id, None)
            raise
        except TimeoutError:
            self._seen_responses.remember(request_id)
            if not pending_future.done():
                pending_future.cancel()
            self._pending.pop(request_id, None)
            raise

    async def _dispatch(self, message: dict[str, Any]) -> None:
        if message["type"] == "RESPONSE":
            self._dispatch_response(message)
            return
        if message["type"] == "EVENT":
            await self._dispatch_event(message)
            return
        raise ControlProtocolError("unexpected control message")

    def _dispatch_response(self, message: dict[str, Any]) -> None:
        request_id = message["request_id"]
        pending_future = self._pending.pop(request_id, None)
        if pending_future is None or pending_future.done():
            self._seen_responses.remember(request_id)
            self._late_responses += 1
            return
        self._seen_responses.remember(request_id)
        if message["status"] == "OK":
            pending_future.set_result(message["result"])
            return
        if message["error_code"] == "CONTROL_SERVICE_BUSY":
            pending_future.set_exception(ControlServiceBusy())
        else:
            pending_future.set_exception(
                ControlServiceOperationError(message["error_code"])
            )

    async def _dispatch_event(self, message: dict[str, Any]) -> None:
        event_id = message["event_id"]
        event = message["event"]
        event_handler = self._event_handlers.get(event)
        if event_handler is None:
            await self._send_event_ack(event_id, "ERROR", "EVENT_NOT_ENABLED")
            return

        event_task = asyncio.create_task(
            self._run_event_handler(event_id, event, event_handler, message["arguments"])
        )
        self._event_tasks.add(event_task)
        event_task.add_done_callback(self._event_tasks.discard)

    async def _run_event_handler(
        self,
        event_id: str,
        event: str,
        event_handler: EventHandler,
        arguments: dict[str, Any],
    ) -> None:
        # Acquire here, not in the reader: a task cancelled before its first
        # step never runs the ``finally`` below and would leak the slot.
        if not self._event_admission.acquire():
            try:
                await self._send_event_ack(event_id, "ERROR", "CONTROL_SERVICE_BUSY")
            except Exception:
                return
            return
        try:
            await event_handler(arguments)
            await self._send_event_ack(event_id, "OK", None)
        except asyncio.CancelledError:
            return
        except ControlServiceBusy:
            try:
                await self._send_event_ack(event_id, "ERROR", "CONTROL_SERVICE_BUSY")
            except Exception:
                return
        except Exception:
            try:
                await self._send_event_ack(event_id, "ERROR", "CONTROL_SERVICE_FAILED")
            except Exception:
                return
        finally:
            self._event_admission.release()

    async def _send_event_ack(
        self,
        event_id: str,
        status: str,
        error_code: str | None,
    ) -> None:
        await self._send(
            validate_message(
                {
                    "protocol_version": 1,
                    "group_id": self.identity.group_id,
                    "slot_id": self.identity.slot_id,
                    "generation": self.identity.generation,
                    "type": "EVENT_ACK",
                    "event_id": event_id,
                    "status": status,
                    "error_code": error_code,
                }
            )
        )

    async def _quiesce(self) -> None:
        for event_task in tuple(self._event_tasks):
            event_task.cancel()

    async def _finish_owned_tasks(self) -> None:
        if self._event_tasks:
            await self._reclaim_owned_tasks(self._event_tasks)
            self._event_tasks.clear()

    def _fail_pending(self, error: ControlServiceError) -> None:
        for request_id in tuple(self._pending):
            self._seen_responses.remember(request_id)
        for pending_future in tuple(self._pending.values()):
            if not pending_future.done():
                pending_future.set_exception(error)
        self._pending.clear()
        for event_task in tuple(self._event_tasks):
            event_task.cancel()

    @staticmethod
    def _new_identifier(existing: set[str]) -> str:
        while True:
            identifier = secrets.token_hex(16)
            if identifier not in existing:
                return identifier


class ControlServiceServer(_ControlServiceEndpoint):
    """Control-side request dispatcher with bounded owned handlers."""

    def __init__(
        self,
        channel: ControlServiceChannel,
        identity: ControlServiceIdentity,
        *,
        handlers: Mapping[str, Handler],
        admission: ControlServiceAdmission,
        max_handlers: int = 8,
        max_pending: int = 32,
    ) -> None:
        super().__init__(channel, identity)
        if not isinstance(handlers, Mapping):
            raise ValueError("handlers must be a mapping of reserved operations")
        unknown_operations = set(handlers) - set(OPERATIONS)
        if unknown_operations:
            raise ValueError("handlers may only use reserved operations")
        for handler in handlers.values():
            if not callable(handler):
                raise ValueError("control handlers must be callable")
        if not isinstance(admission, ControlServiceAdmission):
            raise TypeError("admission must be a ControlServiceAdmission")
        if type(max_handlers) is not int or max_handlers <= 0:
            raise ValueError("max_handlers must be a positive integer")
        if type(max_pending) is not int or max_pending <= 0:
            raise ValueError("max_pending must be a positive integer")

        self._handlers = MappingProxyType(dict(handlers))
        self._admission = admission
        self.max_handlers = max_handlers
        self.max_pending = max_pending
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._active_request_ids: set[str] = set()
        self._event_pending: dict[str, asyncio.Future[None]] = {}
        self._seen_event_acks = _RecentIdentifiers()
        self._late_event_acks = 0
        self._accepting_requests = True

    @property
    def residual_handlers(self) -> tuple[asyncio.Task[None], ...]:
        return tuple(task for task in self._handler_tasks if not task.done())

    async def start(self, *, timeout: float = 5.0) -> None:
        if self._reader is not None:
            return
        _positive_finite_timeout(timeout)
        try:
            async with asyncio.timeout(timeout):
                hello = validate_message(
                    await self.channel.receive(),
                    expected=self.identity.__dict__,
                )
                if hello["type"] != "HELLO" or hello["pid"] != self.identity.worker_pid:
                    raise ControlProtocolError("control handshake mismatch")
                await self.channel.send(
                    validate_message(
                        {
                            "protocol_version": 1,
                            "group_id": self.identity.group_id,
                            "slot_id": self.identity.slot_id,
                            "generation": self.identity.generation,
                            "type": "SERVICE_READY",
                            "pid": self.identity.control_pid,
                            "worker_pid": self.identity.worker_pid,
                        }
                    )
                )
        except asyncio.CancelledError:
            self.channel.close()
            raise
        except Exception:
            self.channel.close()
            raise ControlServiceError() from None

        self._reader = asyncio.create_task(
            self._run_reader(), name="gateway-control-service-server"
        )

    async def notify(
        self,
        event: str,
        arguments: dict[str, Any],
        *,
        timeout: float,
    ) -> None:
        _positive_finite_timeout(timeout)
        if not isinstance(event, str) or event not in EVENTS:
            raise ValueError("event must be a reserved control event")
        if not isinstance(arguments, dict):
            raise ValueError("arguments must be an object")
        if self._reader is None or self.channel.closed or not self._accepting_requests:
            raise ControlServiceBusy()
        if len(self._event_pending) >= self.max_pending:
            raise ControlServiceBusy()

        event_id = self._new_identifier(
            self._event_pending.keys() | self._seen_event_acks.identifiers()
        )
        loop = asyncio.get_running_loop()
        message = validate_message(
            {
                "protocol_version": 1,
                "group_id": self.identity.group_id,
                "slot_id": self.identity.slot_id,
                "generation": self.identity.generation,
                "type": "EVENT",
                "event_id": event_id,
                "event": event,
                "arguments": arguments,
            }
        )
        event_future: asyncio.Future[None] = loop.create_future()
        self._event_pending[event_id] = event_future
        deadline = loop.time() + timeout

        try:
            async with asyncio.timeout_at(deadline):
                await self.channel.send(message)
        except asyncio.CancelledError:
            self._seen_event_acks.remember(event_id)
            self._event_pending.pop(event_id, None)
            raise
        except TimeoutError:
            self._seen_event_acks.remember(event_id)
            self._event_pending.pop(event_id, None)
            raise
        except Exception as error:
            self._seen_event_acks.remember(event_id)
            self._event_pending.pop(event_id, None)
            if isinstance(error, ControlChannelSendQueueFull):
                raise ControlServiceBusy() from None
            if isinstance(error, ControlFrameTooLarge):
                raise
            raise ControlServiceError() from None

        try:
            async with asyncio.timeout_at(deadline):
                await event_future
        except asyncio.CancelledError:
            self._seen_event_acks.remember(event_id)
            if not event_future.done():
                event_future.cancel()
            self._event_pending.pop(event_id, None)
            raise
        except TimeoutError:
            self._seen_event_acks.remember(event_id)
            if not event_future.done():
                event_future.cancel()
            self._event_pending.pop(event_id, None)
            raise

    async def _dispatch(self, message: dict[str, Any]) -> None:
        if message["type"] == "REQUEST":
            await self._dispatch_request(message)
            return
        if message["type"] == "EVENT_ACK":
            self._dispatch_event_ack(message)
            return
        raise ControlProtocolError("unexpected control message")

    async def _dispatch_request(self, message: dict[str, Any]) -> None:
        request_id = message["request_id"]
        if request_id in self._active_request_ids:
            raise ControlProtocolError("duplicate in-flight request_id")

        handler = self._handlers.get(message["operation"])
        if handler is None:
            await self._send_response(
                request_id,
                "ERROR",
                None,
                "OPERATION_NOT_ENABLED",
            )
            return
        if not self._accepting_requests or len(self._handler_tasks) >= self.max_handlers:
            await self._send_response(
                request_id,
                "ERROR",
                None,
                "CONTROL_SERVICE_BUSY",
            )
            return

        self._active_request_ids.add(request_id)
        handler_task = asyncio.create_task(
            self._run_handler(request_id, message["operation"], handler, message["arguments"])
        )
        self._handler_tasks.add(handler_task)
        handler_task.add_done_callback(self._handler_tasks.discard)

    async def _run_handler(
        self,
        request_id: str,
        operation: str,
        handler: Handler,
        arguments: dict[str, Any],
    ) -> None:
        # Acquire here, not in the reader: a task cancelled before its first
        # step never runs the ``finally`` below and would leak the slot.
        if not self._admission.acquire():
            self._active_request_ids.discard(request_id)
            try:
                await self._send_response(
                    request_id,
                    "ERROR",
                    None,
                    "CONTROL_SERVICE_BUSY",
                )
            except Exception:
                return
            return
        try:
            result = await handler(arguments)
            if not isinstance(result, dict):
                raise TypeError("control handler result must be an object")
            await self._send_response(request_id, "OK", result, None)
        except asyncio.CancelledError:
            return
        except ControlServiceBusy:
            try:
                await self._send_response(request_id, 'ERROR', None, 'CONTROL_SERVICE_BUSY')
            except Exception:
                return
        except Exception:
            try:
                await self._send_response(
                    request_id,
                    "ERROR",
                    None,
                    "CONTROL_SERVICE_FAILED",
                )
            except Exception:
                return
        finally:
            self._active_request_ids.discard(request_id)
            self._admission.release()

    def _dispatch_event_ack(self, message: dict[str, Any]) -> None:
        event_id = message["event_id"]
        event_future = self._event_pending.pop(event_id, None)
        if event_future is None or event_future.done():
            self._seen_event_acks.remember(event_id)
            self._late_event_acks += 1
            return
        self._seen_event_acks.remember(event_id)
        if message["status"] == "OK":
            event_future.set_result(None)
            return
        if message["error_code"] == "CONTROL_SERVICE_BUSY":
            event_future.set_exception(ControlServiceBusy())
        else:
            event_future.set_exception(
                ControlServiceOperationError(message["error_code"])
            )

    async def _send_response(
        self,
        request_id: str,
        status: str,
        result: dict[str, Any] | None,
        error_code: str | None,
    ) -> None:
        await self._send(
            validate_message(
                {
                    "protocol_version": 1,
                    "group_id": self.identity.group_id,
                    "slot_id": self.identity.slot_id,
                    "generation": self.identity.generation,
                    "type": "RESPONSE",
                    "request_id": request_id,
                    "status": status,
                    "result": result,
                    "error_code": error_code,
                }
            )
        )

    async def quiesce(self) -> None:
        """Stop accepting requests and cancel owned handlers with a hard budget.

        A same-process asyncio task cannot be forcibly killed. If a handler
        swallows cancellation and remains after the budget, this method raises
        a fixed control-service error and leaves the task in
        ``residual_handlers`` for observation. It does not claim that no
        handler remains.
        """
        self._accepting_requests = False
        self._fail_pending(ControlServiceError())
        self._quiescing = True
        try:
            for handler_task in tuple(self._handler_tasks):
                handler_task.cancel()
            if self._handler_tasks:
                await self._reclaim_owned_tasks(self._handler_tasks)
                self._handler_tasks.clear()
        finally:
            self._quiescing = False

    async def _quiesce(self) -> None:
        await self.quiesce()

    async def _finish_owned_tasks(self) -> None:
        if self._handler_tasks:
            await self._reclaim_owned_tasks(self._handler_tasks)
            self._handler_tasks.clear()

    def _fail_pending(self, error: ControlServiceError) -> None:
        for event_id in tuple(self._event_pending):
            self._seen_event_acks.remember(event_id)
        for event_future in tuple(self._event_pending.values()):
            if not event_future.done():
                event_future.set_exception(error)
        self._event_pending.clear()
        for handler_task in tuple(self._handler_tasks):
            handler_task.cancel()

    @staticmethod
    def _new_identifier(existing: set[str]) -> str:
        while True:
            identifier = secrets.token_hex(16)
            if identifier not in existing:
                return identifier
