from __future__ import annotations

import asyncio
import hashlib
import json
import math
from collections import OrderedDict
from dataclasses import dataclass
from logging import getLogger
from time import monotonic
from typing import Any, Callable

from e2a.message import ReqMethod

from agent_gateway.cron.control_commands import validate_cron_command
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.processes.control_service import (
    ControlServiceBusy,
    ControlServiceOperationError,
)
from agent_gateway.runtime.cron.models import resolve_cron_model


logger = getLogger(__name__)

_DEFAULT_ENDPOINT_IDS = frozenset({"", "default"})
_OWNER_METADATA_FIELDS = {
    "user_id": "_jiuwenswarm_cron_owner_user_id",
    "group_id": "_jiuwenswarm_cron_owner_group_id",
    "bot_id": "_jiuwenswarm_cron_owner_bot_id",
}
_FINAL_CONTEXT_TTL = 60.0
_CONTROL_ROUTE_TIMEOUT = 2.0
_ACK_TIMEOUT = 2.0


class WorkerCronContextCapacityError(RuntimeError):
    """The process-local active chat context cache is full."""


class WorkerCronRouteUnknown(RuntimeError):
    """Control could not return a deterministic routing outcome."""


class WorkerCronCommandStopError(RuntimeError):
    """A Worker Cron command task did not stop within the bounded shutdown."""


class _InvalidCronCommand(ValueError):
    """Represent a malformed private Cron command without retaining details."""


@dataclass
class _ChatContext:
    request_id: str
    user_id: str
    session_id: str | None
    auth_session: str
    mode: str | None


@dataclass
class _FinalChatContext:
    context: _ChatContext
    expires_at: float


@dataclass
class _SeenCommand:
    fingerprint: str
    expires_at: float | None


@dataclass
class _QueuedCommand:
    command: dict[str, Any]
    execute: bool
    deadline_at: float
    error_code: str | None = None
    error_text: str | None = None
    execution: dict[str, Any] | None = None


class WorkerCronCommands:
    """Queue trusted AS Cron tool commands for this Worker's local owner."""

    _ACTIVE_CONTEXT_LIMIT = 4096
    _FINAL_CONTEXT_LIMIT = 1024
    _QUEUE_LIMIT = 32
    _COMMAND_LIMIT = 4096
    _COMMAND_TTL = 60.0
    _COMMAND_DEADLINE = 8.0
    _STOP_TIMEOUT = 1.0

    def __init__(
        self,
        *,
        client: Any,
        message_handler: Any,
        ownership: CronOwnership,
        slot_id: int,
        enabled: Callable[[], bool] | None = None,
    ) -> None:
        if type(ownership) is not CronOwnership:
            raise ValueError("Worker Cron commands require CronOwnership")
        if type(slot_id) is not int or slot_id not in ownership.manifest.slots:
            raise ValueError("Worker Cron slot is absent from the configured manifest")
        if not callable(getattr(client, "call", None)):
            raise ValueError("Worker Cron commands require a Control client")
        for method_name in (
            "_handle_cron_push_payload",
            "_push_cron_command_ack",
            "_push_cron_run_now_ack",
        ):
            if not callable(getattr(message_handler, method_name, None)):
                raise ValueError("Worker Cron commands require a MessageHandler")
        if enabled is not None and not callable(enabled):
            raise ValueError("Worker Cron enabled gate must be callable")

        self._client = client
        self._message_handler = message_handler
        self._ownership = ownership
        self._slot_id = slot_id
        self._active_contexts: OrderedDict[str, _ChatContext] = OrderedDict()
        self._final_contexts: OrderedDict[str, _FinalChatContext] = OrderedDict()
        self._seen_commands: OrderedDict[str, _SeenCommand] = OrderedDict()
        self._tasks: set[asyncio.Task[None]] = set()
        self._enabled = enabled if enabled is not None else (lambda: True)
        self._closed = False

    def remember(self, envelope: Any, metadata: dict[str, Any] | None) -> None:
        """Remember only the chat identity needed by a later Cron tool push."""
        if self._closed:
            raise RuntimeError("Worker Cron command runtime is stopped")

        method = getattr(envelope, "method", None)
        if getattr(method, "value", method) != ReqMethod.CHAT_SEND.value:
            return

        raw_request_id = getattr(envelope, "request_id", None)
        raw_user_id = getattr(envelope, "user_id", None)
        request_id = raw_request_id if isinstance(raw_request_id, str) and raw_request_id else ""
        user_id = raw_user_id if isinstance(raw_user_id, str) and raw_user_id else ""
        if len(request_id) > 256 or not _is_canonical_named(user_id):
            return
        try:
            if self._ownership.owner_slot(user_id) != self._slot_id:
                return
        except ValueError:
            return

        raw_session = getattr(envelope, "session_id", None)
        session_id = raw_session if isinstance(raw_session, str) and raw_session else None
        if session_id is not None and len(session_id) > 256:
            return
        params = getattr(envelope, "params", None)
        raw_mode = params.get("mode") if isinstance(params, dict) else None
        mode = _text(raw_mode) or None
        auth_session = ""
        if isinstance(metadata, dict):
            auth_session = _text(metadata.get("auth_session"))

        self._purge_final_contexts()
        existing = self._active_contexts.get(request_id)
        if existing is None:
            existing = (
                self._final_contexts[request_id].context
                if request_id in self._final_contexts
                else None
            )
        if existing is not None:
            if (
                existing.user_id != user_id
                or existing.session_id != session_id
                or existing.auth_session != auth_session
            ):
                raise ValueError("request_id context conflict: user/session/auth must match")
            return

        if len(self._active_contexts) >= self._ACTIVE_CONTEXT_LIMIT:
            raise WorkerCronContextCapacityError(
                "Worker Cron active context cache is full"
            )
        self._active_contexts[request_id] = _ChatContext(
            request_id=request_id,
            user_id=user_id,
            session_id=session_id,
            auth_session=auth_session,
            mode=mode,
        )

    def complete(self, request_id: str) -> None:
        """Retain a completed chat context for the bounded terminal window."""
        if self._closed:
            return
        self._purge_final_contexts()
        request_id = request_id if isinstance(request_id, str) and request_id else ""
        context = self._active_contexts.pop(request_id, None)
        if context is None:
            return
        self._final_contexts.pop(context.request_id, None)
        self._final_contexts[context.request_id] = _FinalChatContext(
            context=context,
            expires_at=self._now() + _FINAL_CONTEXT_TTL,
        )
        while len(self._final_contexts) > self._FINAL_CONTEXT_LIMIT:
            self._final_contexts.popitem(last=False)

    async def handle_push(
        self,
        *,
        payload: dict[str, Any],
        request_id: str,
        channel_id: str,
        session_id: str | None,
        metadata: dict[str, Any] | None,
        user_id: str | None = None,
    ) -> bool:
        """Route one AS Cron push to its fixed owner without leaking auth context."""
        command = self._raw_command_from_push(
            payload=payload,
            request_id=request_id,
            channel_id=channel_id,
            session_id=session_id,
            metadata=metadata,
        )
        try:
            command = self._validate_command(command)
        except _InvalidCronCommand:
            return not await self._reject_invalid_command(command)

        if self._is_duplicate_command(command):
            return True

        owner_slot = self._ownership.owner_slot(command["owner_user_id"])
        if owner_slot == self._slot_id:
            try:
                await self.accept(command)
            except ControlServiceBusy:
                rejected = await self._reject_command_once(
                    command,
                    code="CRON_COMMAND_BUSY",
                    text="Worker Cron command queue is full",
                )
                return not rejected
            return True

        try:
            result = await self._client.call(
                "cron.route_command",
                command,
                timeout=_CONTROL_ROUTE_TIMEOUT,
            )
        except ControlServiceBusy:
            rejected = await self._reject_command_once(
                command,
                code="CRON_ROUTE_BUSY",
                text="Control Cron routing is busy",
            )
            return not rejected
        except ControlServiceOperationError as error:
            if error.error_code == "OPERATION_NOT_ENABLED":
                rejected = await self._reject_command_once(
                    command,
                    code="CRON_ROUTE_REJECTED",
                    text="Control rejected the Cron command",
                )
                return not rejected
            raise WorkerCronRouteUnknown("Control Cron routing is uncertain") from error
        except Exception as error:
            raise WorkerCronRouteUnknown("Control Cron routing is uncertain") from error

        if not isinstance(result, dict):
            raise WorkerCronRouteUnknown("Control returned an invalid Cron route result")
        status = result.get("status")
        if status == "accepted":
            return True
        if status == "rejected":
            rejected = await self._reject_command_once(
                command,
                code="CRON_ROUTE_REJECTED",
                text="Control rejected the Cron command",
            )
            return not rejected
        raise WorkerCronRouteUnknown("Control returned an unknown Cron route status")

    async def accept(self, command: dict[str, Any]) -> None:
        """Admit one owner-local command and return before business execution."""
        if self._closed:
            raise RuntimeError("Worker Cron command runtime is stopped")

        try:
            command = self._validate_command(command)
        except _InvalidCronCommand:
            await self._reject_invalid_command(command)
            return

        owner_slot = self._ownership.owner_slot(command["owner_user_id"])
        if owner_slot != self._slot_id:
            await self._queue_error(
                command,
                code="CRON_OWNER_MISMATCH",
                text="Cron command owner belongs to a different Worker",
            )
            return

        if not self._enabled():
            await self._queue_error(
                command,
                code="CRON_NOT_ACTIVE",
                text="Worker Cron runtime is not active",
            )
            return

        context = self._find_context(command["request_id"])
        if context is not None:
            if context.user_id != command["owner_user_id"]:
                await self._queue_error(
                    command,
                    code="CRON_CONTEXT_CONFLICT",
                    text="Cron command owner conflicts with the original chat context",
                )
                return
            if context.session_id is not None and command["session_id"] != context.session_id:
                await self._queue_error(
                    command,
                    code="CRON_CONTEXT_CONFLICT",
                    text="Cron command session conflicts with the original chat context",
                )
                return
            if context.session_id is None and command["session_id"] is not None:
                context.session_id = command["session_id"]

        controller = getattr(self._message_handler, "_cron_controller", None)
        login_enabled = getattr(controller, "_cron_login_credentials", None) is not None
        if not login_enabled and _cron_model_source(command["payload"]["data"]) == "login":
            await self._queue_error(
                command,
                code="CRON_LOGIN_MODEL_DISABLED",
                text="Cron login models are not enabled before B4",
            )
            return

        session_id = command["session_id"]
        if context is not None and context.session_id is not None:
            session_id = context.session_id
        trusted_metadata: dict[str, Any] = {}
        if context is not None:
            trusted_metadata = {
                "auth_session": context.auth_session,
                "mode": context.mode,
            }
        execution_data = dict(command["payload"]["data"])
        execution_data.pop("user_id", None)
        execution_data.pop("_auth_session", None)
        execution_data["group_id"] = ""
        execution_data["bot_id"] = ""
        item = _QueuedCommand(
            command=command,
            execute=True,
            deadline_at=self._loop_now() + self._COMMAND_DEADLINE,
            execution={
                "payload": {**command["payload"], "data": execution_data},
                "request_id": command["request_id"],
                "channel_id": "",
                "session_id": session_id,
                "metadata": {
                    "_jiuwenswarm_cron_owner_user_id": command["owner_user_id"],
                    "_jiuwenswarm_cron_owner_group_id": command["owner_group_id"],
                    "_jiuwenswarm_cron_owner_bot_id": command["owner_bot_id"],
                },
                "user_id": command["owner_user_id"],
                "_trusted_cron_metadata": trusted_metadata,
            },
        )
        await self._queue_command(item)

    async def stop(self) -> None:
        """Stop admission and cancel bounded work without an unbounded wait."""
        if self._closed:
            return
        self._closed = True
        tasks = tuple(self._tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        pending: set[asyncio.Task[None]] = set()
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=self._STOP_TIMEOUT)
            for task in done:
                if task.cancelled():
                    continue
                error = task.exception()
                if error is not None:
                    logger.error(
                        "[WorkerCronCommands] command task failed during stop: task_name=%s",
                        task.get_name(),
                        exc_info=error,
                    )
            for task in pending:
                logger.error(
                    "[WorkerCronCommands] command task did not stop within %.2fs; "
                    "leaving it for process recovery: task_name=%s",
                    self._STOP_TIMEOUT,
                    task.get_name(),
                )

        self._active_contexts.clear()
        self._final_contexts.clear()
        self._seen_commands.clear()
        if pending:
            raise WorkerCronCommandStopError(
                f"Worker Cron command tasks did not stop: count={len(pending)}"
            )
        self._tasks.clear()

    def _now(self) -> float:
        return monotonic()

    def _loop_now(self) -> float:
        return asyncio.get_running_loop().time()

    def _purge_final_contexts(self) -> None:
        now = self._now()
        while self._final_contexts:
            record = next(iter(self._final_contexts.values()))
            if record.expires_at > now:
                break
            self._final_contexts.popitem(last=False)

    def _find_context(self, request_id: str) -> _ChatContext | None:
        self._purge_final_contexts()
        context = self._active_contexts.get(request_id)
        if context is not None:
            return context
        record = self._final_contexts.get(request_id)
        return record.context if record is not None else None

    def _raw_command_from_push(
        self,
        *,
        payload: dict[str, Any],
        request_id: str,
        channel_id: str,
        session_id: str | None,
        metadata: dict[str, Any] | None,
    ) -> dict[str, Any]:
        if not isinstance(payload, dict):
            raise _InvalidCronCommand("invalid Cron command") from None
        owner_user_id = (
            metadata.get(_OWNER_METADATA_FIELDS["user_id"])
            if isinstance(metadata, dict)
            else None
        )
        if not _is_canonical_named(owner_user_id):
            raise ValueError("AS Cron push requires a canonical named owner")
        raw_group = (
            metadata.get(_OWNER_METADATA_FIELDS["group_id"])
            if isinstance(metadata, dict)
            else None
        )
        raw_bot = (
            metadata.get(_OWNER_METADATA_FIELDS["bot_id"])
            if isinstance(metadata, dict)
            else None
        )
        if raw_group is None or raw_group == "":
            owner_group_id = "default"
        elif raw_group in _DEFAULT_ENDPOINT_IDS:
            owner_group_id = raw_group
        else:
            raise ValueError("AS Cron push has an invalid owner group")
        if raw_bot is None or raw_bot == "":
            owner_bot_id = "default"
        elif raw_bot in _DEFAULT_ENDPOINT_IDS:
            owner_bot_id = raw_bot
        else:
            raise ValueError("AS Cron push has an invalid owner bot")
        command = {
            "request_id": request_id,
            "session_id": session_id,
            "channel_id": channel_id,
            "owner_user_id": owner_user_id,
            "owner_group_id": owner_group_id,
            "owner_bot_id": owner_bot_id,
            "payload": {
                "event_type": payload.get("event_type"),
                "action": payload.get("action"),
                "command_id": payload.get("command_id"),
                "data": payload.get("data", {}),
            },
        }
        return command

    def _validate_command(self, command: Any) -> dict[str, Any]:
        try:
            command = validate_cron_command(command)
            if not _is_canonical_named(command["owner_user_id"]):
                raise ValueError("owner must be canonical")
            owner_slot = self._ownership.owner_slot(command["owner_user_id"])
        except ValueError as error:
            raise _InvalidCronCommand("invalid Cron command") from error
        if owner_slot not in self._ownership.manifest.slots:
            raise _InvalidCronCommand("invalid Cron command") from None
        return command

    async def _reject_invalid_command(self, command: Any) -> bool:
        command_id = _safe_command_id(command)
        if not command_id:
            raise _InvalidCronCommand("invalid Cron command") from None
        if not self._remember_rejected_command(command, bounded=True):
            return False
        async with asyncio.timeout(_ACK_TIMEOUT):
            await self._message_handler._push_cron_command_ack(
                command_id=command_id,
                data={
                    "error": "Invalid Cron command",
                    "code": "CRON_INVALID_COMMAND",
                },
                user_id=None,
                session_id=None,
            )
        return True

    async def _reject_command_once(
        self,
        command: dict[str, Any],
        *,
        code: str,
        text: str,
    ) -> bool:
        if not self._remember_rejected_command(command):
            return False
        await self._send_error_ack(command, code=code, text=text)
        return True

    def _is_duplicate_command(self, command: dict[str, Any]) -> bool:
        self._purge_seen_commands()
        command_id = _safe_command_id(command)
        if not command_id:
            return False
        existing = self._seen_commands.get(command_id)
        if existing is None:
            return False
        if existing.fingerprint != _command_fingerprint(command):
            logger.warning(
                "[WorkerCronCommands] duplicate command_id has a different payload: command_id=%s",
                command_id,
            )
        return True

    def _remember_rejected_command(
        self,
        command: Any,
        *,
        bounded: bool = False,
    ) -> bool:
        command_id = _safe_command_id(command)
        if not command_id:
            raise _InvalidCronCommand("invalid Cron command") from None
        self._purge_seen_commands()
        fingerprint = _command_fingerprint(command, bounded=bounded)
        existing = self._seen_commands.get(command_id)
        if existing is not None:
            if existing.fingerprint != fingerprint:
                logger.warning(
                    "[WorkerCronCommands] duplicate command_id has a different payload: command_id=%s",
                    command_id,
                )
            return False
        if len(self._seen_commands) >= self._COMMAND_LIMIT:
            raise ControlServiceBusy()
        self._seen_commands[command_id] = _SeenCommand(
            fingerprint=fingerprint,
            expires_at=self._now() + self._COMMAND_TTL,
        )
        return True

    async def _queue_error(
        self,
        command: dict[str, Any],
        *,
        code: str,
        text: str,
    ) -> None:
        item = _QueuedCommand(
            command=command,
            execute=False,
            deadline_at=self._loop_now() + self._COMMAND_DEADLINE,
            error_code=code,
            error_text=text,
        )
        try:
            await self._queue_command(item)
        except ControlServiceBusy:
            await self._reject_command_once(command, code=code, text=text)

    async def _queue_command(self, item: _QueuedCommand) -> bool:
        self._purge_seen_commands()
        command_id = item.command["payload"]["command_id"]
        fingerprint = _command_fingerprint(item.command)
        existing = self._seen_commands.get(command_id)
        if existing is not None:
            if existing.fingerprint != fingerprint:
                logger.warning(
                    "[WorkerCronCommands] duplicate command_id has a different payload: command_id=%s",
                    command_id,
                )
            return False
        if len(self._seen_commands) >= self._COMMAND_LIMIT:
            raise ControlServiceBusy()
        if len(self._tasks) >= self._QUEUE_LIMIT:
            raise ControlServiceBusy()

        self._seen_commands[command_id] = _SeenCommand(
            fingerprint=fingerprint,
            expires_at=None,
        )
        try:
            task = asyncio.create_task(
                self._run_command(item),
                name=f"gateway-worker-cron-command-{command_id}",
            )
        except Exception:
            self._seen_commands.pop(command_id, None)
            raise
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return True

    def _purge_seen_commands(self) -> None:
        now = self._now()
        expired = [
            command_id
            for command_id, record in self._seen_commands.items()
            if record.expires_at is not None and record.expires_at <= now
        ]
        for command_id in expired:
            del self._seen_commands[command_id]

    async def _run_command(self, item: _QueuedCommand) -> None:
        command_id = item.command["payload"]["command_id"]
        try:
            if self._loop_now() >= item.deadline_at:
                await self._send_error_ack(
                    item.command,
                    code="CRON_COMMAND_DEADLINE",
                    text="Worker Cron command deadline expired before execution",
                )
                return

            try:
                if item.execute:
                    assert item.execution is not None
                    async with asyncio.timeout_at(item.deadline_at):
                        await self._message_handler._handle_cron_push_payload(
                            **item.execution,
                        )
                else:
                    assert item.error_code is not None
                    assert item.error_text is not None
                    await self._send_error_ack(
                        item.command,
                        code=item.error_code,
                        text=item.error_text,
                    )
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                raise
            except Exception:
                logger.exception(
                    "[WorkerCronCommands] queued command failed: command_id=%s",
                    command_id,
                )
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            logger.warning(
                "[WorkerCronCommands] command deadline expired in flight; result is unknown: command_id=%s",
                command_id,
            )
        except Exception:
            logger.exception(
                "[WorkerCronCommands] queued command failed: command_id=%s",
                command_id,
            )
        finally:
            self._finish_command(command_id)

    def _finish_command(self, command_id: str) -> None:
        record = self._seen_commands.get(command_id)
        if record is not None:
            self._seen_commands.move_to_end(command_id)
            record.expires_at = self._now() + self._COMMAND_TTL

    async def _send_error_ack(
        self,
        command: dict[str, Any],
        *,
        code: str,
        text: str,
    ) -> None:
        payload = command["payload"]
        async with asyncio.timeout(_ACK_TIMEOUT):
            await self._message_handler._push_cron_command_ack(
                command_id=payload["command_id"],
                data={"error": text, "code": code},
                user_id=command["owner_user_id"],
                session_id=command["session_id"],
            )
            if payload["action"] == "run_now":
                data = payload["data"]
                job_id = data.get("job_id") if isinstance(data, dict) else ""
                await self._message_handler._push_cron_run_now_ack(
                    request_id=command["request_id"],
                    job_id=_text(job_id),
                    run_id="",
                    user_id=command["owner_user_id"],
                    session_id=command["session_id"],
                    error=text,
                )


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _safe_command_id(command: Any) -> str:
    if not isinstance(command, dict):
        return ""
    payload = command.get("payload")
    if not isinstance(payload, dict):
        return ""
    command_id = payload.get("command_id")
    if not isinstance(command_id, str) or not command_id or len(command_id) > 256:
        return ""
    return command_id


def _command_fingerprint(command: dict[str, Any], *, bounded: bool = False) -> str:
    value = _safe_fingerprint_value(command) if bounded else command
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _safe_fingerprint_value(
    value: Any,
    depth: int = 0,
    budget: list[int] | None = None,
) -> Any:
    if budget is None:
        budget = [1024]
    if depth >= 8 or budget[0] <= 0:
        return {"kind": "bounded-limit"}
    budget[0] -= 1
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, str):
        return {
            "kind": "string",
            "length": len(value),
            "prefix": value[:256],
        }
    if isinstance(value, int):
        encoded = str(value)
        return {
            "kind": "integer",
            "length": len(encoded),
            "prefix": encoded[:256],
        }
    if isinstance(value, float):
        if not math.isfinite(value):
            return {"kind": "non-finite-float"}
        return value
    if isinstance(value, (list, tuple)):
        return {
            "kind": "sequence",
            "type": type(value).__name__,
            "length": len(value),
            "items": [
                _safe_fingerprint_value(child, depth + 1, budget)
                for child in value[:32]
            ],
        }
    if isinstance(value, dict):
        entries: list[list[Any]] = []
        for index, (key, child) in enumerate(value.items()):
            if index >= 32:
                break
            entries.append(
                [
                    _safe_fingerprint_value(key, depth + 1, budget),
                    _safe_fingerprint_value(child, depth + 1, budget),
                ]
            )
        entries.sort(
            key=lambda entry: json.dumps(
                entry[0],
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
        return {
            "kind": "mapping",
            "length": len(value),
            "entries": entries,
        }
    return {"kind": "unsupported-type", "type": type(value).__name__}


def _is_canonical_named(value: Any) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value != "default"
        and value == value.strip()
        and len(value) <= 256
    )


def _cron_model_source(data: dict[str, Any]) -> str:
    raw_model = data.get("model_name")
    if not _text(raw_model):
        model_selection = data.get("model_selection")
        if isinstance(model_selection, dict) and model_selection.get("type") == "model":
            raw_model = model_selection.get("id")
    if not _text(raw_model):
        return ""
    try:
        return resolve_cron_model(raw_model)[1]
    except Exception:
        return ""
