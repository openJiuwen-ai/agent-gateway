"""Worker-side request leases and bounded AgentServer credential operations."""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from inspect import isawaitable
from time import monotonic, time
from typing import Any, AsyncIterator, Awaitable, Callable, Literal, Mapping, Protocol

from e2a.constants import E2A_MODEL_AUTH_PARAM_KEY

from agent_gateway.auth.credential_access import CredentialAccessError, CredentialUpdate
from agent_gateway.processes.control_service import ControlServiceIdentity


logger = logging.getLogger(__name__)


CredentialOperationOutcome = Literal[
    "applied", "rejected", "failed", "timeout_unknown", "confirmed_unknown", "superseded"
]


@dataclass(eq=False)
class WorkerCredentialLease:
    """A private, request-scoped authorization issued by Worker runtime."""

    credential_ref: str
    ref_revision: int
    server_id: str
    group_id: str
    slot_id: int
    generation: int
    revoked: bool = field(default=False)


@dataclass(frozen=True)
class CredentialOperationResult:
    """A bounded credential-operation result without token or address details."""

    operation_id: str
    server_id: str
    credential_ref: str
    outcome: CredentialOperationOutcome
    written: bool
    response_received: bool
    as_ok: bool
    as_applied: bool


class CredentialOperationClient(Protocol):
    """The explicit AgentServer client used by this runtime."""

    async def send_request(
        self, envelope: Any, *, on_written: Callable[[], None] | None = None
    ) -> Any: ...


ReportResult = Callable[[CredentialOperationResult], Awaitable[None] | object]


@dataclass
class _TargetState:
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    cutoff_revision: int = 0
    last_written_revision: int = 0
    unknown: bool = False
    scheduled_revoke_revision: int = 0
    operation_count: int = 0
    active_writes: dict[int, "_ActiveWrite"] = field(default_factory=dict)


@dataclass
class _ActiveWrite:
    ref_revision: int
    finished: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass
class _Operation:
    operation_id: str
    credential_ref: str
    ref_revision: int
    server_id: str
    revoke: bool
    params: dict[str, Any]
    deadline: float


def _text(value: object, *, max_length: int, allow_empty: bool = False) -> str:
    if type(value) is not str:
        raise CredentialAccessError("invalid")
    normalized = value.strip()
    if not normalized and not allow_empty:
        raise CredentialAccessError("invalid")
    if len(normalized) > max_length:
        raise CredentialAccessError("invalid")
    return normalized


def _positive_revision(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise CredentialAccessError("invalid")
    return value


def _exact_fields(value: object, fields: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise CredentialAccessError("invalid")
    return value


class WorkerCredentialRuntime:
    """Own one Worker's credential leases and explicit AgentServer target."""

    def __init__(
        self,
        *,
        identity: ControlServiceIdentity,
        server_id: str,
        client: CredentialOperationClient,
        report_result: ReportResult | None = None,
        operation_timeout_s: float = 5.0,
        report_timeout_s: float = 2.0,
        max_leases: int = 32,
        max_waiting_operations_per_target: int = 8,
        max_operations: int = 32,
        max_target_refs: int = 1024,
    ) -> None:
        if not isinstance(identity, ControlServiceIdentity):
            raise TypeError("identity must be a ControlServiceIdentity")
        self._identity = identity
        self._server_id = _text(server_id, max_length=256)
        self._client = client
        self._report_result = report_result
        self._operation_timeout_s = operation_timeout_s
        self._report_timeout_s = report_timeout_s
        self._max_leases = max_leases
        self._max_operations_per_target = max_waiting_operations_per_target + 1
        self._max_operations = max_operations
        self._max_target_refs = max_target_refs
        self._leases: dict[int, WorkerCredentialLease] = {}
        self._targets: dict[tuple[str, str], _TargetState] = {}
        self._blocked_targets: set[tuple[str, str]] = set()
        self._new_targets_rejected = False
        self._operation_tasks: dict[str, asyncio.Task[None]] = {}
        self._results: deque[CredentialOperationResult] = deque(maxlen=max_operations)
        self._closed = False

    @property
    def identity(self) -> ControlServiceIdentity:
        return self._identity

    @property
    def server_id(self) -> str:
        return self._server_id

    @property
    def active_lease_count(self) -> int:
        return len(self._leases)

    @property
    def pending_operation_count(self) -> int:
        return len(self._operation_tasks)

    def issue(
        self, credential_ref: str, ref_revision: int, server_id: str
    ) -> WorkerCredentialLease:
        if self._closed:
            raise CredentialAccessError("unavailable")
        ref = _text(credential_ref, max_length=256)
        revision = _positive_revision(ref_revision)
        target = _text(server_id, max_length=256)
        if target != self._server_id:
            raise CredentialAccessError("invalid")
        key = (target, ref)
        if self._new_targets_rejected or key in self._blocked_targets:
            raise CredentialAccessError("invalid")
        state = self._target_state((target, ref))
        if state.unknown or revision <= state.cutoff_revision:
            raise CredentialAccessError("invalid")
        if len(self._leases) >= self._max_leases:
            raise CredentialAccessError("unavailable")
        lease = WorkerCredentialLease(
            credential_ref=ref,
            ref_revision=revision,
            server_id=target,
            group_id=self._identity.group_id,
            slot_id=self._identity.slot_id,
            generation=self._identity.generation,
        )
        self._leases[id(lease)] = lease
        return lease

    @asynccontextmanager
    async def before_send(self, envelope: Any) -> AsyncIterator[None]:
        lease = self._envelope_lease(envelope)
        if lease is None:
            yield
            return
        state = self._target_state((lease.server_id, lease.credential_ref))
        async with state.lock:
            self._validate_lease(lease, state)
            yield

    def check(self, envelope: Any) -> None:
        lease = self._envelope_lease(envelope)
        if lease is None:
            return
        state = self._target_state((lease.server_id, lease.credential_ref))
        self._validate_lease(lease, state)
        if id(lease) not in state.active_writes:
            state.active_writes[id(lease)] = _ActiveWrite(lease.ref_revision)

    def mark_written(self, envelope: Any) -> None:
        lease = self._envelope_lease(envelope)
        if lease is None:
            return
        state = self._target_state((lease.server_id, lease.credential_ref))
        active_write = state.active_writes.get(id(lease))
        if active_write is None or active_write.ref_revision != lease.ref_revision:
            raise CredentialAccessError("invalid")
        state.last_written_revision = max(state.last_written_revision, lease.ref_revision)

    def release(self, envelope: Any) -> None:
        params = getattr(envelope, "params", None)
        auth = params.get(E2A_MODEL_AUTH_PARAM_KEY) if isinstance(params, Mapping) else None
        lease = getattr(auth, "credential_lease", None)
        if isinstance(lease, WorkerCredentialLease):
            state = self._targets.get((lease.server_id, lease.credential_ref))
            if state is not None:
                active_write = state.active_writes.pop(id(lease), None)
                if active_write is not None:
                    active_write.finished.set()
            self._leases.pop(id(lease), None)

    async def wait_for_writes(
        self,
        credential_ref: str,
        server_id: str,
        cutoff_revision: int,
        *,
        timeout: float = 1.0,
    ) -> bool:
        ref = _text(credential_ref, max_length=256)
        target = _text(server_id, max_length=256)
        revision = _positive_revision(cutoff_revision)
        state = self._targets.get((target, ref))
        if state is None:
            return True
        events = [
            active_write.finished
            for active_write in state.active_writes.values()
            if active_write.ref_revision <= revision
        ]
        if not events:
            return True
        try:
            async with asyncio.timeout(timeout):
                for event in events:
                    await event.wait()
        except TimeoutError:
            return False
        return True

    def invalidate(self, event: Mapping[str, Any]) -> bool:
        fields = _exact_fields(
            event,
            {"operation_id", "server_id", "credential_ref", "cutoff_revision"},
        )
        operation_id = _text(fields["operation_id"], max_length=128)
        server_id = _text(fields["server_id"], max_length=256)
        credential_ref = _text(fields["credential_ref"], max_length=256)
        cutoff_revision = _positive_revision(fields["cutoff_revision"])
        if server_id != self._server_id:
            return False
        key = (server_id, credential_ref)
        try:
            state = self._target_state(key)
        except CredentialAccessError:
            if len(self._blocked_targets) < self._max_target_refs:
                self._blocked_targets.add(key)
            else:
                self._new_targets_rejected = True
            return False
        state.cutoff_revision = max(state.cutoff_revision, cutoff_revision)
        for lease in self._leases.values():
            if (
                lease.server_id == server_id
                and lease.credential_ref == credential_ref
                and lease.ref_revision <= cutoff_revision
            ):
                lease.revoked = True
        return self._schedule(
            _Operation(
                operation_id=operation_id,
                credential_ref=credential_ref,
                ref_revision=cutoff_revision,
                server_id=server_id,
                revoke=True,
                params={"credential_ref": credential_ref, "revoked": True},
                deadline=monotonic() + self._operation_timeout_s,
            )
        )

    def refresh(self, event: Mapping[str, Any]) -> bool:
        fields = _exact_fields(
            event,
            {"operation_id", "server_id", "credential_ref", "ref_revision", "update"},
        )
        operation_id = _text(fields["operation_id"], max_length=128)
        server_id = _text(fields["server_id"], max_length=256)
        credential_ref = _text(fields["credential_ref"], max_length=256)
        ref_revision = _positive_revision(fields["ref_revision"])
        update_value = fields["update"]
        if not isinstance(update_value, Mapping):
            return False
        update = CredentialUpdate(**dict(update_value))
        if server_id != self._server_id:
            return False
        state = self._target_state((server_id, credential_ref))
        revoke = update.revoked
        if revoke:
            state.cutoff_revision = max(state.cutoff_revision, ref_revision)
            for lease in self._leases.values():
                if (
                    lease.server_id == server_id
                    and lease.credential_ref == credential_ref
                    and lease.ref_revision <= ref_revision
                ):
                    lease.revoked = True
        return self._schedule(
            _Operation(
                operation_id=operation_id,
                credential_ref=credential_ref,
                ref_revision=ref_revision,
                server_id=server_id,
                revoke=revoke,
                params=update.to_params(),
                deadline=monotonic() + self._operation_timeout_s,
            )
        )

    async def wait_for_operations(self, *, timeout: float) -> bool:
        operations = list(self._operation_tasks.values())
        if not operations:
            return True
        try:
            await asyncio.wait_for(asyncio.gather(*operations), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return True

    async def stop(self, *, timeout: float = 1.0) -> None:
        self._closed = True
        operations = list(self._operation_tasks.values())
        for operation in operations:
            operation.cancel()
        await asyncio.wait_for(asyncio.gather(*operations, return_exceptions=True), timeout=timeout)

    def _target_state(self, key: tuple[str, str]) -> _TargetState:
        state = self._targets.get(key)
        if state is None:
            if len(self._targets) >= self._max_target_refs:
                raise CredentialAccessError("unavailable")
            state = _TargetState()
            self._targets[key] = state
        return state

    def _envelope_lease(
        self, envelope: Any, *, required: bool = False
    ) -> WorkerCredentialLease | None:
        params = getattr(envelope, "params", None)
        auth = params.get(E2A_MODEL_AUTH_PARAM_KEY) if isinstance(params, Mapping) else None
        if auth is None:
            if required:
                raise CredentialAccessError("invalid")
            return None
        lease = getattr(auth, "credential_lease", None)
        if not isinstance(lease, WorkerCredentialLease):
            raise CredentialAccessError("invalid")
        if self._leases.get(id(lease)) is not lease:
            raise CredentialAccessError("invalid")
        return lease

    def _validate_lease(
        self, lease: WorkerCredentialLease, state: _TargetState
    ) -> None:
        if (
            lease.revoked
            or state.unknown
            or lease.server_id != self._server_id
            or lease.ref_revision <= state.cutoff_revision
            or lease.group_id != self._identity.group_id
            or lease.slot_id != self._identity.slot_id
            or lease.generation != self._identity.generation
        ):
            raise CredentialAccessError("invalid")

    def _schedule(self, operation: _Operation) -> bool:
        if (
            self._closed
            or operation.operation_id in self._operation_tasks
            or any(
                result.operation_id == operation.operation_id
                for result in self._results
            )
        ):
            return False
        if len(self._operation_tasks) >= self._max_operations:
            return False
        state = self._target_state((operation.server_id, operation.credential_ref))
        if state.operation_count >= self._max_operations_per_target:
            return False
        if (
            operation.revoke
            and operation.ref_revision <= state.scheduled_revoke_revision
        ):
            return False
        task = asyncio.create_task(
            self._run_operation(operation, state),
            name=f"gateway-worker-credential-{operation.operation_id}",
        )
        state.operation_count += 1
        if operation.revoke:
            state.scheduled_revoke_revision = max(
                state.scheduled_revoke_revision, operation.ref_revision
            )
        self._operation_tasks[operation.operation_id] = task
        task.add_done_callback(
            lambda done, state=state, operation_id=operation.operation_id: (
                self._operation_done(done, state, operation_id)
            )
        )
        return True

    def _operation_done(
        self, task: asyncio.Task[None], state: _TargetState, operation_id: str
    ) -> None:
        if self._operation_tasks.get(operation_id) is task:
            del self._operation_tasks[operation_id]
        state.operation_count = max(0, state.operation_count - 1)
        if not task.cancelled() and task.exception() is not None:
            task.exception()

    async def _run_operation(self, operation: _Operation, state: _TargetState) -> None:
        lock_acquired = False
        written = False
        response = None
        response_received = False
        error: BaseException | None = None
        try:
            remaining = max(0.0, operation.deadline - monotonic())
            await asyncio.wait_for(state.lock.acquire(), timeout=remaining)
            lock_acquired = True
            if state.unknown:
                await self._report(
                    CredentialOperationResult(
                        operation_id=operation.operation_id,
                        server_id=operation.server_id,
                        credential_ref=operation.credential_ref,
                        outcome="rejected",
                        written=False,
                        response_received=False,
                        as_ok=False,
                        as_applied=False,
                    )
                )
                return
            stale_refresh = (
                not operation.revoke
                and operation.ref_revision <= state.cutoff_revision
            )
            superseded = stale_refresh or state.last_written_revision > operation.ref_revision
            if superseded:
                await self._report(
                    CredentialOperationResult(
                        operation_id=operation.operation_id,
                        server_id=operation.server_id,
                        credential_ref=operation.credential_ref,
                        outcome="superseded",
                        written=False,
                        response_received=False,
                        as_ok=False,
                        as_applied=False,
                    )
                )
                return
            remaining = max(0.0, operation.deadline - monotonic())
            envelope = self._operation_envelope(operation)

            def mark_written() -> None:
                nonlocal written
                written = True

            response = await asyncio.wait_for(
                self._client.send_request(envelope, on_written=mark_written),
                timeout=remaining,
            )
            response_received = True
        except asyncio.TimeoutError:
            error = TimeoutError()
        except asyncio.CancelledError:
            error = TimeoutError()
        except Exception as exc:
            error = exc
        finally:
            if lock_acquired:
                state.lock.release()

        outcome, as_ok, as_applied = self._operation_outcome(response, response_received, error)
        result = CredentialOperationResult(
            operation_id=operation.operation_id,
            server_id=operation.server_id,
            credential_ref=operation.credential_ref,
            outcome=outcome,
            written=written,
            response_received=response_received,
            as_ok=as_ok,
            as_applied=as_applied,
        )
        if (
            operation.revoke
            and written
            and outcome not in {"applied", "rejected", "superseded"}
        ):
            state.unknown = True
        await self._report(result)

    def _operation_outcome(
        self, response: Any, response_received: bool, error: BaseException | None
    ) -> tuple[CredentialOperationOutcome, bool, bool]:
        if error is not None:
            return ("timeout_unknown" if isinstance(error, TimeoutError) else "failed", False, False)
        if not response_received:
            return ("failed", False, False)
        as_ok = getattr(response, "ok", False) is True
        payload = getattr(response, "payload", None)
        has_applied = isinstance(payload, Mapping) and "applied" in payload
        applied = payload.get("applied") if has_applied else None
        if has_applied and type(applied) is not bool:
            return ("failed", as_ok, False)
        as_applied = applied is True if has_applied else False
        if not as_ok:
            return ("rejected" if not as_applied else "failed", as_ok, as_applied)
        if not has_applied:
            return ("confirmed_unknown", as_ok, as_applied)
        if as_applied:
            return ("applied", as_ok, as_applied)
        return ("failed", as_ok, as_applied)

    def _operation_envelope(self, operation: _Operation) -> Any:
        from e2a.gateway_normalize import e2a_from_agent_fields
        from e2a.message import ReqMethod

        return e2a_from_agent_fields(
            request_id=f"auth-credentials-{operation.operation_id}",
            req_method=ReqMethod.AUTH_CREDENTIALS_UPDATE,
            params=dict(operation.params),
            timestamp=time(),
        )

    async def _report(self, result: CredentialOperationResult) -> None:
        self._results.append(result)
        callback = self._report_result
        if callback is None:
            return
        try:
            value = callback(result)
            if isawaitable(value):
                await asyncio.wait_for(value, timeout=self._report_timeout_s)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("[WorkerCredentialRuntime] result report failed", exc_info=True)
            pass
