"""Multi-target Worker credential leases backed by one runtime per target."""
from __future__ import annotations

import asyncio
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from e2a.constants import E2A_MODEL_AUTH_PARAM_KEY

from agent_gateway.auth.credential_access import CredentialAccessError
from agent_gateway.auth.worker_credential_runtime import (
    CredentialOperationClient,
    ReportResult,
    WorkerCredentialLease,
    WorkerCredentialRuntime,
)
from agent_gateway.processes.control_service import ControlServiceIdentity


_MAX_SERVER_ID_LENGTH = 256


def _server_id(value: object) -> str:
    if type(value) is not str:
        raise ValueError("invalid target server_id")
    normalized = value.strip()
    if not normalized or len(normalized) > _MAX_SERVER_ID_LENGTH:
        raise ValueError("invalid target server_id")
    return normalized


class WorkerCredentialPool:
    """Dispatch credential leases to explicitly admitted AgentServer targets.

    Each target keeps the original single-target runtime semantics. The pool
    never derives a target from an arbitrary envelope field; request dispatch
    follows only the private ``WorkerCredentialLease`` installed in model-auth
    parameters, while lifecycle events must name an admitted target.
    """

    def __init__(
        self,
        *,
        identity: ControlServiceIdentity,
        operation_clients: Mapping[str, CredentialOperationClient],
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
        if not isinstance(operation_clients, Mapping):
            raise TypeError("operation_clients must be a Mapping")
        if report_result is not None and not callable(report_result):
            raise TypeError("report_result must be callable")

        self._identity = identity
        self._runtimes: dict[str, WorkerCredentialRuntime] = {}
        for raw_server_id, client in operation_clients.items():
            server_id = _server_id(raw_server_id)
            if server_id in self._runtimes:
                raise ValueError(f"duplicate target server_id: {server_id}")
            if not callable(getattr(client, "send_request", None)):
                raise TypeError("operation client must provide send_request")
            self._runtimes[server_id] = WorkerCredentialRuntime(
                identity=identity,
                server_id=server_id,
                client=client,
                report_result=report_result,
                operation_timeout_s=operation_timeout_s,
                report_timeout_s=report_timeout_s,
                max_leases=max_leases,
                max_waiting_operations_per_target=max_waiting_operations_per_target,
                max_operations=max_operations,
                max_target_refs=max_target_refs,
            )

    @property
    def identity(self) -> ControlServiceIdentity:
        return self._identity

    @property
    def server_ids(self) -> tuple[str, ...]:
        return tuple(self._runtimes)

    @property
    def active_lease_count(self) -> int:
        return sum(runtime.active_lease_count for runtime in self._runtimes.values())

    @property
    def pending_operation_count(self) -> int:
        return sum(
            runtime.pending_operation_count for runtime in self._runtimes.values()
        )

    def runtime_for(self, server_id: str) -> WorkerCredentialRuntime:
        runtime = self._runtimes.get(_server_id(server_id))
        if runtime is None:
            raise CredentialAccessError("invalid")
        return runtime

    def issue(
        self, credential_ref: str, ref_revision: int, server_id: str
    ) -> WorkerCredentialLease:
        return self.runtime_for(server_id).issue(
            credential_ref, ref_revision, server_id
        )

    @asynccontextmanager
    async def before_send(self, envelope: Any) -> AsyncIterator[None]:
        lease = self._envelope_lease(envelope)
        if lease is None:
            yield
            return
        async with self._runtime_for_lease(lease).before_send(envelope):
            yield

    def check(self, envelope: Any) -> None:
        lease = self._envelope_lease(envelope)
        if lease is not None:
            self._runtime_for_lease(lease).check(envelope)

    def mark_written(self, envelope: Any) -> None:
        lease = self._envelope_lease(envelope)
        if lease is not None:
            self._runtime_for_lease(lease).mark_written(envelope)

    def release(self, envelope: Any) -> None:
        lease = self._envelope_lease(envelope)
        if lease is None:
            return
        runtime = self._runtimes.get(lease.server_id)
        if runtime is not None:
            runtime.release(envelope)

    def invalidate(self, event: Mapping[str, Any]) -> bool:
        runtime = self._runtime_for_event(event)
        return runtime is not None and runtime.invalidate(event)

    def refresh(self, event: Mapping[str, Any]) -> bool:
        runtime = self._runtime_for_event(event)
        return runtime is not None and runtime.refresh(event)

    async def wait_for_writes(
        self,
        credential_ref: str,
        server_id: str,
        cutoff_revision: int,
        *,
        timeout: float = 1.0,
    ) -> bool:
        return await self.runtime_for(server_id).wait_for_writes(
            credential_ref, server_id, cutoff_revision, timeout=timeout
        )

    async def wait_for_operations(self, *, timeout: float) -> bool:
        results = await asyncio.gather(*(
            runtime.wait_for_operations(timeout=timeout)
            for runtime in self._runtimes.values()
        ))
        return all(results)

    async def stop(self, *, timeout: float = 1.0) -> None:
        await asyncio.gather(*(
            runtime.stop(timeout=timeout) for runtime in self._runtimes.values()
        ))

    def _runtime_for_event(
        self, event: Mapping[str, Any]
    ) -> WorkerCredentialRuntime | None:
        if not isinstance(event, Mapping):
            return None
        try:
            server_id = _server_id(event.get("server_id"))
        except ValueError:
            return None
        return self._runtimes.get(server_id)

    def _runtime_for_lease(
        self, lease: WorkerCredentialLease
    ) -> WorkerCredentialRuntime:
        try:
            server_id = _server_id(lease.server_id)
        except ValueError as error:
            raise CredentialAccessError("invalid") from error
        runtime = self._runtimes.get(server_id)
        if runtime is None:
            raise CredentialAccessError("invalid")
        return runtime

    @staticmethod
    def _envelope_lease(envelope: Any) -> WorkerCredentialLease | None:
        params = getattr(envelope, "params", None)
        auth = params.get(E2A_MODEL_AUTH_PARAM_KEY) if isinstance(params, Mapping) else None
        lease = getattr(auth, "credential_lease", None)
        return lease if isinstance(lease, WorkerCredentialLease) else None
