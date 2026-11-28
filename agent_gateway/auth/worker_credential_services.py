# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Explicit credential-lifecycle assembly, separate from cold Worker admission."""
from __future__ import annotations

from dataclasses import asdict
from collections.abc import Callable, Mapping
import logging
from typing import Any

from e2a.wire_codec import (
    is_e2a_response_wire_dict, parse_agent_server_wire_chunk,
)

from agent_gateway.auth.login_credentials import CREDENTIAL_REFRESH_EVENT, is_credential_ref
from agent_gateway.auth.remote_credential_access import RemoteCredentialAccess, RemoteSessionLoginBindings
from agent_gateway.auth.worker_credential_pool import WorkerCredentialPool
from agent_gateway.auth.worker_credential_runtime import (
    CredentialOperationResult, WorkerCredentialRuntime,
)
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceBusy, ControlServiceClient, ControlServiceError, ControlServiceIdentity,
)


logger = logging.getLogger(__name__)


class RoutedCredentialAccessRouter:
    """Select a fixed target port from a trusted Worker-owned user mapping."""

    def __init__(
        self,
        client: ControlServiceClient,
        *,
        pool: WorkerCredentialPool,
        resolve_server_id: Callable[[str], str],
    ) -> None:
        self._client = client
        self._pool = pool
        self._resolve_server_id = resolve_server_id
        self._ports = {
            server_id: RemoteCredentialAccess(
                client, runtime=pool.runtime_for(server_id), server_id=server_id,
            )
            for server_id in pool.server_ids
        }

    def for_user(self, user_id: str) -> RemoteCredentialAccess:
        if type(user_id) is not str or not user_id:
            raise ValueError('credential target requires a bound user')
        return self._ports[self._resolve_server_id(user_id)]

    def for_server(self, server_id: str) -> RemoteCredentialAccess:
        return self._ports[self._pool.runtime_for(server_id).server_id]

    def release_request(self, envelope: Any) -> None:
        self._pool.release(envelope)


class RoutedSessionLoginBindingsRouter:
    """Select target-scoped bindings from a trusted Worker-owned user mapping."""

    def __init__(
        self,
        client: ControlServiceClient,
        *,
        pool: WorkerCredentialPool,
        resolve_server_id: Callable[[str], str],
    ) -> None:
        self._client = client
        self._pool = pool
        self._resolve_server_id = resolve_server_id
        self._ports = {
            server_id: RemoteSessionLoginBindings(
                client, runtime=pool.runtime_for(server_id), server_id=server_id,
            )
            for server_id in pool.server_ids
        }

    def for_user(self, user_id: str) -> RemoteSessionLoginBindings:
        if type(user_id) is not str or not user_id:
            raise ValueError('credential target requires a bound user')
        return self._ports[self._resolve_server_id(user_id)]

    def for_server(self, server_id: str) -> RemoteSessionLoginBindings:
        return self._ports[self._pool.runtime_for(server_id).server_id]


class WorkerCredentialServices:
    """Own private ports and invalidation for one explicit direct AS client.

    This is an opt-in composition seam, not a production account feature flag.
    The owner must start it before issuing credentials and stop it before closing
    the target client. No login store or blocking credential work lives here.
    """

    def __init__(
        self, channel: ControlServiceChannel, identity: ControlServiceIdentity,
        *, server_id: str, agent_client: Any,
    ) -> None:
        client = ControlServiceClient(
            channel, identity, event_handlers={'credentials.invalidate': self._invalidate},
        )
        self._initialize(
            client=client, identity=identity, server_id=server_id,
            agent_client=agent_client, owns_client=True,
        )

    @classmethod
    def from_client(
        cls, client: ControlServiceClient, *, server_id: str, agent_client: Any,
    ) -> 'WorkerCredentialServices':
        """Compose around an already-started Worker-owned control client."""
        services = cls.__new__(cls)
        services._initialize(
            client=client, identity=client.identity, server_id=server_id,
            agent_client=agent_client, owns_client=False,
        )
        return services

    def _initialize(
        self, *, client: ControlServiceClient, identity: ControlServiceIdentity,
        server_id: str, agent_client: Any, owns_client: bool,
    ) -> None:
        self.client = client
        self._owns_client = owns_client
        self.runtime = WorkerCredentialRuntime(
            identity=identity, server_id=server_id, client=agent_client,
            report_result=self._report,
        )
        self.credentials = RemoteCredentialAccess(self.client, runtime=self.runtime)
        self.bindings = RemoteSessionLoginBindings(self.client, runtime=self.runtime)
        agent_client.set_credential_runtime(self.runtime)

    async def handle_invalidation(self, arguments: dict[str, Any]) -> None:
        await self._invalidate(arguments)

    async def _invalidate(self, arguments: dict[str, Any]) -> None:
        # Runtime fences queued leases synchronously, including on a full queue.
        accepted = self.runtime.invalidate(arguments)
        # A write that entered the transport before the fence is already in
        # flight. Do not ACK settled invalidation until that short send ends.
        settled = await self.runtime.wait_for_writes(
            arguments['credential_ref'], arguments['server_id'], arguments['cutoff_revision'],
            timeout=1.0,
        )
        if not accepted or not settled:
            raise ControlServiceBusy()

    async def _report(self, result: CredentialOperationResult) -> None:
        response = await self.client.call('credentials.update_result', asdict(result), timeout=2)
        if response != {'status': 'ok'}:
            raise ControlServiceError()

    def attach_handler(self, handler: Any) -> None:
        """Install the same source-bound runtime and remote ports on its MH."""
        handler.set_credential_access(self.credentials)
        handler.set_session_login_bindings(self.bindings)
        handler.set_credential_runtime(self.runtime)

    async def start(self) -> None:
        if self._owns_client:
            await self.client.start()

    async def stop(self) -> None:
        try:
            await self.runtime.stop()
        finally:
            if self._owns_client:
                await self.client.stop()


class RoutedWorkerCredentialServices:
    """Compose static multi-target credentials without a ContextVar."""

    def __init__(
        self,
        client: ControlServiceClient,
        *,
        operation_clients: Mapping[str, Any],
        resolve_server_id: Callable[[str], str],
        agent_client: Any,
    ) -> None:
        if not isinstance(operation_clients, Mapping):
            raise TypeError('operation_clients must be a Mapping')
        if not callable(resolve_server_id):
            raise TypeError('resolve_server_id must be callable')
        for method_name in (
            'credential_operation_clients', 'server_id_for_user',
            'set_credential_runtime', 'set_credential_lifecycle_handler',
        ):
            if not callable(getattr(agent_client, method_name, None)):
                raise TypeError(f'agent_client must provide {method_name}')

        self.client = client
        self.runtime = WorkerCredentialPool(
            identity=client.identity,
            operation_clients=operation_clients,
            report_result=self._report,
        )
        self.credentials = RoutedCredentialAccessRouter(
            client, pool=self.runtime, resolve_server_id=resolve_server_id,
        )
        self.bindings = RoutedSessionLoginBindingsRouter(
            client, pool=self.runtime, resolve_server_id=resolve_server_id,
        )
        agent_client.set_credential_runtime(self.runtime)
        agent_client.set_credential_lifecycle_handler(self.handle_credential_refresh)

    @classmethod
    def from_client(
        cls, client: ControlServiceClient, *, agent_client: Any,
    ) -> 'RoutedWorkerCredentialServices':
        """Compose around an already-started Worker-owned control client."""
        operation_clients = agent_client.credential_operation_clients()
        return cls(
            client,
            operation_clients=operation_clients,
            resolve_server_id=agent_client.server_id_for_user,
            agent_client=agent_client,
        )

    @staticmethod
    def _credential_refresh_payload(wire: dict[str, Any]) -> dict[str, Any] | None:
        if not isinstance(wire, dict):
            return None
        payload = None
        if is_e2a_response_wire_dict(wire) or 'is_complete' in wire:
            try:
                payload = parse_agent_server_wire_chunk(wire).payload
            except Exception:
                return None
        elif 'is_complete' not in wire:
            payload = wire.get('payload')
        if not isinstance(payload, dict):
            return None
        event_type = payload.get('event_type')
        if event_type is not None and event_type != CREDENTIAL_REFRESH_EVENT:
            return None
        credential_ref = payload.get('credential_ref')
        if type(credential_ref) is not str or not is_credential_ref(credential_ref):
            return None
        return payload

    async def handle_credential_refresh(self, server_id: str, wire: dict[str, Any]) -> None:
        payload = self._credential_refresh_payload(wire)
        if payload is None:
            return
        credential_ref = payload['credential_ref']
        try:
            access = self.credentials.for_server(server_id)
            runtime = self.runtime.runtime_for(server_id)
            refresh = await access.credential_refresh_for_ref(credential_ref, server_id)
            if refresh is None:
                return
            accepted = runtime.refresh({
                'operation_id': refresh.operation_id,
                'server_id': refresh.server_id,
                'credential_ref': credential_ref,
                'ref_revision': refresh.ref_revision,
                'update': refresh.update.to_params(),
            })
            if not accepted:
                logger.warning(
                    '[RoutedWorkerCredentialServices] credential update queue is full: server_id=%s',
                    server_id,
                )
        except Exception:
            logger.warning(
                '[RoutedWorkerCredentialServices] credential refresh failed: server_id=%s',
                server_id,
                exc_info=True,
            )

    async def handle_invalidation(self, arguments: dict[str, Any]) -> None:
        accepted = self.runtime.invalidate(arguments)
        settled = await self.runtime.wait_for_writes(
            arguments['credential_ref'], arguments['server_id'],
            arguments['cutoff_revision'], timeout=1.0,
        )
        if not accepted or not settled:
            raise ControlServiceBusy()

    async def _report(self, result: CredentialOperationResult) -> None:
        response = await self.client.call(
            'credentials.update_result', asdict(result), timeout=2,
        )
        if response != {'status': 'ok'}:
            raise ControlServiceError()

    def attach_handler(self, handler: Any) -> None:
        """Install source-selected ports and the pool runtime on its MH."""
        handler.set_credential_access(self.credentials)
        handler.set_session_login_bindings(self.bindings)
        handler.set_credential_runtime(self.runtime)

    async def start(self) -> None:
        """The Worker owns the borrowed control client and routed client."""

    async def stop(self) -> None:
        await self.runtime.stop()
