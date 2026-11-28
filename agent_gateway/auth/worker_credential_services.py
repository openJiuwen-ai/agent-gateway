# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Explicit credential-lifecycle assembly, separate from cold Worker admission."""
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from agent_gateway.auth.remote_credential_access import RemoteCredentialAccess, RemoteSessionLoginBindings
from agent_gateway.auth.worker_credential_runtime import CredentialOperationResult, WorkerCredentialRuntime
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_service import (
    ControlServiceBusy, ControlServiceClient, ControlServiceError, ControlServiceIdentity,
)


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
