# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Managed control adapter: existing platform operations and endpoint metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol
from urllib.parse import urlencode, urlsplit, urlunsplit

from agent_gateway.extensions.yuanrong_frontend_client import AgentRuntimeSpec, SandboxInfo


@dataclass(frozen=True)
class SandboxCreateRequest:
    namespace: str
    name: str
    workspace: str
    runtime_spec: AgentRuntimeSpec | Mapping[str, Any]
    env_vars: dict[str, str] | None = None


class SandboxBackend(Protocol):
    @property
    def frontend_endpoint(self) -> str: ...

    @property
    def agent_namespace(self) -> str: ...

    async def create_sandbox(self, *, namespace: str, name: str, workspace: str,
                             runtime_spec: Any, env_vars: dict[str, str] | None = None) -> SandboxInfo: ...

    async def get_agent_info(self, instance_id: str) -> dict[str, Any]: ...

    async def delete_sandbox(self, sandbox_id: str) -> None: ...


class ManagedSandboxControl:
    """Preserve platform exceptions, trace context and exact physical IDs.

    No retries, polling policy, connection establishment or agent state here.
    The underlying platform client remains owned by the existing router.
    """

    def __init__(self, backend: SandboxBackend) -> None:
        self.backend = backend

    async def create(self, request: SandboxCreateRequest) -> SandboxInfo:
        return await self.backend.create_sandbox(
            namespace=request.namespace, name=request.name, workspace=request.workspace,
            runtime_spec=request.runtime_spec, env_vars=request.env_vars,
        )

    async def query(self, instance_id: str) -> dict[str, Any]:
        return await self.backend.get_agent_info(instance_id)

    async def delete(self, instance_id: str) -> None:
        await self.backend.delete_sandbox(instance_id)


class YuanRongControl(ManagedSandboxControl):
    def _url(self, instance_id: str, port: int, protocol: str) -> str:
        parsed = urlsplit(str(self.backend.frontend_endpoint or "").rstrip("/"))
        secure = parsed.scheme == "https"
        scheme = ("wss" if secure else "ws") if protocol == "ws" else ("https" if secure else "http")
        query = urlencode({"instance": instance_id, "tenant_id": self.backend.agent_namespace or "default",
                           "port": str(port)})
        return f"{scheme}://{parsed.netloc}/serverless/v1/{protocol}?{query}"

    def ws_url(self, instance_id: str, port: int) -> str:
        return self._url(instance_id, port, "ws")

    def http_url(self, instance_id: str, port: int) -> str:
        return self._url(instance_id, port, "http")

    def web_ws_url(self, instance_id: str, port: int) -> str:
        parsed = urlsplit(self.http_url(instance_id, port))
        return urlunsplit(("wss" if parsed.scheme == "https" else "ws", parsed.netloc,
                           parsed.path, parsed.query, parsed.fragment))
