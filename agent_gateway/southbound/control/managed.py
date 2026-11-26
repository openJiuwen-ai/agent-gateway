# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared managed-resource contracts and lifecycle adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, TypedDict

from agent_gateway.southbound.control.interfaces import ManagedControl


class AgentMount(TypedDict, total=False):
    source: str
    target: str
    readonly: bool


class AgentRootfsSpec(TypedDict, total=False):
    imageurl: str
    user: str
    ports: list[str]


class AgentRuntimeSpec(TypedDict, total=False):
    runtime: str
    sandbox_type: str
    rootfs: AgentRootfsSpec
    cpu: int
    memory: int
    code_path: str
    cmds: list[list[str]]
    probes: dict[str, Any]


@dataclass
class SandboxInfo:
    """Physical agent instance lifecycle record."""

    sandbox_id: str
    status: str = "ready"
    metadata: dict[str, Any] = field(default_factory=dict)


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


class ManagedSandboxControl(ManagedControl[SandboxCreateRequest, SandboxInfo, dict[str, Any]]):
    """Forward lifecycle operations to the platform client without changing errors or IDs."""

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
