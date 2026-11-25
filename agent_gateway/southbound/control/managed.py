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
    builtin: bool = False
    web_port: int | None = None


class SandboxBackend(Protocol):
    @property
    def frontend_endpoint(self) -> str: ...

    @property
    def agent_namespace(self) -> str: ...

    @property
    def server_ready(self) -> bool: ...

    async def connect(self, uri: str) -> None: ...

    async def disconnect(self) -> None: ...

    def set_or_update_server_config(self, *, config: dict[str, Any], env: dict[str, str] | None) -> None: ...

    async def wait_until_running(self, instance_id: str, **kwargs: Any) -> dict[str, Any]: ...

    async def create_sandbox(self, *, namespace: str, name: str, workspace: str,
                             runtime_spec: Any, env_vars: dict[str, str] | None = None) -> SandboxInfo: ...

    async def get_agent_info(self, instance_id: str) -> dict[str, Any]: ...

    async def delete_sandbox(self, sandbox_id: str) -> None: ...


class ManagedSandboxControl(ManagedControl[SandboxCreateRequest, SandboxInfo, dict[str, Any]]):
    """Forward lifecycle operations to the platform client without changing errors or IDs."""

    def __init__(self, backend: SandboxBackend) -> None:
        self.backend = backend

    def ws_headers(self) -> dict[str, str]:
        raise NotImplementedError

    def ws_url(self, instance_id: str, port: int) -> str:
        raise NotImplementedError

    def http_url(self, instance_id: str, port: int) -> str:
        raise NotImplementedError

    def web_ws_url(self, instance_id: str, port: int) -> str:
        raise NotImplementedError

    async def create(self, request: SandboxCreateRequest) -> SandboxInfo:
        return await self.backend.create_sandbox(
            namespace=request.namespace, name=request.name, workspace=request.workspace,
            runtime_spec=request.runtime_spec, env_vars=request.env_vars,
        )

    async def query(self, instance_id: str) -> dict[str, Any]:
        return await self.backend.get_agent_info(instance_id)

    async def delete(self, instance_id: str) -> None:
        await self.backend.delete_sandbox(instance_id)

    @property
    def server_ready(self) -> bool:
        return self.backend.server_ready

    @property
    def agent_namespace(self) -> str:
        return self.backend.agent_namespace

    async def connect(self, uri: str) -> None:
        await self.backend.connect(uri)

    async def disconnect(self) -> None:
        await self.backend.disconnect()

    def set_or_update_server_config(self, *, config: dict[str, Any], env: dict[str, str] | None) -> None:
        self.backend.set_or_update_server_config(config=config, env=env)

    def set_server_push_handler(self, handler) -> None:
        setter = getattr(self.backend, "set_server_push_handler", None)
        if callable(setter):
            setter(handler)

    async def wait_until_running(self, instance_id: str, *, trace_id: str) -> dict[str, Any]:
        waiter = getattr(self.backend, "wait_until_running", None)
        if not callable(waiter):
            getter = getattr(self.backend, "get_agent_info", None)
            info = await getter(instance_id) if callable(getter) else {}
        else:
            try:
                info = await waiter(instance_id, trace_id=trace_id)
            except TypeError:
                info = await waiter(instance_id)
        return info if isinstance(info, dict) else {}

    async def verify_generation(self, sandbox: SandboxInfo, *, generation: str, deadline: float) -> None:
        await self.backend.wait_until_running(sandbox.sandbox_id, expected_generation=generation, deadline=deadline)
