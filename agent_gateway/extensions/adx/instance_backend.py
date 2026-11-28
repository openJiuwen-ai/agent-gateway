# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Worker-owned dynamic instances over the public ADX Environment lifecycle API.

The leaf composes the shared owner provider and sandbox lifecycle with the ADX
control plane. From its private spec it owns admission, the builtin launch
specification, registry publication and the cleanup of instances it created
itself. Request routing, credentials for the data connection and idle
reclamation stay with the caller.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from agent_gateway.common.agentos_runtime import is_agentos_runtime
from agent_gateway.common.instance_routes import InstanceProviderSpec
from agent_gateway.extensions.adx.config import AdxRouterConfig
from agent_gateway.extensions.adx.control_adapter import AdxControl
from agent_gateway.extensions.adx.control_client import AdxControlClient
from agent_gateway.extensions.adx.router_client import adx_sandbox_name
from agent_gateway.extensions.adx.workspace import check_workspace_config
from agent_gateway.extensions.agentos.agentos_router.backend import (
    AGENT_GENERATION_ENV,
    SandboxInfo,
    deterministic_agent_instance_id,
)
from agent_gateway.extensions.agentos.agentos_router.config import (
    TOOL_SANDBOX_CPU_ENV,
    TOOL_SANDBOX_IDLE_TIMEOUT_ENV,
    TOOL_SANDBOX_MEMORY_ENV,
)
from agent_gateway.extensions.agentos.agentos_router.models import AgentInfo, AgentStatus
from agent_gateway.extensions.agentos.agentos_router.registry_client import (
    RegistryClient,
    RegistryConfig,
)
from agent_gateway.southbound.control.managed import SandboxCreateRequest
from agent_gateway.southbound.orchestration.agent_manager import (
    BUILTIN_AGENT_TYPE,
    AgentCreateInterrupted,
    AgentManager,
    AgentPreCreateError,
    AgentRebuildPending,
)
from agent_gateway.southbound.orchestration.instance_lifecycle import (
    AgentOSInstanceLifecycle,
    AgentOSSandboxAttempt,
    AgentOSSandboxLifecycle,
)
from agent_gateway.southbound.orchestration.instance_provider import AgentOSInstanceProvider
from agent_gateway.southbound.orchestration.managed_router import (
    USER_DIRECTORY_ENV_KEY,
    resolve_agent_workspace,
    sandbox_name,
)


_DEFAULT_AGENT_PORT = 18092
# Placement is all the registry reads from the sandbox record. ADX instance views
# may also return Environment variables, which carry the node credential.
_PLACEMENT_FIELDS = ('node', 'address')


def _origin(url: str) -> tuple[str, str]:
    parsed = urlsplit(url)
    return parsed.scheme, parsed.netloc


class WorkerInstanceBackend:
    """Build the trusted local dynamic-instance leaf from a private spec."""

    def __init__(
        self, spec: InstanceProviderSpec, config: AdxRouterConfig, *, slot_id: int,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if type(spec) is not InstanceProviderSpec:
            raise ValueError('worker instance backend requires an InstanceProviderSpec')
        if type(config) is not AdxRouterConfig:
            raise ValueError('worker instance backend requires an AdxRouterConfig')
        if type(slot_id) is not int or slot_id not in spec.manifest.slots:
            raise ValueError('worker instance backend slot is not in the manifest')
        self._check_agreement(spec, config)
        self.spec = spec
        self.slot_id = slot_id
        self._config = config
        self._transport = transport
        self.manager: AgentManager | None = None
        self.registry: RegistryClient | None = None
        self.control: AdxControl | None = None
        self.client: AdxControlClient | None = None
        self._provider: AgentOSInstanceProvider | None = None
        self._control: AdxControl | None = None
        self._instance_lifecycle: AgentOSInstanceLifecycle | None = None
        self._lifecycles: dict[str, AgentOSSandboxLifecycle] = {}
        self._started = False
        self._closed = False

    @staticmethod
    def _check_agreement(spec: InstanceProviderSpec, config: AdxRouterConfig) -> None:
        """Refuse a spec that describes a deployment other than this Worker's.

        The spec arrives separately from the Worker configuration. Both must name
        the same ingress, namespace, managed workspaces and registry before the
        leaf can create or publish anything.
        """
        router = config.router

        def disagrees(field: str) -> ValueError:
            return ValueError(
                f'instance provider spec disagrees with the worker ADX configuration: {field}'
            )

        try:
            same_origin = _origin(spec.frontend_endpoint) == _origin(config.ingress_url)
            same_root = (
                Path(spec.workspace_root).expanduser().resolve()
                == Path(router.workspace_root).expanduser().resolve()
            )
        except (OSError, RuntimeError, ValueError):
            raise disagrees('workspace_root') from None
        if not same_origin:
            raise disagrees('frontend_endpoint')
        if spec.namespace != router.agent_namespace:
            raise disagrees('namespace')
        if not same_root:
            raise disagrees('workspace_root')
        if spec.registry_endpoint != router.registry.endpoint:
            raise disagrees('registry_endpoint')

    async def start(self) -> AgentOSInstanceProvider:
        if self._started or self._closed:
            raise RuntimeError('worker instance backend is not startable')
        client = AdxControlClient(self._config, transport=self._transport)
        registry = None
        try:
            control = AdxControl(client, self._config)
            registry = RegistryClient(RegistryConfig(endpoint=self.spec.registry_endpoint))
            provider = AgentOSInstanceProvider(
                AgentManager(
                    creating_timeout_seconds=self._config.router.creating_timeout_seconds,
                    key_fields=self._config.router.agent_key_fields,
                ),
                creator=self._create_agent,
                control=control,
                namespace=self.spec.namespace,
                identity_policy=self.spec.identity_policy,
                manifest=self.spec.manifest,
                slot_id=self.slot_id,
                admitted_users=self.spec.admitted_users,
            )
        except BaseException:
            try:
                if registry is not None:
                    await registry.close()
            finally:
                await client.close()
            raise
        self.client = client
        self.control = control
        self.manager = provider.manager
        self.registry = registry
        self._control = control
        self._instance_lifecycle = AgentOSInstanceLifecycle(
            control,
            self._config.router.probes,
            cleanup_step=lambda step, outcome, reason: None,
        )
        self._provider = provider
        self._started = True
        return provider

    async def close(self, *, deadline: float | None = None) -> None:
        """Stop local acquisitions and close local clients; never delete an Environment."""
        self._closed = True
        provider, registry, client = self._provider, self.registry, self.client
        self._provider = None
        self._instance_lifecycle = None
        self._control = None
        self.client = None
        self.control = None
        self.registry = None
        self.manager = None
        try:
            if provider is not None:
                await provider.close(deadline=deadline)
        finally:
            try:
                if registry is not None:
                    await registry.close()
            finally:
                if client is not None:
                    await client.close()

    async def _workspace(self, user_id: str) -> str:
        """Admit one user's workspace before any ADX request."""
        try:
            workspace = resolve_agent_workspace(user_id, workspace_root=self.spec.workspace_root)
            managed_root = Path(self.spec.workspace_root).expanduser().resolve()
            if Path(workspace).resolve().parent != managed_root:
                raise ValueError('agent workspace is not a direct managed child')
            await asyncio.to_thread(
                check_workspace_config,
                Path(workspace) / '.jiuwenswarm/config/config.yaml',
                require_prepared=is_agentos_runtime(),
            )
        except (ValueError, OSError, RuntimeError) as exc:
            # No Environment was requested. A later successful deployment can admit
            # this user without restarting the Worker.
            raise AgentPreCreateError(str(exc)) from exc
        return workspace

    async def _create_agent(self, agent_info: AgentInfo) -> AgentInfo:
        try:
            return await self._publish_agent(agent_info)
        except asyncio.CancelledError as exc:
            # Nothing was published, and every request this creator made keeps its
            # name. Discard the unpublished runtime so the next request replays the
            # retained attempt instead of inheriting a permanent failure.
            raise AgentCreateInterrupted from exc

    async def _publish_agent(self, agent_info: AgentInfo) -> AgentInfo:
        if agent_info.agent_type != BUILTIN_AGENT_TYPE:
            raise AgentPreCreateError('dynamic instance routes only support builtin jiuwenswarm')
        workspace = await self._workspace(agent_info.user_id)
        router = self._config.router
        control = self._control
        assert control is not None, 'worker instance backend is not started'
        port = self.spec.agent_port or _DEFAULT_AGENT_PORT
        runtime_spec: dict[str, Any] = {
            'sandbox_type': 'supervisor',
            'runtime': 'python3.11',
            'rootfs': {'imageurl': f'{BUILTIN_AGENT_TYPE}-agent-runtime:latest', 'user': 'agentos'},
            'cmds': [['sh', '-c', f'exec {BUILTIN_AGENT_TYPE}-agentserver --port {port}']],
            'probes': router.probes.tcp_probes(port, with_liveness=True),
            'cpu': self.spec.cpu or router.agent_sandbox_cpu,
            'memory': self.spec.memory or router.agent_sandbox_memory,
        }
        env_vars = {
            USER_DIRECTORY_ENV_KEY: workspace,
            'JIUWENSWARM_RUNTIME_PROFILE': 'agentos',
            TOOL_SANDBOX_IDLE_TIMEOUT_ENV: str(router.tool_sandbox_idle_timeout_seconds),
            TOOL_SANDBOX_CPU_ENV: str(router.tool_sandbox_cpu),
            TOOL_SANDBOX_MEMORY_ENV: str(router.tool_sandbox_memory),
        }

        lifecycle = self._lifecycles.setdefault(
            sandbox_name(agent_info.user_id, agent_info.agent_type), AgentOSSandboxLifecycle(),
        )
        attempt = lifecycle.attempt
        if attempt is None:
            # One generation owns one name. A retained attempt keeps its own request,
            # so ADX can only replay that name, never start a second Environment.
            generation = agent_info.agent_id
            physical_name = adx_sandbox_name(agent_info.user_id, agent_info.agent_type, generation)
            trace_id = str(agent_info.metadata.get('trace_id') or '').strip() or None

            async def create_sandbox_once() -> SandboxInfo:
                return await control.create(SandboxCreateRequest(
                    namespace=self.spec.namespace,
                    name=physical_name,
                    workspace=workspace,
                    runtime_spec=runtime_spec,
                    env_vars={**env_vars, AGENT_GENERATION_ENV: generation},
                    builtin=True,
                    web_port=None,
                    trace_id=trace_id,
                ))

            attempt = AgentOSSandboxAttempt(
                generation,
                deterministic_agent_instance_id(self.spec.namespace, physical_name),
                create_sandbox_once,
            )
        async with lifecycle.lock:
            lifecycle.attempt = attempt
            sandbox = await self._instance_lifecycle.provision_sandbox(lifecycle, attempt)
            lifecycle.provisioned_instance = sandbox.sandbox_id
            lifecycle.attempt = attempt

        ready_info = agent_info.copy()
        ready_info.status = AgentStatus.READY
        ready_info.sandbox_id = sandbox.sandbox_id
        ready_info.metadata.update(
            {
                'instance_id': sandbox.sandbox_id,
                'workspace': workspace,
                'runtime_spec': runtime_spec,
                'agent_port': port,
                'physical_generation': attempt.generation,
                'sandbox': {
                    key: sandbox.metadata[key] for key in _PLACEMENT_FIELDS if key in sandbox.metadata
                },
            }
        )
        registry = self.registry
        assert registry is not None, 'worker instance backend is not started'
        try:
            await registry.register_agent(ready_info.copy())
        except Exception as exc:
            raise AgentRebuildPending(
                f'AGENT_REBUILD_PENDING: registry registration failed: {exc}'
            ) from exc

        if attempt.generation != agent_info.agent_id:
            recovered = await self.manager.recover_creating_agent(
                agent_info.user_id,
                agent_info.agent_type,
                expected_agent_id=agent_info.agent_id,
                created=ready_info,
            )
            if recovered is None:
                raise AgentRebuildPending(
                    'AGENT_REBUILD_PENDING: manager runtime changed during recovery'
                )
            lifecycle.attempt = None
            raise AgentRebuildPending(
                'AGENT_REBUILD_PENDING: instance logical identity was recovered'
            )

        lifecycle.attempt = None
        agent_info.status = ready_info.status
        agent_info.sandbox_id = ready_info.sandbox_id
        agent_info.metadata.update(ready_info.metadata)
        return agent_info
