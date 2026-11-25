# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""ADX-specific capabilities composed with the shared managed router."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import aclosing

from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.models import E2AEnvelope

from agent_gateway.common.client.agent_client import AgentServerClient
from agent_gateway.extensions.adx.ssh_relay import AdxSshRelay
from agent_gateway.channel_manager.base import ChannelType
from agent_gateway.extensions.adx.config import AdxRouterConfig
from agent_gateway.extensions.adx.control_client import AdxControlClient
from agent_gateway.extensions.adx.control_adapter import AdxControl
from agent_gateway.extensions.adx.errors import AdxRoutingError
from agent_gateway.extensions.adx.file_client import AdxFileMixin
from agent_gateway.extensions.agentos.agentos_router.agentos_authenticator import AgentOSAuthenticator
from agent_gateway.extensions.agentos.agentos_router.registry_client import RegistryClient
from agent_gateway.extensions.auth.credential_authenticator import AuthResult
from agent_gateway.health_check import HEALTH_CHECK_CHANNEL_ID, HEALTH_CHECK_OK
from agent_gateway.southbound.orchestration.agent_manager import AgentManager
from agent_gateway.southbound.orchestration.managed_router import ManagedRouter


class AdxRouterClient(AdxFileMixin, AgentServerClient):
    """ADX platform entry point, with lifecycle and request coordination delegated."""

    def __init__(self, config: AdxRouterConfig, *, ws_client_factory: Callable | None = None,
                 control_client: AdxControlClient | None = None,
                 registry_client: RegistryClient | None = None,
                 auth_client: AgentOSAuthenticator | None = None) -> None:
        self._config = config
        self._control = control_client or AdxControlClient(config)
        settings = config.router
        self._router = ManagedRouter(
            AdxControl(self._control),
            registry_client or RegistryClient(settings.registry),
            AgentManager(
                creating_timeout_seconds=settings.creating_timeout_seconds, key_fields=settings.agent_key_fields,
            ),
            workspace_root=settings.workspace_root, agent_sandbox_cpu=settings.agent_sandbox_cpu,
            agent_sandbox_memory=settings.agent_sandbox_memory,
            tool_sandbox_idle_timeout_seconds=settings.tool_sandbox_idle_timeout_seconds,
            tool_sandbox_cpu=settings.tool_sandbox_cpu, tool_sandbox_memory=settings.tool_sandbox_memory,
            sandbox_idle_timeout_seconds=settings.sandbox_idle_timeout_seconds,
            sandbox_idle_check_interval_seconds=settings.sandbox_idle_check_interval_seconds,
            disconnect_cleanup_timeout_seconds=settings.disconnect_cleanup_timeout_seconds,
            connect_warmup_enabled=settings.connect_warmup_enabled,
            probe_settings=settings.probes, auth_client=auth_client,
            ssh_relay=AdxSshRelay(config), ssh_channel_endpoint=settings.ssh_channel,
            ws_client_factory=ws_client_factory,
            strict_ws_headers=True,
        )
        self._router._closed = True
        self.set_or_update_server_config(config={})

    def __getattr__(self, name):
        """Retain optional managed-client capabilities for existing callers."""
        return getattr(object.__getattribute__(self, "_router"), name)

    async def connect(self, uri: str) -> None:
        await self._router.connect(uri)

    async def disconnect(self) -> None:
        await self._router.disconnect()

    async def shutdown(self) -> None:
        await self._router.shutdown()
        await self._control.close()

    def set_channel_manager(self, channel_manager) -> None:
        self._router.set_channel_manager(channel_manager, authenticate_http=self.authenticate_http)
        channel = channel_manager.get_channel(ChannelType.WEB)
        if channel is not None:
            channel.container_file_client = self

    async def authenticate_http(self, *, path: str, headers: Mapping[str, str], remote: str = '',
                                channel: str = 'file-api', allow_query_token: bool = True) -> AuthResult:
        if channel == 'file-api':
            self._begin_file_transfer()
        return await self._router.authenticate_http(
            path=path, headers=headers, remote=remote, channel=channel,
            allow_query_token=allow_query_token,
        )

    def set_or_update_server_config(self, *, config: dict, env: dict | None = None) -> None:
        self._router.set_or_update_server_config(config=config, env=env)

    async def send_request(self, envelope: E2AEnvelope, *, timeout: float | None = None) -> AgentResponse:
        if envelope.channel == HEALTH_CHECK_CHANNEL_ID:
            if self._router._closed:
                raise AdxRoutingError('ADX router is closed', code='ADX_ROUTER_CLOSED')
            return AgentResponse(
                envelope.request_id, envelope.channel,
                payload={"health_check": f"{HEALTH_CHECK_OK}: mocked by adx_router"},
            )
        return await self._router.send_request(envelope, timeout=timeout)

    async def send_request_stream(self, envelope: E2AEnvelope) -> AsyncIterator[AgentResponseChunk]:
        async with aclosing(self._router.send_request_stream(envelope)) as stream:
            async for chunk in stream:
                yield chunk
