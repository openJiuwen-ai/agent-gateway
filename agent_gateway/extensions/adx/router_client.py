# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Reuse Gateway business routing with ADX Environment lifecycle and forwarding."""
from __future__ import annotations

import asyncio
import ipaddress
import logging
from collections.abc import Callable, Mapping
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from agent_gateway.common.agentos_runtime import is_agentos_runtime
from e2a.agent_models import AgentResponse
from e2a.models import E2AEnvelope

from agent_gateway.extensions.adx.ssh_relay import AdxSshRelay
from agent_gateway.channel_manager.base import ChannelType
from agent_gateway.extensions.adx.config import AdxRouterConfig
from agent_gateway.extensions.adx.control_client import AdxControlClient
from agent_gateway.extensions.adx.errors import AdxRoutingError
from agent_gateway.extensions.adx.file_client import AdxFileMixin
from agent_gateway.extensions.agentos.agentos_router.agent_manager import (
    AgentPreCreateError,
    AgentManager,
    BUILTIN_AGENT_TYPE,
)
from agent_gateway.extensions.agentos.agentos_router.agentos_authenticator import (
    AgentOSAuthenticator,
)
from agent_gateway.extensions.agentos.agentos_router.registry_client import RegistryClient
from agent_gateway.extensions.agentos.agentos_router.models import AgentInfo
from agent_gateway.extensions.agentos.agentos_router.router_client import (
    AgentOSRouterClient,
    is_ws_connect_retryable,
    ws_connect_ready_timeout_seconds,
    ws_connect_retry_interval_seconds,
    resolve_agent_workspace,
    _sandbox_name,
)
from agent_gateway.extensions.auth.credential_authenticator import AuthResult
from agent_gateway.extensions.agentos.agentos_router.backend import apply_trace_header
from agent_gateway.health_check import HEALTH_CHECK_CHANNEL_ID, HEALTH_CHECK_OK


logger = logging.getLogger(__name__)


class AdxRouterClient(AdxFileMixin, AgentOSRouterClient):
    """Keep Registry, launch-spec assembly and user routing in the business Gateway."""

    def __init__(self, config: AdxRouterConfig, *, ws_client_factory: Callable | None = None,
                 control_client: AdxControlClient | None = None,
                 registry_client: RegistryClient | None = None,
                 auth_client: AgentOSAuthenticator | None = None) -> None:
        self._config = config
        host = urlsplit(config.ingress_url).hostname
        self._local_ingress = host == 'localhost'
        if host and not self._local_ingress:
            try:
                self._local_ingress = ipaddress.ip_address(host).is_loopback
            except ValueError:
                pass
        self._control = control_client or AdxControlClient(config)
        settings = config.router
        super().__init__(
            self._control, registry_client or RegistryClient(settings.registry),
            AgentManager(
                creating_timeout_seconds=settings.creating_timeout_seconds, key_fields=settings.agent_key_fields,
            ),
            workspace_root=settings.workspace_root,
            sandbox_idle_timeout_seconds=settings.sandbox_idle_timeout_seconds,
            agent_sandbox_cpu=settings.agent_sandbox_cpu,
            agent_sandbox_memory=settings.agent_sandbox_memory,
            tool_sandbox_idle_timeout_seconds=settings.tool_sandbox_idle_timeout_seconds,
            tool_sandbox_cpu=settings.tool_sandbox_cpu,
            tool_sandbox_memory=settings.tool_sandbox_memory,
            sandbox_idle_check_interval_seconds=settings.sandbox_idle_check_interval_seconds,
            disconnect_cleanup_timeout_seconds=settings.disconnect_cleanup_timeout_seconds,
            connect_warmup_enabled=settings.connect_warmup_enabled,
            probe_settings=settings.probes, auth_client=auth_client,
            ssh_relay=AdxSshRelay(config), ssh_channel_endpoint=settings.ssh_channel,
            ws_client_factory=ws_client_factory,
        )
        self._closed = True
        self.set_or_update_server_config(config={})

    async def shutdown(self) -> None:
        await super().shutdown()
        await self._control.close()

    def _sandbox_instance_id(self, namespace: str, name: str) -> str:
        return f'{namespace}-{name}'

    def _sandbox_physical_name(self, agent_info: AgentInfo) -> str:
        return f'{_sandbox_name(agent_info.user_id, agent_info.agent_type)}-{agent_info.agent_id[:12]}'

    async def _create_agent_impl(self, agent_info, operation):
        if agent_info.agent_type == BUILTIN_AGENT_TYPE:
            from pathlib import Path
            from agent_gateway.extensions.adx.workspace import check_workspace_config
            try:
                workspace = resolve_agent_workspace(agent_info.user_id, workspace_root=self._workspace_root)
                await asyncio.to_thread(check_workspace_config,
                    Path(workspace) / '.jiuwenswarm/config/config.yaml',
                    require_prepared=is_agentos_runtime())
            except ValueError as exc:
                # No Environment was requested. A later successful deployment
                # can admit this user without restarting Gateway or clearing caches.
                raise AgentPreCreateError(str(exc)) from exc
        return await super()._create_agent_impl(agent_info, operation)

    async def _create_sandbox_for_agent(self, agent_info, *, reuse_existing, web_port=None, **kwargs):
        builtin = agent_info.agent_type == BUILTIN_AGENT_TYPE
        return await self._control.create_sandbox(
            builtin=builtin, web_port=web_port if self._local_ingress else None, **kwargs)

    def set_channel_manager(self, channel_manager) -> None:
        super().set_channel_manager(channel_manager)
        channel = channel_manager.get_channel(ChannelType.WEB)
        if channel is not None:
            channel.container_file_client = self

    async def authenticate_http(self, *, path: str, headers: Mapping[str, str], remote: str = '',
                                channel: str = 'file-api', allow_query_token: bool = True) -> AuthResult:
        if channel == 'file-api':
            self._begin_file_transfer()
        return await super().authenticate_http(
            path=path, headers=headers, remote=remote, channel=channel,
            allow_query_token=allow_query_token,
        )

    def set_or_update_server_config(self, *, config: dict, env: dict | None = None) -> None:
        super().set_or_update_server_config(config=config, env=env)
        self._control.agent_idle_timeout_seconds = self._sandbox_idle_timeout_seconds

    def _agent_url(self, instance: str, port: int, protocol: str) -> str:
        origin = urlsplit(self._config.ingress_url)
        target = 'urn:adx:instance:' + quote(instance, safe='')
        scheme = ('wss' if origin.scheme == 'https' else 'ws') if protocol == 'ws' else origin.scheme
        return urlunsplit((scheme, origin.netloc,
                          f'/agent/{protocol}', urlencode({'target': target, 'port': port}), ''))

    def _port_url(self, instance_id: str, agent_port: int, protocol: str) -> str:
        if not self._local_ingress:
            return self._agent_url(instance_id, agent_port, protocol)
        origin = urlsplit(self._config.ingress_url)
        scheme = ('wss' if origin.scheme == 'https' else 'ws') if protocol == 'ws' else origin.scheme
        path = f'/{quote(instance_id, safe="")}/{agent_port}'
        return urlunsplit((scheme, origin.netloc, path, '', ''))

    def _agent_ws_url(self, instance_id: str, agent_port: int) -> str:
        return self._port_url(instance_id, agent_port, 'ws')

    def _agent_http_url(self, instance_id: str, agent_port: int) -> str:
        return self._port_url(instance_id, agent_port, 'http')

    def _agent_web_ws_url(self, instance_id: str, agent_port: int) -> str:
        return self._agent_ws_url(instance_id, agent_port)

    async def _connect_ws_until_ready(self, *, instance_id: str, agent_port: int,
                                      user_id: str = '', session_id: str = '', agent_type: str = ''):
        uri = self._agent_ws_url(instance_id, agent_port)
        deadline = asyncio.get_running_loop().time() + ws_connect_ready_timeout_seconds()
        headers = apply_trace_header({'Authorization': 'Bearer ' + self._config.token})
        while True:
            client = self._ws_client_factory()
            if self._push_handler is not None:
                client.set_server_push_handler(self._push_handler)
            try:
                await client.connect(uri, extra_headers=headers)
                self._register_ws_disconnect_handler(client, user_id=user_id, session_id=session_id,
                                                     agent_type=agent_type, instance_id=instance_id)
                return client
            except asyncio.CancelledError:
                try:
                    await asyncio.shield(client.disconnect())
                except BaseException:
                    logger.warning('ADX WebSocket cancellation cleanup failed', exc_info=True)
                raise
            except Exception as exc:
                try:
                    await client.disconnect()
                except Exception:
                    logger.warning('ADX WebSocket cleanup failed', exc_info=True)
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0 or not is_ws_connect_retryable(exc):
                    raise
                await asyncio.sleep(min(ws_connect_retry_interval_seconds(), remaining))

    async def send_request(self, envelope: E2AEnvelope, *, timeout: float | None = None) -> AgentResponse:
        if envelope.channel == HEALTH_CHECK_CHANNEL_ID:
            if self._closed:
                raise AdxRoutingError('ADX router is closed', code='ADX_ROUTER_CLOSED')
            return AgentResponse(
                envelope.request_id, envelope.channel,
                payload={"health_check": f"{HEALTH_CHECK_OK}: mocked by ADX backend"},
            )
        return await super().send_request(envelope, timeout=timeout)
