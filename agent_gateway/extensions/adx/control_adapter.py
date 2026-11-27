# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""ADX Environment control and ingress endpoint metadata."""

from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from agent_gateway.extensions.adx.config import AdxRouterConfig
from agent_gateway.southbound.control.managed import ManagedSandboxControl, SandboxCreateRequest, SandboxInfo
from agent_gateway.extensions.adx.control_client import AdxControlClient
from agent_gateway.extensions.yuanrong_frontend_client import apply_trace_header


class AdxControl(ManagedSandboxControl):
    backend: AdxControlClient

    def __init__(self, backend: AdxControlClient, config: AdxRouterConfig | None = None) -> None:
        super().__init__(backend)
        self._config = config

    async def create(self, request: SandboxCreateRequest) -> SandboxInfo:
        if request.builtin is None and request.web_port is None:
            return await super().create(request)
        return await self.backend.create_sandbox(
            namespace=request.namespace, name=request.name, workspace=request.workspace,
            runtime_spec=request.runtime_spec, env_vars=request.env_vars,
            builtin=request.builtin, web_port=request.web_port,
        )

    async def verify_generation(self, sandbox: SandboxInfo, *, generation: str, deadline: float) -> None:
        await self.backend.wait_until_running(
            sandbox.sandbox_id, expected_generation=generation, deadline=deadline,
            confirmed_runtime_id=sandbox.metadata.get("runtime_id"),
        )

    def ws_headers(self) -> dict[str, str]:
        config = self._config if self._config is not None else self.backend.config
        return apply_trace_header({'Authorization': 'Bearer ' + config.token})

    def _url(self, instance_id: str, port: int, protocol: str) -> str:
        origin = urlsplit(self.backend.frontend_endpoint)
        target = 'urn:adx:instance:' + quote(instance_id, safe='')
        return urlunsplit(('wss' if protocol == 'ws' else 'https', origin.netloc,
                          f'/agent/{protocol}', urlencode({'target': target, 'port': port}), ''))

    def ws_url(self, instance_id: str, port: int) -> str:
        return self._url(instance_id, port, 'ws')

    def http_url(self, instance_id: str, port: int) -> str:
        return self._url(instance_id, port, 'http')

    def web_ws_url(self, instance_id: str, port: int) -> str:
        return self.ws_url(instance_id, port)
