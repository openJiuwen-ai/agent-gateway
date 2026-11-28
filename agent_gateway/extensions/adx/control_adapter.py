# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""ADX Environment control and ingress endpoint metadata."""

import logging
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from agent_gateway.extensions.adx.config import AdxRouterConfig, is_loopback_ingress
from agent_gateway.southbound.control.managed import ManagedSandboxControl, SandboxCreateRequest, SandboxInfo
from agent_gateway.extensions.adx.control_client import AdxControlClient, AdxControlError, trace_headers
from agent_gateway.extensions.agentos.agentos_router.registry_client import RegistryClient


logger = logging.getLogger(__name__)


class AdxControl(ManagedSandboxControl):
    backend: AdxControlClient

    def __init__(self, backend: AdxControlClient, config: AdxRouterConfig | None = None) -> None:
        super().__init__(backend)
        self._config = config

    def _has_platform_recovery(self, runtime_spec: Mapping[str, Any], *, builtin: bool) -> bool:
        # The compatibility router also accepts the basic managed-sandbox backend.
        resolve_runtime = getattr(self.backend, "resolve_runtime_class", None)
        if resolve_runtime is None:
            return False
        return resolve_runtime(runtime_spec, builtin=builtin) == "runsc" or self.backend.config.restart_policy is not None

    async def find_reusable(
        self, request: SandboxCreateRequest, *, registry: RegistryClient, user: str, framework: str, logical_name: str,
    ) -> SandboxInfo | None:
        if not self._has_platform_recovery(request.runtime_spec, builtin=bool(request.builtin)):
            return None
        prefix = request.namespace + "-"
        candidates = {prefix + logical_name}
        records = await registry.list_instances(user=user, framework=framework, include_unhealthy=True)
        candidates.update(
            record.instance_id for record in records
            if record.user == user and record.framework == framework and record.instance_id.startswith(prefix)
        )
        available = []
        for instance in sorted(candidates):
            try:
                view = await self.backend.get_agent_info(instance, trace_id=request.trace_id)
            except AdxControlError as exc:
                if exc.code == "NOT_FOUND":
                    continue
                raise
            status = view.get("status")
            if status in ("running", "paused", "pausing", "resuming") or (status == "failed" and view.get("restart_pending")):
                available.append(instance)
            elif status not in ("failed", "deleted", "stopped"):
                raise AdxControlError("Existing ADX Environment is still transitioning", code="CONFLICT")
        if len(available) > 1:
            raise AdxControlError("Multiple native Environments match the Agent identity", code="CONFLICT")
        return await self.backend._ready_sandbox_info(available[0], trace_id=request.trace_id) if available else None

    async def retain_after_disconnect(
        self, instance_id: str, runtime_spec: Mapping[str, Any], *, builtin: bool,
    ) -> bool:
        if not self._has_platform_recovery(runtime_spec, builtin=builtin):
            return False
        try:
            view = await self.backend.get_agent_info(instance_id)
        except AdxControlError as exc:
            if exc.code == "NOT_FOUND":
                return False
            logger.warning("[ADX] retaining Environment after uncertain lifecycle lookup instance_id=%s", instance_id)
            return True
        if view.get("status") in ("failed", "deleted", "deleting", "stopped") and not view.get("restart_pending"):
            return False
        logger.info("[ADX] retaining Environment after transport failure instance_id=%s state=%s",
                    instance_id, view.get("status"))
        return True

    async def create(self, request: SandboxCreateRequest) -> SandboxInfo:
        if request.builtin is None and request.web_port is None:
            return await super().create(request)
        return await self.backend.create_sandbox(
            namespace=request.namespace, name=request.name, workspace=request.workspace,
            runtime_spec=request.runtime_spec, env_vars=request.env_vars,
            builtin=request.builtin, web_port=request.web_port,
            trace_id=request.trace_id,
        )

    async def verify_generation(self, sandbox: SandboxInfo, *, generation: str, deadline: float) -> None:
        await self.backend.wait_until_running(
            sandbox.sandbox_id, expected_generation=generation, deadline=deadline,
            confirmed_runtime_id=sandbox.metadata.get("runtime_id"),
        )

    def ws_headers(self, trace_id: str | None = None) -> dict[str, str]:
        config = self._config if self._config is not None else self.backend.config
        return trace_headers({'Authorization': 'Bearer ' + config.token}, trace_id)

    def _url(self, instance_id: str, port: int, protocol: str) -> str:
        origin = urlsplit(self.backend.frontend_endpoint)
        scheme = ('wss' if origin.scheme == 'https' else 'ws') if protocol == 'ws' else origin.scheme
        if is_loopback_ingress(origin.hostname):
            return urlunsplit((scheme, origin.netloc, f'/{quote(instance_id, safe="")}/{port}', '', ''))
        # ADX decodes the query value and the instance segment of its target URN separately.
        target = 'urn:adx:instance:' + quote(instance_id, safe='')
        return urlunsplit((scheme, origin.netloc,
                          f'/agent/{protocol}', urlencode({'target': target, 'port': port}), ''))

    def ws_url(self, instance_id: str, port: int) -> str:
        return self._url(instance_id, port, 'ws')

    def http_url(self, instance_id: str, port: int) -> str:
        return self._url(instance_id, port, 'http')

    def web_ws_url(self, instance_id: str, port: int) -> str:
        return self.ws_url(instance_id, port)
