# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Managed control adapter: existing platform operations and endpoint metadata."""

from __future__ import annotations

from urllib.parse import urlencode, urlsplit, urlunsplit

from agent_gateway.southbound.control.managed import ManagedSandboxControl
from agent_gateway.extensions.yuanrong_frontend_client import apply_trace_header
from agent_gateway.telemetry import inject_request_headers


class YuanRongControl(ManagedSandboxControl):
    def ws_headers(self) -> dict[str, str]:
        return inject_request_headers(apply_trace_header({}))

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
