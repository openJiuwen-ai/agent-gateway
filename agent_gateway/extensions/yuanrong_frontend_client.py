# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""YuanrongFrontendAgentClient - openYuanRong Frontend HTTP 客户端.

经 POST/DELETE/GET /api/agent 管理常驻 agent 实例
（create_sandbox / delete_sandbox / get_agent_info / wait_until_running），
并提供 /api/agent/:id/files/* 文件接口（upload/download/list/mkdir）。
消息数据面由 AgentOS Router 经 frontend WS 代理直连实例，不经本客户端。
"""

from __future__ import annotations

import asyncio
import contextvars
import http.client
import json
import logging
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import nullcontext
from dataclasses import dataclass, field
from typing import Any, Mapping, TypedDict

TRACE_ID_HEADER = "X-Trace-Id"
AGENT_GENERATION_ENV = "JIUWEN_GATEWAY_SANDBOX_GENERATION"
_TRACE_ID_MAX_LEN = 128
_southbound_trace_id: contextvars.ContextVar[str] = contextvars.ContextVar(
    "yuanrong_southbound_trace_id", default=""
)


def deterministic_agent_instance_id(namespace: str, name: str) -> str:
    """Return the frontend's deterministic instance ID for a namespace and name."""
    normalized_namespace = str(namespace or "").strip()
    normalized_name = str(name or "").strip()
    if not normalized_namespace or not normalized_name:
        raise ValueError("namespace and name are required to create sandbox")
    canonical = (
        f"yuanrong-agent-instance:v1|{len(normalized_namespace.encode('utf-8'))}:"
        f"{normalized_namespace}{len(normalized_name.encode('utf-8'))}:{normalized_name}"
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, canonical))


def normalize_trace_id(value: str | None = None) -> str:
    """Use the caller value, or mint a UUID (frontend also caps at 128 chars)."""
    text = str(value or "").strip()
    if not text:
        text = str(uuid.uuid4())
    return text[:_TRACE_ID_MAX_LEN]


def bind_southbound_trace_id(value: str | None = None) -> str:
    """Normalize, remember on this task, and return the id sent southbound."""
    resolved = normalize_trace_id(value)
    _southbound_trace_id.set(resolved)
    return resolved


def current_southbound_trace_id() -> str:
    """Last southbound mint in this task; empty if YuanRong was not called."""
    return str(_southbound_trace_id.get() or "").strip()


def clear_southbound_trace_id() -> None:
    """Drop the task-local id so a later 401 cannot inherit a previous call."""
    _southbound_trace_id.set("")


def extract_trace_id(headers: Mapping[str, Any] | None) -> str:
    """Read ``X-Trace-Id`` from headers (any case). Empty when absent."""
    if not headers:
        return ""
    for key, raw in headers.items():
        if str(key).lower() == "x-trace-id":
            return str(raw or "").strip()
    return ""


def apply_trace_header(
    headers: Mapping[str, str] | None,
    trace_id: str | None = None,
) -> dict[str, str]:
    """Set canonical ``X-Trace-Id``; prefer explicit, then existing header, then mint."""
    merged = {str(key): str(value) for key, value in dict(headers or {}).items()}
    resolved = bind_southbound_trace_id(trace_id or extract_trace_id(merged))
    return {
        key: value
        for key, value in merged.items()
        if str(key).lower() != "x-trace-id"
    } | {TRACE_ID_HEADER: resolved}

from agent_gateway.routing.container_files import AgentFileDownloadChunk
from agent_gateway.telemetry import (
    current_request_trace,
    inject_request_headers,
    record_cleanup_result,
    trace_operation,
)

logger = logging.getLogger(__name__)

# YuanRong POST /files/mkdir ``mode``: 3-4 octal digits (e.g. 755 / 0700).
_MKDIR_MODE_RE = re.compile(r"^[0-7]{3,4}$")

# POST /api/agent is async: create returns instance_id before startup probes
# succeed. GET /api/agent/:id reports status; ``running`` means the probed
# port is accepting connections (then WS/SSH may connect).
_AGENT_RUNNING_STATUS = "running"
_AGENT_RUNNING_TIMEOUT_SECONDS = 90.0
_AGENT_RUNNING_RETRY_INTERVAL_SECONDS = 1.0
_AGENT_FAILED_STATUSES = frozenset(
    {"failed", "error", "deleted", "stopped", "killed"}
)


@dataclass(frozen=True)
class RuntimeProbeSettings:
    """TCP probe timings and GET wait knobs (YuanRong camelCase).

    Startup/liveness override via ``gateway.agentos.probes`` /
    ``AGENTOS_PROBE_*``. GET wait override via
    ``gateway.agentos.wait_running_{timeout,interval}_seconds`` /
    ``AGENTOS_WAIT_RUNNING_*`` (see ``load_router_config``).
    """

    startup_initial_delay_seconds: int = 3
    startup_period_seconds: int = 3
    startup_timeout_seconds: int = 2
    startup_failure_threshold: int = 6
    liveness_timeout_seconds: int = 2
    liveness_failure_threshold: int = 3
    wait_running_timeout_seconds: float = 90.0
    wait_running_interval_seconds: float = 1.0

    def startup_budget_seconds(self) -> float:
        """Worst-case startup TCP probe window before ``status=running``.

        ``initialDelaySeconds + periodSeconds * failureThreshold``.
        Liveness runs after the instance is already running and is not part
        of the GET wait.
        """
        return float(
            int(self.startup_initial_delay_seconds)
            + int(self.startup_period_seconds)
            * int(self.startup_failure_threshold)
        )

    def same_tcp_timings(self, other: RuntimeProbeSettings) -> bool:
        """True when startup/liveness knobs match (GET wait knobs ignored)."""
        return (
            int(self.startup_initial_delay_seconds)
            == int(other.startup_initial_delay_seconds)
            and int(self.startup_period_seconds)
            == int(other.startup_period_seconds)
            and int(self.startup_timeout_seconds)
            == int(other.startup_timeout_seconds)
            and int(self.startup_failure_threshold)
            == int(other.startup_failure_threshold)
            and int(self.liveness_timeout_seconds)
            == int(other.liveness_timeout_seconds)
            and int(self.liveness_failure_threshold)
            == int(other.liveness_failure_threshold)
        )

    def startup_tcp(self, port: int) -> dict[str, Any]:
        return {
            "tcpSocket": {"port": int(port)},
            "initialDelaySeconds": int(self.startup_initial_delay_seconds),
            "periodSeconds": int(self.startup_period_seconds),
            "timeoutSeconds": int(self.startup_timeout_seconds),
            "failureThreshold": int(self.startup_failure_threshold),
        }

    def liveness_tcp(self, port: int) -> dict[str, Any]:
        return {
            "tcpSocket": {"port": int(port)},
            "timeoutSeconds": int(self.liveness_timeout_seconds),
            "failureThreshold": int(self.liveness_failure_threshold),
        }

    def tcp_probes(self, port: int, *, with_liveness: bool = False) -> dict[str, Any]:
        probes: dict[str, Any] = {"startup": self.startup_tcp(port)}
        if with_liveness:
            probes["liveness"] = self.liveness_tcp(port)
        return probes


DEFAULT_RUNTIME_PROBE_SETTINGS = RuntimeProbeSettings()
DEFAULT_THIRD_AGENT_PROBE_SETTINGS = RuntimeProbeSettings(
    startup_initial_delay_seconds=2,
    startup_period_seconds=3,
    startup_timeout_seconds=2,
    startup_failure_threshold=8,
    liveness_timeout_seconds=2,
    liveness_failure_threshold=3,
)


def _normalize_port_probe(value: Any) -> dict[str, Any] | None:
    """Normalize a TCP port label to ``{"port": int, "protocol": "tcp"}``."""
    if value is None or value is False:
        return None
    if isinstance(value, Mapping):
        raw_port = value.get("port")
        if raw_port is None and isinstance(value.get("tcpSocket"), Mapping):
            raw_port = value["tcpSocket"].get("port")
        protocol = str(value.get("protocol") or "tcp").strip().lower() or "tcp"
        try:
            port = int(raw_port)
        except (TypeError, ValueError):
            return None
        if port <= 0:
            return None
        return {"port": port, "protocol": protocol}
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        if value <= 0:
            return None
        return {"port": int(value), "protocol": "tcp"}
    text = str(value).strip()
    if not text:
        return None
    protocol = "tcp"
    port_str = text
    if ":" in text:
        proto, port_str = text.split(":", 1)
        protocol = proto.strip().lower() or "tcp"
        port_str = port_str.strip()
    try:
        port = int(port_str)
    except ValueError:
        return None
    if port <= 0:
        return None
    return {"port": port, "protocol": protocol}


def _port_probe_from_ports(ports: Any) -> dict[str, Any] | None:
    """Build a port label from ``rootfs.ports`` such as ``tcp:18092``."""
    if not isinstance(ports, list) or not ports:
        return None
    return _normalize_port_probe(ports[0])




def _normalize_probes(value: Any) -> dict[str, Any] | None:
    """Keep ``startup`` / ``liveness`` / ``readiness`` objects if present."""
    if not isinstance(value, Mapping) or not value:
        return None
    probes: dict[str, Any] = {}
    for key in ("startup", "liveness", "readiness"):
        item = value.get(key)
        if isinstance(item, Mapping) and item:
            probes[key] = dict(item)
    return probes or None


def _probes_from_port(
    port: int,
    *,
    with_liveness: bool = False,
    settings: RuntimeProbeSettings | None = None,
) -> dict[str, Any]:
    return (settings or DEFAULT_RUNTIME_PROBE_SETTINGS).tcp_probes(
        port, with_liveness=with_liveness
    )


def _probes_from_ports(
    ports: Any, settings: RuntimeProbeSettings | None = None
) -> dict[str, Any] | None:
    """Startup TCP probe from ``rootfs.ports`` (e.g. ``tcp:22``)."""
    probe = _port_probe_from_ports(ports)
    if probe is None:
        return None
    return _probes_from_port(int(probe["port"]), settings=settings)


def _instance_status(instance: Mapping[str, Any] | None) -> str:
    if not isinstance(instance, Mapping):
        return ""
    return str(instance.get("status") or instance.get("state") or "").strip().lower()


def _is_agent_running(instance: Mapping[str, Any] | None) -> bool:
    """True only when GET explicitly reports ``status=running``.

    A non-empty instance without ``status`` (partial body with only
    ``instance_id`` / ``node_ip``, or a Legacy GET that omitted the field)
    is **not** ready. ``wait_until_running`` keeps polling until status is
    ``running``, a failed status, or timeout — otherwise a mid-probe GET
    would skip the startup probe wait and WS/SSH would connect too early.
    """
    if not isinstance(instance, Mapping) or not instance:
        return False
    return _instance_status(instance) == _AGENT_RUNNING_STATUS


# Placement / status fields YuanRong may put on ``instance`` or at the top
# level (legacy GET, jiuwenbox ``ip_address`` passthrough).
_AGENT_INSTANCE_PROMOTE_KEYS = (
    "status",
    "state",
    "node_ip",
    "nodeIp",
    "sandbox_ip",
    "sandboxIp",
    "ip_address",
    "ipAddress",
    "env_vars",
)


def _merge_agent_instance_payload(parsed: Mapping[str, Any]) -> dict[str, Any]:
    """Flatten GET /api/agent payload into the instance dict used by callers.

    Nested ``instance`` values win; missing placement/status fields are
    copied from the top-level body so registry PATCH can see sandbox IP
    even when YuanRong/jiuwenbox emit them outside ``instance``.
    """
    raw = parsed.get("instance")
    instance = dict(raw) if isinstance(raw, dict) else {}
    for key in _AGENT_INSTANCE_PROMOTE_KEYS:
        if str(instance.get(key) or "").strip():
            continue
        value = parsed.get(key)
        if value is not None and str(value).strip():
            instance[key] = value
    return instance


class AgentMount(TypedDict, total=False):
    """Bind mount for POST /api/agent ``mounts``."""

    source: str
    target: str
    readonly: bool


class AgentRootfsSpec(TypedDict, total=False):
    """Inline ``runtime_spec.rootfs`` for POST /api/agent."""

    imageurl: str
    user: str
    ports: list[str]


class AgentRuntimeSpec(TypedDict, total=False):
    """Inline ``runtime_spec`` for POST /api/agent (bypass meta_service)."""

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
    """YuanRong agent instance lifecycle record returned by /api/agent."""

    sandbox_id: str
    status: str = "ready"
    metadata: dict[str, Any] = field(default_factory=dict)


class YuanrongAgentApiError(RuntimeError):
    """Raised when YuanRong /api/agent returns a non-success response."""


class YuanrongAgentRequestNotSentError(YuanrongAgentApiError):
    """The request could not be dispatched because connection setup failed."""


class YuanrongAgentDeleteRejectedError(YuanrongAgentApiError):
    """The frontend explicitly rejected a delete request."""


class YuanrongAgentCreateUncertainError(YuanrongAgentApiError):
    """A create response does not establish whether an instance was created."""


class YuanrongAgentTimeoutError(YuanrongAgentApiError):
    """YuanRong did not respond in time. A write may already have taken effect."""


class YuanrongAgentNotVisibleError(YuanrongAgentTimeoutError):
    """Every completed observation during a guarded wait reported absence."""


class YuanrongAgentNotFoundError(YuanrongAgentApiError):
    """GET explicitly reports the instance as not found or not running."""


class YuanrongAgentGenerationMismatchError(YuanrongAgentApiError):
    """GET returned an instance belonging to a different generation."""


class YuanrongAgentFailedError(YuanrongAgentApiError):
    """The requested generation reached a terminal failure status."""


@dataclass(frozen=True)
class _AgentFileRequest:
    """Shared identity/auth for agent file HTTP calls (G.FNM.03)."""

    instance_id: str
    path: str
    auth_headers: dict[str, str]
    trace_id: str | None = None


class YuanrongAgentFileError(RuntimeError):
    """Raised when YuanRong agent file upload/download/list/mkdir fails."""

    def __init__(self, message: str, *, http_status: int = 500, error_code: str = "INTERNAL_ERROR") -> None:
        super().__init__(message)
        self.http_status = int(http_status)
        self.error_code = str(error_code)


class YuanrongFrontendAgentClient:
    """openYuanRong Frontend HTTP 客户端.

    经 /api/agent 管理常驻 agent 实例（create_sandbox / delete_sandbox /
    get_agent_info / wait_until_running），并提供 /api/agent/:id/files/*
    文件接口。消息数据面由 AgentOS Router 经 frontend WS 代理直连实例，
    不经本客户端（历史 FaaS invoke 链路已删除）。
    """

    def __init__(
        self,
        *,
        frontend_endpoint: str,
        function_version_urn: str,
        agent_timeout_s: float = 300.0,
        agent_namespace: str = "default",
        probe_settings: RuntimeProbeSettings | None = None,
        wait_running_timeout_s: float | None = None,
        wait_running_interval_s: float | None = None,
    ) -> None:
        self._frontend_endpoint = (frontend_endpoint or "").rstrip("/")
        self._function_version_urn = (function_version_urn or "").strip()
        self._agent_timeout_s = float(agent_timeout_s)
        self._agent_namespace = str(agent_namespace or "default").strip() or "default"
        self._probe_settings = probe_settings or DEFAULT_RUNTIME_PROBE_SETTINGS
        # None keeps reading the module constants at call time (unit tests
        # monkeypatch ``_AGENT_RUNNING_TIMEOUT_SECONDS``).
        self._wait_running_timeout_s = wait_running_timeout_s
        self._wait_running_interval_s = wait_running_interval_s
        self._connected = False
        self._server_ready = False

    @property
    def function_version_urn(self) -> str:
        return self._function_version_urn

    @property
    def agent_namespace(self) -> str:
        return self._agent_namespace

    @property
    def frontend_endpoint(self) -> str:
        return self._frontend_endpoint

    def set_or_update_server_config(
        self,
        *,
        config: dict[str, Any],
        env: dict[str, str] | None = None,
    ) -> None:
        return None

    @property
    def server_ready(self) -> bool:
        return self._server_ready

    async def connect(self, uri: str) -> None:
        endpoint = (uri or "").strip()
        if endpoint and endpoint.lower().startswith(("http://", "https://")):
            self._frontend_endpoint = endpoint.rstrip("/")
        if not self._frontend_endpoint:
            raise ValueError("frontend_endpoint cannot be empty")
        if not self._function_version_urn:
            raise ValueError("function_version_urn cannot be empty")
        self._connected = True
        self._server_ready = True
        logger.info(
            "[YuanrontFrontendAgentClient] connected: endpoint=%s",
            self._frontend_endpoint,
        )

    async def disconnect(self) -> None:
        self._connected = False
        self._server_ready = False
        logger.info("[YuanrongFrontendAgentClient] disconnected")

    def _normalize_runtime_spec(
        self,
        runtime_spec: AgentRuntimeSpec | Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        """Normalize inline ``runtime_spec`` for POST /api/agent."""
        if not isinstance(runtime_spec, Mapping):
            raise ValueError("runtime_spec is required to create sandbox")
        runtime = str(runtime_spec.get("runtime") or "").strip()
        rootfs_raw = runtime_spec.get("rootfs")
        if not isinstance(rootfs_raw, Mapping):
            raise ValueError("runtime_spec.rootfs is required to create sandbox")
        imageurl = str(
            rootfs_raw.get("imageurl") or rootfs_raw.get("image_url") or ""
        ).strip()
        if not runtime:
            raise ValueError("runtime_spec.runtime is required to create sandbox")
        if not imageurl:
            raise ValueError(
                "runtime_spec.rootfs.imageurl is required to create sandbox"
            )

        rootfs: dict[str, Any] = {"imageurl": imageurl}
        user = str(rootfs_raw.get("user") or "").strip()
        if user:
            rootfs["user"] = user
        ports = rootfs_raw.get("ports")
        if isinstance(ports, list) and ports:
            rootfs["ports"] = [str(port) for port in ports]

        normalized: dict[str, Any] = {"runtime": runtime, "rootfs": rootfs}
        sandbox_type = str(runtime_spec.get("sandbox_type") or "").strip()
        if sandbox_type:
            normalized["sandbox_type"] = sandbox_type
        if runtime_spec.get("cpu") is not None:
            normalized["cpu"] = int(runtime_spec["cpu"])
        if runtime_spec.get("memory") is not None:
            normalized["memory"] = int(runtime_spec["memory"])
        cmds = runtime_spec.get("cmds")
        if isinstance(cmds, list) and cmds:
            normalized["cmds"] = cmds
        probes = _normalize_probes(runtime_spec.get("probes"))
        if probes is None:
            legacy = _normalize_port_probe(
                runtime_spec.get("port_probe")
            ) or _port_probe_from_ports(rootfs.get("ports"))
            if legacy is not None:
                probes = _probes_from_port(
                    int(legacy["port"]), settings=self._probe_settings
                )
        if probes is not None:
            normalized["probes"] = probes
        return normalized

    async def create_sandbox(
        self,
        *,
        namespace: str,
        name: str,
        workspace: str,
        runtime_spec: AgentRuntimeSpec | Mapping[str, Any],
        env_vars: dict[str, str] | None = None,
        mounts: list[AgentMount] | None = None,
        trace_id: str | None = None,
    ) -> SandboxInfo:
        """Create a detached agent instance via POST /api/agent (inline mode).

        Mirrors Frontend ``CreateAgentRequest`` inline path:

        - ``namespace`` / ``name`` / ``workspace`` / ``runtime_spec``: required
        - ``runtime_spec.runtime`` + ``runtime_spec.rootfs.imageurl``: required
        - ``env_vars`` / ``mounts``: optional
        - ``runtime_spec.probes``: optional startup / liveness probes
          (Frontend marks ``running`` after startup succeeds)
        - does not send ``urn`` (inline takes priority over registered)
        """
        self._ensure_connected()
        normalized_namespace = str(namespace or "").strip()
        normalized_name = str(name or "").strip()
        normalized_workspace = str(workspace or "").strip()
        if not normalized_namespace:
            raise ValueError("namespace is required to create sandbox")
        if not normalized_name:
            raise ValueError("name is required to create sandbox")
        if not normalized_workspace:
            raise ValueError("workspace is required to create sandbox")
        if not normalized_workspace.startswith("/"):
            raise ValueError("workspace must be an absolute path")

        normalized_runtime_spec = self._normalize_runtime_spec(runtime_spec)
        payload: dict[str, Any] = {
            "namespace": normalized_namespace,
            "name": normalized_name,
            "workspace": normalized_workspace,
            "runtime_spec": normalized_runtime_spec,
        }

        if env_vars:
            payload["env_vars"] = {
                str(key): str(value) for key, value in dict(env_vars).items()
            }

        if mounts:
            payload["mounts"] = list(mounts)

        resolved_trace_id = bind_southbound_trace_id(
            inject_request_headers({}).get(TRACE_ID_HEADER) or trace_id
        )
        with (trace_operation("gw.frontend.create", attributes={"http.request.method": "POST"})
              if current_request_trace() is not None else nullcontext(None)) as operation:
            status, body = await asyncio.to_thread(
                self._do_agent_create, payload, resolved_trace_id
            )
            if operation is not None:
                operation.set_attribute("http.response.status_code", status)
            if status == 408 or status >= 500:
                raise YuanrongAgentCreateUncertainError(
                    f"create agent result uncertain: http_status={status}"
                )
            if 200 <= status < 300:
                try:
                    parsed = self._parse_agent_api_response(body, status)
                except YuanrongAgentApiError:
                    raise YuanrongAgentCreateUncertainError(
                        f"create agent returned an invalid response: http_status={status}"
                    ) from None
            else:
                parsed = self._parse_agent_api_response(body, status)
            raw_instance_id = parsed.get("instance_id")
            instance_id = raw_instance_id.strip() if isinstance(raw_instance_id, str) else ""
            if not instance_id:
                raise YuanrongAgentCreateUncertainError(
                    f"create agent returned an invalid instance_id: http_status={status}"
                )

        info = SandboxInfo(
            sandbox_id=instance_id,
            status="ready",
            metadata={
                "instance_id": instance_id,
                "namespace": normalized_namespace,
                "name": normalized_name,
                "workspace": normalized_workspace,
                "runtime_spec": dict(normalized_runtime_spec),
                "env_vars": dict(payload.get("env_vars") or {}),
                "mounts": list(payload.get("mounts") or []),
                "provisioning": "yuanrong_agent_api_inline",
                "trace_id": resolved_trace_id,
            },
        )
        logger.info(
            "[YuanrongFrontendAgentClient] create_sandbox: "
            "instance_id=%s name=%s namespace=%s runtime=%s imageurl=%s "
            "trace_id=%s",
            instance_id,
            normalized_name,
            normalized_namespace,
            normalized_runtime_spec.get("runtime"),
            (normalized_runtime_spec.get("rootfs") or {}).get("imageurl"),
            resolved_trace_id,
        )
        return info

    async def delete_sandbox(
        self, sandbox_id: str, *, trace_id: str | None = None
    ) -> None:
        """Destroy a detached agent instance via DELETE /api/agent/:instanceId."""
        self._ensure_connected()
        normalized_sandbox_id = str(sandbox_id or "").strip()
        if not normalized_sandbox_id:
            raise ValueError("sandbox_id is required to delete sandbox")

        resolved_trace_id = bind_southbound_trace_id(
            inject_request_headers({}).get(TRACE_ID_HEADER) or trace_id
        )
        try:
            with (trace_operation("gw.frontend.delete", attributes={"http.request.method": "DELETE"})
                  if current_request_trace() is not None else nullcontext(None)) as operation:
                status, body = await asyncio.to_thread(
                    self._do_agent_delete,
                    normalized_sandbox_id,
                    resolved_trace_id,
                )
                if operation is not None:
                    operation.set_attribute("http.response.status_code", status)
                if self._agent_instance_not_found(status, body, normalized_sandbox_id):
                    if operation is not None:
                        operation.record_outcome("skipped", reason="not_found")
                    record_cleanup_result("frontend.delete", "skipped", reason="not_found")
                    logger.info(
                        "[YuanrongFrontendAgentClient] delete_sandbox: already gone "
                        "instance_id=%s trace_id=%s",
                        normalized_sandbox_id,
                        resolved_trace_id,
                    )
                    return
                if 400 <= status < 500 and status != 408:
                    raise YuanrongAgentDeleteRejectedError(
                        f"delete agent rejected: http_status={status}"
                    )
                self._parse_agent_api_response(body, status)
        except asyncio.CancelledError:
            record_cleanup_result("frontend.delete", "cancelled", reason="cancelled")
            raise
        except Exception as exc:
            outcome = "timeout" if self._is_timeout_error(exc) else "failed"
            record_cleanup_result("frontend.delete", outcome, reason=type(exc).__name__)
            raise
        record_cleanup_result("frontend.delete", "success", reason="accepted")
        logger.info(
            "[YuanrongFrontendAgentClient] delete_sandbox: instance_id=%s trace_id=%s",
            normalized_sandbox_id,
            resolved_trace_id,
        )

    async def get_agent_info(
        self, instance_id: str, *, trace_id: str | None = None
    ) -> dict[str, Any]:
        """Query agent instance info via GET /api/agent/:instanceId.

        Returns the ``instance`` dict (contains node_ip, sandbox_ip,
        sandbox_type, rootfs, workspace, env_vars, status, etc.).
        """
        self._ensure_connected()
        normalized_id = str(instance_id or "").strip()
        if not normalized_id:
            raise ValueError("instance_id is required to get agent info")
        sdk_trace_id = inject_request_headers({}).get(TRACE_ID_HEADER)
        if sdk_trace_id:
            trace_id = bind_southbound_trace_id(sdk_trace_id)
        with (
            trace_operation(
                "gw.frontend.get",
                attributes={"instance_id": normalized_id, "http.request.method": "GET"},
            )
            if current_request_trace() is not None else nullcontext(None)
        ) as operation:
            status, body = await asyncio.to_thread(
                self._do_agent_get, normalized_id, trace_id
            )
            if operation is not None:
                operation.set_attribute("http.response.status_code", status)
            if self._agent_instance_not_found(status, body, normalized_id):
                raise YuanrongAgentNotFoundError(
                    f"agent instance not found: instance_id={normalized_id}"
                )
            parsed = self._parse_agent_api_response(body, status)
            return _merge_agent_instance_payload(parsed)

    async def wait_until_running(
        self, instance_id: str, *, trace_id: str | None = None,
        expected_generation: str | None = None,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        """Poll GET /api/agent/:id until ``status`` is explicitly ``running``.

        Create is asynchronous: the port probe completes after POST returns
        ``instance_id``. Connect WS/SSH only after this method succeeds.

        Missing ``status`` is not treated as ready: keep polling. Only an
        explicit ``running`` (or a failed status / timeout) ends the loop.
        When a generation is expected, only its matching marker can satisfy
        the status check. ``deadline`` is an event-loop monotonic timestamp.
        """
        normalized_id = str(instance_id or "").strip()
        if not normalized_id:
            raise ValueError("instance_id is required to wait for running")
        timeout_s = (
            _AGENT_RUNNING_TIMEOUT_SECONDS
            if self._wait_running_timeout_s is None
            else float(self._wait_running_timeout_s)
        )
        interval_s = (
            _AGENT_RUNNING_RETRY_INTERVAL_SECONDS
            if self._wait_running_interval_s is None
            else float(self._wait_running_interval_s)
        )
        with (trace_operation("gw.frontend.wait_running", attributes={"instance_id": normalized_id})
              if current_request_trace() is not None else nullcontext(None)) as operation:
            loop = asyncio.get_running_loop()
            wait_deadline = loop.time() + timeout_s
            if deadline is not None:
                wait_deadline = min(deadline, wait_deadline)
            attempt = 0
            last_error: BaseException | None = None
            last_status = ""
            not_found_count = 0
            all_observations_not_found = True
            marker_missing_logged = False
            # Only GET poll retries share one id; every other YuanRong call mints.
            poll_trace_id = bind_southbound_trace_id(
                inject_request_headers({}).get(TRACE_ID_HEADER) or trace_id
            )
            def raise_wait_timeout() -> None:
                if operation is not None:
                    operation.record_outcome("timeout", reason="wait_running_timeout")
                detail = str(last_error) if last_error is not None else f"status={last_status or 'empty'}"
                error_type = (
                    YuanrongAgentNotVisibleError
                    if expected_generation is not None and not_found_count and all_observations_not_found
                    else YuanrongAgentTimeoutError
                    if expected_generation is not None or deadline is not None
                    else YuanrongAgentApiError
                )
                raise error_type(
                    f"agent instance not running after {timeout_s:.0f}s: "
                    f"instance_id={normalized_id}, last={detail}, trace_id={poll_trace_id}"
                )

            while True:
                if attempt and loop.time() >= wait_deadline:
                    raise_wait_timeout()
                attempt += 1
                instance: dict[str, Any] = {}
                try:
                    get_call = self.get_agent_info(normalized_id, trace_id=poll_trace_id)
                    if expected_generation is not None or deadline is not None:
                        instance = await asyncio.wait_for(
                            get_call, timeout=max(0, wait_deadline - loop.time())
                        )
                    else:
                        instance = await get_call
                    last_error = None
                    all_observations_not_found = False
                except YuanrongAgentApiError as exc:
                    last_error = exc
                    if isinstance(exc, YuanrongAgentNotFoundError):
                        not_found_count += 1
                    else:
                        all_observations_not_found = False
                except asyncio.TimeoutError as exc:
                    last_error = exc
                    all_observations_not_found = False
                status = _instance_status(instance)
                last_status = status
                if operation is not None:
                    operation.set_attribute("poll.count", attempt)
                    operation.set_attribute(
                        "poll.status",
                        status if status in _AGENT_FAILED_STATUSES
                        or status == _AGENT_RUNNING_STATUS else "other",
                    )
                if expected_generation is not None:
                    env_vars = instance.get("env_vars")
                    actual = (
                        env_vars.get(AGENT_GENERATION_ENV)
                        if isinstance(env_vars, Mapping) else None
                    )
                    if actual is None:
                        # The ID comes from a per-generation name, so status alone is still safe.
                        if instance and not marker_missing_logged:
                            logger.warning(
                                "[YuanrongFrontendAgentClient] generation marker missing, "
                                "falling back to status: instance_id=%s",
                                normalized_id,
                            )
                            marker_missing_logged = True
                    elif actual != expected_generation:
                        raise YuanrongAgentGenerationMismatchError(
                            f"agent instance generation mismatch: instance_id={normalized_id}"
                        )
                if status in _AGENT_FAILED_STATUSES:
                    raise YuanrongAgentFailedError(
                        f"agent instance failed: instance_id={normalized_id}, "
                        f"status={status}"
                    )
                if _is_agent_running(instance):
                    logger.info(
                        "[YuanrongFrontendAgentClient] instance running after GET poll: "
                        "instance_id=%s attempt=%s status=%s trace_id=%s",
                        normalized_id,
                        attempt,
                        status or _AGENT_RUNNING_STATUS,
                        poll_trace_id,
                    )
                    return instance
                remaining = wait_deadline - loop.time()
                if remaining <= 0:
                    raise_wait_timeout()
                sleep_for = min(interval_s, remaining)
                logger.debug(
                    "[YuanrongFrontendAgentClient] GET not running yet: "
                    "instance_id=%s attempt=%s status=%s sleep=%.1fs last_error=%s "
                    "trace_id=%s",
                    normalized_id,
                    attempt,
                    status or "empty",
                    sleep_for,
                    type(last_error).__name__ if last_error is not None else "-",
                    poll_trace_id,
                )
                await asyncio.sleep(sleep_for)

    async def upload_agent_file(
        self,
        instance_id: str,
        path: str,
        data: bytes,
        *,
        auth_headers: Mapping[str, str] | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        """Upload a file into an agent container via POST /api/agent/:id/files/upload."""
        self._ensure_connected()
        normalized_id = str(instance_id or "").strip()
        normalized_path = str(path or "").strip()
        if not normalized_id:
            raise ValueError("instance_id is required to upload agent file")
        if not normalized_path:
            raise ValueError("path is required to upload agent file")
        resolved_trace_id = bind_southbound_trace_id(trace_id)
        logger.info(
            "[YuanrongFrontendAgentClient] upload_agent_file: instance_id=%s path=%s trace_id=%s",
            normalized_id,
            normalized_path,
            resolved_trace_id,
        )
        status, body = await asyncio.to_thread(
            self._do_agent_file_upload,
            normalized_id,
            normalized_path,
            data,
            dict(auth_headers or {}),
            resolved_trace_id,
        )
        return self._parse_agent_file_upload_response(body, status, normalized_path, len(data))

    async def download_agent_file(
        self,
        instance_id: str,
        path: str,
        *,
        offset: int = 0,
        limit: int = 65536,
        auth_headers: Mapping[str, str] | None = None,
        trace_id: str | None = None,
    ) -> AgentFileDownloadChunk:
        """Download a file chunk from GET /api/agent/:id/files/download (Range)."""
        self._ensure_connected()
        normalized_id = str(instance_id or "").strip()
        normalized_path = str(path or "").strip()
        if not normalized_id:
            raise ValueError("instance_id is required to download agent file")
        if not normalized_path:
            raise ValueError("path is required to download agent file")
        resolved_offset = max(int(offset), 0)
        resolved_limit = max(int(limit), 1)
        resolved_trace_id = bind_southbound_trace_id(trace_id)
        logger.info(
            "[YuanrongFrontendAgentClient] download_agent_file: instance_id=%s path=%s "
            "offset=%s limit=%s trace_id=%s",
            normalized_id,
            normalized_path,
            resolved_offset,
            resolved_limit,
            resolved_trace_id,
        )
        status, data, content_type, total_size = await asyncio.to_thread(
            self._do_agent_file_download,
            _AgentFileRequest(
                instance_id=normalized_id,
                path=normalized_path,
                auth_headers=dict(auth_headers or {}),
                trace_id=resolved_trace_id,
            ),
            resolved_offset,
            resolved_limit,
        )
        if status == 416:
            # File exists but offset is at/past EOF. Gateway /file-api treats this
            # as a successful empty read (HTTP 200 + eof), not HTTP Range failure.
            resolved_size = total_size if total_size > 0 else resolved_offset
            return AgentFileDownloadChunk(
                data=b"",
                path=normalized_path,
                offset=resolved_offset,
                chunk_size=0,
                size=resolved_size,
                content_type="application/octet-stream",
                eof=True,
            )
        if status in {404, 413} or status >= 500 or not (200 <= status < 300):
            self._raise_agent_file_http_error(status, data)
        chunk_size = len(data)
        if total_size <= 0:
            total_size = resolved_offset + chunk_size
        eof = resolved_offset + chunk_size >= total_size
        return AgentFileDownloadChunk(
            data=data,
            path=normalized_path,
            offset=resolved_offset,
            chunk_size=chunk_size,
            size=total_size,
            content_type=content_type or "application/octet-stream",
            eof=eof,
        )

    async def list_agent_files(
        self,
        instance_id: str,
        path: str,
        *,
        recursive: bool = False,
        max_depth: int = 0,
        auth_headers: Mapping[str, str] | None = None,
        trace_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """List files in an agent container via GET /api/agent/:id/files/list."""
        self._ensure_connected()
        normalized_id = str(instance_id or "").strip()
        normalized_path = str(path or "").strip()
        if not normalized_id:
            raise ValueError("instance_id is required to list agent files")
        if not normalized_path:
            raise ValueError("path is required to list agent files")
        if int(max_depth) < 0:
            raise ValueError("max_depth must be >= 0")
        resolved_trace_id = bind_southbound_trace_id(trace_id)
        logger.info(
            "[YuanrongFrontendAgentClient] list_agent_files: instance_id=%s path=%s trace_id=%s",
            normalized_id,
            normalized_path,
            resolved_trace_id,
        )
        status, body = await asyncio.to_thread(
            self._do_agent_file_list,
            _AgentFileRequest(
                instance_id=normalized_id,
                path=normalized_path,
                auth_headers=dict(auth_headers or {}),
                trace_id=resolved_trace_id,
            ),
            bool(recursive),
            int(max_depth),
        )
        return self._parse_agent_file_list_response(body, status)

    async def mkdir_agent_dir(
        self,
        instance_id: str,
        path: str,
        *,
        mode: str | None = None,
        recursive: bool = False,
        auth_headers: Mapping[str, str] | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        """Create a directory in an agent container via POST /api/agent/:id/files/mkdir."""
        self._ensure_connected()
        normalized_id = str(instance_id or "").strip()
        normalized_path = str(path or "").strip()
        if not normalized_id:
            raise ValueError("instance_id is required to mkdir agent directory")
        if not normalized_path:
            raise ValueError("path is required to mkdir agent directory")
        if "\x00" in normalized_path:
            raise YuanrongAgentFileError(
                "path must not contain NUL",
                http_status=400,
                error_code="BAD_REQUEST",
            )
        normalized_mode = self._normalize_mkdir_mode(mode)
        resolved_trace_id = bind_southbound_trace_id(trace_id)
        logger.info(
            "[YuanrongFrontendAgentClient] mkdir_agent_dir: instance_id=%s path=%s trace_id=%s",
            normalized_id,
            normalized_path,
            resolved_trace_id,
        )
        status, body = await asyncio.to_thread(
            self._do_agent_file_mkdir,
            _AgentFileRequest(
                instance_id=normalized_id,
                path=normalized_path,
                auth_headers=dict(auth_headers or {}),
                trace_id=resolved_trace_id,
            ),
            normalized_mode,
            bool(recursive),
        )
        return self._parse_agent_file_mkdir_response(body, status, normalized_path)

    def _ensure_connected(self) -> None:
        if not self._connected:
            raise RuntimeError("client not connected")

    def _agent_create_url(self) -> str:
        return f"{self._frontend_endpoint}/api/agent"

    def _agent_delete_url(self, instance_id: str) -> str:
        encoded = urllib.parse.quote(instance_id, safe="")
        return f"{self._frontend_endpoint}/api/agent/{encoded}"

    def _agent_files_upload_url(self, instance_id: str) -> str:
        encoded = urllib.parse.quote(instance_id, safe="")
        return f"{self._frontend_endpoint}/api/agent/{encoded}/files/upload"

    def _agent_files_download_url(self, instance_id: str, path: str) -> str:
        encoded_id = urllib.parse.quote(instance_id, safe="")
        encoded_path = urllib.parse.quote(path, safe="")
        return (
            f"{self._frontend_endpoint}/api/agent/{encoded_id}/files/download"
            f"?path={encoded_path}"
        )

    def _agent_files_list_url(
        self,
        instance_id: str,
        path: str,
        *,
        recursive: bool,
        max_depth: int,
    ) -> str:
        encoded_id = urllib.parse.quote(instance_id, safe="")
        query = urllib.parse.urlencode(
            {
                "path": path,
                "recursive": "true" if recursive else "false",
                "max_depth": str(int(max_depth)),
            }
        )
        return f"{self._frontend_endpoint}/api/agent/{encoded_id}/files/list?{query}"

    def _agent_files_mkdir_url(
        self,
        instance_id: str,
        path: str,
        *,
        mode: str | None,
        recursive: bool,
    ) -> str:
        encoded_id = urllib.parse.quote(instance_id, safe="")
        query: dict[str, str] = {
            "path": path,
            "recursive": "true" if recursive else "false",
        }
        if mode:
            query["mode"] = mode
        return f"{self._frontend_endpoint}/api/agent/{encoded_id}/files/mkdir?{urllib.parse.urlencode(query)}"

    @staticmethod
    def _normalize_mkdir_mode(mode: str | None) -> str | None:
        """Return 3-4 octal digits, or None when omitted (caller umask)."""
        if mode is None:
            return None
        text = str(mode).strip()
        if not text:
            return None
        if _MKDIR_MODE_RE.fullmatch(text) is None:
            raise YuanrongAgentFileError(
                "mode must be 3-4 octal digits (e.g. 755 or 0700)",
                http_status=400,
                error_code="BAD_REQUEST",
            )
        return text

    @staticmethod
    def _merge_auth_headers(
        base_headers: dict[str, str],
        auth_headers: dict[str, str],
        trace_id: str | None = None,
    ) -> dict[str, str]:
        merged = dict(base_headers)
        for key, value in auth_headers.items():
            if value is not None and str(value).strip():
                merged[str(key)] = str(value)
        return apply_trace_header(merged, trace_id)

    @staticmethod
    def _encode_multipart_form(
        fields: dict[str, str],
        *,
        file_field: str,
        file_bytes: bytes,
        filename: str = "file",
    ) -> tuple[bytes, str]:
        boundary = f"----YuanrongFormBoundary{uuid.uuid4().hex}"
        body = bytearray()
        for name, value in fields.items():
            body.extend(f"--{boundary}\r\n".encode())
            body.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
            body.extend(str(value).encode("utf-8"))
            body.extend(b"\r\n")
        body.extend(f"--{boundary}\r\n".encode())
        body.extend(
            f'Content-Disposition: form-data; name="{file_field}"; filename="{filename}"\r\n'.encode()
        )
        body.extend(b"Content-Type: application/octet-stream\r\n\r\n")
        body.extend(file_bytes)
        body.extend(b"\r\n")
        body.extend(f"--{boundary}--\r\n".encode())
        return bytes(body), f"multipart/form-data; boundary={boundary}"

    @staticmethod
    def _parse_content_range_total(content_range: str, *, fallback_size: int) -> int:
        text = str(content_range or "").strip()
        match = re.match(r"bytes\s+(?:\d+-\d+|\*)/(\d+|\*)", text, flags=re.IGNORECASE)
        if not match:
            return fallback_size
        total_text = match.group(1)
        if total_text == "*":
            return fallback_size
        try:
            return int(total_text)
        except ValueError:
            return fallback_size

    @staticmethod
    def _parse_agent_file_error_body(raw: bytes | str) -> str:
        if isinstance(raw, bytes):
            text = raw.decode("utf-8", errors="replace")
        else:
            text = str(raw or "")
        text = text.strip()
        if not text:
            return "agent file request failed"
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return text
        if isinstance(parsed, dict):
            return str(parsed.get("error") or parsed.get("message") or text)
        return text

    @staticmethod
    def _agent_file_404_error_code(message: str) -> str:
        """Classify YuanRong file-API 404. Default is path miss, not sandbox miss."""
        lowered = str(message or "").lower()
        instance_markers = (
            "instance not found",
            "instance_not_found",
            "sandbox not found",
            "agent not found",
            "no such instance",
            "instance does not exist",
        )
        if any(marker in lowered for marker in instance_markers):
            return "instance_not_found"
        return "file_not_found"

    def _agent_file_http_error(self, status: int, body: bytes | str) -> YuanrongAgentFileError:
        message = self._parse_agent_file_error_body(body)
        if status == 404:
            code = self._agent_file_404_error_code(message)
        elif status == 413:
            code = "file_too_large"
        elif status == 400:
            code = "BAD_REQUEST"
        else:
            code = "INTERNAL_ERROR"
        return YuanrongAgentFileError(message, http_status=status, error_code=code)

    def _raise_agent_file_http_error(self, status: int, body: bytes | str) -> None:
        raise self._agent_file_http_error(status, body)

    def _parse_agent_file_upload_response(
        self,
        body: str,
        status: int,
        path: str,
        uploaded_size: int,
    ) -> dict[str, Any]:
        if not (200 <= status < 300):
            raise self._agent_file_http_error(status, body)
        try:
            parsed = json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            raise YuanrongAgentFileError(
                f"invalid upload response: {body!r}",
                http_status=status,
                error_code="INTERNAL_ERROR",
            ) from exc
        if not isinstance(parsed, dict):
            raise YuanrongAgentFileError(
                f"invalid upload response shape: {body!r}",
                http_status=status,
                error_code="INTERNAL_ERROR",
            )
        if parsed.get("success") is True:
            return {
                "success": True,
                "path": str(parsed.get("path") or path),
                "size": int(parsed.get("size") or uploaded_size),
            }
        error = str(parsed.get("error") or parsed.get("message") or "upload failed")
        raise self._agent_file_http_error(status or 500, json.dumps({"error": error}))

    def _parse_agent_file_list_response(self, body: str, status: int) -> list[dict[str, Any]]:
        if not (200 <= status < 300):
            raise self._agent_file_http_error(status, body)
        try:
            parsed = json.loads(body) if body else []
        except json.JSONDecodeError as exc:
            raise YuanrongAgentFileError(
                f"invalid list response: {body!r}",
                http_status=status,
                error_code="INTERNAL_ERROR",
            ) from exc
        # Frontend GET /files/list 返回的是 FaaS invoke 外壳
        # {"body": {"items": [...]}, "innerCode": "0", ...}
        # chat invoke 已经剥这层；list 必须同样处理，否则 gateway 看到空列表
        if isinstance(parsed, dict) and self._is_faas_envelope(parsed):
            parsed, faas_err = self._normalize_faas_body(parsed)
            if faas_err:
                raise self._agent_file_http_error(
                    status or 500,
                    json.dumps({"error": f"faas list failed: innerCode={faas_err}"}),
                )
        if isinstance(parsed, list):
            items = parsed
        elif isinstance(parsed, dict):
            if parsed.get("success") is False:
                error = str(parsed.get("error") or parsed.get("message") or "list failed")
                raise self._agent_file_http_error(status or 500, json.dumps({"error": error}))
            raw_items = parsed.get("items")
            if raw_items is None:
                raw_items = parsed.get("files")
            if raw_items is None:
                raw_items = parsed.get("data")
            if raw_items is None:
                raw_items = []
            if not isinstance(raw_items, list):
                raise YuanrongAgentFileError(
                    f"invalid list response shape: {body!r}",
                    http_status=status,
                    error_code="INTERNAL_ERROR",
                )
            items = raw_items
        else:
            raise YuanrongAgentFileError(
                f"invalid list response shape: {body!r}",
                http_status=status,
                error_code="INTERNAL_ERROR",
            )
        return [item for item in items if isinstance(item, dict)]

    def _parse_agent_file_mkdir_response(
        self,
        body: str,
        status: int,
        path: str,
    ) -> dict[str, Any]:
        if not (200 <= status < 300):
            raise self._agent_file_http_error(status, body)
        try:
            parsed = json.loads(body) if body else {}
        except json.JSONDecodeError as exc:
            raise YuanrongAgentFileError(
                f"invalid mkdir response: {body!r}",
                http_status=status,
                error_code="INTERNAL_ERROR",
            ) from exc
        if not isinstance(parsed, dict):
            raise YuanrongAgentFileError(
                f"invalid mkdir response shape: {body!r}",
                http_status=status,
                error_code="INTERNAL_ERROR",
            )
        # Frontend POST /files/mkdir 可能包 FaaS invoke 外壳，与 list 一致。
        if self._is_faas_envelope(parsed):
            parsed, faas_err = self._normalize_faas_body(parsed)
            if faas_err:
                raise self._agent_file_http_error(
                    status or 500,
                    json.dumps({"error": f"faas mkdir failed: innerCode={faas_err}"}),
                )
            if not isinstance(parsed, dict):
                raise YuanrongAgentFileError(
                    f"invalid mkdir response shape: {body!r}",
                    http_status=status,
                    error_code="INTERNAL_ERROR",
                )
        code = parsed.get("code")
        if code not in (None, 200, "200"):
            raise self._agent_file_http_error(status or 500, body)
        payload = parsed.get("data") if isinstance(parsed.get("data"), dict) else parsed
        if payload.get("success") is False:
            error = str(payload.get("error") or payload.get("message") or "mkdir failed")
            raise self._agent_file_http_error(status or 500, json.dumps({"error": error}))
        created = payload.get("created")
        if created is None:
            created = True
        return {
            "success": True,
            "path": str(payload.get("path") or path),
            "created": bool(created),
        }

    def _do_agent_file_upload(
        self,
        instance_id: str,
        path: str,
        data: bytes,
        auth_headers: dict[str, str],
        trace_id: str | None = None,
    ) -> tuple[int, str]:
        payload, content_type = self._encode_multipart_form(
            {"path": path},
            file_field="file",
            file_bytes=data,
            filename=path.rsplit("/", 1)[-1] or "file",
        )
        headers = self._merge_auth_headers(
            {"Content-Type": content_type}, auth_headers, trace_id
        )
        req = urllib.request.Request(
            self._agent_files_upload_url(instance_id),
            data=payload,
            headers=headers,
            method="POST",
        )
        return self._urlopen_request(
            req,
            timeout=self._agent_timeout_s,
            raise_on_timeout=True,
        )

    def _do_agent_file_download(
        self,
        request: _AgentFileRequest,
        offset: int,
        limit: int,
    ) -> tuple[int, bytes, str, int]:
        headers = self._merge_auth_headers(
            {"Accept": "*/*"}, request.auth_headers, request.trace_id
        )
        end = offset + limit - 1
        headers["Range"] = f"bytes={offset}-{end}"
        req = urllib.request.Request(
            self._agent_files_download_url(request.instance_id, request.path),
            headers=headers,
            method="GET",
        )
        resolved_timeout = float(self._agent_timeout_s)
        try:
            with urllib.request.urlopen(req, timeout=resolved_timeout) as resp:
                status = int(getattr(resp, "status", 200))
                data = resp.read()
                content_type = str(resp.headers.get("Content-Type") or "application/octet-stream")
                content_range = str(resp.headers.get("Content-Range") or "")
                content_length = int(resp.headers.get("Content-Length") or len(data) or 0)
                total_size = self._parse_content_range_total(
                    content_range,
                    fallback_size=offset + content_length,
                )
                return status, data, content_type, total_size
        except urllib.error.HTTPError as err:
            body = err.read() if err.fp else b""
            status = int(getattr(err, "code", 500) or 500)
            err_headers = getattr(err, "headers", None)
            content_type = "application/octet-stream"
            total_size = 0
            if err_headers is not None:
                content_type = str(err_headers.get("Content-Type") or content_type)
                content_range = str(err_headers.get("Content-Range") or "")
                total_size = self._parse_content_range_total(
                    content_range,
                    fallback_size=0,
                )
            log_fn = logger.info if status == 416 else logger.error
            log_fn(
                "[YuanrongFrontendAgentClient] file download HTTP error: "
                "instance=%s path=%s code=%d trace_id=%s",
                request.instance_id,
                request.path,
                status,
                request.trace_id or "",
            )
            return status, body, content_type, total_size
        except Exception as err:
            logger.error(
                "[YuanrongFrontendAgentClient] file download failed: "
                "instance=%s path=%s error=%s trace_id=%s",
                request.instance_id,
                request.path,
                err,
                request.trace_id or "",
            )
            if self._is_timeout_error(err):
                raise YuanrongAgentApiError(
                    f"file download timeout after {resolved_timeout}s: {err}"
                ) from err
            return 500, str(err).encode("utf-8"), "application/octet-stream", 0

    def _do_agent_file_list(
        self,
        request: _AgentFileRequest,
        recursive: bool,
        max_depth: int,
    ) -> tuple[int, str]:
        headers = self._merge_auth_headers(
            {"Accept": "application/json"}, request.auth_headers, request.trace_id
        )
        req = urllib.request.Request(
            self._agent_files_list_url(
                request.instance_id,
                request.path,
                recursive=recursive,
                max_depth=max_depth,
            ),
            headers=headers,
            method="GET",
        )
        return self._urlopen_request(
            req,
            timeout=self._agent_timeout_s,
            raise_on_timeout=True,
        )

    def _do_agent_file_mkdir(
        self,
        request: _AgentFileRequest,
        mode: str | None,
        recursive: bool,
    ) -> tuple[int, str]:
        headers = self._merge_auth_headers(
            {"Accept": "application/json"}, request.auth_headers, request.trace_id
        )
        req = urllib.request.Request(
            self._agent_files_mkdir_url(
                request.instance_id,
                request.path,
                mode=mode,
                recursive=recursive,
            ),
            data=b"",
            headers=headers,
            method="POST",
        )
        return self._urlopen_request(
            req,
            timeout=self._agent_timeout_s,
            raise_on_timeout=True,
        )

    @staticmethod
    def _parse_agent_api_response(body: str, status: int) -> dict[str, Any]:
        try:
            parsed = json.loads(body) if body else {}
        except Exception as exc:
            raise YuanrongAgentApiError(
                f"invalid agent API response: status={status}, body={body!r}"
            ) from exc
        if not isinstance(parsed, dict):
            raise YuanrongAgentApiError(
                f"invalid agent API response shape: status={status}, body={body!r}"
            )
        code = parsed.get("code")
        if not (200 <= status < 300) or code not in (200, "200"):
            message = parsed.get("message") or parsed.get("status") or body
            raise YuanrongAgentApiError(
                f"agent API failed: http_status={status}, code={code}, message={message!r}"
            )
        return parsed

    @staticmethod
    def _agent_instance_not_found(status: int, body: str, instance_id: str) -> bool:
        """Recognize an instance absence, excluding a missing HTTP route."""
        if status != 404:
            return False
        try:
            parsed = json.loads(body)
        except (TypeError, ValueError):
            return False
        if not isinstance(parsed, dict) or parsed.get("code") not in (404, "404"):
            return False
        message = str(parsed.get("message") or "").strip().casefold()
        if not message.startswith("instance not found"):
            return False
        # A message that names an instance must name this one.
        named = message.partition(":")[2].strip()
        return not named or named == instance_id.strip().casefold()

    def _urlopen_request(
        self,
        req: urllib.request.Request,
        *,
        timeout: float | None = None,
        raise_on_timeout: bool = False,
        trace_response: bool = False,
        preparation_trace: bool = False,
        classify_not_sent: bool = False,
    ) -> tuple[int, str]:
        resolved_timeout = (
            self._agent_timeout_s if timeout is None else float(timeout)
        )
        response_started = False
        try:
            with urllib.request.urlopen(req, timeout=resolved_timeout) as resp:
                response_started = True
                status = int(getattr(resp, "status", 200))
                if trace_response:
                    operation = current_request_trace()
                    if operation is not None:
                        operation.event("http.response_headers", {"status_code": status})
                        operation.event("response.first", {"outcome": "success" if 200 <= status < 400 else "failed"})
                text = resp.read().decode("utf-8", errors="replace")
                if trace_response and operation is not None:
                    operation.event("http.response.completed", {"status_code": status})
                return status, text
        except urllib.error.HTTPError as err:
            if trace_response:
                operation = current_request_trace()
                if operation is not None:
                    operation.event("http.response_headers", {"status_code": int(err.code)})
                    operation.event("response.first", {"outcome": "failed"})
            try:
                text = err.read().decode("utf-8", errors="replace") if err.fp else str(err)
            except (OSError, http.client.HTTPException):
                if not classify_not_sent:
                    raise
                # Keep the observed status even if the error body was truncated.
                text = ""
            if trace_response and operation is not None:
                operation.event("http.response.completed", {"status_code": int(err.code)})
            logger.error(
                "[YuanrongFrontendAgentClient] HTTP error: url=%s code=%d",
                req.full_url,
                getattr(err, "code", 500),
            )
            return int(getattr(err, "code", 500) or 500), text
        except Exception as err:
            logger.error(
                "[YuanrongFrontendAgentClient] request failed: url=%s error=%s",
                req.full_url,
                str(err),
            )
            if trace_response or preparation_trace:
                operation = current_request_trace()
                if operation is not None:
                    operation.record_outcome("timeout" if self._is_timeout_error(err) else "failed",
                                             reason=type(err).__name__)
            if (
                classify_not_sent
                and not response_started
                and isinstance(err, urllib.error.URLError)
                and isinstance(err.reason, (ConnectionRefusedError, socket.gaierror))
            ):
                raise YuanrongAgentRequestNotSentError(
                    f"request could not connect: url={req.full_url}, error={err}"
                ) from err
            if raise_on_timeout and self._is_timeout_error(err):
                raise YuanrongAgentTimeoutError(
                    f"request timeout after {resolved_timeout}s: "
                    f"url={req.full_url}, error={err}"
                ) from err
            return 500, str(err)

    @staticmethod
    def _is_timeout_error(err: BaseException) -> bool:
        if isinstance(err, TimeoutError):
            return True
        reason = getattr(err, "reason", None)
        if isinstance(reason, TimeoutError):
            return True
        text = str(err).lower()
        return "timed out" in text or "timeout" in type(err).__name__.lower()

    def _do_agent_create(
        self, payload: dict[str, Any], trace_id: str | None = None
    ) -> tuple[int, str]:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            self._agent_create_url(),
            data=data,
            headers=inject_request_headers(apply_trace_header(
                {"Content-Type": "application/json"}, trace_id
            )),
            method="POST",
        )
        return self._urlopen_request(
            req,
            timeout=self._agent_timeout_s,
            preparation_trace=True,
            raise_on_timeout=True,
            classify_not_sent=True,
        )

    def _do_agent_delete(
        self, instance_id: str, trace_id: str | None = None
    ) -> tuple[int, str]:
        req = urllib.request.Request(
            self._agent_delete_url(instance_id),
            headers=inject_request_headers(apply_trace_header(
                {"Content-Type": "application/json"}, trace_id
            )),
            method="DELETE",
        )
        return self._urlopen_request(
            req,
            timeout=self._agent_timeout_s,
            raise_on_timeout=True,
            classify_not_sent=True,
        )

    def _do_agent_get(
        self, instance_id: str, trace_id: str | None = None
    ) -> tuple[int, str]:
        req = urllib.request.Request(
            self._agent_delete_url(instance_id),  # same URL: /api/agent/{instanceId}
            headers=inject_request_headers(apply_trace_header(
                {"Content-Type": "application/json"}, trace_id
            )),
            method="GET",
        )
        return self._urlopen_request(
            req,
            timeout=self._agent_timeout_s,
            preparation_trace=True,
        )

    @staticmethod
    def _is_faas_envelope(parsed: Any) -> bool:
        """是否为 faas executor 的外层封装形状.

        faas executor 把 clawee 返回值包成 {"body": <result>, "innerCode": ..., ...}，
        仅当确实识别到此形状（含 body+innerCode，且非标准 AgentResponse 形状）时才剥离，
        避免误吞 websocket 直连等其它路径返回的普通 dict。
        """
        if not isinstance(parsed, dict):
            return False
        if "body" not in parsed or "innerCode" not in parsed:
            return False
        # 已是标准 AgentResponse 形状则不当作 faas 封装处理
        return "payload" not in parsed and "ok" not in parsed

    @staticmethod
    def _normalize_faas_body(parsed: Any) -> tuple[Any, str | None]:
        """对 faas 返回体做「剥外层封装 + 二次解析」统一规范化.

        faas executor 把 clawee 返回值包成
        {"body": <result>, "innerCode": "0", "traceId":..., ...} 再 to_json_string，
        clawee.handler 返回 response_to_payload(resp) = json.dumps(asdict(resp)) 即 str，
        故 body 字段常是内层 JSON 字符串。本函数取出内层 body 并二次解析。

        GET /files/list 与 POST /files/mkdir 响应共用此规范化，保证两条
        解析路径逻辑一致。仅当确实识别到 faas 外层形状（有 body+innerCode
        且非 AgentResponse 形状）时剥离，避免误吞 websocket 直连等其它
        路径返回的普通 dict。

        Returns:
            (normalized, faas_error_code):faas_error_code 非 None 表示 faas 层错误（innerCode != "0"）。
        """
        # 剥 faas executor 外层封装
        if YuanrongFrontendAgentClient._is_faas_envelope(parsed):
            inner = parsed.get("body")
            if isinstance(inner, str) and inner.strip():
                try:
                    inner = json.loads(inner)
                except Exception:
                    inner = {"content": inner}
            if inner is None:
                inner = {}
            if not isinstance(inner, dict):
                inner = {"content": inner}
            inner_code = str(parsed.get("innerCode", "0"))
            if inner_code != "0":
                inner = dict(inner)
                inner["_faas_error_code"] = inner_code
                return inner, inner_code
            parsed = inner

        # 二次解析：faas 可能把 JSON 字符串放进 body 后再序列化一次，导致首次 json.loads 拿到 str
        if isinstance(parsed, str) and parsed.strip():
            try:
                parsed = json.loads(parsed)
            except Exception:
                parsed = {"content": parsed}

        return parsed, None
