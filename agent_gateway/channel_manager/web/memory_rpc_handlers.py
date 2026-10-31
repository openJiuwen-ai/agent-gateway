# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Memory RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import logging
from typing import Any

try:
    import psutil as _psutil
    _HAS_PSUTIL = True
except ImportError:
    _psutil = None  # type: ignore[assignment]
    _HAS_PSUTIL = False

from openjiuwen_protocol.message import ReqMethod

from agent_gateway.common.config import (
    get_config,
    update_memory_forbidden_in_config,
)
from agent_gateway.routing.e2a_proxy import (
    is_legacy_shared_directory_client,
    proxy_unary_request,
)

logger = logging.getLogger(__name__)


def register_memory_handlers(channel: Any, agent_client: Any, _resolve: Any) -> None:
    """Register memory RPC handlers in their original route order."""

    async def _memory_compute(ws, req_id, params, session_id):
        if _HAS_PSUTIL:
            process = _psutil.Process()
            rss_mb = process.memory_info().rss / (1024 * 1024)
            mem = _psutil.virtual_memory()
            total_mb = mem.total / (1024 * 1024)
            available_mb = mem.available / (1024 * 1024)
        else:
            rss_mb, total_mb, available_mb = _read_proc_meminfo()

        await channel.send_response(ws, req_id, ok=True,
                                    payload={"rss_mb": rss_mb, "total_mb": total_mb,
                                             "available_mb": available_mb})

    async def _memory_forbidden_get(ws, req_id, params, session_id, user_id=None):
        resolved_client = _resolve(agent_client)
        if resolved_client is not None and not is_legacy_shared_directory_client(resolved_client):
            await proxy_unary_request(channel=channel, agent_client=resolved_client, ws=ws, req_id=req_id,
                                      params={}, session_id=session_id, user_id=user_id,
                                      req_method=ReqMethod.MEMORY_FORBIDDEN_GET, label="memory.forbidden.get")
            return
        try:
            cfg = get_config() or {}
            payload = cfg.get("memory", {}).get("forbidden_memory_definition", {})
            await channel.send_response(ws, req_id, ok=True, payload=payload)
        except Exception as e:
            logger.exception("[memory.forbidden.get] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _memory_forbidden_set(ws, req_id, params, session_id, user_id=None):
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        resolved_client = _resolve(agent_client)
        if resolved_client is not None and not is_legacy_shared_directory_client(resolved_client):
            await proxy_unary_request(channel=channel, agent_client=resolved_client, ws=ws, req_id=req_id,
                                      params=params, session_id=session_id, user_id=user_id,
                                      req_method=ReqMethod.MEMORY_FORBIDDEN_SET, label="memory.forbidden.set")
            return
        try:
            update_memory_forbidden_in_config(params)
            await channel.send_response(ws, req_id, ok=True, payload={"ok": True})
        except Exception as e:
            logger.exception("[memory.forbidden.set] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    channel.register_method("memory.compute", _memory_compute)
    channel.register_method("memory.forbidden.get", _memory_forbidden_get)
    channel.register_method("memory.forbidden.set", _memory_forbidden_set)


def _read_proc_meminfo() -> tuple[float, float, float]:
    """Fallback for platforms without psutil: read /proc/meminfo.

    Returns (rss_mb, total_mb, available_mb).  Returns (0, 0, 0) on failure.
    """
    try:
        info: dict[str, int] = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    info[parts[0].rstrip(":")] = int(parts[1])  # kB
        total_mb = info.get("MemTotal", 0) / 1024
        available_mb = info.get("MemAvailable", info.get("MemFree", 0)) / 1024
        # RSS from /proc/self/status
        with open("/proc/self/status", "r") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss_mb = int(line.split()[1]) / 1024  # kB → MB
                    return rss_mb, total_mb, available_mb
        return 0.0, total_mb, available_mb
    except OSError:
        return 0.0, 0.0, 0.0
