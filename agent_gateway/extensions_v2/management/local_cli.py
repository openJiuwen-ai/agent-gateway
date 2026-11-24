# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Local CLI adapter owned by the Gateway runtime.

Package management belongs to the AgentServer; the Gateway CLI forwards its
actions over the same E2A channel the Web entry uses instead of deciding the
package domain locally.
"""

from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import Any

from e2a.message import ReqMethod

from agent_gateway.common.extension_cli import METHODS, LocalControl
from agent_gateway.routing.e2a_proxy import fetch_agent_unary

logger = logging.getLogger(__name__)

_PACKAGE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")


async def start_local_cli(runtime: Any) -> LocalControl:
    """Start a CLI adapter that forwards package actions to the AgentServer."""

    async def handle(action: str, params: dict[str, Any]) -> dict[str, Any]:
        if action == "presentation.query":
            return runtime.browser.presentations.query(params)
        if action == "presentation.start":
            return await runtime.browser.presentations.start(params)
        if action not in METHODS:
            return _error("INVALID_PARAMS", "不支持的包管理动作。")
        if action == "query":
            try:
                package_id = params.get("package_id")
                if package_id is None:
                    if set(params):
                        raise ValueError("query accepts an optional package_id")
                    package_id = ""
                else:
                    package_id = _package_id(package_id)
            except ValueError:
                return _error("INVALID_PARAMS", "包管理参数或 ZIP 路径无效。")
            return {"ok": True, "payload": {"state": runtime.packages.query(package_id or None)}}
        client = getattr(runtime.channel, "agent_client", None)
        if client is None:
            return _error("HOST_UNAVAILABLE", "当前实例没有可用的 AgentServer 连接。")
        try:
            request_params = _forward_params(action, params)
        except ValueError:
            return _error("INVALID_PARAMS", "包管理参数或 ZIP 路径无效。")
        method = ReqMethod(f"extensions.v2.{action}")
        forwarding = asyncio.create_task(
            fetch_agent_unary(
                agent_client=client,
                req_method=method,
                params=request_params,
                session_id=None,
                user_id=None,
                channel_id=getattr(runtime.channel, "channel_id", "web"),
                label=method.value,
                timeout_seconds=float(getattr(runtime.config, "dependency_timeout", 300.0)) + 30.0,
                metadata=(
                    {"extensions_v2_require_all_members": True}
                    if action == "install"
                    else None
                ),
            )
        )
        if action == "install":
            # The AgentServer waits for this host's members; keep reconciling
            # while it works so an install reports one result for both domains.
            while True:
                done, _ = await asyncio.wait({forwarding}, timeout=0.5)
                if done:
                    break
                try:
                    await runtime.sync_container_members()
                except Exception:  # noqa: BLE001 - the forwarded request stays authoritative
                    logger.warning("[GatewayExtensions] CLI member sync failed", exc_info=True)
        ok, payload = await forwarding
        if not ok:
            return {"ok": False, "payload": dict(payload)}
        if action != "query":
            try:
                await runtime.sync_container_members()
                from agent_gateway.extensions_v2.management import containers
                from agent_gateway.extensions_v2.management.install_state import ExtensionInstallState

                store = ExtensionInstallState.for_install_dir(runtime.config.install_dir)
                payload = containers.adjust_management_reports(store, dict(payload))
            except Exception:  # noqa: BLE001 - the AgentServer result already committed
                logger.warning("[GatewayExtensions] CLI member sync failed", exc_info=True)
        return {"ok": True, "payload": dict(payload)}

    control = LocalControl("gateway", handle, methods=METHODS | {"presentation.query", "presentation.start"})
    try:
        await control.start()
    except BaseException:
        await control.close()
        raise
    return control


def _forward_params(action: str, params: dict[str, Any]) -> dict[str, Any]:
    """Validate CLI params and convert them to the AgentServer request shape."""

    if action == "install":
        raw_paths = params.get("paths")
        if set(params) != {"paths"} or not isinstance(raw_paths, list) or not raw_paths:
            raise ValueError("install requires paths")
        paths = [_zip_path(value) for value in raw_paths]
        return {"paths": paths}
    if set(params) - {"package_id"}:
        raise ValueError("only package_id is accepted")
    if "package_id" not in params:
        raise ValueError("package_id is required")
    return {"package_id": _package_id(params["package_id"])}


def _package_id(value: object) -> str:
    if not isinstance(value, str) or _PACKAGE_ID.fullmatch(value) is None or ".." in value:
        raise ValueError("invalid package_id")
    return value


def _zip_path(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("path must be a string")
    path = Path(value)
    if not path.is_absolute() or path.suffix.lower() != ".zip" or not path.is_file():
        raise ValueError("path must be an existing absolute ZIP")
    return str(path)


def _error(code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "payload": {"code": code, "error": message}}
