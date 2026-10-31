# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Updater RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from agent_gateway.common.config import update_updater_in_config
from agent_gateway.common.updater import DEFAULT_SOURCE_CONFIG, UpdaterService

logger = logging.getLogger(__name__)


def register_updater_handlers(channel: Any, updater_service: UpdaterService | None = None) -> None:
    """Register updater RPC handlers in their original route order."""

    async def _updater_get_status(ws, req_id, params, session_id):
        service = updater_service or UpdaterService()
        await channel.send_response(ws, req_id, ok=True, payload=service.get_status())

    async def _updater_check(ws, req_id, params, session_id):
        service = updater_service or UpdaterService()
        manual = bool((params or {}).get("manual", False)) if isinstance(params, dict) else False
        payload = await asyncio.to_thread(service.check, manual)
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _updater_download(ws, req_id, params, session_id):
        service = updater_service or UpdaterService()
        payload = service.start_download()
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _updater_upgrade(ws, req_id, params, session_id):
        service = updater_service or UpdaterService()
        payload = await asyncio.to_thread(service.start_upgrade)
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _updater_get_conf(ws, req_id, params, session_id):
        service = updater_service or UpdaterService()
        await channel.send_response(ws, req_id, ok=True, payload=service.get_runtime_config())

    async def _updater_reset_source(ws, req_id, params, session_id):
        try:
            update_updater_in_config(dict(DEFAULT_SOURCE_CONFIG))
        except Exception as exc:  # noqa: BLE001
            logger.warning("[updater.reset_source] 写回 config.yaml 失败: %s", exc)
            await channel.send_response(ws, req_id, ok=False,
                                        error=str(exc), code="INTERNAL_ERROR")
            return

        service = updater_service or UpdaterService()
        await channel.send_response(ws, req_id, ok=True, payload=service.get_runtime_config())

    async def _updater_set_conf(ws, req_id, params, session_id):
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return

        updates: dict[str, Any] = {}
        if "enabled" in params:
            updates["enabled"] = bool(params.get("enabled"))
        for key in ("repo_owner", "repo_name", "release_api_url", "asset_name_pattern",
                "release_api_type", "pypi_mirror"):
            if key in params:
                updates[key] = str(params.get(key) or "").strip()
        for plat in ("windows", "macos", "linux"):
            key = f"asset_name_pattern_{plat}"
            if key in params:
                updates[key] = str(params.get(key) or "").strip()
        if "timeout_seconds" in params:
            try:
                updates["timeout_seconds"] = max(5, int(params.get("timeout_seconds")))
            except (TypeError, ValueError):
                await channel.send_response(ws, req_id, ok=False,
                                            error="timeout_seconds must be integer", code="BAD_REQUEST")
                return

        try:
            update_updater_in_config(updates)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[updater.set_conf] 写回 config.yaml 失败: %s", exc)
            await channel.send_response(ws, req_id, ok=False,
                                        error=str(exc), code="INTERNAL_ERROR")
            return

        service = updater_service or UpdaterService()
        await channel.send_response(ws, req_id, ok=True, payload=service.get_runtime_config())

    channel.register_method("updater.get_status", _updater_get_status)
    channel.register_method("updater.check", _updater_check)
    channel.register_method("updater.download", _updater_download)
    channel.register_method("updater.upgrade", _updater_upgrade)
    channel.register_method("updater.get_conf", _updater_get_conf)
    channel.register_method("updater.reset_source", _updater_reset_source)
    channel.register_method("updater.set_conf", _updater_set_conf)
