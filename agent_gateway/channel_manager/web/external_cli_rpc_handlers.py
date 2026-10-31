# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from __future__ import annotations

import asyncio
from typing import Any


def register_external_cli_handlers(channel: Any) -> None:
    """Register the external CLI RPC domain in its original route order."""

    async def _external_cli_detect(ws, req_id, params, session_id):
        # Import at dispatch time so characterization tests can patch the
        # historical shared-handler module without creating an import cycle.
        from agent_gateway.channel_manager.web.app_web_handlers import (
            _activate_managed_external_cli_paths_if_needed,
            _detect_external_cli_agent,
        )

        del session_id
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        cli_agent = str(params.get("cli_agent") or "").strip().lower()
        cli_path = str(params.get("cli_path") or "").strip()
        raw_use_builtin = params.get("use_builtin")
        use_builtin = raw_use_builtin is True or str(raw_use_builtin).strip().lower() == "true"
        _activate_managed_external_cli_paths_if_needed()
        result = _detect_external_cli_agent(cli_agent, cli_path, use_builtin)
        await channel.send_response(ws, req_id, ok=True, payload=result)

    async def _external_cli_codex_install_status(ws, req_id, params, session_id):
        from agent_gateway.channel_manager.web.app_web_handlers import (
            _snapshot_codex_dependency_install_status,
        )

        del session_id
        await channel.send_response(ws, req_id, ok=True, payload=_snapshot_codex_dependency_install_status())

    async def _external_cli_install_status(ws, req_id, params, session_id):
        from agent_gateway.channel_manager.web.app_web_handlers import (
            _is_frozen_runtime,
            _known_external_cli_agents,
            _managed_external_cli_runtime_status,
            _snapshot_external_cli_dependency_install_status,
        )

        del session_id
        cli_agent = str((params or {}).get("cli_agent") or "").strip().lower()
        if cli_agent not in _known_external_cli_agents():
            await channel.send_response(
                ws, req_id, ok=False, error="unsupported external cli agent", code="BAD_REQUEST"
            )
            return
        payload = _snapshot_external_cli_dependency_install_status(cli_agent)
        if _is_frozen_runtime():
            payload["runtime"] = _managed_external_cli_runtime_status(cli_agent)
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _external_cli_install(ws, req_id, params, session_id):
        from agent_gateway.channel_manager.web.app_web_handlers import (
            _install_managed_external_cli_runtime_if_needed,
            _is_frozen_runtime,
            _known_external_cli_agents,
        )

        del session_id
        cli_agent = str((params or {}).get("cli_agent") or "").strip().lower()
        if cli_agent not in _known_external_cli_agents():
            await channel.send_response(
                ws, req_id, ok=False, error="unsupported external cli agent", code="BAD_REQUEST"
            )
            return
        if not _is_frozen_runtime():
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="managed external CLI runtimes are installed by the desktop app only",
                code="UNSUPPORTED",
            )
            return
        payload = _install_managed_external_cli_runtime_if_needed(cli_agent)
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _external_cli_custom_agent_remove(ws, req_id, params, session_id):
        from agent_gateway.channel_manager.web.app_web_handlers import (
            _remove_custom_external_cli_agent,
        )

        del session_id
        cli_agent = str((params or {}).get("cli_agent") or "").strip().lower()
        try:
            await asyncio.to_thread(_remove_custom_external_cli_agent, cli_agent)
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST")
            return
        await channel.send_response(ws, req_id, ok=True, payload={"cli_agent": cli_agent})

    channel.register_method("external_cli.detect", _external_cli_detect)
    channel.register_method("external_cli.codex_install_status", _external_cli_codex_install_status)
    channel.register_method("external_cli.install_status", _external_cli_install_status)
    channel.register_method("external_cli.install", _external_cli_install)
    channel.register_method("external_cli.custom_agent.remove", _external_cli_custom_agent_remove)
