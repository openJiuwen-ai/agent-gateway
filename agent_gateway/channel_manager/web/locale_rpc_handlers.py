# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Locale RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import logging
from typing import Any

from openjiuwen_protocol.message import ReqMethod

from agent_gateway.common.config import (
    get_config,
    update_preferred_language_in_config,
)
from agent_gateway.routing.e2a_proxy import (
    is_legacy_shared_directory_client,
    proxy_unary_request,
)

logger = logging.getLogger(__name__)


def register_locale_handlers(channel: Any, agent_client: Any, _resolve: Any) -> None:
    """Register locale RPC handlers in their original route order."""

    async def _locale_get_conf(ws, req_id, params, session_id, user_id=None):
        """Return the selected AgentServer user's preferred language."""
        resolved_client = _resolve(agent_client)
        if resolved_client is None or is_legacy_shared_directory_client(resolved_client):
            try:
                cfg = get_config()
                lang = str(cfg.get("preferred_language") or "zh").strip().lower()
                if lang not in ("zh", "en"):
                    lang = "zh"
                await channel.send_response(
                    ws, req_id, ok=True, payload={"preferred_language": lang}
                )
            except Exception as e:
                logger.exception("[locale.get_conf] %s", e)
                await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")
            return

        await proxy_unary_request(
            channel=channel, agent_client=_resolve(agent_client), ws=ws,
            req_id=req_id, params={}, session_id=session_id, user_id=user_id,
            req_method=ReqMethod.LOCALE_GET_CONF, label="locale.get_conf",
        )

    async def _locale_set_conf(ws, req_id, params, session_id, user_id=None):
        """Update preferred language in the selected AgentServer directory."""
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        lang_raw = params.get("preferred_language")
        if not isinstance(lang_raw, str):
            await channel.send_response(
                ws, req_id, ok=False, error="preferred_language must be string", code="BAD_REQUEST"
            )
            return
        lang = lang_raw.strip().lower()
        if lang not in ("zh", "en"):
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="preferred_language must be zh or en",
                code="BAD_REQUEST"
            )
            return
        resolved_client = _resolve(agent_client)
        if resolved_client is None or is_legacy_shared_directory_client(resolved_client):
            try:
                update_preferred_language_in_config(lang)
                await channel.send_response(ws, req_id, ok=True, payload={"preferred_language": lang})
            except Exception as e:
                logger.warning("[locale.set_conf] 写回 config.yaml 失败: %s", e)
                await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")
            return
        await proxy_unary_request(
            channel=channel, agent_client=_resolve(agent_client), ws=ws,
            req_id=req_id, params={"preferred_language": lang}, session_id=session_id,
            user_id=user_id, req_method=ReqMethod.LOCALE_SET_CONF,
            label="locale.set_conf",
        )

    channel.register_method("locale.get_conf", _locale_get_conf)
    channel.register_method("locale.set_conf", _locale_set_conf)
