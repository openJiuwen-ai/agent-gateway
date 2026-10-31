# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Config-save side effects shared by Web and TUI gateways."""

from __future__ import annotations

import logging
import asyncio
import uuid as uuid_module
from dataclasses import dataclass
from typing import Any, Callable

from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
from openjiuwen_protocol.message import ReqMethod

logger = logging.getLogger(__name__)

_BROWSER_RUNTIME_KEYS = frozenset(
    {
        "MODEL_PROVIDER",
        "MODEL_NAME",
        "API_BASE",
        "API_KEY",
        "VIDEO_PROVIDER",
        "VIDEO_MODEL_NAME",
        "VIDEO_API_BASE",
        "VIDEO_API_KEY",
        "AUDIO_PROVIDER",
        "AUDIO_MODEL_NAME",
        "AUDIO_API_BASE",
        "AUDIO_API_KEY",
        "VISION_PROVIDER",
        "VISION_MODEL_NAME",
        "VISION_API_BASE",
        "VISION_API_KEY",
    }
)
_RELOAD_MAX_RETRIES = 3
_RELOAD_RETRY_BACKOFF_BASE = 2.0  # seconds: 2, 4, 8
_PROACTIVE_KEYS = frozenset({"proactive_recommendation_enabled"})


@dataclass(frozen=True)
class ConfigSavedContext:
    """Dependencies required by the config-save callback."""

    client: Any
    message_handler: Any
    cron_controller: Any
    restart_request: Any
    schedule_agent_prewarm_sync: Callable[..., Any]
    schedule_gateway_restart: Callable[[Any], Any]


async def sync_saved_topic_recommendation(
    client: Any,
    *,
    updated_env_keys: Any,
    reload_scopes: Any,
    config_payload: dict[str, Any] | None,
) -> None:
    """Ask AgentServer to apply a saved topic switch or model.

    ``fetch_agent_unary`` reports a down or rejecting AgentServer as
    ``(False, payload)`` and does not raise. The config file is already saved,
    so this only logs; it does not fail the save.
    """
    from agent_gateway.routing.e2a_proxy import fetch_agent_unary

    try:
        ok, payload = await fetch_agent_unary(
            agent_client=client,
            req_method=ReqMethod.RECOMMENDATION,
            params={
                "action": "apply_saved_config",
                "updated_env_keys": sorted(updated_env_keys or []),
                "reload_scopes": sorted(reload_scopes),
                "config": dict(config_payload or {}),
            },
            session_id="sess_reload",
            user_id="agentos_test",
            channel_id="web",
            label="recommendation",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("[App] topic recommendation config sync failed: %s", exc)
        return
    if ok:
        return
    detail = payload if isinstance(payload, dict) else {}
    logger.warning(
        "[App] topic recommendation config sync failed: code=%s error=%s",
        detail.get("code"),
        detail.get("error"),
    )


async def handle_config_saved(
    context: ConfigSavedContext,
    *,
    updated_env_keys: set[str] | None = None,
    env_updates: dict[str, str] | None = None,
    config_payload: dict[str, Any] | None = None,
    reload_options: dict[str, Any] | None = None,
) -> bool:
    """Apply config-save side effects after AgentServer reload."""
    context.client.set_or_update_server_config(
        config=dict(config_payload or {}),
        env=dict(env_updates or {}),
    )

    from agent_gateway.common.config_panel.config_set_handlers import (
        needs_agent_reload,
    )

    reload_scopes = {
        str(scope)
        for scope in (dict(reload_options or {}).get("reload_scopes") or [])
        if isinstance(scope, str) and scope
    }

    if needs_agent_reload(reload_scopes):
        reload_env = e2a_from_agent_fields(
            request_id=f"agent-reload-{uuid_module.uuid4().hex[:8]}",
            channel_id="",
            session_id="sess_reload",
            user_id="agentos_test",
            req_method=ReqMethod.AGENT_RELOAD_CONFIG,
            params={
                "config": dict(config_payload or {}),
                "env": dict(env_updates or {}),
                **dict(reload_options or {}),
            },
        )

        reload_ok = False
        last_error: Exception | None = None
        runtime_failed_reject = False
        for attempt in range(_RELOAD_MAX_RETRIES + 1):
            try:
                reload_resp = await context.client.send_request(reload_env)
                if getattr(reload_resp, "ok", False):
                    reload_ok = True
                    break
                err_payload = getattr(reload_resp, "payload", None) or {}
                err_msg = (
                    err_payload.get("error")
                    if isinstance(err_payload, dict)
                    else err_payload
                )
                err_str = str(err_msg or "")
                if any(kw in err_str for kw in ("ValidationError", "validation error", "Field required")):
                    logger.warning(
                        "[App] agent.reload_config validation error (non-fatal): %s", err_str
                    )
                    return False
                last_error = RuntimeError(f"agent.reload_config rejected: {err_msg}")
                runtime_failed_reject = (
                    "RUNTIME_FAILED" in err_str or "runtime failed" in err_str.lower()
                )
            except Exception as e:  # noqa: BLE001
                last_error = e
                runtime_failed_reject = False

            if attempt < _RELOAD_MAX_RETRIES:
                delay = _RELOAD_RETRY_BACKOFF_BASE ** (attempt + 1)
                logger.warning(
                    "[App] agent.reload_config attempt %d/%d failed, retrying in %.1fs: %s",
                    attempt + 1,
                    _RELOAD_MAX_RETRIES + 1,
                    delay,
                    last_error,
                )
                await asyncio.sleep(delay)

        if not reload_ok and runtime_failed_reject:
            for _attempt in range(2):
                await asyncio.sleep(8.0)
                try:
                    reload_resp = await context.client.send_request(reload_env)
                    if getattr(reload_resp, "ok", False):
                        reload_ok = True
                        break
                except Exception as e:  # noqa: BLE001
                    last_error = e
            if reload_ok:
                logger.info("[App] agent.reload_config succeeded after runtime recovery")

        if not reload_ok and not runtime_failed_reject:
            logger.critical(
                "[App] agent.reload_config failed after %d attempts, scheduling restart: %s",
                _RELOAD_MAX_RETRIES + 1,
                last_error,
            )
            context.schedule_gateway_restart(context.restart_request)
            return False

        if not reload_ok:
            logger.critical(
                "[App] agent.reload_config rejected with RUNTIME_FAILED; "
                "AgentServer runtime recovery failed, keeping Gateway up: %s",
                last_error,
            )
            return False

        context.message_handler.update_evolution_auto_save(config_payload)
        context.schedule_agent_prewarm_sync(
            "agent-prewarm-sync-after-config",
            delay_seconds=3.0,
        )

        if updated_env_keys and (_BROWSER_RUNTIME_KEYS & set(updated_env_keys)):
            restart_env = e2a_from_agent_fields(
                request_id=f"browser-restart-{uuid_module.uuid4().hex[:8]}",
                channel_id="",
                session_id="sess_reload",
                user_id="agentos_test",
                req_method=ReqMethod.BROWSER_RUNTIME_RESTART,
            )
            try:
                await context.client.send_request(restart_env)
            except Exception as e:  # noqa: BLE001
                logger.warning("[App] browser runtime restart failed (non-fatal): %s", e)

    if updated_env_keys and (_PROACTIVE_KEYS & set(updated_env_keys)):
        try:
            from agent_gateway.cron.proactive_cron_sync import (
                sync_proactive_tick_job,
            )
            await sync_proactive_tick_job(context.cron_controller, config_payload)
        except Exception as e:  # noqa: BLE001
            logger.warning("[App] proactive.tick sync on config save failed: %s", e)

    should_sync_topic = bool(
        (updated_env_keys and "topic_recommendation_enabled" in updated_env_keys)
        or "model" in reload_scopes
    )
    if should_sync_topic:
        from agent_gateway.channel_manager.web.web_handler_constants import (
            spawn_background_task,
        )
        spawn_background_task(
            sync_saved_topic_recommendation(
                context.client,
                updated_env_keys=updated_env_keys,
                reload_scopes=reload_scopes,
                config_payload=config_payload,
            ),
            name="topic-recommendation-config-sync",
        )
    return True
