# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Coordinate effects after a local save; never write or own configuration."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
import logging
from typing import Any, Protocol, TYPE_CHECKING
import uuid as uuid_module

from e2a.gateway_normalize import e2a_from_agent_fields
from agent_gateway.common.protocol_methods import ReqMethod

if TYPE_CHECKING:
    from e2a.agent_models import AgentResponse
    from e2a.models import E2AEnvelope

logger = logging.getLogger("gateway")


class ConfigReloadClient(Protocol):
    """The existing AS configuration snapshot and unary request boundary."""

    def set_or_update_server_config(
        self, *, config: dict[str, Any], env: dict[str, str] | None = None,
    ) -> None: ...

    async def send_request(self, envelope: E2AEnvelope) -> AgentResponse: ...


class PrewarmScheduler(Protocol):
    """Keep the original prewarm task name and delay keyword."""

    def __call__(self, name: str, *, delay_seconds: float = 1.0) -> None: ...


class BackgroundTaskSpawner(Protocol):
    """Create and retain a task with the original exception reporting policy."""

    def __call__(self, coro: Any, *, name: str) -> None: ...


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


class ConfigSavedEffectCoordinator:
    """Preserve local post-save ordering and failures through explicit actions."""

    def __init__(
        self,
        *,
        client: ConfigReloadClient,
        update_evolution_auto_save: Callable[[dict[str, Any] | None], None],
        schedule_prewarm: PrewarmScheduler,
        sync_proactive_tick: Callable[[dict[str, Any] | None], Awaitable[None]],
        schedule_restart: Callable[[], None],
        spawn_background: BackgroundTaskSpawner,
    ) -> None:
        self._client = client
        self._update_evolution_auto_save = update_evolution_auto_save
        self._schedule_prewarm = schedule_prewarm
        self._sync_proactive_tick = sync_proactive_tick
        self._schedule_restart = schedule_restart
        self._spawn_background = spawn_background

    async def __call__(
            self,
            updated_env_keys: set[str] | None = None,
            *,
            env_updates: dict[str, str] | None = None,
            config_payload: dict[str, Any] | None = None,
            reload_options: dict[str, Any] | None = None,
    ) -> bool:
        browser_runtime_keys = {
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
        _reload_max_retries = 3
        _reload_retry_backoff_base = 2.0  # seconds: 2, 4, 8

        self._client.set_or_update_server_config(
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
            # reload 是全局配置热重载，不绑定特定 user。但 faas 沙箱 acquire instance
            # 需要 X-Session-Context（由 envelope.user_id 透传成 sessionCtxID）；user_id 为空
            # 时 faas 拿不到 sessionCtxID，acquire lease 走 init 路径失败
            # （"connect runtime failed"），invocation 60s 超时，首登 config.set 后前端卡死+1001。
            # user_id 还必须是 faas 沙箱配置了可工作区的真实用户：faas 沙箱按 user_id
            # 映射 /home/<user>/.jiuwenswarm 工作区，非配置用户（infra/root）建沙箱失败
            # （code 80004 "Failed to create sandbox"）。agentos_test 是 agentos 部署的默认
            # 用户（gateway-config.yaml ssh username），有完整工作区，复用其 warm instance。
            reload_env = e2a_from_agent_fields(
                request_id=f"agent-reload-{uuid_module.uuid4().hex[:8]}",
                channel_id="",
                session_id="sess_reload",
                user_id="agentos_test",
                req_method=ReqMethod.AGENT_RELOAD_CONFIG,
                params={
                    # config: full config snapshot after save; Agent should prefer this over local yaml.
                    "config": dict(config_payload or {}),
                    # env: incremental environment updates; missing keys mean unchanged.
                    "env": dict(env_updates or {}),
                    **dict(reload_options or {}),
                },
            )

            def persistent_models_unapplied(response: Any) -> bool:
                payload = getattr(response, "payload", None)
                if isinstance(payload, dict) and payload.get("code") == "PERSISTENT_MODELS_NOT_APPLIED":
                    logger.warning("[App] worker could not apply persistent models: %s", payload.get("error"))
                    return True
                return False

            reload_ok = False
            last_error: Exception | None = None
            runtime_failed_reject = False
            for attempt in range(_reload_max_retries + 1):
                try:
                    reload_resp = await self._client.send_request(reload_env)
                    if getattr(reload_resp, "ok", False):
                        reload_ok = True
                        break
                    err_payload = getattr(reload_resp, "payload", None) or {}
                    if persistent_models_unapplied(reload_resp):
                        return False
                    err_msg = (
                        err_payload.get("error")
                        if isinstance(err_payload, dict)
                        else err_payload
                    )
                    err_str = str(err_msg or "")
                    # ValidationError 是配置格式问题，不需要重试也不需要重启 gateway
                    if any(kw in err_str for kw in ("ValidationError", "validation error", "Field required")):
                        logger.warning("[App] agent.reload_config validation error (non-fatal): %s", err_str)
                        return False
                    last_error = RuntimeError(f"agent.reload_config rejected: {err_msg}")
                    # AgentServer runtime 处于 FAILED：server 侧已因本次 reload 请求
                    # 触发 worker 重启自愈；Gateway 自重启修复不了它，只会断开前端。
                    runtime_failed_reject = (
                        "RUNTIME_FAILED" in err_str or "runtime failed" in err_str.lower()
                    )
                except Exception as e:  # noqa: BLE001
                    last_error = e
                    runtime_failed_reject = False

                if attempt < _reload_max_retries:
                    delay = _reload_retry_backoff_base ** (attempt + 1)
                    logger.warning(
                        "[App] agent.reload_config attempt %d/%d failed, retrying in %.1fs: %s",
                        attempt + 1, _reload_max_retries + 1, delay, last_error,
                    )
                    await asyncio.sleep(delay)

            if not reload_ok and runtime_failed_reject:
                # 恢复尝试未达成时再多等两轮：worker 可能仍在 warming（import 慢）。
                for _attempt in range(2):
                    await asyncio.sleep(8.0)
                    try:
                        reload_resp = await self._client.send_request(reload_env)
                        if getattr(reload_resp, "ok", False):
                            reload_ok = True
                            break
                        if persistent_models_unapplied(reload_resp):
                            return False
                    except Exception as e:  # noqa: BLE001
                        last_error = e
                if reload_ok:
                    logger.info("[App] agent.reload_config succeeded after runtime recovery")

            if not reload_ok and not runtime_failed_reject:
                logger.critical(
                    "[App] agent.reload_config failed after %d attempts, scheduling restart: %s",
                    _reload_max_retries + 1, last_error,
                )
                self._schedule_restart()
                return False

            if not reload_ok:
                # RUNTIME_FAILED：重启 Gateway 修复不了 AgentServer 的 runtime
                # （反而把前端断线）。保持 Gateway 在线；配置已写盘，下次保存
                # 会再次触发 reload 并重新尝试恢复。
                logger.critical(
                    "[App] agent.reload_config rejected with RUNTIME_FAILED; "
                    "AgentServer runtime recovery failed, keeping Gateway up: %s",
                    last_error,
                )
                return False

            # AgentServer 已确认热重载成功后，再刷新 Gateway 内存中的值。
            # MessageHandler 的流式 chunk 热路径只读取该值，不重新读盘。
            self._update_evolution_auto_save(config_payload)

            # reload 成功：config 变更后让 agent 侧 prewarm channels 落到新配置。
            self._schedule_prewarm(
                "agent-prewarm-sync-after-config",
                delay_seconds=3.0,
            )

            if updated_env_keys and (browser_runtime_keys & set(updated_env_keys)):
                restart_env = e2a_from_agent_fields(
                    request_id=f"browser-restart-{uuid_module.uuid4().hex[:8]}",
                    channel_id="",
                    session_id="sess_reload",
                    user_id="agentos_test",
                    req_method=ReqMethod.BROWSER_RUNTIME_RESTART,
                )
                try:
                    await self._client.send_request(restart_env)
                except Exception as e:  # noqa: BLE001
                    logger.warning("[App] browser runtime restart failed (non-fatal): %s", e)

        # 主动推荐：enabled 变更时同步 proactive.tick job（创建/删除）
        proactive_keys = {
            "proactive_recommendation_enabled",
        }
        if updated_env_keys and (proactive_keys & set(updated_env_keys)):
            try:
                await self._sync_proactive_tick(config_payload)
            except Exception as e:  # noqa: BLE001  # 兜底：proactive 同步失败不阻断配置保存
                logger.warning("[App] proactive.tick sync on config save failed: %s", e)

        # YAML is already on disk. Pool seed and model swap run on AgentServer.
        should_sync_topic = bool(
            (updated_env_keys and "topic_recommendation_enabled" in updated_env_keys)
            or "model" in reload_scopes
        )
        if should_sync_topic:
            self._spawn_background(
                sync_saved_topic_recommendation(
                    self._client,
                    updated_env_keys=updated_env_keys,
                    reload_scopes=reload_scopes,
                    config_payload=config_payload,
                ),
                name="topic-recommendation-config-sync",
            )
        return True
