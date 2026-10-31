# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Channel configuration RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from typing import Any, Callable

from agent_gateway.common.config import (
    get_config_raw,
    replace_channel_subsection_with_cleanup,
    update_channel_in_config,
    update_health_check_in_config,
)


logger = logging.getLogger(__name__)

async def _send_rpc_error(
    channel: Any,
    ws: Any,
    req_id: Any,
    *,
    error: str,
    code: str,
) -> None:
    await channel.send_response(ws, req_id, ok=False, error=error, code=code)


from agent_gateway.channel_manager.web.channel_config_normalizers import (
    _FEISHU_APP_DEFAULTS,
    _XIAOYI_APP_DEFAULTS,
    _merge_apps_by_id,
    _normalize_feishu_conf,
    _normalize_single_feishu_to_app,
    _normalize_single_xiaoyi_to_app,
    _normalize_xiaoyi_conf,
    _validate_wechat_numeric_params,
)


@dataclass(frozen=True)
class _StandardChannelSpec:
    channel_id: str
    validate_params: Callable[[dict[str, Any]], str | None] | None = None


_STANDARD_CHANNEL_SPECS_BEFORE_DINGTALK = (
    _StandardChannelSpec("telegram"),
)
_STANDARD_CHANNEL_SPECS_AFTER_DINGTALK = (
    _StandardChannelSpec("whatsapp"),
    _StandardChannelSpec("discord"),
    _StandardChannelSpec("slack"),
    _StandardChannelSpec("wecom"),
    _StandardChannelSpec("wechat", _validate_wechat_numeric_params),
)


def register_channel_config_handlers(
    channel: Any,
    channel_manager: Any,
    heartbeat_service: Any,
    agent_client: Any,
    _resolve: Any,
    _schedule_clear_agent_config_cache: Any,
    _clear_agent_config_cache: Any,
) -> None:
    """Register channel configuration RPC handlers in original order."""

    def _register_standard_channel_conf_handlers(
        channel: Any,
        channel_manager: Any,
        agent_client: Any,
        _resolve: Any,
        _clear_agent_config_cache: Any,
        *,
        channel_id: str,
        validate_params: Callable[[dict[str, Any]], str | None] | None = None,
    ) -> None:
        async def _get_conf(ws, req_id, params, session_id):
            cm = _resolve(channel_manager)
            if cm is None:
                await _send_rpc_error(
                    channel,
                    ws,
                    req_id,
                    error="channel manager not available",
                    code="SERVICE_UNAVAILABLE",
                )
                return
            try:
                conf = cm.get_conf(channel_id)
                await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
            except Exception as e:  # noqa: BLE001
                logger.exception(f"[channel.{channel_id}.get_conf] %s", e)
                await _send_rpc_error(
                    channel,
                    ws,
                    req_id,
                    error=str(e),
                    code="INTERNAL_ERROR",
                )

        async def _set_conf(ws, req_id, params, session_id):
            cm = _resolve(channel_manager)
            if cm is None:
                await _send_rpc_error(
                    channel,
                    ws,
                    req_id,
                    error="channel manager not available",
                    code="SERVICE_UNAVAILABLE",
                )
                return
            if not isinstance(params, dict):
                await _send_rpc_error(
                    channel,
                    ws,
                    req_id,
                    error="params must be object",
                    code="BAD_REQUEST",
                )
                return
            if validate_params is not None:
                validation_error = validate_params(params)
                if validation_error is not None:
                    await _send_rpc_error(
                        channel,
                        ws,
                        req_id,
                        error=validation_error,
                        code="BAD_REQUEST",
                    )
                    return
            try:
                await cm.set_conf(channel_id, params)
                conf = cm.get_conf(channel_id)
                try:
                    update_channel_in_config(channel_id, conf)
                    await _clear_agent_config_cache(_resolve(agent_client))
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"[channel.{channel_id}.set_conf] 写回 config.yaml 失败: %s", e)
                await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
            except Exception as e:  # noqa: BLE001
                logger.exception(f"[channel.{channel_id}.set_conf] %s", e)
                await _send_rpc_error(
                    channel,
                    ws,
                    req_id,
                    error=str(e),
                    code="INTERNAL_ERROR",
                )

        channel.register_method(f"channel.{channel_id}.get_conf", _get_conf)
        channel.register_method(f"channel.{channel_id}.set_conf", _set_conf)

    async def _health_check_get_conf(ws, req_id, params, session_id):
        """返回当前探活配置（every / target / active_hours）。"""
        hb = _resolve(heartbeat_service)
        if hb is None:
            await channel.send_response(ws, req_id, ok=False, error="health_check service not available",
                                        code="SERVICE_UNAVAILABLE")
            return
        try:
            payload = dict(hb.get_health_check_conf())
            await channel.send_response(ws, req_id, ok=True, payload=payload)
        except Exception as e:
            logger.exception("[health_check.get_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _health_check_set_conf(ws, req_id, params, session_id):
        """更新探活配置并重启探活服务；params 可含 every、target、active_hours。"""
        hb = _resolve(heartbeat_service)
        if hb is None:
            await channel.send_response(ws, req_id, ok=False, error="heartbeat service not available",
                                        code="SERVICE_UNAVAILABLE")
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        try:
            every = params.get("every")
            target = params.get("target")
            active_hours = params.get("active_hours")
            if every is not None:
                every = float(every)
            if target is not None:
                target = str(target)
            if active_hours is not None:
                if not isinstance(active_hours, dict):
                    active_hours = None
                elif active_hours and ("start" not in active_hours or "end" not in active_hours):
                    # 必须同时包含 start/end，否则视为清除时间段（始终生效）
                    active_hours = None

            # 先检查：如果目标渠道是飞书，检测是否有可用的推送目标
            if target == "feishu":
                try:
                    raw = get_config_raw() or {}
                    ch_cfg = (raw.get("channels") or {}).get("feishu") or {}
                    # V2 多应用：心跳 relay 会 fan-out 到同 channel_id 的全部 app，每个 app 各走
                    # 自己的 last_chat_id/chat_id 投递；故要求「每个 app 都有目标」才算可用，
                    # 否则缺失目标的 app 每次 tick 都会静默投递失败。
                    apps = ch_cfg.get("apps") or []
                    if isinstance(apps, list) and apps:
                        has_target = all(
                            isinstance(app, dict)
                            and (
                                bool(str(app.get("last_chat_id") or "").strip())
                                or bool(str(app.get("chat_id") or "").strip())
                            )
                            for app in apps
                        )
                    else:
                        # 旧平铺格式（单应用）：兜底看顶层 last_chat_id/chat_id。
                        has_target = bool(
                            str(ch_cfg.get("last_chat_id") or "").strip()
                            or str(ch_cfg.get("chat_id") or "").strip()
                        )
                    if not has_target:
                        await channel.send_response(
                            ws, req_id, ok=False,
                            error="feishuNoTarget",
                            code="feishuNoTarget",
                        )
                        return
                except Exception as e:
                    logger.debug("[health_check.set_conf] 飞书目标检测异常: %s", e)
                    await channel.send_response(
                        ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR",
                    )
                    return

            # 检查通过后再保存配置
            await hb.set_health_check_conf(every=every, target=target, active_hours=active_hours)
            payload = dict(hb.get_health_check_conf())
            should_clear_agent_config_cache = False
            try:
                update_health_check_in_config(payload)
                should_clear_agent_config_cache = True
            except Exception as e:  # noqa: BLE001
                logger.warning("[health_check.set_conf] 写回 config.yaml 失败: %s", e)
            try:
                await channel.send_response(ws, req_id, ok=True, payload=payload)
            finally:
                if should_clear_agent_config_cache:
                    _schedule_clear_agent_config_cache("health_check.set_conf")
        except ValueError as e:
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="BAD_REQUEST")
        except Exception as e:
            logger.exception("[health_check.set_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_feishu_get_conf(ws, req_id, params, session_id):
        """返回 FeishuChannel 的当前配置（由 ChannelManager 管理）。"""
        cm = _resolve(channel_manager)
        if cm is None:
            logger.warning("[channel.feishu.get_conf] channel_manager not available, req_id=%s", req_id)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="channel manager not available",
                code="SERVICE_UNAVAILABLE",
            )
            return
        try:
            raw = cm.get_conf("feishu")
            conf = _normalize_feishu_conf(raw)
            apps = conf.get("apps", [])
            app_names = [a.get("name", "?") for a in apps]
            logger.debug(
                "[channel.feishu.get_conf] ok, req_id=%s, apps=%d, names=%s",
                req_id, len(apps), app_names,
            )
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.feishu.get_conf] 异常, req_id=%s: %s", req_id, e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_feishu_set_conf(ws, req_id, params, session_id):
        """更新 FeishuChannel 的配置，并按新配置重新实例化通道。

        ``params`` 必须含 ``apps`` 键，保存到 channels.feishu.apps。
        """
        cm = _resolve(channel_manager)
        if cm is None:
            logger.warning("[channel.feishu.set_conf] channel_manager not available, req_id=%s", req_id)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="channel manager not available",
                code="SERVICE_UNAVAILABLE",
            )
            return
        if not isinstance(params, dict):
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="params must be object",
                code="BAD_REQUEST",
            )
            return
        try:
            # 多应用模式：params 必须含 apps 键
            apps = params["apps"]
            app_names = [a.get("name", "?") for a in apps]
            logger.debug(
                "[channel.feishu.set_conf] req_id=%s, apps=%d, names=%s",
                req_id, len(apps), app_names,
            )
            # 先归一化（用 _FEISHU_APP_DEFAULTS 补充前端未发送的字段），再持久化
            normalized_apps = _normalize_feishu_conf({"apps": apps})["apps"]
            # 从 cm 读取已有 apps，按 app_id 合并保留未发送的敏感字段
            existing_feishu = cm.get_conf("feishu")
            existing_apps = existing_feishu.get("apps", []) if isinstance(existing_feishu, dict) else []
            merged_apps = _merge_apps_by_id(normalized_apps, existing_apps)
            await cm.set_conf("feishu", {"apps": merged_apps})
            should_clear_agent_config_cache = False
            try:
                replace_channel_subsection_with_cleanup("feishu", "apps", merged_apps, {"apps", "send_file_allowed"})
                should_clear_agent_config_cache = True
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.feishu.set_conf] 写回 config.yaml apps 失败: %s", e)
            try:
                await channel.send_response(ws, req_id, ok=True, payload={"config": {"apps": merged_apps}})
            finally:
                if should_clear_agent_config_cache:
                    _schedule_clear_agent_config_cache("channel.feishu.set_conf")
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.feishu.set_conf] 异常, req_id=%s: %s", req_id, e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_xiaoyi_get_conf(ws, req_id, params, session_id):
        """返回 XiaoyiChannel 的当前配置（由 ChannelManager 管理）。"""
        cm = _resolve(channel_manager)
        if cm is None:
            logger.warning("[channel.xiaoyi.get_conf] channel_manager not available, req_id=%s", req_id)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="channel manager not available",
                code="SERVICE_UNAVAILABLE",
            )
            return
        try:
            raw = cm.get_conf("xiaoyi")
            conf = _normalize_xiaoyi_conf(raw)
            apps = conf.get("apps", [])
            app_names = [a.get("name", "?") for a in apps]
            logger.debug(
                "[channel.xiaoyi.get_conf] ok, req_id=%s, apps=%d, names=%s",
                req_id, len(apps), app_names,
            )
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.xiaoyi.get_conf] 异常, req_id=%s: %s", req_id, e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_xiaoyi_set_conf(ws, req_id, params, session_id):
        """更新 XiaoyiChannel 的配置，并按新配置重新实例化通道。

        ``params`` 必须含 ``apps`` 键，保存到 channels.xiaoyi.apps。
        """
        cm = _resolve(channel_manager)
        if cm is None:
            logger.warning("[channel.xiaoyi.set_conf] channel_manager not available, req_id=%s", req_id)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="channel manager not available",
                code="SERVICE_UNAVAILABLE",
            )
            return
        if not isinstance(params, dict):
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="params must be object",
                code="BAD_REQUEST",
            )
            return
        try:
            # 多应用模式：params 必须含 apps 键
            apps = params["apps"]
            app_names = [a.get("name", "?") for a in apps]
            logger.debug(
                "[channel.xiaoyi.set_conf] req_id=%s, apps=%d, names=%s",
                req_id, len(apps), app_names,
            )
            # 先归一化（用 _XIAOYI_APP_DEFAULTS 补充前端未发送的字段），再持久化
            normalized_apps = _normalize_xiaoyi_conf({"apps": apps})["apps"]
            # 从 cm 读取已有 apps，按 app_id 合并保留未发送的敏感字段
            existing_xiaoyi = cm.get_conf("xiaoyi")
            existing_apps = existing_xiaoyi.get("apps", []) if isinstance(existing_xiaoyi, dict) else []
            merged_apps = _merge_apps_by_id(normalized_apps, existing_apps)
            await cm.set_conf("xiaoyi", {"apps": merged_apps})
            try:
                replace_channel_subsection_with_cleanup("xiaoyi", "apps", merged_apps, {"apps", "send_file_allowed"})
                await _clear_agent_config_cache(_resolve(agent_client))
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.xiaoyi.set_conf] 写回 config.yaml apps 失败: %s", e)
            await channel.send_response(ws, req_id, ok=True, payload={"config": {"apps": merged_apps}})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.xiaoyi.set_conf] 异常, req_id=%s: %s", req_id, e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_dingtalk_get_conf(ws, req_id, params, session_id):
        cm = _resolve(channel_manager)
        if cm is None:
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="channel manager not available",
                code="SERVICE_UNAVAILABLE",
            )
            return
        try:
            conf = cm.get_conf("dingtalk")
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.dingtalk.get_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_dingtalk_set_conf(ws, req_id, params, session_id):
        cm = _resolve(channel_manager)
        if cm is None:
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="channel manager not available",
                code="SERVICE_UNAVAILABLE",
            )
            return
        if not isinstance(params, dict):
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="params must be object",
                code="BAD_REQUEST",
            )
            return
        try:
            await cm.set_conf("dingtalk", params)
            conf = cm.get_conf("dingtalk")
            should_clear_agent_config_cache = False
            try:
                update_channel_in_config("dingtalk", conf)
                should_clear_agent_config_cache = True
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.dingtalk.set_conf] 写回 config.yaml 失败: %s", e)
            try:
                await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
            finally:
                if should_clear_agent_config_cache:
                    _schedule_clear_agent_config_cache("channel.dingtalk.set_conf")
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.dingtalk.set_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_wechat_get_login_ui(ws, req_id, params, session_id):
        from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import (
            snapshot_wechat_login_ui_state,
        )

        try:
            ui = await snapshot_wechat_login_ui_state()
            if "updated_at" in ui and isinstance(ui["updated_at"], (int, float)):
                ui["updated_at"] = int(ui["updated_at"])
            await channel.send_response(ws, req_id, ok=True, payload=ui)
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.wechat.get_login_ui] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_wechat_unbind(ws, req_id, params, session_id):
        cm = _resolve(channel_manager)
        if cm is None:
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="channel manager not available",
                code="SERVICE_UNAVAILABLE",
            )
            return
        try:
            from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import (
                clear_wechat_bound_session,
                reset_wechat_login_ui_state,
            )

            conf = cm.get_conf("wechat")
            new_conf = clear_wechat_bound_session(conf)
            await reset_wechat_login_ui_state()
            # 若 YAML 里 bot_token 本就为空，仅删凭据文件时 dict 与上次相同，
            # _should_restart_channel 不会重启，扫码 UI 会一直停在 idle
            cm.mark_channel_restart_pending("wechat")
            await cm.set_conf("wechat", new_conf)
            final = cm.get_conf("wechat")
            try:
                update_channel_in_config("wechat", final)
                await _clear_agent_config_cache(_resolve(agent_client))
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.wechat.unbind] 写回 config.yaml 失败: %s", e)
            await channel.send_response(ws, req_id, ok=True, payload={"config": final})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.wechat.unbind] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    channel.register_method("health_check.get_conf", _health_check_get_conf)
    channel.register_method("health_check.set_conf", _health_check_set_conf)
    channel.register_method("heartbeat.get_conf", _health_check_get_conf)
    channel.register_method("heartbeat.set_conf", _health_check_set_conf)
    channel.register_method("channel.feishu.get_conf", _channel_feishu_get_conf)
    channel.register_method("channel.feishu.set_conf", _channel_feishu_set_conf)
    channel.register_method("channel.xiaoyi.get_conf", _channel_xiaoyi_get_conf)
    channel.register_method("channel.xiaoyi.set_conf", _channel_xiaoyi_set_conf)
    for spec in _STANDARD_CHANNEL_SPECS_BEFORE_DINGTALK:
        _register_standard_channel_conf_handlers(
            channel,
            channel_manager,
            agent_client,
            _resolve,
            _clear_agent_config_cache,
            channel_id=spec.channel_id,
            validate_params=spec.validate_params,
        )
    channel.register_method("channel.dingtalk.get_conf", _channel_dingtalk_get_conf)
    channel.register_method("channel.dingtalk.set_conf", _channel_dingtalk_set_conf)
    for spec in _STANDARD_CHANNEL_SPECS_AFTER_DINGTALK:
        _register_standard_channel_conf_handlers(
            channel,
            channel_manager,
            agent_client,
            _resolve,
            _clear_agent_config_cache,
            channel_id=spec.channel_id,
            validate_params=spec.validate_params,
        )
    channel.register_method("channel.wechat.get_login_ui", _channel_wechat_get_login_ui)
    channel.register_method("channel.wechat.unbind", _channel_wechat_unbind)
