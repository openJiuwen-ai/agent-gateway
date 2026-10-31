# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Channel configuration RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import logging
import math
from typing import Any

from agent_gateway.common.config import (
    get_config_raw,
    replace_channel_subsection_with_cleanup,
    update_channel_in_config,
    update_health_check_in_config,
)


logger = logging.getLogger(__name__)

_WECHAT_NUMERIC_BOUNDS: dict[str, tuple[float, float, bool]] = {
    "qrcode_poll_interval_sec": (0.1, 3600.0, False),
    "long_poll_timeout_sec": (1, 600, True),
    "backoff_base_sec": (0.1, 3600.0, False),
    "backoff_max_sec": (0.1, 3600.0, False),
}


def _validate_wechat_numeric_params(params: dict) -> str | None:
    """校验微信通道四个数值参数。合法返回 None，非法返回中文错误描述。"""
    def _as_number(value: Any) -> float | None:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        val = float(value)
        return val if math.isfinite(val) else None

    for key, (lo, hi, is_int) in _WECHAT_NUMERIC_BOUNDS.items():
        if key not in params:
            continue
        val = _as_number(params[key])
        if val is None:
            return f"{key} 需为有限数值"
        if is_int and val != int(val):
            return f"{key} 需为整数"
        if not (lo <= val <= hi):
            return f"{key} 需在 {lo}–{hi} 之间"

    base = _as_number(params.get("backoff_base_sec"))
    mx = _as_number(params.get("backoff_max_sec"))
    if base is not None and mx is not None and mx < base:
        return "backoff_max_sec 不得小于 backoff_base_sec"
    return None


_FEISHU_APP_DEFAULTS: dict[str, Any] = {
    "name": "默认应用",
    "is_default": False,
    "enabled": True,
    "app_id": "",
    "app_secret": "",
    "encrypt_key": "",
    "verification_token": "",
    "allow_from": ["0.0.0.0/0"],
    "enable_streaming": True,
    "group_digital_avatar": False,
    "my_user_id": "",
    "bot_name": "",
    "enable_memory": False,
}


def _merge_apps_by_id(
    new_apps: list[dict],
    existing_apps: list[dict],
) -> list[dict]:
    if not isinstance(existing_apps, list) or not existing_apps:
        return new_apps

    existing_by_app_id = {
        a["app_id"]: a
        for a in existing_apps
        if isinstance(a, dict) and a.get("app_id")
    }
    if not existing_by_app_id:
        return new_apps

    return [
        {
            **existing_by_app_id[app["app_id"]],
            **app,
            **{
                key: old
                for key, old in existing_by_app_id[app["app_id"]].items()
                if key in _SENSITIVE_APP_FIELDS and not app.get(key)
            },
        }
        if isinstance(app, dict) and app.get("app_id") in existing_by_app_id
        else app
        for app in new_apps
    ]


_SENSITIVE_APP_FIELDS = {"app_secret", "sk"}


def _normalize_feishu_conf(raw: dict) -> dict:
    if not isinstance(raw, dict):
        logger.debug("[normalize_feishu] 输入非 dict (%s), 返回空 apps", type(raw).__name__)
        return {"apps": []}
    if "apps" in raw:
        apps_raw = raw["apps"]
        app_names = [a.get("name", "?") for a in apps_raw] if isinstance(apps_raw, list) else []
        logger.debug(
            "[normalize_feishu] 多应用格式, apps=%d, names=%s",
            len(apps_raw) if isinstance(apps_raw, list) else -1,
            app_names,
        )
        apps = [
            {**_FEISHU_APP_DEFAULTS, **app}
            for app in apps_raw
        ]
        return {**raw, "apps": apps}
    keys_present = [k for k in ("app_id", "app_secret", "encrypt_key", "verification_token") if k in raw]
    logger.debug("[normalize_feishu] 旧平铺格式, keys=%s, 转为单 app", keys_present)
    return {
        **raw,
        "apps": [_normalize_single_feishu_to_app(raw)],
    }


def _normalize_single_feishu_to_app(raw: dict) -> dict:
    return {
        **_FEISHU_APP_DEFAULTS,
        "is_default": True,
        "name": raw.get("name", "默认应用"),
        "enabled": bool(raw.get("enabled", True)),
        "app_id": raw.get("app_id", ""),
        "app_secret": raw.get("app_secret", ""),
        "encrypt_key": raw.get("encrypt_key", ""),
        "verification_token": raw.get("verification_token", ""),
        "allow_from": raw.get("allow_from") or ["0.0.0.0/0"],
        "enable_streaming": bool(raw.get("enable_streaming", True)),
        "group_digital_avatar": bool(raw.get("group_digital_avatar", False)),
        "my_user_id": raw.get("my_user_id", ""),
        "bot_name": raw.get("bot_name", ""),
        "enable_memory": bool(raw.get("enable_memory", False)),
        **raw,
    }


_XIAOYI_APP_DEFAULTS: dict[str, Any] = {
    "name": "默认应用",
    "is_default": False,
    "enabled": True,
    "ak": "",
    "sk": "",
    "app_id": "",
    "api_id": "",
    "agent_id": "",
    "enable_streaming": True,
    "mode": "xiaoyi_channel",
    "push_id": "",
    "ws_url1": "wss://hag.cloud.huawei.com/openclaw/v1/ws/link",
    "ws_url2": "wss://116.63.174.231/openclaw/v1/ws/link",
    "phone_tools_enabled": False,
    "uid": "",
    "api_key": "",
    "push_url": "",
    "file_upload_url": "",
}


def _normalize_xiaoyi_conf(raw: dict) -> dict:
    if not isinstance(raw, dict):
        logger.debug("[normalize_xiaoyi] 输入非 dict (%s), 返回空 apps", type(raw).__name__)
        return {"apps": []}
    if "apps" in raw:
        apps_raw = raw["apps"]
        app_names = [a.get("name", "?") for a in apps_raw] if isinstance(apps_raw, list) else []
        logger.debug(
            "[normalize_xiaoyi] 多应用格式, apps=%d, names=%s",
            len(apps_raw) if isinstance(apps_raw, list) else -1,
            app_names,
        )
        apps = [
            {**_XIAOYI_APP_DEFAULTS, **app}
            for app in apps_raw
        ]
        return {**raw, "apps": apps}
    keys_present = [k for k in ("ak", "sk", "agent_id") if k in raw]
    logger.debug("[normalize_xiaoyi] 旧平铺格式, keys=%s, 转为单 app", keys_present)
    return {
        **raw,
        "apps": [_normalize_single_xiaoyi_to_app(raw)],
    }


def _normalize_single_xiaoyi_to_app(raw: dict) -> dict:
    return {
        **_XIAOYI_APP_DEFAULTS,
        "is_default": True,
        "name": raw.get("name", "默认应用"),
        "enabled": bool(raw.get("enabled", True)),
        "ak": raw.get("ak", ""),
        "sk": raw.get("sk", ""),
        "app_id": raw.get("app_id", ""),
        "api_id": str(raw.get("api_id") or ""),
        "agent_id": raw.get("agent_id", ""),
        "enable_streaming": bool(raw.get("enable_streaming", True)),
        "mode": raw.get("mode", "xiaoyi_channel"),
        "push_id": raw.get("push_id", ""),
        "ws_url1": raw.get("ws_url1", "wss://hag.cloud.huawei.com/openclaw/v1/ws/link"),
        "ws_url2": raw.get("ws_url2", "wss://116.63.174.231/openclaw/v1/ws/link"),
        "phone_tools_enabled": bool(raw.get("phone_tools_enabled", False)),
        "uid": raw.get("uid", ""),
        "api_key": raw.get("api_key", ""),
        "push_url": raw.get("push_url", ""),
        "file_upload_url": raw.get("file_upload_url", ""),
        **raw,
    }


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

    async def _channel_telegram_get_conf(ws, req_id, params, session_id):
        """返回 TelegramChannel 的当前配置（由 ChannelManager 管理）。"""
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
            conf = cm.get_conf("telegram")
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.telegram.get_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_telegram_set_conf(ws, req_id, params, session_id):
        """更新 TelegramChannel 的配置，并按新配置重新实例化通道。"""
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
            await cm.set_conf("telegram", params)
            conf = cm.get_conf("telegram")
            try:
                update_channel_in_config("telegram", conf)
                await _clear_agent_config_cache(_resolve(agent_client))
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.telegram.set_conf] 写回 config.yaml 失败: %s", e)
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.telegram.set_conf] %s", e)
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

    async def _channel_whatsapp_get_conf(ws, req_id, params, session_id):
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
            conf = cm.get_conf("whatsapp")
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.whatsapp.get_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_whatsapp_set_conf(ws, req_id, params, session_id):
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
            await cm.set_conf("whatsapp", params)
            conf = cm.get_conf("whatsapp")
            try:
                update_channel_in_config("whatsapp", conf)
                await _clear_agent_config_cache(_resolve(agent_client))
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.whatsapp.set_conf] 写回 config.yaml 失败: %s", e)
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.whatsapp.set_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_discord_get_conf(ws, req_id, params, session_id):
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
            conf = cm.get_conf("discord")
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.discord.get_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_discord_set_conf(ws, req_id, params, session_id):
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
            await cm.set_conf("discord", params)
            conf = cm.get_conf("discord")
            try:
                update_channel_in_config("discord", conf)
                await _clear_agent_config_cache(_resolve(agent_client))
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.discord.set_conf] 写回 config.yaml 失败: %s", e)
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.discord.set_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_slack_get_conf(ws, req_id, params, session_id):
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
            conf = cm.get_conf("slack")
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.slack.get_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_slack_set_conf(ws, req_id, params, session_id):
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
            await cm.set_conf("slack", params)
            conf = cm.get_conf("slack")
            try:
                update_channel_in_config("slack", conf)
                await _clear_agent_config_cache(_resolve(agent_client))
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.slack.set_conf] failed to persist config.yaml: %s", e)
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.slack.set_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_wecom_get_conf(ws, req_id, params, session_id):
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
            conf = cm.get_conf("wecom")
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.wecom.get_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_wecom_set_conf(ws, req_id, params, session_id):
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
            await cm.set_conf("wecom", params)
            conf = cm.get_conf("wecom")
            try:
                update_channel_in_config("wecom", conf)
                await _clear_agent_config_cache(_resolve(agent_client))
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.wecom.set_conf] 写回 config.yaml 失败: %s", e)
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.wecom.set_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_wechat_get_conf(ws, req_id, params, session_id):
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
            conf = cm.get_conf("wechat")
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.wechat.get_conf] %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")

    async def _channel_wechat_set_conf(ws, req_id, params, session_id):
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
        # 数值参数写盘前校验：拒绝负数 / 0 / 极大值 / 浮点越界 / 非数字，早于 set_conf 中断。
        numeric_error = _validate_wechat_numeric_params(params)
        if numeric_error is not None:
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=numeric_error,
                code="BAD_REQUEST",
            )
            return
        try:
            await cm.set_conf("wechat", params)
            conf = cm.get_conf("wechat")
            try:
                update_channel_in_config("wechat", conf)
                await _clear_agent_config_cache(_resolve(agent_client))
            except Exception as e:  # noqa: BLE001
                logger.warning("[channel.wechat.set_conf] 写回 config.yaml 失败: %s", e)
            await channel.send_response(ws, req_id, ok=True, payload={"config": conf})
        except Exception as e:  # noqa: BLE001
            logger.exception("[channel.wechat.set_conf] %s", e)
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
    channel.register_method("channel.telegram.get_conf", _channel_telegram_get_conf)
    channel.register_method("channel.telegram.set_conf", _channel_telegram_set_conf)
    channel.register_method("channel.dingtalk.get_conf", _channel_dingtalk_get_conf)
    channel.register_method("channel.dingtalk.set_conf", _channel_dingtalk_set_conf)
    channel.register_method("channel.whatsapp.get_conf", _channel_whatsapp_get_conf)
    channel.register_method("channel.whatsapp.set_conf", _channel_whatsapp_set_conf)
    channel.register_method("channel.discord.get_conf", _channel_discord_get_conf)
    channel.register_method("channel.discord.set_conf", _channel_discord_set_conf)
    channel.register_method("channel.slack.get_conf", _channel_slack_get_conf)
    channel.register_method("channel.slack.set_conf", _channel_slack_set_conf)
    channel.register_method("channel.wecom.get_conf", _channel_wecom_get_conf)
    channel.register_method("channel.wecom.set_conf", _channel_wecom_set_conf)
    channel.register_method("channel.wechat.get_conf", _channel_wechat_get_conf)
    channel.register_method("channel.wechat.set_conf", _channel_wechat_set_conf)
    channel.register_method("channel.wechat.get_login_ui", _channel_wechat_get_login_ui)
    channel.register_method("channel.wechat.unbind", _channel_wechat_unbind)
