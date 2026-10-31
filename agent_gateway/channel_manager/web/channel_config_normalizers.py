# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

"""Pure helpers for normalizing and validating channel configuration."""

from __future__ import annotations

import logging
import math
from typing import Any


logger = logging.getLogger(__name__)

_WECHAT_NUMERIC_BOUNDS: dict[str, tuple[float, float, bool]] = {
    "qrcode_poll_interval_sec": (0.1, 3600.0, False),
    "long_poll_timeout_sec": (1, 600, True),
    "backoff_base_sec": (0.1, 3600.0, False),
    "backoff_max_sec": (0.1, 3600.0, False),
}

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

_SENSITIVE_APP_FIELDS = {"app_secret", "sk"}


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
