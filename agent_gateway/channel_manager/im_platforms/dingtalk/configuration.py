"""DingTalk app configuration: normalization, the default app and safe saving."""

import logging
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger(__name__)

_TEXT_FIELDS = ("name", "client_id", "client_secret", "persistent_agent_id", "robot_code", "api_base", "oapi_base")
_FILE_FIELDS = ("max_download_size", "download_timeout", "send_file_allowed", "enable_file_download", "workspace_dir")
# Keys that describe one app. In the legacy flat form they live directly under channels.dingtalk.
_APP_KEYS = ("name", "enabled", "client_id", "client_secret", "persistent_agent_id", "robot_code", "allow_from",
             "is_default")
# Ordinary-channel identity read by the existing cron fallback; it belongs to the default app only.
CRON_IDENTITY_KEYS = ("last_sender_id", "last_conversation_id", "last_conversation_type")


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _allow_list(value: Any) -> list[str] | None:
    """The allowlist, or None when the configured value is not a list of strings."""
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        return None
    return [item.strip() for item in value if item.strip()]


def allow_from_valid(app: Mapping[str, Any]) -> bool:
    """An empty list admits everyone, so a malformed allowlist must never be read as empty."""
    return _allow_list(app.get("allow_from")) is not None


def _normalize_app(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Give every field its declared type; YAML leaves empty keys as None."""
    app = dict(raw)
    app.pop("apps", None)
    for key in _TEXT_FIELDS:
        app[key] = _text(raw.get(key))
    allow_from = _allow_list(raw.get("allow_from"))
    # A malformed allowlist stays as written and makes the app invalid instead of admitting everyone.
    app["allow_from"] = raw.get("allow_from") if allow_from is None else allow_from
    enabled = raw.get("enabled")
    # An unset flag follows the channel startup rule: enabled once credentials exist.
    app["enabled"] = enabled if isinstance(enabled, bool) else bool(app["client_id"] and app["client_secret"])
    app["is_default"] = raw.get("is_default") is True
    return app


def _choose_default(apps: list[dict[str, Any]]) -> None:
    """The ordinary app is the default; otherwise only an app already marked as default stays default.

    Messages without an app identity, such as cron pushes, use the default app. A JM app
    never becomes the default by position, so deleting the ordinary app cannot silently
    move those messages to another robot.
    """
    ordinary = next((index for index, app in enumerate(apps) if not app["persistent_agent_id"]), None)
    marked = next((index for index, app in enumerate(apps) if app["is_default"]), None)
    default = ordinary if ordinary is not None else marked
    for index, app in enumerate(apps):
        app["is_default"] = index == default


def dingtalk_apps(raw: Any) -> list[dict[str, Any]]:
    """Read the app list, or the legacy single-app object, without mutating either."""
    if not isinstance(raw, Mapping):
        return []
    configured = raw.get("apps")
    if isinstance(configured, list):
        apps = [_normalize_app(app) for app in configured if isinstance(app, Mapping)]
    else:
        legacy = _normalize_app({key: raw[key] for key in _APP_KEYS if key in raw})
        if not legacy["client_id"]:
            return []
        legacy["is_default"] = True
        apps = [legacy]
    _choose_default(apps)
    return apps


def dingtalk_channel_settings(raw: Any, *, api_base: str, oapi_base: str) -> list[dict[str, Any]]:
    """Typed settings for each DingTalk channel to start; skipped apps are left out.

    Disabled, incomplete and duplicated apps do not start, and only one ordinary app is
    started. ``api_base`` and ``oapi_base`` are the defaults when neither the app nor the
    shared section names a domain. File settings use app values before shared values;
    unset values keep the channel model's defaults.
    """
    shared = raw if isinstance(raw, Mapping) else {}
    apps = dingtalk_apps(raw)
    ids = [app["client_id"] for app in apps]
    settings = []
    ordinary_started = False
    for app in apps:
        if not app["enabled"] or not app["client_id"] or not app["client_secret"] or ids.count(app["client_id"]) != 1:
            continue
        if not allow_from_valid(app):
            logger.warning("[DingTalk] app not started: allow_from must be a list of staff IDs: app_id=%s",
                           app["client_id"])
            continue
        if not app["persistent_agent_id"]:
            if ordinary_started:
                continue
            ordinary_started = True
        channel_settings = {
            "enabled": True,
            "client_id": app["client_id"],
            "client_secret": app["client_secret"],
            "agent_id": app["persistent_agent_id"],
            "robot_code": app["robot_code"],
            "is_default": app["is_default"],
            "allow_from": app["allow_from"],
            "api_base": app["api_base"] or _text(shared.get("api_base")) or api_base,
            "oapi_base": app["oapi_base"] or _text(shared.get("oapi_base")) or oapi_base,
        }
        for key in _FILE_FIELDS:
            value = app.get(key)
            if value is None:
                value = shared.get(key)
            if value is not None:
                channel_settings[key] = value
        settings.append(channel_settings)
    return settings
