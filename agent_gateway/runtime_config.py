# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Gateway startup configuration loading."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("gateway")


def resolve_health_check_config(
    full_cfg: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Prefer health_check and read legacy heartbeat probe keys during migration."""
    if not isinstance(full_cfg, dict):
        return None
    current = full_cfg.get("health_check")
    if isinstance(current, dict):
        return current
    legacy = full_cfg.get("heartbeat")
    if not isinstance(legacy, dict):
        return None
    migrated = {
        key: legacy[key]
        for key in ("every", "target", "active_hours")
        if key in legacy
    }
    return migrated or None


async def load_gateway_runtime_config(
    message_handler: Any,
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any] | None]:
    """Load Gateway config without coupling optional migrations to channels."""
    import agent_gateway.common.config as config_module

    try:
        if config_module.migrate_legacy_heartbeat_probe_config():
            logger.info(
                "[App] migrated legacy heartbeat probe config to health_check"
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[App] failed to migrate legacy heartbeat probe config; "
            "continuing with the existing config: %s",
            exc,
        )

    try:
        full_cfg = config_module.get_config()
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[App] failed to read config.yaml, using defaults: %s",
            exc,
        )
        return {}, None, None

    if not isinstance(full_cfg, dict):
        logger.warning(
            "[App] config.yaml root must be an object, using defaults"
        )
        return {}, None, None

    try:
        message_handler.update_evolution_auto_save(full_cfg)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[App] failed to apply evolution auto-save config; "
            "continuing with channel config: %s",
            exc,
        )

    health_check_cfg = resolve_health_check_config(full_cfg)
    if (
        isinstance(health_check_cfg, dict)
        and not isinstance(full_cfg.get("health_check"), dict)
    ):
        logger.warning(
            "[App] legacy heartbeat probe config detected; "
            "migrate it to health_check"
        )
    channels_cfg = full_cfg.get("channels")
    return (
        full_cfg,
        health_check_cfg,
        channels_cfg if isinstance(channels_cfg, dict) else None,
    )
