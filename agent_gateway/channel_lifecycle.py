# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Lifecycle helpers for optional Gateway channels."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger("gateway")

_OPTIONAL_CHANNEL_NAMES: tuple[str, ...] = (
    "feishu",
    "feishu_enterprise",
    "xiaoyi",
    "dingtalk",
    "telegram",
    "whatsapp",
    "discord",
    "slack",
    "wecom",
    "wechat",
    "ssh",
    "web_proxy",
)


def _should_restart_channel(channel_name: str, old_conf: dict, new_conf: dict) -> bool:
    old_channel_conf = old_conf.get(channel_name) if isinstance(old_conf, dict) else None
    new_channel_conf = new_conf.get(channel_name) if isinstance(new_conf, dict) else None
    if (old_channel_conf is None) != (new_channel_conf is None):
        return True
    if old_channel_conf is None:
        return False
    return old_channel_conf != new_channel_conf


def _changed_optional_channels(
    old_conf: dict,
    new_conf: dict,
    restart_pending: set[str],
) -> list[str]:
    """Return channels to restart, preserving the configured optional-channel order."""
    changed: list[str] = []
    for channel_name in _OPTIONAL_CHANNEL_NAMES:
        config_changed = _should_restart_channel(channel_name, old_conf, new_conf)
        force_restart = channel_name in restart_pending
        if config_changed or force_restart:
            if force_restart and not config_changed:
                logger.info(
                    "[App] channels.%s force restart requested; cached runtime state must be dropped",
                    channel_name,
                )
            changed.append(channel_name)
    return changed


async def _stop_channel(
    channel,
    task,
    channel_name: str,
    background_wait: bool = False,
    *,
    channel_manager,
) -> None:
    if task is not None:
        task.cancel()
        if background_wait:

            async def wait_cancel():
                try:
                    await task
                except (TypeError, asyncio.CancelledError):
                    logger.info("[App] cancelled previous %sChannel task", channel_name.capitalize())
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        "[App] ignored exception while waiting for previous %sChannel task: %s",
                        channel_name.capitalize(),
                        e,
                    )

            asyncio.create_task(wait_cancel(), name=f"wait_{channel_name}_cancel")
        else:
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "[App] timeout while waiting for %sChannel task cancellation",
                    channel_name.capitalize(),
                )
            except asyncio.CancelledError:
                pass
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "[App] ignored exception while waiting for previous %sChannel task: %s",
                    channel_name.capitalize(),
                    e,
                )

    if channel is not None:
        try:
            await asyncio.wait_for(channel.stop(), timeout=10.0)
        except asyncio.TimeoutError:
            logger.warning("[App] timeout while stopping %sChannel", channel_name.capitalize())
        except Exception as e:  # noqa: BLE001
            logger.warning("[App] failed to stop previous %sChannel: %s", channel_name.capitalize(), e)
        channel_manager.unregister_channel(channel.channel_id)


def _is_channel_enabled(conf: dict | None, required_fields: list[str]) -> tuple[bool, str]:
    if conf is None:
        return False, "missing or invalid config"
    enabled_raw = conf.get("enabled", None)
    if enabled_raw is None:
        all_fields_present = all(conf.get(f) for f in required_fields)
        return all_fields_present, f"missing {','.join(required_fields)}" if not all_fields_present else ""
    return bool(enabled_raw), "enabled = false" if not enabled_raw else ""
