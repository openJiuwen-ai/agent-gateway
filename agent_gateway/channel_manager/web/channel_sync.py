# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Push Gateway-owned channel settings to the local personal AgentServer."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from copy import deepcopy
import logging
from typing import Any
from uuid import uuid4

from e2a.message import ReqMethod
from e2a.models import E2AEnvelope

from agent_gateway.routing.e2a_proxy import is_agentos_routing_client

logger = logging.getLogger(__name__)

_SYNC_TIMEOUT_SECONDS = 5.0
_AS_CHANNEL_SECTIONS = frozenset(
    {"feishu", "feishu_enterprise", "dingtalk", "welink", "xiaoyi"}
)
_PENDING_CHANNEL_SYNC_TASKS: set[asyncio.Task[Any]] = set()


def select_channel_snapshot(channels: Any) -> dict[str, Any]:
    """Keep only the channel data AgentServer consumes."""
    if not isinstance(channels, Mapping):
        return {}
    selected: dict[str, Any] = {}
    for channel_id, section in channels.items():
        if not isinstance(channel_id, str) or not isinstance(section, Mapping):
            continue
        if channel_id in _AS_CHANNEL_SECTIONS:
            selected[channel_id] = deepcopy(dict(section))
        elif "send_file_allowed" in section:
            selected[channel_id] = {
                "send_file_allowed": deepcopy(section["send_file_allowed"])
            }
    return selected


def merge_channel_snapshots(*sources: Any) -> dict[str, Any]:
    """Merge channel snapshots and drop sections AgentServer does not need."""
    merged: dict[str, Any] = {}
    for source in sources:
        if isinstance(source, Mapping):
            merged.update(source)
    return select_channel_snapshot(merged)


def _channel_sync_task_done(task: asyncio.Task[Any]) -> None:
    _PENDING_CHANNEL_SYNC_TASKS.discard(task)
    if task.cancelled():
        return
    error = task.exception()
    if error is not None:
        logger.warning("[ChannelSync] background sync failed: %s", type(error).__name__)


def schedule_channel_config(agent_client: Any, channel_id: str, section: Any) -> None:
    """Schedule a best-effort channel sync without blocking the caller."""
    async def run() -> None:
        await sync_channel_config(agent_client, channel_id, section)

    try:
        task = asyncio.create_task(run())
    except RuntimeError:
        logger.debug("[ChannelSync] no running loop; skip %s sync", channel_id)
        return
    _PENDING_CHANNEL_SYNC_TASKS.add(task)
    task.add_done_callback(_channel_sync_task_done)


async def sync_channel_config(
    agent_client: Any,
    channel_id: str,
    section: Any,
) -> bool:
    """Best-effort snapshot sync; Gateway persistence must not depend on it."""
    if agent_client is None or not isinstance(section, dict):
        return False
    changes = select_channel_snapshot({channel_id: section})
    if not changes:
        return False

    if is_agentos_routing_client(agent_client):
        fanout = getattr(agent_client, "sync_channel_config", None)
        if not callable(fanout):
            return False
        try:
            return bool(
                await asyncio.wait_for(
                    fanout(changes),
                    timeout=_SYNC_TIMEOUT_SECONDS,
                )
            )
        except Exception as exc:  # noqa: BLE001 - best effort
            logger.warning(
                "[ChannelSync] %s fanout snapshot push failed: %s",
                channel_id,
                type(exc).__name__,
            )
            return False

    envelope = E2AEnvelope(
        request_id=uuid4().hex,
        channel="web",
        method=ReqMethod.JIUWENMATE_CHANNEL_CONFIG_SYNC.value,
        params={"channels": changes},
    )
    try:
        response = await asyncio.wait_for(
            agent_client.send_request(envelope), timeout=_SYNC_TIMEOUT_SECONDS
        )
    except Exception as exc:  # noqa: BLE001 - best effort
        logger.warning(
            "[ChannelSync] %s snapshot push failed: %s",
            channel_id,
            type(exc).__name__,
        )
        return False
    if not getattr(response, "ok", False):
        logger.warning("[ChannelSync] %s snapshot rejected by AgentServer", channel_id)
        return False
    return True
