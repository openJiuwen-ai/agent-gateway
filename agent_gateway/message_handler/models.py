# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure value models and payload helpers shared by message-handler adapters."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

from agent_gateway.common.mode_matrix import (
    NEW_AGENT_CODE_NORMAL,
    NEW_AGENT_CODE_PLAN,
    NEW_AGENT_WORK_NORMAL,
    NEW_AGENT_WORK_PLAN,
    NEW_CANONICAL_MODES,
    NEW_TEAM_CODE_NORMAL,
    deprecate_mode,
    is_team_mode,
)

logger = logging.getLogger(__name__)

_A2UI_OPEN_TAG_MARKER = "<a2ui-json>"
_SWITCH_AGENT_WORK_MODES: frozenset[str] = frozenset(
    {NEW_AGENT_WORK_NORMAL, NEW_AGENT_WORK_PLAN}
)
_SWITCH_CODE_MODES: frozenset[str] = frozenset(
    {NEW_AGENT_CODE_PLAN, NEW_AGENT_CODE_NORMAL, NEW_TEAM_CODE_NORMAL}
)


def apply_a2ui_text_fallback_to_gateway_payload(
    payload: dict[str, Any],
    *,
    channel_id: str,
) -> dict[str, Any]:
    """Convert A2UI blocks to text for non-Web channel payloads."""
    if not any(_A2UI_OPEN_TAG_MARKER in value for value in payload.values() if isinstance(value, str)):
        return payload

    from agent_gateway.runtime.a2ui_text import apply_non_web_text_fallback_to_payload

    return apply_non_web_text_fallback_to_payload(payload, channel_id=channel_id)


def normalize_legacy_health_check_relay_payload(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Normalize the pre-split probe relay before channel fan-out."""
    if payload.get("event_type") != "heartbeat.relay":
        return payload
    payload["event_type"] = "health_check.relay"
    if "health_check" not in payload and "heartbeat" in payload:
        payload["health_check"] = payload["heartbeat"]
    return payload


class ChannelMode(str, Enum):
    AGENT = "agent"
    # Historical persisted channel states may contain these short modes.
    AGENT_PLAN = "agent.plan"
    AGENT_FAST = "agent.fast"
    CODE_PLAN = "code.plan"
    CODE_NORMAL = "code.normal"
    CODE_TEAM = "code.team"
    TEAM = "team"
    TEAM_PLAN = "team.plan.normal"
    TEAM_PLAN_NORMAL = "team.plan.normal"
    TEAM_PLAN_CODE = "team.plan.code"
    AGENT_WORK_NORMAL = "agent.work.normal"
    AGENT_WORK_PLAN = "agent.work.plan"
    AGENT_CODE_NORMAL = "agent.code.normal"
    AGENT_CODE_PLAN = "agent.code.plan"
    TEAM_WORK_NORMAL = "team.work.normal"
    TEAM_WORK_PLAN = "team.work.plan"
    TEAM_CODE_NORMAL = "team.code.normal"
    TEAM_CODE_PLAN = "team.code.plan"

    @classmethod
    def is_team_mode(cls, mode: str) -> bool:
        """Return True if *mode* resolves to any team variant."""
        return is_team_mode(mode)


def channel_mode_from_str(mode_str: str) -> ChannelMode:
    """Normalize any mode string to the canonical channel-mode enum."""
    new_mode_str = deprecate_mode(mode_str)
    if new_mode_str in NEW_CANONICAL_MODES:
        try:
            return ChannelMode(new_mode_str)
        except ValueError:
            logger.warning(
                "channel_mode_from_str: new canonical '%s' hit NEW_CANONICAL_MODES "
                "but enum construction failed; falling back to AGENT (raw=%r)",
                new_mode_str,
                mode_str,
            )
    if new_mode_str != (mode_str or "").strip().lower():
        logger.debug(
            "channel_mode_from_str: '%s' -> new canonical '%s' "
            "(not in NEW_CANONICAL_MODES; falling back to AGENT)",
            mode_str,
            new_mode_str,
        )
    else:
        logger.debug(
            "channel_mode_from_str: '%s' was not in NEW_CANONICAL_MODES; "
            "falling back to AGENT",
            mode_str,
        )
    return ChannelMode.AGENT


@dataclass
class ChannelControlState:
    session_id: str | None = None
    mode: ChannelMode = ChannelMode.AGENT


@dataclass
class NewSessionCancelParams:
    """Named parameters for cancelling a session after ``/new_session``."""

    user_infos: dict[str, Any]
    channel_id: str
    reply_session_id: str | None
    new_sid: str
    old_sid: str | None


@dataclass
class ModeChangeCancelParams:
    """Named parameters for cancelling a session after ``/mode``."""

    user_infos: dict[str, Any]
    channel_id: str
    reply_session_id: str | None
    old_sid: str | None
    new_mode_label: str
