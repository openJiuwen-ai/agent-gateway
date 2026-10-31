# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Channel notice construction helpers extracted from MessageHandler."""

from __future__ import annotations

import time
from typing import Any

from openjiuwen_protocol.message import EventType, Message


def build_channel_notice_message(
    user_infos: dict,
    channel_id: str,
    session_id: str | None,
    text_or_payload: str | dict[str, Any],
) -> Message:
    """Build a terminal notice Message for one channel."""
    if isinstance(text_or_payload, dict):
        payload = dict(text_or_payload)
        payload.setdefault("is_complete", True)
    else:
        payload = {"content": text_or_payload, "is_complete": True}

    _app_id = user_infos.get("app_id") or user_infos.get("bot_id", "")
    return Message(
        id=user_infos["id"],
        type="event",
        channel_id=channel_id,
        session_id=session_id,
        params={},
        timestamp=time.time(),
        ok=True,
        payload=payload,
        event_type=EventType.CHAT_FINAL,
        metadata=user_infos["meta_data"],
        app_id=_app_id or None,
    )
