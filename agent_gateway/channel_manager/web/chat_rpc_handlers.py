# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Chat RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

from typing import Any


def register_chat_send_handler(channel: Any) -> None:
    """Register the immediate chat.send acknowledgment before media routes."""

    async def _chat_send(ws, req_id, params, session_id):
        await channel.send_response(
            ws,
            req_id,
            ok=True,
            payload={"accepted": True, "session_id": session_id},
        )

    channel.register_method("chat.send", _chat_send)
