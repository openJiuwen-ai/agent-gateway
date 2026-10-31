# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""WebChannel connection lifecycle handlers extracted from app_web_handlers."""

from __future__ import annotations

import logging
import secrets
import time
from typing import Any


logger = logging.getLogger(__name__)


def _make_session_id() -> str:
    # 与前端 generateSessionId 保持一致：毫秒时间戳(16进制) + 6位随机16进制
    ts = format(int(time.time() * 1000), "x")
    suffix = secrets.token_hex(3)
    return f"sess_{ts}_{suffix}"


def register_connection_handlers(
    channel: Any,
    agent_client: Any,
    message_handler: Any,
    _resolve: Any,
) -> None:
    """Register connection and disconnection hooks for the WebChannel."""

    from openjiuwen_protocol.message import EventType, Message

    async def _on_connect(ws):
        ac = _resolve(agent_client)
        if ac is None or not getattr(ac, "server_ready", False):
            logger.debug("[_on_connect] Agent 未就绪，跳过 connection.ack")
            return
        # V2: 复用 ws 握手时注册的占位 session_id，而不是另 make 一个新 sid。
        # 原实现凭空生成 sid_B 与 ws 在 _clients_by_key 中注册的 sid_A 不一致，
        # 导致 send() 按 session_id 反查落空、ACK 被丢弃，前端收不到 connection.ack。
        # 复用 sid_A 后，ACK 走标准 send 流程即可命中本 ws，无需特殊路由兜底。
        sid = getattr(ws, "_jiuwen_initial_sid", None) or _make_session_id()

        ack_msg = Message(
            id=f"ack-{sid}",
            type="event",
            channel_id=channel.channel_id,
            session_id=sid,
            params={},
            timestamp=time.time(),
            ok=True,
            event_type=EventType.CONNECTION_ACK,
            payload={
                "session_id": sid,
                "mode": "BUILD",
                "tools": [],
                "protocol_version": "1.0",
            },
        )
        mh = _resolve(message_handler)
        if mh:
            await mh.publish_robot_messages(ack_msg)
        else:
            await channel.send(ack_msg)

    channel.on_connect(_on_connect)

    async def _on_disconnect(ws, _session_ids):
        mh = _resolve(message_handler)
        cleanup = getattr(mh, "unregister_ws_subscriptions", None)
        ws_id = str(getattr(ws, "_jiuwen_ws_id", "") or "").strip()
        if callable(cleanup) and ws_id:
            await cleanup(channel.channel_id, ws_id)

    register_disconnect = getattr(channel, "on_disconnect", None)
    if callable(register_disconnect):
        register_disconnect(_on_disconnect)
