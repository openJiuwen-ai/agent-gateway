# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Outbound app routing: AgentServer pushes may carry the route in metadata."""

from __future__ import annotations

from e2a.message import Message

from agent_gateway.channel_manager.channel_manager import ChannelManager


class _MessageHandler:
    """Only the accessor ChannelManager needs for app-id resolution."""

    @staticmethod
    def resolve_app_id(msg: Message) -> str:
        return getattr(msg, "app_id", None) or getattr(msg, "bot_id", None) or "default"


def _manager() -> ChannelManager:
    return ChannelManager(_MessageHandler())


def _message(*, app_id: str = "", bot_id: str = "", metadata: dict | None = None) -> Message:
    return Message(
        id="hb_run-1",
        type="event",
        channel_id="feishu",
        session_id="feishu_session",
        params={},
        timestamp=0.0,
        ok=True,
        payload={"event_type": "chat.final", "content": "answer"},
        event_type=None,
        metadata=metadata if metadata is not None else {},
        app_id=app_id or None,
        bot_id=bot_id or None,
    )


def test_explicit_app_id_wins() -> None:
    assert _manager()._resolve_dispatch_app_id(_message(app_id="cli_explicit")) == "cli_explicit"


def test_explicit_app_id_beats_metadata() -> None:
    msg = _message(app_id="cli_explicit", metadata={"app_id": "cli_metadata"})
    assert _manager()._resolve_dispatch_app_id(msg) == "cli_explicit"


def test_metadata_app_id_is_used_when_message_has_none() -> None:
    msg = _message(metadata={"app_id": "cli_metadata"})
    assert _manager()._resolve_dispatch_app_id(msg) == "cli_metadata"


def test_metadata_bot_id_is_used_when_app_id_is_absent() -> None:
    msg = _message(metadata={"bot_id": "cli_bot"})
    assert _manager()._resolve_dispatch_app_id(msg) == "cli_bot"


def test_unroutable_message_keeps_default() -> None:
    assert _manager()._resolve_dispatch_app_id(_message()) == "default"
