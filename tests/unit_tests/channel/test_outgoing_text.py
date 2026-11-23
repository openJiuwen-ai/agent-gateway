"""Behavioral tests for shared IM outgoing text extraction."""

import time
from typing import Any

import pytest
from e2a.message import EventType, Message

from agent_gateway.channel_manager.im_platforms.discord.discord_connect import DiscordChannel
from agent_gateway.channel_manager.im_platforms.slack.slack_connect import SlackChannel
from agent_gateway.channel_manager.im_platforms.whatsapp.whatsapp_connect import WhatsAppChannel


def _message(
    *,
    payload: Any = None,
    params: dict[str, Any] | None = None,
    event_type: EventType = EventType.CHAT_FINAL,
) -> Message:
    return Message(
        id="outgoing-1",
        type="event",
        channel_id="test",
        session_id="session-1",
        params=params or {},
        timestamp=time.time(),
        ok=True,
        payload=payload,
        event_type=event_type,
    )


@pytest.mark.parametrize(
    ("payload", "params", "event_type", "expected"),
    [
        ({"health_check": "  ready  ", "content": "answer"}, None, EventType.HEALTH_CHECK_RELAY, "ready"),
        ({"content": {"output": "  answer  "}}, None, EventType.CHAT_FINAL, "answer"),
        ({"content": {"text": "answer"}}, None, EventType.CHAT_FINAL, "answer"),
        ({"content": {"text": {"unexpected": "value"}}}, None, EventType.CHAT_FINAL, ""),
        ({"content": None}, {"content": "fallback"}, EventType.CHAT_FINAL, ""),
        ({"content": 0}, None, EventType.CHAT_FINAL, ""),
        ({"content": False}, None, EventType.CHAT_FINAL, ""),
        ({"content": {"output": 0}}, None, EventType.CHAT_FINAL, "0"),
        ({}, {"content": "  fallback  "}, EventType.CHAT_FINAL, "fallback"),
        ("  plain text  ", None, EventType.CHAT_FINAL, "plain text"),
        ({}, None, EventType.CHAT_FINAL, ""),
    ],
)
def test_discord_and_slack_extract_common_payloads(payload, params, event_type, expected) -> None:
    msg = _message(payload=payload, params=params, event_type=event_type)

    assert DiscordChannel._extract_outgoing_text(msg) == expected
    assert SlackChannel._extract_outgoing_text(msg) == expected


def test_slack_uses_error_when_content_is_absent() -> None:
    msg = _message(payload={"error": "  failed  "})

    assert SlackChannel._extract_outgoing_text(msg) == "failed"
    assert DiscordChannel._extract_outgoing_text(msg) == ""


def test_whatsapp_preserves_outgoing_whitespace() -> None:
    msg = _message(
        payload={"health_check": "  ready  ", "content": "answer"},
        event_type=EventType.HEALTH_CHECK_RELAY,
    )

    assert WhatsAppChannel._extract_outgoing_text(msg) == "  ready  "


def test_whatsapp_preserves_whitespace_for_each_content_source() -> None:
    assert WhatsAppChannel._extract_outgoing_text(
        _message(payload={"content": {"output": "  answer  "}})
    ) == "  answer  "
    assert WhatsAppChannel._extract_outgoing_text(
        _message(payload={}, params={"content": "  fallback  "})
    ) == "  fallback  "
    assert WhatsAppChannel._extract_outgoing_text(_message(payload="  plain  ")) == "  plain  "
