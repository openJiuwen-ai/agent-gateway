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
    ("payload", "params", "expected"),
    [
        ({"health_check": "  ready  ", "content": "answer"}, None, "ready"),
        ({"content": {"output": "  answer  "}}, None, "answer"),
        ({"content": {"text": "answer"}}, None, "{'text': 'answer'}"),
        ({"content": None}, {"content": "fallback"}, ""),
        ({}, {"content": "  fallback  "}, "fallback"),
        ("  plain text  ", None, "plain text"),
        ({}, None, ""),
    ],
)
def test_discord_and_slack_extract_common_payloads(payload, params, expected) -> None:
    event_type = (
        EventType.HEALTH_CHECK_RELAY
        if isinstance(payload, dict) and "health_check" in payload
        else EventType.CHAT_FINAL
    )
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
        _message(payload={"content": "  answer  "})
    ) == "  answer  "
    assert WhatsAppChannel._extract_outgoing_text(
        _message(payload={}, params={"content": "  fallback  "})
    ) == "  fallback  "
    assert WhatsAppChannel._extract_outgoing_text(_message(payload="  plain  ")) == "  plain  "
