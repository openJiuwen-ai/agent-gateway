# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""IM channels must not render Web/TUI-only runtime telemetry as chat body text."""

import asyncio
from unittest.mock import MagicMock

import pytest
from openjiuwen_protocol.message import EventType, Message

from agent_gateway.channel_manager.base import (
    WEB_ONLY_TELEMETRY_EVENTS,
    RobotMessageRouter,
    is_web_only_telemetry_event,
    resolve_event_name,
)
from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect import (
    DingTalkChannel,
    DingTalkConfig,
)
from agent_gateway.channel_manager.im_platforms.discord.discord_connect import (
    DiscordChannel,
    DiscordChannelConfig,
)
from agent_gateway.channel_manager.im_platforms.slack.slack_connect import (
    SlackChannel,
    SlackChannelConfig,
)
from agent_gateway.channel_manager.im_platforms.telegram.telegram_connect import (
    TelegramChannel,
    TelegramChannelConfig,
)
from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import (
    WechatChannel,
    WechatConfig,
)
from agent_gateway.channel_manager.im_platforms.wecom.wecom_connect import (
    WecomChannel,
    WecomConfig,
)
from agent_gateway.channel_manager.im_platforms.whatsapp.whatsapp_connect import (
    WhatsAppChannel,
    WhatsAppChannelConfig,
)
from agent_gateway.channel_manager.im_platforms.xiaoyi.xiaoyi_connect import (
    XiaoyiChannel,
    XiaoyiChannelConfig,
)


def _message(
    event_name: str,
    *,
    msg_type: str = "res",
    event_type: EventType | str | None = None,
    metadata: dict | None = None,
    content: str = "duplicate answer",
) -> Message:
    return Message(
        id="hb_run-1",
        type=msg_type,
        channel_id="im",
        session_id="telegram_123",
        params={},
        timestamp=0.0,
        ok=True,
        payload={"event_type": event_name, "content": content},
        event_type=event_type,
        metadata=metadata or {},
        enable_streaming=False,
    )


def _forbid_extraction(*_args, **_kwargs):
    raise AssertionError("telemetry event reached the channel content extractor")


def _dingtalk() -> DingTalkChannel:
    channel = DingTalkChannel(DingTalkConfig(enabled=True), RobotMessageRouter())
    channel._extract_message_content = _forbid_extraction
    return channel


def _telegram() -> TelegramChannel:
    channel = TelegramChannel(TelegramChannelConfig(enabled=True), RobotMessageRouter())
    channel._application = MagicMock()
    channel._running = True
    channel._extract_content = _forbid_extraction
    return channel


def _discord() -> DiscordChannel:
    channel = DiscordChannel(DiscordChannelConfig(enabled=True), RobotMessageRouter())
    channel._client = MagicMock()
    channel._client.is_closed.return_value = False
    channel._extract_outgoing_text = _forbid_extraction
    return channel


def _slack() -> SlackChannel:
    channel = SlackChannel(SlackChannelConfig(enabled=True), RobotMessageRouter())
    channel._client = MagicMock()
    channel._extract_outgoing_text = _forbid_extraction
    return channel


def _whatsapp() -> WhatsAppChannel:
    channel = WhatsAppChannel(WhatsAppChannelConfig(enabled=True), RobotMessageRouter())
    channel._ws = MagicMock()
    channel._whatsapp_connected = True
    channel._extract_outgoing_text = _forbid_extraction
    return channel


def _wechat() -> WechatChannel:
    channel = WechatChannel(WechatConfig(enabled=True, bot_token="token"), RobotMessageRouter())
    channel._http = MagicMock()
    channel._extract_content = _forbid_extraction
    return channel


def _wecom() -> WecomChannel:
    channel = WecomChannel(WecomConfig(enabled=True, enable_streaming=True), RobotMessageRouter())
    channel._ws_client = MagicMock()
    channel._ws_client.is_connected = True
    channel._pending_streams["req-1"] = {"frame": {}, "stream_id": "stream-1", "accumulated": ""}
    channel._extract_content_from_payload = _forbid_extraction
    return channel


def _xiaoyi() -> XiaoyiChannel:
    channel = XiaoyiChannel(XiaoyiChannelConfig(enabled=True), RobotMessageRouter())
    channel._ws_connections = {"ws1": MagicMock()}

    async def _forbid_legacy(*_args, **_kwargs):
        raise AssertionError("telemetry event reached Xiaoyi legacy send path")

    channel._send_legacy = _forbid_legacy
    return channel


IM_CHANNEL_FACTORIES = {
    "dingtalk": _dingtalk,
    "telegram": _telegram,
    "discord": _discord,
    "slack": _slack,
    "whatsapp": _whatsapp,
    "wechat": _wechat,
    "wecom": _wecom,
    "xiaoyi": _xiaoyi,
}


@pytest.mark.parametrize("event_name", sorted(WEB_ONLY_TELEMETRY_EVENTS))
def test_event_name_resolves_from_payload_when_event_type_missing(event_name: str) -> None:
    assert resolve_event_name(_message(event_name)) == event_name
    assert is_web_only_telemetry_event(_message(event_name))


@pytest.mark.parametrize(
    "event_type",
    [EventType.CHAT_USAGE_SUMMARY, EventType.CHAT_USAGE_METADATA, EventType.CONTEXT_USAGE],
)
def test_event_name_resolves_from_event_type_enum(event_type: EventType) -> None:
    msg = Message(
        id="hb_run-1",
        type="event",
        channel_id="im",
        session_id="session",
        params={},
        timestamp=0.0,
        ok=True,
        payload={},
        event_type=event_type,
    )
    assert resolve_event_name(msg) == event_type.value
    assert is_web_only_telemetry_event(msg)


def test_event_name_resolves_from_plain_string() -> None:
    assert resolve_event_name(_message("", event_type="chat.llm_call_end")) == "chat.llm_call_end"


@pytest.mark.parametrize("event_name", ["chat.final", "chat.delta", "chat.error", "keepalive", ""])
def test_non_telemetry_events_are_not_filtered(event_name: str) -> None:
    assert not is_web_only_telemetry_event(_message(event_name))


@pytest.mark.parametrize("channel_name", sorted(IM_CHANNEL_FACTORIES))
@pytest.mark.parametrize("event_name", sorted(WEB_ONLY_TELEMETRY_EVENTS))
def test_im_channels_drop_web_only_telemetry(channel_name: str, event_name: str) -> None:
    channel = IM_CHANNEL_FACTORIES[channel_name]()
    asyncio.run(channel.send(_message(event_name)))


def test_dingtalk_still_delivers_chat_final() -> None:
    channel = DingTalkChannel(DingTalkConfig(enabled=True), RobotMessageRouter())
    delivered: list[str] = []

    async def _token() -> str:
        return "token"

    async def _send_http(_url, data, _token, _chat_id) -> None:
        delivered.append(str(data))

    async def _noop(*_args, **_kwargs) -> None:
        return None

    channel._get_access_token = _token  # type: ignore[method-assign]
    channel._extract_chat_id = lambda _msg: "chat-1"  # type: ignore[method-assign]
    channel._build_send_request = lambda *_args, **_kwargs: ("url", {"content": "the answer"})  # type: ignore[method-assign]
    channel._send_http_request = _send_http  # type: ignore[method-assign]
    channel._send_fallback_files = _noop  # type: ignore[method-assign]

    asyncio.run(channel.send(_message("chat.final")))

    assert len(delivered) == 1
    assert "the answer" in delivered[0]
