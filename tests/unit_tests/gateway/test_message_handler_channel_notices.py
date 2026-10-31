# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from agent_gateway.message_handler.channel_notices import build_channel_notice_message
from openjiuwen_protocol.message import EventType


def test_build_channel_notice_message_wraps_text_as_complete_final_event():
    user_infos = {
        "id": "notice-1",
        "meta_data": {"channel": "feishu"},
        "app_id": "app-1",
    }

    msg = build_channel_notice_message(
        user_infos,
        "feishu",
        "sess-1",
        "switch complete",
    )

    assert msg.id == "notice-1"
    assert msg.type == "event"
    assert msg.channel_id == "feishu"
    assert msg.session_id == "sess-1"
    assert msg.ok is True
    assert msg.payload == {"content": "switch complete", "is_complete": True}
    assert msg.event_type == EventType.CHAT_FINAL
    assert msg.metadata == {"channel": "feishu"}
    assert msg.app_id == "app-1"


def test_build_channel_notice_message_preserves_custom_dict_payload():
    user_infos = {
        "id": "notice-2",
        "meta_data": None,
        "bot_id": "bot-1",
    }
    source_payload = {"content": "streaming", "is_complete": False}

    msg = build_channel_notice_message(
        user_infos,
        "web",
        None,
        source_payload,
    )

    assert msg.payload == {"content": "streaming", "is_complete": False}
    assert msg.metadata is None
    assert msg.app_id == "bot-1"
