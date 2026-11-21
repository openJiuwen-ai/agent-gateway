# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from agent_gateway.channel_manager.im_platforms.group_ack import (
    DEFAULT_GROUP_ACK,
    build_group_ack_prompt,
    generate_group_ack_text,
    normalize_group_ack_text,
)
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import FeishuChannel
from agent_gateway.channel_manager.im_platforms.wecom.wecom_connect import WecomChannel


def test_normalize_group_ack_rejects_narration_and_target_name() -> None:
    assert normalize_group_ack_text("小王", "已提醒小王参加会议") == ""
    assert normalize_group_ack_text("小王", "收到，我会参加会议\n补充说明") == "收到，我会参加会议"


def test_build_group_ack_prompt_keeps_platform_context() -> None:
    prompt = build_group_ack_prompt("企业微信", "小王", "会议安排")
    assert "企业微信群聊机器人" in prompt
    assert "会议安排" in prompt


def test_generate_group_ack_uses_fallback_without_credentials(monkeypatch) -> None:
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.delenv("API_BASE", raising=False)
    assert generate_group_ack_text("小王", "会议安排", platform_name="飞书") == DEFAULT_GROUP_ACK


def test_generate_group_ack_filters_invalid_model_reply(monkeypatch) -> None:
    monkeypatch.setenv("API_KEY", "secret")
    monkeypatch.setenv("API_BASE", "https://example.test")

    class Response:
        def raise_for_status(self) -> None:
            return None

        @staticmethod
        def json() -> dict:
            return {"choices": [{"message": {"content": "已提醒小王"}}]}

    seen: dict[str, object] = {}

    def post(url: str, **kwargs):
        seen["url"] = url
        seen["kwargs"] = kwargs
        return Response()

    result = generate_group_ack_text("小王", "会议安排", platform_name="飞书", request_post=post)
    assert result == DEFAULT_GROUP_ACK
    assert seen["url"] == "https://example.test/chat/completions"


def test_channel_generators_supply_platform_context(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def generate(target_name: str, content: str, **kwargs: object) -> str:
        calls.append({"target_name": target_name, "content": content, **kwargs})
        return "收到，我会跟进"

    monkeypatch.setattr(
        "agent_gateway.channel_manager.im_platforms.feishu.feishu_connect.generate_group_ack_text",
        generate,
    )
    monkeypatch.setattr(
        "agent_gateway.channel_manager.im_platforms.wecom.wecom_connect.generate_group_ack_text",
        generate,
    )

    feishu = object.__new__(FeishuChannel)
    wecom = object.__new__(WecomChannel)
    assert feishu._generate_group_ack_sync("小王", "会议安排") == "收到，我会跟进"
    assert wecom._generate_group_ack_sync("小王", "会议安排") == "收到，我会跟进"
    assert [call["platform_name"] for call in calls] == ["飞书", "企业微信"]
