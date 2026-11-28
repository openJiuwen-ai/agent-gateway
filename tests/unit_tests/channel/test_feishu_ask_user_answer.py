# pylint: disable=protected-access
"""A Feishu ask_user card answer names the interrupt it answers."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from e2a.message import EventType, Message

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import FeishuChannel, FeishuConfig


def _question(payload: dict) -> Message:
    return Message(
        id="req_001",
        type="event",
        channel_id="feishu",
        session_id="sess_001",
        params={},
        timestamp=1000.0,
        ok=True,
        payload={"event_type": "chat.ask_user_question", **payload},
        event_type=EventType.CHAT_ASK_USER_QUESTION,
        metadata={"feishu_chat_id": "oc_test123"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("interrupt_id", ["intr-1", ""])
async def test_card_answer_echoes_the_interrupt_id_of_the_question(interrupt_id):
    channel = FeishuChannel(FeishuConfig(enabled=True, app_id="app", app_secret="secret"), MagicMock(spec=RobotMessageRouter))
    channel._create_and_send_message = AsyncMock()
    sent: list[Message] = []
    channel._message_callback = sent.append
    payload = {
        "request_id": "call-1",
        "source": "ask_user_interrupt",
        "questions": [{"question": "Which?", "options": [{"label": "A"}]}],
    }
    if interrupt_id:
        payload["interrupt_id"] = interrupt_id

    await channel._send_ask_user_question_card(_question(payload))
    # 新卡片回执按 (request_id, 题号, 取值) 定位上下文；选项无 value 时以 label 为取值。
    await channel._handle_card_callback_async("token-1", "call-1", 0, "A", "A", "ou_user")

    params = sent[-1].params
    assert params["source"] == "ask_user_interrupt"
    assert params["request_id"] == "call-1"
    assert params.get("interrupt_id", "") == interrupt_id
    assert ("interrupt_id" in params) is bool(interrupt_id)


def test_approval_button_normalizes_other_placeholder_value():
    """评审意见 #201589591：label="Other" 而 value 为其他取值时，按钮回传 value
    规范化为专用占位 token（#201832613 升级为不与真实选项冲突的取值）——渲染侧占位判定（label 优先）与点击侧 value
    比较同源，原取值不再被当真实答案静默提交。"""
    from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
        _ApprovalCardContext,
        _OTHER_PLACEHOLDER_TOKEN,  # 评审意见 #201832613
    )

    ctx = _ApprovalCardContext(
        request_id="req_001",
        source="ask_user_interrupt",
        questions=[{"question": "请选择", "options": [
            {"label": "Other", "value": "custom_free_input"},
        ]}],
    )
    option = {"label": "Other", "value": "custom_free_input"}
    btn = FeishuChannel._approval_button(ctx, 0, option)
    assert btn["value"]["value"] == _OTHER_PLACEHOLDER_TOKEN


def test_other_hint_gated_by_ask_user_source():
    """评审意见 #201589594：非 ask_user 来源的单题卡片不给「直接回复文本」
    指引，统一引导网页端/终端填写。"""
    from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
        _ApprovalCardContext,
        _OTHER_HINT_WEB,
    )

    ch = FeishuChannel(FeishuConfig(enabled=True, app_id="app", app_secret="secret"), MagicMock(spec=RobotMessageRouter))
    ctx = _ApprovalCardContext(
        request_id="req_001",
        # atomgit 2026-11-18：真实权限中断来源是 permission_interrupt（生产与
        # _APPROVAL_SOURCE_* 同源）；假源名 permission_request 不在协议面，
        # 用例仅因「非 ask_user」而恒绿，钉不住真实门控语义。
        source="permission_interrupt",
        questions=[{"question": "是否批准", "options": [
            {"label": "Other", "value": "other"},
        ]}],
    )
    elements = ch._build_approval_card_elements(ctx)
    hint_texts = [
        e["text"]["content"]
        for e in elements
        if isinstance(e, dict) and e.get("tag") == "div"
        and isinstance(e.get("text"), dict) and isinstance(e["text"].get("content"), str)
    ]
    assert any(_OTHER_HINT_WEB in t for t in hint_texts)
    assert not any("直接回复本条消息" in t for t in hint_texts)


def test_other_only_question_does_not_block_card_completion():
    """评审意见 #201553270：仅含 Other 占位的题目不计入可答题——is_answered
    只看真按钮题，最后一道真按钮题答完即完成，remaining 提示不再被
    Other-only 题永久卡住。"""
    from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
        _ApprovalCardContext,
    )

    ctx = _ApprovalCardContext(
        request_id="req_001",
        source="ask_user_interrupt",
        questions=[
            {"question": "选择环境", "options": [
                {"label": "测试", "value": "test"},
                {"label": "生产", "value": "prod"},
            ]},
            {"question": "补充说明", "options": [
                {"label": "Other", "value": "other"},
            ]},
        ],
    )
    assert ctx.answerable_indexes() == [0]   # Other-only 第二题不在可答题
    ctx.answers[0] = "test"
    assert ctx.is_answered() is True          # 真按钮题答完即完成
