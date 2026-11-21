# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""飞书审批卡片的 Gateway 结构、逐题作答与一次性投递行为。"""

from __future__ import annotations

import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from e2a.message import EventType, Message

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel,
    FeishuConfig,
    _ApprovalCardContext,
    _OTHER_OPTION_TEXT,
    _OTHER_PLACEHOLDER_TOKEN,
)

ALLOW_ONCE = {"label": "本次允许", "value": "allow_once"}
ALWAYS_ALLOW = {"label": "永久记住", "value": "always_allow"}
REJECT = {"label": "拒绝", "value": "reject"}


def _question(
    text: str,
    options: list[dict[str, str]] | None = None,
    *,
    card_id: str = "",
    tool_payload: dict | None = None,
) -> dict:
    question = {
        "question": text,
        "header": "权限审批: bash",
        "options": list(options or []),
        "multi_select": False,
    }
    if card_id:
        question["card_id"] = card_id
    if tool_payload is not None:
        question["tool_payload"] = tool_payload
    return question


def _ctx(**overrides) -> _ApprovalCardContext:
    defaults = {
        "request_id": "call_1",
        "source": "permission_interrupt",
        "questions": [_question("执行 bash", [ALLOW_ONCE, REJECT], card_id="card-1")],
        "session_id": "feishu_oc_chat",
        "chat_id": "oc_chat",
        "metadata": {"feishu_chat_id": "oc_chat", "feishu_open_id": "ou_requester"},
    }
    defaults.update(overrides)
    return _ApprovalCardContext(**defaults)


@pytest.fixture()
def channel():
    channel = FeishuChannel(
        FeishuConfig(channel_id="feishu", app_id="app", app_secret="secret"),
        RobotMessageRouter(),
    )
    published: list[Message] = []
    channel._message_callback = published.append
    channel._main_loop = None
    channel._api_client = None
    channel.published = published  # type: ignore[attr-defined]
    return channel


def _toast_content(response) -> str:
    if isinstance(response, dict):
        return str((response.get("toast") or {}).get("content") or "")
    toast = getattr(response, "toast", None)
    return str(getattr(toast, "content", "") or "")


def _card_action(value: dict, operator: str = "ou_requester") -> SimpleNamespace:
    return SimpleNamespace(
        event=SimpleNamespace(
            token="tok_1",
            action=SimpleNamespace(value=value),
            operator=SimpleNamespace(open_id=operator),
        )
    )


def _event_message(request_id: str, source: str, questions: list[dict]) -> Message:
    return Message(
        id="evt_1",
        type="event",
        channel_id="feishu",
        session_id="feishu_oc_chat",
        params={},
        timestamp=time.time(),
        ok=True,
        payload={
            "event_type": "chat.ask_user_question",
            "request_id": request_id,
            "source": source,
            "questions": questions,
        },
        event_type=EventType.CHAT_ASK_USER_QUESTION,
        metadata={
            "feishu_chat_id": "oc_chat",
            "feishu_open_id": "ou_requester",
            "feishu_chat_type": "group",
        },
    )


def test_permission_allow_once_builds_resume_params(channel):
    ctx = _ctx()
    ctx.answers[0] = "allow_once"
    ctx.answer_labels[0] = "本次允许"

    params = channel._build_resume_params(ctx)

    assert params["source"] == "permission_interrupt"
    assert params["request_id"] == "call_1"
    assert params["supports_user_interaction"] is True
    assert params["answers"] == [{"selected_options": ["allow_once"], "card_id": "card-1"}]


def test_permission_always_allow_is_returned_as_selected_option(channel):
    ctx = _ctx()
    ctx.answers[0] = "always_allow"
    ctx.answer_labels[0] = "永久记住"

    params = channel._build_resume_params(ctx)

    assert params["answers"] == [{"selected_options": ["always_allow"], "card_id": "card-1"}]


def test_permission_reject_is_returned_as_selected_option(channel):
    ctx = _ctx()
    ctx.answers[0] = "reject"
    ctx.answer_labels[0] = "拒绝"

    params = channel._build_resume_params(ctx)

    assert params["answers"] == [{"selected_options": ["reject"], "card_id": "card-1"}]


def test_permission_without_card_id_keys_answer_by_request_id(channel):
    ctx = _ctx(questions=[_question("执行 bash", [ALLOW_ONCE, REJECT])])
    ctx.answers[0] = "allow_once"

    params = channel._build_resume_params(ctx)

    assert params["request_id"] == "call_1"
    assert params["answers"] == [{"selected_options": ["allow_once"]}]


def test_confirm_source_never_carries_card_id(channel):
    ctx = _ctx(
        source="confirm_interrupt",
        questions=[_question("执行计划", [{"label": "批准", "value": "approve"}], card_id="card-9")],
    )
    ctx.answers[0] = "approve"

    params = channel._build_resume_params(ctx)

    assert params["source"] == "confirm_interrupt"
    assert params["answers"] == [{"selected_options": ["approve"]}]


def test_plan_extra_is_returned_with_resume_params(channel):
    ctx = _ctx(
        source="confirm_interrupt",
        questions=[_question("执行计划", [{"label": "批准", "value": "approve"}])],
        plan_extra={
            "plan_approval_kind": "plan_approval",
            "plan_content": "计划正文",
            "plan_language": "cn",
        },
    )
    ctx.answers[0] = "approve"

    params = channel._build_resume_params(ctx)

    assert params["plan_approval_kind"] == "plan_approval"
    assert params["plan_content"] == "计划正文"
    assert params["plan_language"] == "cn"


def test_evolution_source_uses_selected_option(channel):
    ctx = _ctx(
        source="evolution_interrupt",
        questions=[_question("技能演进", [ALLOW_ONCE, ALWAYS_ALLOW, REJECT])],
    )
    ctx.answers[0] = "allow_once"

    params = channel._build_resume_params(ctx)

    assert params["source"] == "evolution_interrupt"
    assert params["answers"] == [{"selected_options": ["allow_once"]}]


def test_ask_user_answers_are_keyed_by_question_text(channel):
    ctx = _ctx(
        source="ask_user_interrupt",
        questions=[
            _question("选颜色", [{"label": "红", "value": "red"}, {"label": "蓝", "value": "blue"}]),
            _question("选尺寸", [{"label": "大", "value": "L"}, {"label": "小", "value": "S"}]),
        ],
    )
    ctx.answers[0] = "blue"
    ctx.answers[1] = "L"

    params = channel._build_resume_params(ctx)

    assert params["source"] == "ask_user_interrupt"
    assert params["answers"] == [
        {"question": "选颜色", "selected_options": ["blue"]},
        {"question": "选尺寸", "selected_options": ["L"]},
    ]


@pytest.mark.asyncio
async def test_send_card_renders_every_question_and_saves_context(channel):
    channel._create_and_send_message = AsyncMock()
    questions = [
        _question("第一题", [{"label": "甲", "value": "a"}, {"label": "乙", "value": "b"}]),
        _question("第二题", [{"label": "丙", "value": "c"}, {"label": "丁", "value": "d"}]),
    ]

    await channel._send_ask_user_question_card(
        _event_message("call_2", "ask_user_interrupt", questions)
    )

    request = channel._create_and_send_message.await_args.args[0]
    card = json.loads(request.content)
    action_blocks = [element for element in card["elements"] if element.get("tag") == "action"]
    assert len(action_blocks) == 2
    assert action_blocks[0]["actions"][0]["value"] == {
        "ref": {"request_id": "call_2", "index": 0},
        "value": "a",
    }
    assert action_blocks[1]["actions"][1]["value"] == {
        "ref": {"request_id": "call_2", "index": 1},
        "value": "d",
    }

    ctx = channel._card_contexts["call_2"]
    assert ctx.source == "ask_user_interrupt"
    assert ctx.chat_id == "oc_chat"


# 评审意见 #202641835：恢复被本 diff 误删的 asyncio 标记，防 strict 模式静默跳过
@pytest.mark.asyncio
async def test_send_card_saves_interrupt_id_for_echo(channel):
    channel._create_and_send_message = AsyncMock()
    msg = _event_message(
        "call_6",
        "ask_user_interrupt",
        [_question("选颜色", [{"label": "蓝", "value": "blue"}])],
    )
    msg.payload["interrupt_id"] = "intr_6"

    await channel._send_ask_user_question_card(msg)

    assert channel._card_contexts["call_6"].interrupt_id == "intr_6"


def test_resume_params_echo_interrupt_id(channel):
    params = channel._build_resume_params(_ctx(interrupt_id="intr_1"))

    assert params["interrupt_id"] == "intr_1"


def test_resume_params_omit_missing_interrupt_id(channel):
    params = channel._build_resume_params(_ctx())

    assert "interrupt_id" not in params


@pytest.mark.asyncio
async def test_other_option_renders_as_custom_answer_button(channel):
    """#140: Other 不再被丢弃，渲染为「其他（自定义答案）」并带文本作答引导。"""
    channel._create_and_send_message = AsyncMock()
    questions = [
        _question("第一题", [{"label": "红", "value": "red"}, {"label": "Other", "description": "Custom input"}]),
        _question("第二题", [{"label": "Other", "description": "Custom input"}]),
    ]

    await channel._send_ask_user_question_card(
        _event_message("call_5", "ask_user_interrupt", questions)
    )

    card = json.loads(channel._create_and_send_message.await_args.args[0].content)
    action_blocks = [element for element in card["elements"] if element.get("tag") == "action"]
    assert len(action_blocks) == 2
    # 评审意见 #201604544→#201832613：占位按钮回传规范化为专用占位 token
    # （与真实选项 value=other 不冲突），断言与过滤统一取常量，避免硬编码脱节。
    assert [button["value"]["value"] for button in action_blocks[0]["actions"]] == ["red", _OTHER_PLACEHOLDER_TOKEN]
    other_buttons = [
        button for block in action_blocks for button in block["actions"]
        if button["value"]["value"] == _OTHER_PLACEHOLDER_TOKEN
    ]
    assert other_buttons and all(
        button["text"]["content"] == "其他（自定义答案）" for button in other_buttons
    )
    rendered = json.dumps(card, ensure_ascii=False)
    assert "该问题需要文字作答，请在网页端或终端完成。" not in rendered
    assert "请在网页端或终端填写" in rendered  # 多题卡片：自定义答案引导到网页端/终端


def test_other_only_question_answerable_via_text_not_button(channel):
    """#140: Other-only 题目不计入可答题——不阻塞卡片完成，答案走文本回复路径（评审意见 #201604539）。"""
    ctx = _ctx(
        source="ask_user_interrupt",
        questions=[_question("自由题", [{"label": "Other", "description": "Custom input"}])],
    )

    assert len(ctx.options_of(0)) == 1
    assert ctx.options_of(0)[0].get("label") == "Other"
    assert ctx.answerable_indexes() == []  # Other-only 不计入可答题（评审意见 #201604539）
    assert ctx.is_answered() is False


def test_other_placeholder_click_guides_text_reply(channel):
    """#140: 点击 Other 返回文本作答引导（info），不写入答案。"""
    ctx = _ctx(
        source="ask_user_interrupt",
        questions=[_question("Q1", [{"label": "A1", "value": "a1"}, {"label": "Other", "description": "Custom input"}])],
    )
    channel._save_card_context(ctx)

    response = channel._on_card_action_trigger_sync(_card_action(
        {"ref": {"request_id": "call_1", "index": 0}, "value": "Other"},
    ))

    assert "自定义答案" in _toast_content(response)
    assert channel._card_contexts["call_1"].answers == {}


@pytest.mark.asyncio
async def test_permission_card_shows_tool_preview(channel):
    channel._create_and_send_message = AsyncMock()
    questions = [
        _question(
            "执行 bash",
            [ALLOW_ONCE, REJECT],
            card_id="card-1",
            tool_payload={"command": "rm -rf /tmp/x", "api_key": "[REDACTED]"},
        )
    ]

    await channel._send_ask_user_question_card(
        _event_message("call_3", "permission_interrupt", questions)
    )

    card = json.loads(channel._create_and_send_message.await_args.args[0].content)
    rendered = json.dumps(card, ensure_ascii=False)
    assert "参数：" in rendered
    assert "rm -rf /tmp/x" in rendered


# 评审意见 #202641835：恢复被本 diff 误删的 asyncio 标记，防 strict 模式静默跳过
@pytest.mark.asyncio
async def test_send_failure_drops_context(channel):
    channel._create_and_send_message = AsyncMock(side_effect=RuntimeError("boom"))

    await channel._send_ask_user_question_card(
        _event_message("call_4", "permission_interrupt", [_question("执行 bash", [ALLOW_ONCE, REJECT])])
    )

    assert "call_4" not in channel._card_contexts


# 评审意见 #202641835：恢复被本 diff 误删的 asyncio 标记，防 strict 模式静默跳过
@pytest.mark.asyncio
async def test_multi_question_partial_answer_does_not_submit(channel):
    ctx = _ctx(
        source="ask_user_interrupt",
        questions=[
            _question("Q1", [{"label": "A1", "value": "a1"}]),
            _question("Q2", [{"label": "A2", "value": "a2"}]),
        ],
    )
    channel._save_card_context(ctx)

    await channel._handle_card_callback_async("tok", "call_1", 0, "a1", "A1", "ou_requester")

    assert channel.published == []
    saved = channel._card_contexts["call_1"]
    assert saved.answers == {0: "a1"}

    await channel._handle_card_callback_async("tok", "call_1", 1, "a2", "A2", "ou_requester")

    assert len(channel.published) == 1
    message = channel.published[0]
    assert message.req_method.value == "chat.send"
    assert message.params["source"] == "ask_user_interrupt"
    assert message.params["answers"] == [
        {"question": "Q1", "selected_options": ["a1"]},
        {"question": "Q2", "selected_options": ["a2"]},
    ]
    assert "call_1" not in channel._card_contexts


# 评审意见 #202641835：恢复被本 diff 误删的 asyncio 标记，防 strict 模式静默跳过
@pytest.mark.asyncio
async def test_permission_answer_submits_chat_send_with_card_id(channel):
    channel._save_card_context(_ctx())

    await channel._handle_card_callback_async("tok", "call_1", 0, "allow_once", "本次允许", "ou_requester")

    assert len(channel.published) == 1
    message = channel.published[0]
    assert message.req_method.value == "chat.send"
    assert message.params["source"] == "permission_interrupt"
    assert message.params["answers"] == [{"selected_options": ["allow_once"], "card_id": "card-1"}]
    assert message.session_id == "oc_chat"
    assert message.chat_id == "oc_chat"


def test_multi_question_click_shows_remaining_hint(channel):
    channel._save_card_context(_ctx(
        source="ask_user_interrupt",
        questions=[
            _question("Q1", [{"label": "A1", "value": "a1"}]),
            _question("Q2", [{"label": "A2", "value": "a2"}]),
        ],
    ))

    response = channel._on_card_action_trigger_sync(_card_action(
        {"ref": {"request_id": "call_1", "index": 0}, "value": "a1"},
    ))

    assert _toast_content(response) == "已选择，请继续回答剩余问题"


@pytest.mark.asyncio
async def test_expired_context_is_rejected_and_removed(channel):
    ctx = _ctx(created_at=time.time() - 25 * 60 * 60)
    channel._save_card_context(ctx)
    notify = AsyncMock()
    channel._notify_approval_operator = notify

    await channel._handle_card_callback_async("tok", "call_1", 0, "allow_once", "本次允许", "ou_requester")

    assert channel.published == []
    notify.assert_awaited_once()
    assert "过期" in notify.await_args.args[1]
    assert "call_1" not in channel._card_contexts


# 评审意见 #202641835：恢复被本 diff 误删的 asyncio 标记，防 strict 模式静默跳过
@pytest.mark.asyncio
async def test_duplicate_click_after_submit_is_rejected(channel):
    channel._save_card_context(_ctx())
    await channel._handle_card_callback_async("tok", "call_1", 0, "allow_once", "本次允许", "ou_requester")
    assert len(channel.published) == 1

    notify = AsyncMock()
    channel._notify_approval_operator = notify
    await channel._handle_card_callback_async("tok", "call_1", 0, "allow_once", "本次允许", "ou_requester")

    assert len(channel.published) == 1
    notify.assert_awaited_once()


# 评审意见 #202641835：恢复被本 diff 误删的 asyncio 标记，防 strict 模式静默跳过
@pytest.mark.asyncio
async def test_delivery_failure_reports_invalid_card_without_retry_hint(channel):
    channel._save_card_context(_ctx())

    def _raise(_msg):
        raise RuntimeError("bus down")

    channel._message_callback = _raise
    notify = AsyncMock()
    channel._notify_approval_operator = notify

    await channel._handle_card_callback_async("tok", "call_1", 0, "allow_once", "本次允许", "ou_requester")

    assert "call_1" not in channel._card_contexts
    notify.assert_awaited_once()
    text = notify.await_args.args[1]
    assert "请重试" not in text
    assert "重新发起审批" in text


def test_legacy_card_without_ref_reports_expired(channel):
    response = channel._on_card_action_trigger_sync(_card_action({
        "label": "本次允许",
        "request_id": "legacy_call",
    }))

    assert _toast_content(response) == "审批已过期或已处理"


def test_unknown_request_id_reports_expired(channel):
    response = channel._on_card_action_trigger_sync(_card_action(
        {"ref": {"request_id": "missing_call", "index": 0}, "value": "allow_once"},
    ))

    assert _toast_content(response) == "审批已过期或已处理"


def test_click_side_other_placeholder_predicate_same_source(channel):
    """P2 回归（atomgit 2026-11-18）：点击侧与渲染侧占位判定单源同判据。

    {"label":"其他","value":"other"} 渲染为可点真选项（label 非 Other 占位），
    点击侧此前按 value 字面 "other" 误判占位、弹文本引导且答案永不入卡＝死卡。
    修复前：点击命中占位 toast＝红。"""
    ctx = _ctx(
        source="ask_user_interrupt",
        questions=[_question("自定义", [{"label": "其他", "value": "other"}])],
    )
    channel._save_card_context(ctx)
    response = channel._on_card_action_trigger_sync(_card_action(
        {"ref": {"request_id": "call_1", "index": 0}, "value": "other"},
    ))
    # 评审意见 #202649866：裸「不含某串」断言恒真（修复前 toast 同样不含该串），
    # 对「死卡」回归零免疫。改正向反证：修复后此点击走正常选项作答路径
    # （单题应答完成＝「已提交，等待处理」），而非占位文本引导分支。
    toast = _toast_content(response)
    assert "已提交，等待处理" in toast
    assert "请直接回复本条消息的文本" not in toast


def test_click_other_without_text_channel_toasts_web_hint(channel):
    """P3 覆盖补齐（atomgit 2026-11-18）：非 ask_user 来源的 Other 占位点击
    不承诺文本作答，须引导网页端/终端——else-toast 此前无用例。"""
    ctx = _ctx(
        source="permission_interrupt",
        questions=[_question("Q1", [ALLOW_ONCE, {"label": "Other", "description": "Custom input"}])],
    )
    channel._save_card_context(ctx)
    response = channel._on_card_action_trigger_sync(_card_action(
        # 评审意见 #202654738：占位按钮实际回传值是专用 token，非 "other" 字面——
        # 用 _OTHER_OPTION_TEXT 模拟的是卡片不会回传的取值。
        {"ref": {"request_id": "call_1", "index": 0}, "value": _OTHER_PLACEHOLDER_TOKEN},
    ))
    assert "网页端或终端填写" in _toast_content(response)
    assert channel._card_contexts["call_1"].answers == {}


@pytest.mark.asyncio
async def test_single_question_other_hint_names_text_fallback(channel):
    """P2 回归（atomgit 2026-11-18）：单题 Other 引导文案须带文本通路兜底
    指引——文本回复通道不可用时用户知道去网页端/终端。修复前：无该句＝红。"""
    channel._create_and_send_message = AsyncMock()
    await channel._send_ask_user_question_card(_event_message(
        "call_7", "ask_user_interrupt",
        [_question("Q", [{"label": "A", "value": "a"},
                         {"label": "Other", "description": "Custom input"}])],
    ))
    card = json.loads(channel._create_and_send_message.await_args.args[0].content)
    rendered = json.dumps(card, ensure_ascii=False)
    assert "若文本通路不可用，请在网页端或终端完成" in rendered


def test_resume_params_omit_unanswered_other_only_question(channel):
    """inline #201711463：Other-only 未答题不进 resume answers——无空
    selected_options 项，与服务端 #6792「Other 自定义答案走文本通路、
    卡片 resume 仅携真实作答」语义对齐的断言固化。"""
    ctx = _ctx(
        source="ask_user_interrupt",
        questions=[
            _question("真实题", [{"label": "A1", "value": "a1"}]),
            _question("Other题", [{"label": "Other", "description": "Custom input"}]),
        ],
    )
    ctx.answers[0] = "a1"
    params = channel._build_resume_params(ctx)
    assert params["answers"] == [{"question": "真实题", "selected_options": ["a1"]}]
    assert all(a["selected_options"] for a in params["answers"])


def test_real_other_option_and_placeholder_coexist_no_value_collision(channel):
    """评审意见 #201832613 回归：真实选项 value=other 与 Other 占位同题共存——
    渲染侧两按钮回传值必须不同（占位＝专用 token），点击侧占位 token 走文本
    作答引导且答案不写入，裸 other 仍按真实选项处理不被占位吞掉。"""
    opts = [{"label": "Other 选项", "value": "other"},
            {"label": "Other", "description": "Custom input"}]
    ctx = _ctx(source="ask_user_interrupt", questions=[_question("共存", opts)])
    b_real = channel._approval_button(ctx, 0, opts[0])
    b_ph = channel._approval_button(ctx, 0, opts[1])
    assert b_real["value"]["value"] == _OTHER_OPTION_TEXT
    assert b_ph["value"]["value"] == _OTHER_PLACEHOLDER_TOKEN
    assert b_ph["value"]["value"] != b_real["value"]["value"]
    channel._save_card_context(ctx)
    resp = channel._on_card_action_trigger_sync(_card_action(
        {"ref": {"request_id": "call_1", "index": 0}, "value": _OTHER_PLACEHOLDER_TOKEN},
    ))
    assert "请直接回复本条消息的文本" in _toast_content(resp)
    assert channel._card_contexts["call_1"].answers == {}
    resp2 = channel._on_card_action_trigger_sync(_card_action(
        {"ref": {"request_id": "call_1", "index": 0}, "value": "other"},
    ))
    # 评审意见 #202649866：同型恒真断言修正——裸 other 真实选项点击必须按正常
    # 应答路径回「已提交，等待处理」，不得静默/不得落占位引导。
    assert "已提交，等待处理" in _toast_content(resp2)


def test_duplicate_value_with_leading_placeholder_click_hits_real_option(channel):
    """评审意见 #201912955/#202437587：同题两选项 value 相同且占位在前——点击真实
    选项的按钮不得被占位分支吞掉（不落文本作答引导），按正常选项路径应答。"""
    ctx = _ctx(
        source="ask_user_interrupt",
        questions=[_question("Q1", [
            {"label": "Other", "description": "Custom input", "value": "x"},
            {"label": "A1", "value": "x"},
        ])],
    )
    channel._save_card_context(ctx)

    response = channel._on_card_action_trigger_sync(_card_action(
        {"ref": {"request_id": "call_1", "index": 0}, "value": "x"},
    ))

    toast = _toast_content(response)
    assert "自定义答案" not in toast, "真实选项点击被占位分支吞掉"
    assert "已提交" in toast or "已选择" in toast
    # 回显文案反查不得落在占位选项上：同值占位在前时反查结果必须是真实选项 A1。
    assert channel._option_label(ctx, 0, "x") == "A1", "回显反查错成占位选项"


def test_all_placeholder_duplicate_value_still_guides_text_reply(channel):
    """全部命中皆为占位时维持占位语义（回落首个命中→文本作答引导）。"""
    channel._save_card_context(_ctx(
        source="ask_user_interrupt",
        questions=[_question("Q1", [
            {"label": "Other", "value": "x"},
            {"label": "Other", "description": "alt", "value": "x"},
        ])],
    ))

    response = channel._on_card_action_trigger_sync(_card_action(
        {"ref": {"request_id": "call_1", "index": 0}, "value": "x"},
    ))

    assert "自定义答案" in _toast_content(response)
