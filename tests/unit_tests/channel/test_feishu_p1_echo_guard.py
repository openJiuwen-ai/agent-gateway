"""P1 echo guard tests (2026-11-06 judgement: user text echoed as bot reply)."""
from collections import deque
from unittest.mock import MagicMock, AsyncMock
import pytest
from openjiuwen_protocol.message import Message
from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel, FeishuConfig,
)

def _make_channel() -> FeishuChannel:
    # enable_streaming=False：确定性非流式路径，避免单测真实构造 CardKit
    # 客户端并发起 HTTP（流式路径行为由 mock _handle_cardkit_streaming_event 用例覆盖）。
    config = FeishuConfig(enabled=True, app_id="a", app_secret="b", enable_streaming=False)
    ch = FeishuChannel(config, MagicMock(spec=RobotMessageRouter))
    ch._api_client = MagicMock()
    return ch

def _msg(event_name=None, content="", etype=None, metadata=None):
    p = {"content": content}
    if event_name:
        p["event_type"] = event_name
    return Message(id="req_t1", type="event", channel_id="feishu", session_id="sess_x",
                   params={}, timestamp=0, ok=True, payload=p, event_type=etype,
                   metadata=metadata or {}, app_id="app", agent_ref=None)

def _record_input(ch, key, text):
    dq = ch._recent_user_inputs.setdefault(key, deque((), 8))
    dq.append(text)

@pytest.mark.asyncio
async def test_input_received_not_delivered():
    ch = _make_channel()
    m = _msg(event_name="chat.input_received", content="这么简单的事情，搞不定")
    await ch.send(m)
    ch._api_client.im.v1.message.create.assert_not_called()

@pytest.mark.asyncio
async def test_unknown_event_not_delivered():
    ch = _make_channel()
    m = _msg(event_name="chat.some_future_event", content="hi")
    await ch.send(m)
    ch._api_client.im.v1.message.create.assert_not_called()

@pytest.mark.asyncio
async def test_final_delivered_normally():
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    await ch.send(_msg(event_name="chat.final", content="写入成功，文件在 SMB"))
    ch._send_feishu_message.assert_awaited_once()

@pytest.mark.asyncio
async def test_echo_suppressed():
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    await ch.send(_msg(event_name="chat.final", content="这么简单的事情，搞不定"))
    ch._send_feishu_message.assert_not_awaited()

@pytest.mark.asyncio
async def test_similar_but_different_not_killed():
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    await ch.send(_msg(event_name="chat.final", content="搞不定的问题已经解决"))
    ch._send_feishu_message.assert_awaited_once()

@pytest.mark.asyncio
async def test_output_phase_not_delivered():
    ch = _make_channel()
    m = _msg(event_name="chat.output_phase", content="")
    await ch.send(m)
    ch._api_client.im.v1.message.create.assert_not_called()

# ── 守卫二前移：cardkit 流式早退之前必须先过回声守卫 ──

@pytest.mark.asyncio
async def test_cardkit_echo_guard_runs_before_cardkit():
    """echo 场景：守卫二在 cardkit 流式早退之前拦截（cardkit 处理器未被 await）。"""
    ch = _make_channel()
    ch.config.enable_streaming = True  # 走流式分支，验证守卫先于 cardkit 早退
    ch._handle_cardkit_streaming_event = AsyncMock(return_value=True)
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    await ch.send(_msg(event_name="chat.final", content="这么简单的事情，搞不定"))
    ch._handle_cardkit_streaming_event.assert_not_awaited()

@pytest.mark.asyncio
async def test_cardkit_normal_final_reaches_cardkit():
    """正常 chat.final：守卫放行，cardkit 流式路径照常接管。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._handle_cardkit_streaming_event = AsyncMock(return_value=True)
    await ch.send(_msg(event_name="chat.final", content="正常回复正文，不构成入站回声"))
    ch._handle_cardkit_streaming_event.assert_awaited_once()

@pytest.mark.asyncio
async def test_echo_key_uses_feishu_chat_id_fallback():
    """会话键统一：入站按 chat_id（oc_x）录制；出站 session_id 被 /new_session
    改写为其他值后，守卫仍按 meta.feishu_chat_id 命中拦截。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._handle_cardkit_streaming_event = AsyncMock(return_value=True)
    _record_input(ch, "oc_x", "这么简单的事情，搞不定")
    m = _msg(event_name="chat.final", content="这么简单的事情，搞不定",
             metadata={"feishu_chat_id": "oc_x"})
    m.session_id = "sess_new"  # /new_session 改写后的 session_id，与入站键不再相等
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    ch._handle_cardkit_streaming_event.assert_not_awaited()
