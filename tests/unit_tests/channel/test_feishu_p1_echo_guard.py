"""P1 echo guard tests (2026-11-06 judgement: user text echoed as bot reply)."""
from unittest.mock import MagicMock, AsyncMock
import pytest
from openjiuwen_protocol.message import Message
from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel, FeishuConfig,
)

def _make_channel() -> FeishuChannel:
    config = FeishuConfig(enabled=True, app_id="a", app_secret="b")
    ch = FeishuChannel(config, MagicMock(spec=RobotMessageRouter))
    ch._api_client = MagicMock()
    return ch

def _msg(event_name=None, content="", etype=None):
    p = {"content": content}
    if event_name:
        p["event_type"] = event_name
    return Message(id="req_t1", type="event", channel_id="feishu", session_id="sess_x",
                   params={}, timestamp=0, ok=True, payload=p, event_type=etype,
                   metadata={}, app_id="app", agent_ref=None)

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
    dq = ch._recent_user_inputs.setdefault("sess_x", __import__("collections").deque((), 8))
    dq.append("这么简单的事情，搞不定")
    await ch.send(_msg(event_name="chat.final", content="这么简单的事情，搞不定"))
    ch._send_feishu_message.assert_not_awaited()

@pytest.mark.asyncio
async def test_similar_but_different_not_killed():
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    dq = ch._recent_user_inputs.setdefault("sess_x", __import__("collections").deque((), 8))
    dq.append("这么简单的事情，搞不定")
    await ch.send(_msg(event_name="chat.final", content="搞不定的问题已经解决"))
    ch._send_feishu_message.assert_awaited_once()

@pytest.mark.asyncio
async def test_output_phase_not_delivered():
    ch = _make_channel()
    m = _msg(event_name="chat.output_phase", content="")
    await ch.send(m)
    ch._api_client.im.v1.message.create.assert_not_called()
