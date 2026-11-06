"""P2 suppression + dedup tests (2026-11-06 duplicate delivery judgement)."""
import pytest
from unittest.mock import MagicMock, AsyncMock
from agent_gateway.common.delivery_dedup import DeliveryDedup
from tests.unit_tests.channel.test_feishu_p1_echo_guard import _make_channel, _msg

def test_dedup_component():
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    assert d.check_and_record("s1", "r1", "这是一条足够长的回复内容用于测试去重逻辑abcdef") is None
    dup = d.check_and_record("s1", "r2", "这是一条足够长的回复内容用于测试去重逻辑abcdef")
    assert dup is not None
    assert d.check_and_record("s2", "r3", "这是一条足够长短语重复回复内容用于测试去重xyz") is None

def test_dedup_short_repeat_allowed():
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    assert d.check_and_record("s1", "r1", "好的") is None
    assert d.check_and_record("s1", "r2", "好的") is None
    assert d.check_and_record("s1", "r1", "短的合法重复不丢弃xyzabc") is None

@pytest.mark.asyncio
async def test_suppressed_frame_not_delivered():
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.final", content="被抑制的首份回复内容")
    m.payload["output_suppressed"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()

@pytest.mark.asyncio
async def test_duplicate_final_dropped():
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    long_text = "任务已完成的完整答复正文需要超过二十个字符的长度阈值才触发精确重复兜底"
    m1 = _msg(event_name="chat.final", content=long_text)
    await ch.send(m1)
    m2 = _msg(event_name="chat.final", content=long_text)
    m2.id = "req_t2"
    await ch.send(m2)
    ch._send_feishu_message.assert_awaited_once()
