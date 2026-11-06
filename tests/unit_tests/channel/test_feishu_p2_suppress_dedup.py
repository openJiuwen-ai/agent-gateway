"""P2 suppression + dedup tests (2026-11-06 duplicate delivery judgement)."""
import time
import pytest
from unittest.mock import AsyncMock, MagicMock
from agent_gateway.common.delivery_dedup import DeliveryDedup, request_seen_before
from tests.unit_tests.channel.test_feishu_p1_echo_guard import _make_channel, _msg

# ── 组件级：维度锁定（每条断言只动一个维度）──

def test_dedup_same_session_same_content_different_request_dropped():
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "这是一条足够长的回复内容用于测试去重逻辑abcdef"
    assert d.check_and_record("s1", "r1", t) is None
    assert d.check_and_record("s1", "r2", t) is not None

def test_dedup_same_session_different_content_allowed():
    """维度锁定：同 session、内容不同（>=20 字符）两条都放行（不误杀后续回复）。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t1 = "第一条回复：任务已完成的完整答复正文长度超过阈值线"
    t2 = "第二条回复：另一个问题的完整答复正文同样超过阈值线"
    assert len(d.normalize(t1)) >= 20 and len(d.normalize(t2)) >= 20
    assert d.check_and_record("s1", "r1", t1) is None
    assert d.check_and_record("s1", "r2", t2) is None

def test_dedup_same_content_different_session_allowed():
    """维度锁定：同内容、不同 session 都放行（session 隔离，跨会话不误杀）。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "这是一条足够长的回复内容用于测试会话隔离维度abcdefg"
    assert d.check_and_record("s1", "r1", t) is None
    assert d.check_and_record("s2", "r2", t) is None

def test_dedup_min_chars_boundary():
    """维度锁定：19/20/21 字符边界（<20 不记账不判重，>=20 才判重）。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    assert d.check_and_record("s1", "r1", "a" * 19) is None
    assert d.check_and_record("s1", "r2", "a" * 19) is None
    assert d.check_and_record("s1", "r3", "a" * 20) is None
    assert d.check_and_record("s1", "r4", "a" * 20) is not None
    assert d.check_and_record("s1", "r5", "a" * 21) is None
    assert d.check_and_record("s1", "r6", "a" * 21) is not None

def test_dedup_ttl_expiry_allows_again():
    """维度锁定：TTL 过期后同内容重新放行。"""
    d = DeliveryDedup(ttl_seconds=0.05, min_chars=5)
    t = "x" * 10
    assert d.check_and_record("s1", "r1", t) is None
    assert d.check_and_record("s1", "r2", t) is not None
    time.sleep(0.12)
    assert d.check_and_record("s1", "r3", t) is None

def test_dedup_same_request_replay_exempt():
    """request_id 契约：同 request 重放豁免（刷新时间戳不丢弃），不同 request 才去重。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "同一请求重放豁免的回复正文长度超过二十个字符xyz"
    assert d.check_and_record("s1", "r1", t) is None
    assert d.check_and_record("s1", "r1", t) is None  # 同 request 重放 → 豁免
    assert d.check_and_record("s1", "r2", t) is not None  # 不同 request → 去重
    assert request_seen_before("r1", "r1") is True
    assert request_seen_before("r1", "r2") is False

def test_dedup_short_repeat_allowed():
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    assert d.check_and_record("s1", "r1", "好的") is None
    assert d.check_and_record("s1", "r2", "好的") is None
    assert d.check_and_record("s1", "r1", "短的合法重复不丢弃xyzabc") is None

# ── 通道级 ──

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

@pytest.mark.asyncio
async def test_duplicate_final_dropped_in_cardkit_path():
    """P2 去重前移：cardkit 流式路径（mock 早退成功）下重复 chat.final 仍被去重拦截。"""
    ch = _make_channel()
    ch.config.enable_streaming = True  # 走流式分支，验证去重先于 cardkit 早退
    ch._handle_cardkit_streaming_event = AsyncMock(return_value=True)
    long_text = "任务已完成的完整答复正文需要超过二十个字符的长度阈值才触发精确重复兜底"
    m1 = _msg(event_name="chat.final", content=long_text)
    await ch.send(m1)
    m2 = _msg(event_name="chat.final", content=long_text)
    m2.id = "req_t2"
    await ch.send(m2)
    assert ch._delivery_dedup.suppressed_total == 1
    ch._handle_cardkit_streaming_event.assert_awaited_once()
