"""P1 echo guard tests (2026-11-06 judgement: user text echoed as bot reply)."""
import contextlib
import json
import logging
import time
import types
from collections import deque
from unittest.mock import MagicMock, AsyncMock
import pytest

pytest.importorskip("e2a", reason="fleet CI worker env may transiently lack the pinned git dep (assembly race); full suite verified green locally. develop 37eae76 起协议包更名为 e2a，openjiuwen_protocol 已不在依赖面")
from e2a.message import Message
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

def _record_input(ch, key, text, ts=None):
    """入站录制与生产同构：(text, monotonic 时间戳) 元组；可注入过期时间戳。"""
    dq = ch._recent_user_inputs.setdefault(key, deque((), 8))
    dq.append((text, time.monotonic() if ts is None else ts))

@pytest.mark.asyncio
async def test_input_received_not_delivered(caplog):
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.input_received", content="这么简单的事情，搞不定")
    with caplog.at_level(logging.WARNING):
        await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    ch._api_client.im.v1.message.create.assert_not_called()
    assert "[P1-guard]" in caplog.text

@pytest.mark.asyncio
async def test_unknown_event_not_delivered(caplog):
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.some_future_event", content="hi")
    with caplog.at_level(logging.WARNING):
        await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    ch._api_client.im.v1.message.create.assert_not_called()
    assert "[P1-guard]" in caplog.text

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
async def test_output_phase_not_delivered(caplog):
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.output_phase", content="非空内容确保判别力")
    with caplog.at_level(logging.WARNING):
        await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    ch._api_client.im.v1.message.create.assert_not_called()
    assert "[P1-guard]" in caplog.text

# ── 误杀降级策略：最小长度 / 时间窗 / 命中消费（atomgit gw49-2）──

@pytest.mark.asyncio
async def test_short_reply_not_killed():
    """短句回复（< _ECHO_MIN_CHARS）即使与入站全同也放行，不再整轮吞掉。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "好的")
    await ch.send(_msg(event_name="chat.final", content="好的"))
    ch._send_feishu_message.assert_awaited_once()

@pytest.mark.asyncio
async def test_stale_input_beyond_window_not_matched():
    """入站记录超过时间窗（_ECHO_WINDOW_SECONDS）后不参与比对，误杀窗口有界。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "这么简单的事情，搞不定", ts=time.monotonic() - 400)
    await ch.send(_msg(event_name="chat.final", content="这么简单的事情，搞不定"))
    ch._send_feishu_message.assert_awaited_once()

@pytest.mark.asyncio
async def test_echo_hit_consumes_record():
    """命中即消费：第一次同文回复被拦，之后合法同文回复放行。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    await ch.send(_msg(event_name="chat.final", content="这么简单的事情，搞不定"))
    ch._send_feishu_message.assert_not_awaited()
    m2 = _msg(event_name="chat.final", content="这么简单的事情，搞不定")
    m2.id = "req_t2"
    await ch.send(m2)
    ch._send_feishu_message.assert_awaited_once()

# ── 守卫二前移：cardkit 流式早退之前必须先过回声守卫 ──

@pytest.mark.asyncio
async def test_echo_hit_cleanup_finalizes_cardkit_session():
    """守卫二命中收尾锁定（gw49 force-scan P3-4）：cardkit 会话以已流出缓冲
    finalize 回收、增量缓冲弹出、卡片不卡「生成中」。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    session = MagicMock()
    session.finalize = AsyncMock()
    key = "req_t1\0oc_x\0chat_id"
    ch._cardkit_sessions[key] = session
    ch._cardkit_buffers[key] = "已流出的增量文本"
    ch._stream_text_buffers["req_t1"] = "增量累计"
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    await ch.send(_msg(event_name="chat.final", content="这么简单的事情，搞不定"))
    ch._send_feishu_message.assert_not_awaited()
    session.finalize.assert_awaited_once()
    assert key not in ch._cardkit_sessions
    assert "req_t1" not in ch._stream_text_buffers

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

# ── 键一致性回归：用例驱动 _on_message 真实录制（atomgit gw49-5）──

def _fake_event(chat_id, text, mentions=None):
    msg = MagicMock()
    msg.chat_id = chat_id
    msg.message_id = "om_t1"
    msg.message_type = "text"
    msg.content = json.dumps({"text": text})
    msg.chat_type = "group"
    msg.create_time = "0"
    msg.mentions = mentions or []
    sender = MagicMock()
    sender.sender_type = "user"
    sender.sender_id.open_id = "ou_u1"
    data = MagicMock()
    data.event.message = msg
    data.event.sender = sender
    return data

def _wire_on_message_mocks(ch, text):
    ch._is_duplicate_message = MagicMock(return_value=False)
    ch._add_reaction = AsyncMock()
    ch.config.group_digital_avatar = False
    ch.config.bot_key = "bk"  # 跳过 config.yaml last_* 回写分支
    ch._parse_message_content_with_file = AsyncMock(return_value=(text, None))
    ch._process_batched_message = AsyncMock()

@pytest.mark.asyncio
async def test_on_message_inbound_key_aligns_with_outbound_guard():
    """驱动 _on_message 真实录制：入站按 chat_id（oc_grp9）录制，出站经
    meta.feishu_chat_id 对齐后守卫命中——键错位回归由本用例锁定。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_grp9", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    text = "这么简单的事情，搞不定"
    _wire_on_message_mocks(ch, text)
    await ch._on_message(_fake_event("oc_grp9", text))
    assert "oc_grp9" in ch._recent_user_inputs
    assert any(t == text for t, _ts in ch._recent_user_inputs["oc_grp9"])
    m = _msg(event_name="chat.final", content=text,
             metadata={"feishu_chat_id": "oc_grp9"})
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()

@pytest.mark.asyncio
async def test_on_message_records_raw_text_variant_for_mention_mismatch():
    """双源录制锁定（atomgit gw49-4）：解析态经 mention 名字替换后与出站原文
    不同源时，原始 text 字段变体仍能命中守卫。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_grp9", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    raw = "<at user_id=\"ou_x\"></at> 帮我检查部署状态"
    _wire_on_message_mocks(ch, raw)
    # 模拟 mention 替换：解析态与出站原文不同源
    ch._replace_mentions_with_names = MagicMock(side_effect=lambda m, t: "@张三 " + t)
    await ch._on_message(_fake_event("oc_grp9", raw))
    entries = [t for t, _ts in ch._recent_user_inputs.get("oc_grp9", deque((), 8))]
    assert raw in entries  # 原始态变体已录制
    m = _msg(event_name="chat.final", content=raw,
             metadata={"feishu_chat_id": "oc_grp9"})
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()

@pytest.mark.asyncio
async def test_on_message_bot_messages_not_recorded():
    """机器人自身消息不录制：sender_type=bot 的入站不进回声比对账本。"""
    ch = _make_channel()
    text = "机器人消息不应进入回声比对账本的文本"
    ch._is_duplicate_message = MagicMock(return_value=False)
    data = _fake_event("oc_grp9", text)
    data.event.sender.sender_type = "bot"
    await ch._on_message(data)
    assert "oc_grp9" not in ch._recent_user_inputs

# ── W2 名字形变体（B1 RED，plan §1.3 W2）──

@pytest.mark.asyncio
async def test_on_message_records_name_form_variant_for_mention_echo():
    """名字形回声命中（RED→GREEN）：入站 mention 占位符形经本地零 HTTP 归一化
    增补名字形解析态变体后，出站名字形回声被守卫拦截。"""
    ch = _make_channel()
    ch.config.bot_open_id = "ou_bot"  # 缓存 bot 身份 fixture（零 HTTP 前置）
    ch._fetch_bot_open_id = MagicMock(
        side_effect=AssertionError("零 HTTP 红线：录制路径不得触发 bot 身份拉取"))
    ch._extract_receive_info = MagicMock(return_value=("oc_grp9", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    raw = "@_user_1 请帮我检查部署状态哈"
    name_form = "@小闻 请帮我检查部署状态哈"
    mention = MagicMock()
    mention.key = "@_user_1"
    mention.name = "小闻"
    _wire_on_message_mocks(ch, raw)
    ch._replace_mentions_with_names = MagicMock(side_effect=lambda m, t: t)
    await ch._on_message(_fake_event("oc_grp9", raw, mentions=[mention]))
    entries = [t for t, _ts in ch._recent_user_inputs.get("oc_grp9", deque((), 8))]
    assert name_form in entries  # 名字形变体入列（现码缺位＝RED）
    m = _msg(event_name="chat.final", content=name_form,
             metadata={"feishu_chat_id": "oc_grp9"})
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    ch._fetch_bot_open_id.assert_not_called()  # 零 HTTP 锁定（R3-P1-003）

# ── W3 合并流式+final 比对（B1 RED，plan §1.3 W3）──

@pytest.mark.asyncio
async def test_echo_guard_uses_merged_stream_and_final_text():
    """守卫比 candidate=merge(peek 缓冲, final)：三形态收尾仅一次 pop、终态缓冲空。"""
    echo = "这么简单的事情，搞不定真得好好反思一下"
    # 形态一：仅增量——final 空尾，回声全文在流式缓冲
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", echo)
    ch._stream_text_buffers["req_t1"] = echo
    m = _msg(event_name="chat.final", content="")
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert "req_t1" not in ch._stream_text_buffers
    # 形态二：仅 final（既有腿回归锁定）
    ch2 = _make_channel()
    ch2._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch2._send_feishu_message = AsyncMock()
    _record_input(ch2, "sess_x", echo)
    await ch2.send(_msg(event_name="chat.final", content=echo))
    ch2._send_feishu_message.assert_not_awaited()
    # 形态三：流+final 合并——final 为缓冲前缀截短形，合并值才等于回声全文
    ch3 = _make_channel()
    ch3._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch3._send_feishu_message = AsyncMock()
    _record_input(ch3, "sess_x", echo)
    ch3._stream_text_buffers["req_t1"] = echo
    m3 = _msg(event_name="chat.final", content=echo[:-3])
    m3.id = "req_t1"
    m3.payload["is_complete"] = True
    await ch3.send(m3)
    ch3._send_feishu_message.assert_not_awaited()
    assert "req_t1" not in ch3._stream_text_buffers


@pytest.mark.asyncio
async def test_cleanup_nonterminal_frame_keeps_buffer():
    """P2①（p2 镜像）：非终态事件收尾不得弹出增量缓冲——缓冲弹出唯一在终态集合内。"""
    ch = _make_channel()
    key = "req_t1\x00buf"
    ch._stream_text_buffers[key] = "已流出的增量文本内容"
    m = _msg(event_name="chat.delta", content="被拦截的增量帧")
    await ch._cleanup_suppressed_frame(m, "chat.delta", key, None)
    assert ch._stream_text_buffers.get(key) == "已流出的增量文本内容"

@pytest.mark.asyncio
async def test_cleanup_terminal_frame_still_pops_buffer():
    """P2① 反向锁：终态 chat.final 收尾照常弹出缓冲（勿把修复写成不弹）。"""
    ch = _make_channel()
    key = "req_t1\x00buf"
    ch._stream_text_buffers[key] = "已流出的增量文本内容"
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    m = _msg(event_name="chat.final", content="被拦截的终态正文")
    await ch._cleanup_suppressed_frame(m, "chat.final", key, None)
    assert key not in ch._stream_text_buffers


@pytest.mark.asyncio
async def test_echo_guard_reads_cardkit_stream_buffer():
    """round-e P2#1：cardkit 流式模式 delta 只写 _cardkit_buffers，比对面必含该缓存。"""
    echo = "这么简单的事情，搞不定真得好好反思一下"
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", echo)
    ch._cardkit_buffers["req_t1\x00oc_x\x00chat_id"] = echo
    m = _msg(event_name="chat.final", content="")
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()

@pytest.mark.asyncio
async def test_cleanup_partial_final_frame_not_terminal():
    """round-e P2#2：is_complete=False 的 chat.final 部分帧非终态——不提前 finalize
    卡片会话；round-w（verdict 200184387）契约收窄：拦截即清增量缓冲。"""
    echo = "这么简单的事情，搞不定真得好好反思一下"
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", echo)
    ch._stream_text_buffers["req_t1"] = echo
    m = _msg(event_name="chat.final", content="")
    m.payload["is_complete"] = False
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    # round-w（verdict 200184387）契约收窄：部分帧拦截后增量缓冲按重复内容
    # 清除不再保留（保留可被终态冲刷面再次投递）；卡片会话仍不提前 finalize。
    assert "req_t1" not in ch._stream_text_buffers

def test_repeated_same_inbound_refreshes_window():
    """round-e P3#4：同文二次入站刷新时间戳（摘旧落新），防首条超窗后回声漏拦。"""
    from types import SimpleNamespace
    ch = _make_channel()
    fake = SimpleNamespace(message_type="text",
                           content=json.dumps({"text": "一段足够长的原文用于验证二次入站刷新时间戳窗口"}),
                           chat_id="oc_y", mentions=[])
    ch._record_inbound_user_input(fake)
    ts1 = ch._recent_user_inputs["oc_y"][0][1]
    time.sleep(0.01)
    ch._record_inbound_user_input(fake)
    dq = ch._recent_user_inputs["oc_y"]
    assert len(dq) == 1 and dq[0][1] != ts1


@pytest.mark.asyncio
async def test_cleanup_suppressed_chat_error_registers_errored_key():
    """round-g P2：_cleanup_suppressed_frame 对 chat.error 须镜像正常路径（send 正常
    分支）登记 _errored_stream_keys——被抑制的 chat.error 收尾后，后续终态
    processing_status 不得再补发「[状态] 已完成」（:471 契约）。"""
    ch = _make_channel()
    ch._stream_text_buffers["req_t1"] = "已流出的部分文本"
    m = _msg(event_name="chat.error", content="错误收尾")
    await ch._cleanup_suppressed_frame(m, "chat.error", "req_t1", None)
    assert "req_t1" in ch._errored_stream_keys
    assert "req_t1" not in ch._stream_text_buffers

@pytest.mark.asyncio
async def test_cleanup_processing_status_terminal_not_marked_errored():
    """反向锁：终态 processing_status 抑制收尾不登记 errored（只有 chat.error 登记）。"""
    ch = _make_channel()
    ch._stream_text_buffers["req_t1"] = "已流出的部分文本"
    m = _msg(event_name="chat.processing_status", content="")
    m.payload = {"is_processing": False}
    await ch._cleanup_suppressed_frame(m, "chat.processing_status", "req_t1", None)
    assert "req_t1" not in ch._errored_stream_keys


@pytest.mark.asyncio
async def test_guard1_blocks_blacklisted_event_even_with_parsed_enum():
    """round-g P2：ensure_gateway_event_types() 动态补枚举后，黑名单事件名解析成
    EventType（msg.event_type 非 None）守卫一仍须拦截——旧写法整体挂在
    event_type is None 上会静默失效。"""
    from enum import Enum

    class _FakeEnum(str, Enum):
        CHAT_INPUT_RECEIVED = "chat.input_received"

    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.input_received", content="一段足够长用于回声比对的正文内容测试用")
    m.event_type = _FakeEnum.CHAT_INPUT_RECEIVED
    m.payload["event_type"] = "chat.input_received"
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_output_suppressed_chat_error_registers_errored():
    """round-h P2：output_suppressed 终态 chat.error 裸 return 前须登记 errored——
    cron/scheduler 会对被抑制错误帧打 output_suppressed=True，不登记则后续终态
    processing_status 把失败当「已完成」补发（false success）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.error", content="运行失败")
    m.payload["output_suppressed"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert "req_t1" in ch._errored_stream_keys

@pytest.mark.asyncio
async def test_output_suppressed_processing_status_deferred_no_errored():
    """反向锁（D1 契约）：output_suppressed 的 processing_status 延后收尾，
    不弹缓冲不登记 errored——收尾交给后续终态抑制帧。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._stream_text_buffers["req_t1"] = "已流出文本"
    m = _msg(event_name="chat.processing_status", content="中间进度")
    m.payload["output_suppressed"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._stream_text_buffers.get("req_t1") == "已流出文本"
    assert "req_t1" not in ch._errored_stream_keys

@pytest.mark.asyncio
async def test_guard1_parseable_unknown_event_current_gate_scope():
    """现状锁定（非目标契约）：当前实现中白名单臂仅在 event_type is None 时生效，
    event_type 可解析的未知名不计数——本用例仅钉现状防无意识漂移；白名单口径全面
    化收敛（§intake）落地时本用例应按新口径改写或删除，不得视为长期契约。"""
    from enum import Enum

    class _FakeEnum(str, Enum):
        UNKNOWN = "chat.entirely_unknown_event"

    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.entirely_unknown_event", content="未知事件但枚举可解析的正文用于边界锁测试内容")
    m.event_type = _FakeEnum.UNKNOWN
    m.payload["event_type"] = "chat.entirely_unknown_event"
    await ch.send(m)
    assert ch._guard_blocked_total == 0  # 守卫一不拦截（守卫一拦截必计数）
    ch._send_feishu_message.assert_awaited_once()  # 真实放行至投递面（非别处静默丢弃）


@pytest.mark.asyncio
async def test_whitelist_arm_passes_known_enum_event_in_payload_form():
    """round-k P2：白名单臂只拦「未识别」事件名——payload 形态的已知枚举事件
    （cron 侧 event_type=None＋payload.event_type=chat.file 等）不得静默丢弃，
    放行交下游处置（守卫语义＝未识别不投递，不吞合法事件）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.file", content="以 payload 形态抵达的文件事件说明正文超过二十字符")
    m.event_type = None
    await ch.send(m)
    assert ch._guard_blocked_total == 0
    ch._send_feishu_message.assert_awaited_once()

@pytest.mark.asyncio
async def test_whitelist_arm_still_drops_truly_unknown_payload_event():
    """反向锁：payload 形态的真·未知名事件仍被守卫一拦截（计数＋不投递）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.totally_made_up_event", content="未知事件名以payload形态抵达也应被拦截正文超二十字符")
    m.event_type = None
    await ch.send(m)
    assert ch._guard_blocked_total == 1
    ch._send_feishu_message.assert_not_awaited()

@pytest.mark.asyncio
async def test_whitelist_arm_passes_dynamically_registered_event_value():
    """round-l P2：已知事件值判定必须**动态**求值——ensure_protocol_member 运行期
    向 EventType 注入的成员（ensure_gateway_event_types 机制）不得被导入期快照
    漏掉。动态注册一个测试专用值后，payload 形态该事件必须放行。"""
    from e2a.message import EventType
    from agent_gateway.common.protocol_compat import ensure_protocol_member
    name, value = "CHAT_ZTEST_DYN_EVENT", "chat.ztest_dyn_event"
    try:
        ensure_protocol_member(EventType, name, value)
        ch = _make_channel()
        ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
        ch._send_feishu_message = AsyncMock()
        m = _msg(event_name=value, content="运行期动态注册枚举值以payload形态抵达也应放行正文超二十")
        m.event_type = None
        await ch.send(m)
        assert ch._guard_blocked_total == 0
        ch._send_feishu_message.assert_awaited_once()
    finally:
        # round-q P3（verdict 200095280）：回滚每步独立兜底——回滚依赖
        # _member_names_ 等私有容器类型，单步失败会留下半清理枚举连锁破坏
        # 同进程后续用例；逐步 suppress＋终态复查不留半状态。
        member = EventType.__members__.get(name)
        if member is not None:
            with contextlib.suppress(Exception):
                type.__delattr__(EventType, name)
            with contextlib.suppress(Exception):
                del EventType._member_map_[name]
            with contextlib.suppress(Exception):
                EventType._value2member_map_.pop(value, None)
            with contextlib.suppress(Exception):
                EventType._member_names_.remove(name)
            # round-r（verdict 200136409）：终态复查逐容器覆盖——半清理状态不再漏检
            assert EventType.__members__.get(name) is None
            assert name not in EventType._member_map_
            assert value not in EventType._value2member_map_
            assert name not in getattr(EventType, "_member_names_", ())

@pytest.mark.asyncio
async def test_echo_guard_covers_processing_status_flush():
    """round-m P2：chat.processing_status 终态冲刷缓冲的投递面与守卫二同源比对——
    缓冲回声文本命中入站记录必须抑制（旧路径完全绕过比对）。"""
    echo = "用户原文经流式缓冲后由终态进度帧冲刷投递的正文超过二十字符"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", echo)
    ch._stream_text_buffers["req_t1"] = echo
    m = _msg(event_name="chat.processing_status", content="")
    m.payload["is_processing"] = False
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._echo_blocked_total == 1

@pytest.mark.asyncio
async def test_processing_status_flush_delivers_non_echo_text():
    """反向锁：冲刷文本非回声时照常投递。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    ch._stream_text_buffers["req_t1"] = "这是一段与任何入站用户输入都不相同的流式终稿文本超过二十字符"
    m = _msg(event_name="chat.processing_status", content="")
    m.payload["is_processing"] = False
    await ch.send(m)
    ch._send_feishu_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_flush_echo_suppression_routes_cleanup():
    """round-n P3：冲刷回声抑制不得绕过收尾清理——群进度清理须执行
    （与守卫二命中/去重命中同收尾契约；裸 return＝verdict 200075834）。"""
    echo = "用户原文经流式缓冲后由终态进度帧冲刷投递的正文超过二十字符"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._clear_group_progress_state = MagicMock()
    _record_input(ch, "sess_x", echo)
    ch._stream_text_buffers["req_t1"] = echo
    m = _msg(event_name="chat.processing_status", content="")
    m.payload["is_processing"] = False
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._echo_blocked_total == 1
    ch._clear_group_progress_state.assert_called_once_with("req_t1")


@pytest.mark.asyncio
async def test_record_inbound_atbot_keyless_parsed_variant():
    """round-q P2（verdict 200095280）：@机器人解析态录制——模型回声用户文本会连头部
    @bot 一并剥除，原始态/名字形双源均失配；剔除全部 mention key 并去首尾空的
    解析态变体必须入录。"""
    ch = _make_channel()
    im = types.SimpleNamespace(
        message_type="text",
        content=json.dumps({"text": "@_user_1 帮我查一下今天天气并给出建议"}),
        mentions=[types.SimpleNamespace(key="@_user_1", name="张三")],
        chat_id="oc_kb")
    ch._record_inbound_user_input(im)
    texts = [t for t, _ in ch._recent_user_inputs.get("oc_kb", ())]
    assert "帮我查一下今天天气并给出建议" in texts


@pytest.mark.asyncio
async def test_status_flush_nonterminal_echo_hit_keeps_record():
    """round-q P3（verdict 200095280）：中间 processing_status 帧回声命中只拦不消费记录——
    同文终态帧抵达仍须命中拦截（镜像守卫二仅终帧消费契约）。"""
    text = "中间帧命中回声时记录须留给终态帧再拦一次超长文本超二十字符"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._start_cardkit_session = AsyncMock(return_value=False)
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", text)
    # 中间帧回声命中的触发条件较窄（状态帧提取面通常仅渲染短状态标签）：
    # 强制提取面返回回声文，直达守卫分支验证「中间帧只拦不消费」契约。
    _orig_extract = ch._extract_message_content
    ch._extract_message_content = MagicMock(return_value=text)
    m1 = _msg(event_name="chat.processing_status", content=text)
    m1.payload["is_processing"] = True
    await ch.send(m1)
    ch._extract_message_content = _orig_extract
    assert ch._echo_blocked_total == 1
    assert any(t == text for t, _ in ch._recent_user_inputs.get("sess_x", ()))
    m2 = _msg(event_name="chat.processing_status", content="")
    m2.payload["is_processing"] = False
    ch._stream_text_buffers["req_t1"] = text
    await ch.send(m2)
    assert ch._echo_blocked_total == 2
    assert not any(t == text for t, _ in ch._recent_user_inputs.get("sess_x", ()))


@pytest.mark.asyncio
async def test_record_inbound_atbot_mixed_mentions_parsed_state():
    """round-r P2（verdict 200136390）：混合 @（机器人＋同事）——运行时
    `_replace_mentions_with_names` 解析态＝剥 @机器人 占位＋@其他→@名字；
    录制面必须含该混合变体，否则回声精确比对必失配。"""
    ch = _make_channel()
    im = types.SimpleNamespace(
        message_type="text",
        content=json.dumps({"text": "@_user_1 帮我问下 @_user_2 下午评审时间"}),
        mentions=[types.SimpleNamespace(key="@_user_1", name="小西"),
                  types.SimpleNamespace(key="@_user_2", name="李四")],
        chat_id="oc_mix")
    ch._record_inbound_user_input(im)
    texts = [t for t, _ in ch._recent_user_inputs.get("oc_mix", ())]
    assert "帮我问下 @李四 下午评审时间" in texts


@pytest.mark.asyncio
async def test_record_inbound_bot_mention_not_first():
    """round-s（verdict 200147104 测试缺口）：@机器人 不在首位——解析态＝原位摘除
    占位符其余保留，录制面须锁定该形态命中。"""
    ch = _make_channel()
    im = types.SimpleNamespace(
        message_type="text",
        content=json.dumps({"text": "先夸一句 @_user_1 然后帮我订会议室"}),
        mentions=[types.SimpleNamespace(key="@_user_1", name="小西")],
        chat_id="oc_mid")
    ch._record_inbound_user_input(im)
    texts = [t for t, _ in ch._recent_user_inputs.get("oc_mid", ())]
    assert "先夸一句 然后帮我订会议室" in texts


@pytest.mark.asyncio
async def test_record_inbound_many_mentions_original_survives():
    """round-s P2（verdict 200147104）：多 mention（≥3）变体生成数超环形缓存旧容量
    8，原始态最先入列反被挤出、守卫静默失效——容量须容得下一消息全变体族。"""
    ch = _make_channel()
    text = "@_user_1 帮 @_user_2 和 @_user_3 与 @_user_4 各带一杯咖啡"
    im = types.SimpleNamespace(
        message_type="text",
        content=json.dumps({"text": text}),
        mentions=[types.SimpleNamespace(key=f"@_user_{i}", name=n)
                  for i, n in ((1, "小西"), (2, "李四"), (3, "王五"), (4, "赵六"))],
        chat_id="oc_many")
    ch._record_inbound_user_input(im)
    texts = [t for t, _ in ch._recent_user_inputs.get("oc_many", ())]
    assert text in texts
    assert len(texts) >= 9

@pytest.mark.asyncio
async def test_echo_guard_blocks_overlong_stream_buffer_echo():
    """mingo007 #200679987：入站记录按 _truncate_text(4000) 截断加省略号；增量/
    cardkit 缓冲候选不做同口径截断时，>4000 输入的逐字回声（缓冲全文）与截断
    记录永不精确相等，守卫二在长度边界静默漏拦。补同口径变体后必须命中。
    （注：chat.final 裸正文面由 _extract_message_content 自带 4000 截断免疫本
    缺口，判别面在缓冲候选——本用例走增量缓冲全文形态。）"""
    import json as _json
    from types import SimpleNamespace
    t = "超长回声测试正文" * 1500  # 9000 字符 > 4000 截断线
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    fake = SimpleNamespace(message_type="text",
                           content=_json.dumps({"text": t}),
                           chat_id="oc_x", mentions=[])
    ch._record_inbound_user_input(fake)
    rec = ch._recent_user_inputs.get("oc_x")
    assert rec and len(rec[0][0]) == 4003  # 截断记录＝前4000＋省略号（生产口径）
    ch._stream_text_buffers["req_t1"] = t   # 流式缓冲保存全文（守卫二候选之一）
    m = _msg(event_name="chat.final", content="", metadata={"feishu_chat_id": "oc_x"})
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._echo_blocked_total == 1
    assert "req_t1" not in ch._stream_text_buffers
