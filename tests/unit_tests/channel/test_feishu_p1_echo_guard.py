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
    """短句回复（< _ECHO_MIN_CHARS）超紧窗后与入站全同照常放行，不误杀。

    语义更新（atomgit 2026-11-18 建议2）：紧窗（_ECHO_SHORT_WINDOW_SECONDS）
    内短文本全同按回声拦截（短催办回环治理），本用例锁定紧窗外的合法
    重复短句仍投递——原「任意时刻短句放行」口径由守卫二收紧替代。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "好的", ts=time.monotonic() - 45)
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
    # 评审意见 #202641557：群消息录制门按 mentions 判向——_fake_event 的群事件
    # 语义均为发给本 bot 的应答请求，补齐 mentions 形制与现实入站一致。
    if not msg.mentions:
        msg.mentions = [{"key": "@_user_1", "name": "u"}]
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
    """round-e P2#2：is_complete=False 的 chat.final 部分帧非终态——不弹缓冲不提前 finalize。"""
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
    assert ch._stream_text_buffers.get("req_t1") == echo

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
    # 本用例锁定「非终态进度帧延后收尾」（atomgit gw49 id=200074234 复审定案）：
    # 不显式设 is_processing 即非终态语义；终态收尾形态由配对用例
    # test_output_suppressed_processing_status_terminal_settles_buffer 单独锁定，
    # 两态成对闭合 D1 契约，杜绝单用例两种语义混装。
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._stream_text_buffers.get("req_t1") == "已流出文本"
    assert "req_t1" not in ch._errored_stream_keys


@pytest.mark.asyncio
async def test_output_suppressed_processing_status_terminal_settles_buffer():
    """配对锁（atomgit gw49 id=200074234）：is_processing=False 的终态抑制帧
    当场收尾——缓冲弹出、不登记 errored（抑制帧不投递，收尾非失败），
    终结「抑制终态后缓冲悬挂无归属」形态。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._stream_text_buffers["req_t1"] = "已流出文本"
    m = _msg(event_name="chat.processing_status", content="中间进度")
    m.payload["output_suppressed"] = True
    m.payload["is_processing"] = False
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._stream_text_buffers.get("req_t1") is None
    assert "req_t1" not in ch._errored_stream_keys

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
async def test_on_message_records_mention_position_variants():
    """位置变体（atomgit gw49 id=200150749）：同事 @ 在首位、机器人 @ 不在首位，
    录制集合仍含名字形解析态变体并命中回声守卫；期望值由 mention 元数据
    （key/name）运行时同源构造，非硬编码字面量；零 HTTP 锁定。"""
    ch = _make_channel()
    ch.config.bot_open_id = "ou_bot"
    ch._fetch_bot_open_id = MagicMock(
        side_effect=AssertionError("零 HTTP 红线：录制路径不得触发 bot 身份拉取"))
    ch._extract_receive_info = MagicMock(return_value=("oc_grp9", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    colleague = MagicMock()
    colleague.key = "@_user_9"
    colleague.name = "李四"
    bot = MagicMock()
    bot.key = "@_user_2"
    bot.name = "小闻"
    # 同事 @ 在首位，机器人 @ 在中间（不在首位）
    raw = "@_user_9 麻烦 @_user_2 复查一下昨晚的部署结果好吗"
    _wire_on_message_mocks(ch, raw)
    ch._replace_mentions_with_names = MagicMock(side_effect=lambda m, t: t)
    await ch._on_message(_fake_event("oc_grp9", raw, mentions=[colleague, bot]))
    # 期望名字形由运行时同源数据（mention.key→@mention.name 替换）构造
    parsed = raw
    for m_ in (colleague, bot):
        parsed = parsed.replace(m_.key, "@" + m_.name)
    entries = [t for t, _ts in ch._recent_user_inputs.get("oc_grp9", deque((), 8))]
    assert parsed in entries, "mention 任意位置的入站录制名字形变体缺位"
    m = _msg(event_name="chat.final", content=parsed,
             metadata={"feishu_chat_id": "oc_grp9"})
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    ch._fetch_bot_open_id.assert_not_called()

@pytest.mark.asyncio
async def test_cleanup_salvages_cardkit_when_receive_info_raises():
    """评审意见 #200679559：收件地址解析抛异常——cardkit 会话按请求前缀兜底
    finalize、缓冲回收，卡片不得永久停在「生成中」（终态收尾契约闭环）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(side_effect=RuntimeError("no address"))
    session = MagicMock()
    session.finalize = AsyncMock()
    ch._cardkit_sessions["req_t1\x00oc_ghost\x00chat_id"] = session
    ch._cardkit_buffers["req_t1\x00oc_ghost\x00chat_id"] = "已流出卡片文本"
    ch._stream_text_buffers["req_t1"] = "已流出增量"
    m = _msg(event_name="chat.final", content="守卫拦截的终态帧")
    m.payload["is_complete"] = True
    await ch._cleanup_suppressed_frame(m, "chat.final", "req_t1", None)
    assert not ch._cardkit_sessions  # 会话已兜底弹出
    session.finalize.assert_awaited_once()
    assert session.finalize.await_args.args[0] == "已流出卡片文本"
    assert "req_t1" not in ch._stream_text_buffers
    assert not ch._cardkit_buffers


@pytest.mark.asyncio
async def test_cleanup_error_salvage_still_registers_errored():
    """重排反向锁：解析异常路径 chat.error 的 errored 登记照旧（round-g P2 语义不丢）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(side_effect=RuntimeError("no address"))
    ch._stream_text_buffers["req_t1"] = "部分文本"
    m = _msg(event_name="chat.error", content="错误终帧")
    await ch._cleanup_suppressed_frame(m, "chat.error", "req_t1", None)
    assert "req_t1" in ch._errored_stream_keys
    assert "req_t1" not in ch._stream_text_buffers


@pytest.mark.asyncio
async def test_mention_prefix_keys_do_not_pollute_variants():
    """评审意见 #200715966：同消息内较短 mention key 是较长 key 的前缀
    （@_user_1 / @_user_12），str.replace 连带改写长 key 产生被污染变体。
    边界感知替换（数字前瞻）后长 key 保持完整。"""
    ch = _make_channel()
    ch.config.bot_open_id = "ou_bot"
    ch._fetch_bot_open_id = MagicMock(
        side_effect=AssertionError("零 HTTP 红线：录制路径不得触发 bot 身份拉取"))
    ch._extract_receive_info = MagicMock(return_value=("oc_pref", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    u1 = MagicMock()
    u1.key = "@_user_1"
    u1.name = "甲"
    u12 = MagicMock()
    u12.key = "@_user_12"
    u12.name = "乙"
    raw = "@_user_1 和 @_user_12 都在吗"
    _wire_on_message_mocks(ch, raw)
    ch._replace_mentions_with_names = MagicMock(side_effect=lambda m, t: t)
    await ch._on_message(_fake_event("oc_pref", raw, mentions=[u1, u12]))
    entries = [t for t, _ts in ch._recent_user_inputs.get("oc_pref", deque((), 8))]
    # 名字形：两个 key 各自替换为 @名字，长 key 不被短 key 连带改写
    expected = "@甲 和 @乙 都在吗"
    assert expected in entries, f"边界感知替换变体缺位: {entries}"
    # 反向锁：@_user_12 不得被短 key 污染成 @乙2
    assert all("@乙2" not in e for e in entries), f"长 key 被短 key 连带改写: {entries}"
    m = _msg(event_name="chat.final", content="@甲 和 @乙 都在吗",
             metadata={"feishu_chat_id": "oc_pref"})
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_chat_id_recording_skipped_with_debug_log(caplog):
    """评审意见 #200899315：chat_id 缺失的入站消息不得落空串键——空键与
    出站守卫键（feishu_chat_id/session_id，真实投递帧恒非空）永不匹配，录制
    纯属死重且漏拦不可追溯。跳过录制＋debug 留痕。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    msg = MagicMock()
    msg.message_type = "text"
    msg.content = '{"text": "用户原文超过二十字符以满足任何长度判定的文本"}'
    msg.chat_id = ""  # 键错位形态：chat_id 缺失
    msg.mentions = []
    msg.message_id = "om_noid"
    with caplog.at_level(logging.DEBUG, logger="agent_gateway.channel_manager.im_platforms.feishu.feishu_connect"):
        ch._record_inbound_user_input(msg)
    assert "" not in ch._recent_user_inputs, "空串键不得入队"
    assert any("empty chat_id" in r.message for r in caplog.records), "debug 留痕缺位"


@pytest.mark.asyncio
async def test_errored_stream_nonstream_status_no_completed():
    """建议1 回归（atomgit 2026-11-18）：已出错流的非流式冲刷面不得补发
    「[状态] 已完成」——chat.error 正常投递后，空正文终态 status 帧在
    非流分支此前不查 errored 标记直接走完成文案。修复前：完成行=红。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    await ch.send(_msg(event_name="chat.error", content="任务执行失败"))
    ch._send_feishu_message.reset_mock()
    st = _msg(event_name="chat.processing_status", content="")
    st.payload["is_processing"] = False
    await ch.send(st)
    sent = json.dumps(
        [c.args for c in ch._send_feishu_message.await_args_list], ensure_ascii=False)
    assert "已完成" not in sent
    assert "req_t1" not in ch._errored_stream_keys  # 标记被消费，无泄漏


@pytest.mark.asyncio
async def test_guard2_short_nudge_echo_suppressed_in_tight_window():
    """建议2 回归（atomgit 2026-11-18）：短催办（「继续」）不再被长度阈值
    整类排除比对面——紧窗（30s）内精确重复按回声拦截。修复前：短文被
    `_ECHO_MIN_CHARS` 预筛滤出 cand_set＝漏拦＝红。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "继续")
    m = _msg(event_name="chat.final", content="继续")
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_guard2_short_text_beyond_tight_window_delivered():
    """紧窗镜像：短文本记录超 30s 窗视为用户新意图，投递放行（防误杀）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "继续", ts=time.monotonic() - 45)
    m = _msg(event_name="chat.final", content="继续")
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_record_inbound_six_mentions_full_name_form_recorded():
    """评审意见 #201653214：mention n≥6——旧 ≤5 整块上限下全量替名/@_all 形
    均不生成，「全部 mention 解析为名字」回声形态零录制、守卫二静默漏拦。
    重构后锁定：①全量替名形存在；②原始态不被挤出；③逐摘除组合形封顶 5——
    第 6 个 mention 的摘除形不得出现（成本有界面）；④首 mention 摘除形存在。
    """
    ch = _make_channel()
    text = "@_user_1 请 @_user_2、@_user_3、@_user_4、@_user_5 和 @_user_6 各自确认排期"
    names = {"@_user_1": "小西", "@_user_2": "李四", "@_user_3": "王五",
             "@_user_4": "赵六", "@_user_5": "钱七", "@_user_6": "孙八"}
    im = types.SimpleNamespace(
        message_type="text",
        content=json.dumps({"text": text}),
        mentions=[types.SimpleNamespace(key=k, name=v) for k, v in names.items()],
        chat_id="oc_six")
    ch._record_inbound_user_input(im)
    texts = [t for t, _ in ch._recent_user_inputs.get("oc_six", ())]

    def _form(drop):
        v = text
        for i, (k, nm) in enumerate(names.items()):
            v = v.replace(k, "" if i == drop else "@" + nm)
        return v.strip()

    full = _form(-1)          # 全量替名（_drop=-1）
    drop0 = _form(0)          # 首 mention 摘除
    drop5 = _form(5)          # 第 6 个 mention 摘除（封顶外）
    assert full in texts, "n≥6 全名解析态回声形态必须录制"
    assert text in texts, "原始态不得被变体族挤出（容量裕度）"
    assert drop0 in texts or " ".join(drop0.split()) in texts, "首摘除形须存在（封顶内）"
    assert drop5 not in texts and " ".join(drop5.split()) not in texts, \
        "第 6 mention 摘除形不得生成（组合形封顶 5）"
    assert len(texts) < 64, "变体族须线性有界，不得挤爆环形缓存"


@pytest.mark.asyncio
async def test_salvage_finalize_carries_errored_interruption_note():
    """评审意见 #201821039：errored 快照在兜底路径不丢——收件地址解析失败走
    salvage finalize 时，errored 受抑终帧的 finalize 文本须追加「任务因错误
    中断」说明，与主收尾口径一致（裸缓冲收场形态移除）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(side_effect=RuntimeError("no address"))
    ch._errored_stream_keys.add("req_t1")
    session = MagicMock()
    session.finalize = AsyncMock()
    key = "req_t1\x00oc_ghost\x00chat_id"
    ch._cardkit_sessions[key] = session
    ch._cardkit_buffers[key] = "已流出卡片文本"
    m = _msg(event_name="chat.final", content="守卫拦截的终态帧")
    m.payload["is_complete"] = True
    await ch._cleanup_suppressed_frame(m, "chat.final", "req_t1", None)
    session.finalize.assert_awaited_once()
    assert "[状态] 任务因错误中断，未正常完成" in session.finalize.await_args.args[0]


@pytest.mark.asyncio
async def test_guard2_empty_session_key_leaves_trace_and_counts(caplog):
    """评审意见 #201821028：出站空键留痕——feishu_chat_id 与 session_id 双缺时
    守卫二必不命中，须打 debug 留痕（携 req）并 lifetime 计数 +1（stop() 汇总），
    与入站空 chat_id 的跳过＋留痕对称。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "oc_any", "台账里已有其他会话的记录")
    m = _msg(event_name="chat.final", content="好的")
    m.session_id = ""
    with caplog.at_level(logging.DEBUG, logger="agent_gateway.channel_manager.im_platforms.feishu.feishu_connect"):
        await ch.send(m)
    assert ch._echo_key_empty_total == 1
    assert any("empty session key" in r.getMessage() and "req_t1" in r.getMessage()
               for r in caplog.records)


@pytest.mark.asyncio
async def test_echo_guard_flush_over_4000_truncation_aligned():
    """评审意见 #201844323：>4000 字符流式回声——入站录制形态为截断形（4000+...），
    冲刷面候选比对前套用同一截断，守卫二必须命中抑制（修前冲刷候选为全文，
    与截断录制值必不相等＝静默漏拦，全文回声经冲刷面投递给用户）。"""
    full = "流式回声正文" * 1001  # 5005 字符 > 4000
    truncated = full[:4000].rstrip() + "..."
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", truncated)
    ch._stream_text_buffers["req_t1"] = full
    m = _msg(event_name="chat.processing_status", content="")
    m.payload["is_processing"] = False
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._echo_blocked_total == 1


@pytest.mark.asyncio
async def test_echo_guard_final_over_4000_truncation_aligned():
    """评审意见 #201844323 同族（终帧面 buffered_peek 候选）：>4000 流缓冲文本
    与截断录制值对齐后命中守卫二，修前 cand_set 全文形不等＝漏拦。"""
    full = "终帧回声正文" * 1001
    truncated = full[:4000].rstrip() + "..."
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", truncated)
    ch._stream_text_buffers["req_t1"] = full
    m = _msg(event_name="chat.final", content="")
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._echo_blocked_total == 1


# ── 守卫二 cardkit peek 精确键（多目标同请求 id 不串目标；解析异常退前缀兜底）──

def _routing_to(chat_id):
    from agent_gateway.routing.keys import FeishuDeliveryTarget
    from agent_gateway.routing.session_sharing import RoutingTarget
    return RoutingTarget(intent="mention", delivery=FeishuDeliveryTarget(
        chat_id=chat_id, id_type="chat_id"))


@pytest.mark.asyncio
async def test_guard2_cardkit_peek_targets_exact_delivery():
    """同请求 id 多 cardkit 会话时，peek 取 routing_target.delivery 精确键对应的
    本目标缓冲——比对面不得被其他目标的卡片文本顶替（peek 不 pop）。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_a", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._cardkit_buffers["req_t1\0oc_a\0chat_id"] = "目标A卡片文本"
    ch._cardkit_buffers["req_t1\0oc_b\0chat_id"] = "这么简单的事情，搞不定"
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    m = _msg(event_name="chat.final", content="正常回复正文，与入站不同")
    await ch.send(m, routing_target=_routing_to("oc_b"))
    ch._send_feishu_message.assert_not_awaited()
    assert ch._cardkit_buffers["req_t1\0oc_a\0chat_id"] == "目标A卡片文本"


@pytest.mark.asyncio
async def test_guard2_cardkit_peek_wrong_target_not_echo_killed():
    """投递目标 A 时只取 A 的缓冲：A 缓冲与入站不同文则终稿放行，不得被
    B 目标缓冲（与入站全同）顶替造成误拦。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_a", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._cardkit_buffers["req_t1\0oc_a\0chat_id"] = "目标A卡片文本"
    ch._cardkit_buffers["req_t1\0oc_b\0chat_id"] = "这么简单的事情，搞不定"
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    m = _msg(event_name="chat.final", content="正常回复正文，与入站不同")
    await ch.send(m, routing_target=_routing_to("oc_a"))
    ch._send_feishu_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_guard2_cardkit_peek_receive_parse_precise_key():
    """无 routing_target 时走收件解析重建精确键：解析出的目标缓冲参与比对，
    行为与 delivery 路径一致。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_b", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._cardkit_buffers["req_t1\0oc_a\0chat_id"] = "目标A卡片文本"
    ch._cardkit_buffers["req_t1\0oc_b\0chat_id"] = "这么简单的事情，搞不定"
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    m = _msg(event_name="chat.final", content="正常回复正文，与入站不同")
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_guard2_cardkit_peek_parse_failure_falls_back_to_prefix():
    """收件解析异常时退回请求前缀扫描兜底（与收尾侧主/兜底路径同构），
    前缀下首个非空缓冲仍参与比对，守卫不因解析失败而失效。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(side_effect=RuntimeError("no ctx"))
    ch._send_feishu_message = AsyncMock()
    ch._cardkit_buffers["req_t1\0oc_a\0chat_id"] = "这么简单的事情，搞不定"
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    m = _msg(event_name="chat.final", content="正常回复正文，与入站不同")
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()


# ── 误报面锁定＋抑制日志可观测性＋回退键留痕 ──

@pytest.mark.asyncio
async def test_echo_guard_group_member_unrelated_false_positive_face():
    """已知误报面锁定：录制范围覆盖群内全部非 bot 用户消息（含未 @ 机器人
    者），群成员无关消息与机器人合法回复全文相等时，当轮被守卫二误判回声
    拦截（单次消费后同文回复放行）。按现状锁定该边界——收敛录制范围属行
    为变更，若后续采纳须同步改写本用例与录制点注释。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_g", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "这批数据我先跑一轮基线")
    await ch.send(_msg(event_name="chat.final", content="这批数据我先跑一轮基线"))
    ch._send_feishu_message.assert_not_awaited()
    m2 = _msg(event_name="chat.final", content="这批数据我先跑一轮基线")
    m2.id = "req_t2"
    await ch.send(m2)
    ch._send_feishu_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_echo_guard_suppression_log_carries_matched_snippet_and_age(caplog):
    """抑制命中 warning 日志携命中记录截断片段（前 32 字符）与 age（秒）——
    「机器人没回」排查可直接定位触发的入站消息，免人工时间线对齐。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", "这么简单的事情，搞不定")
    with caplog.at_level(logging.WARNING):
        await ch.send(_msg(event_name="chat.final", content="这么简单的事情，搞不定"))
    recs = [r for r in caplog.records
            if "echo suppressed" in r.getMessage() and "status flush" not in r.getMessage()]
    assert recs, "suppression warning missing"
    assert "matched=这么简单的事情，搞不定" in recs[0].getMessage()
    assert "age=" in recs[0].getMessage()


@pytest.mark.asyncio
async def test_echo_guard_session_key_fallback_logged(caplog):
    """feishu_chat_id 缺失但 session_id 非空时，回退键形态打 debug 留痕＋
    lifetime 计数——排查可区分「台账无记录」与「键错位」两类漏拦。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._handle_cardkit_streaming_event = AsyncMock(return_value=False)
    _record_input(ch, "sess_x", "某条入站文本供守卫块进入")
    with caplog.at_level(logging.DEBUG):
        await ch.send(_msg(event_name="chat.final", content="普通回复正文，不命中守卫"))
    assert any("session-key fallback" in r.getMessage() for r in caplog.records)
    assert ch._echo_key_fallback_total == 1


@pytest.mark.asyncio
async def test_suppressed_terminal_frame_preserves_errored_marker():
    """错误序列：受回声抑制的终帧（非 chat.error）不得撤销 errored 标记——唯一消费
    点交回终态冲刷帧，避免其后 processing_status 补发「[状态] 已完成」。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._errored_stream_keys.add("req_t1")
    await ch._cleanup_suppressed_frame(
        _msg(event_name="chat.final", content="被抑制的终稿"), "chat.final", "req_t1", None)
    assert "req_t1" in ch._errored_stream_keys


@pytest.mark.asyncio
async def test_suppressed_clean_terminal_frame_clears_errored_marker():
    """正常终帧（无错误标记）收尾不留 errored——防泄漏清理契约不变。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    await ch._cleanup_suppressed_frame(
        _msg(event_name="chat.final", content="正常终稿"), "chat.final", "req_t1", None)
    assert "req_t1" not in ch._errored_stream_keys


@pytest.mark.asyncio
async def test_suppressed_error_frame_reinstates_errored_marker():
    """chat.error 受抑帧就地重登记（round-g 不变）：收尾后 errored 在席。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    await ch._cleanup_suppressed_frame(
        _msg(event_name="chat.error", content="错误帧"), "chat.error", "req_t1", None)
    assert "req_t1" in ch._errored_stream_keys


def test_recording_gate_skips_unrelated_group_message():
    """评审意见 #202641557：未被 mention 的群闲聊（数字分身关、非持久群）不得
    入录音——它不触发机器人应答，进相等比对集会吞等字合法回复；mention 群消息
    与 p2p 照常录。修复前：闲聊被录=红。"""
    from types import SimpleNamespace
    ch = _make_channel()
    unrelated = SimpleNamespace(chat_type="group", mentions=None, message_id="m_u",
                                message_type="text", content='{"text": "群員之間的无关闲聊"}')
    assert ch._inbound_input_worth_recording(unrelated) is False
    mentioned = SimpleNamespace(chat_type="group",
                                mentions=[{"key": "@_user_1", "name": "u"}], message_id="m_m",
                                message_type="text", content='{"text": "@bot 帮忙"}')
    assert ch._inbound_input_worth_recording(mentioned) is True
    direct = SimpleNamespace(chat_type="p2p", mentions=None, message_id="m_p",
                             message_type="text", content='{"text": "私聊"}')
    assert ch._inbound_input_worth_recording(direct) is True
