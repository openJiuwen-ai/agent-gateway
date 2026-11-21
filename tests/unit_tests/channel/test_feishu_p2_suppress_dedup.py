"""P2 suppression + dedup tests (2026-11-06 duplicate delivery judgement).

Reservation 契约（D4，取代旧两段式 check/record，atomgit gw50-2）：try_reserve
投递前原子预留 / commit(token) 投递成功落库（TTL 自 commit 起算，命中不续期）/
abort(token) 异常回滚，token 一次性、commit 后 abort 幂等；空 request_id 不豁免
（fail-closed 照常去重）。TTL 用注入时钟，不依赖真实 sleep（atomgit gw50-10）。
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest

# 评审意见 #202645537：模块级 skip（含响亮跳）本质未变——e2a 缺席时整文件依旧
# 零用例且退出码 0，宽 except 还会把真实导入破损吞成 SKIP。改与同目录约定一致
# 的硬导入，依赖缺失成为可见红灯失败；装配竞态在 CI/依赖安装层处理。
from e2a.message import Message
from agent_gateway.common.delivery_dedup import DeliveryDedup, request_seen_before
from tests.unit_tests.channel.test_feishu_p1_echo_guard import _make_channel, _msg

class _FakeClock:
    """注入时钟：advance 手动推时，TTL 用例零真实 sleep。"""
    def __init__(self, start=1000.0):
        self.t = float(start)
    def __call__(self):
        return self.t
    def advance(self, dt):
        self.t += float(dt)

# ── 组件级：维度锁定（每条断言只动一个维度）──

def test_dedup_same_session_same_content_different_request_dropped():
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "这是一条足够长的回复内容用于测试去重逻辑abcdef"
    reason, tok = d.try_reserve("s1", "r1", t)
    d.commit(tok)
    assert d.try_reserve("s1", "r2", t)[0] is not None

def test_dedup_reserve_inflight_blocks_and_abort_releases():
    """预留模型契约（D4 取代旧两段式）：未 commit 的在途预留即对异 rid 判重
    （并发双放行窗口消除）；abort 回滚后放行。（改名：旧名 check_is_pure_no_ledger_write
    描述的是被取代的旧语义，名实相反。）"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "投递前纯检查不落库的回复正文长度超过二十字符"
    reason1, tok = d.try_reserve("s1", "r1", t)
    assert reason1 is None and tok is not None
    reason2, tok2 = d.try_reserve("s1", "r2", t)  # 在途预留判重（非旧「不判重」语义）
    assert reason2 is not None and tok2 is None
    d.abort(tok)
    reason3, tok3 = d.try_reserve("s1", "r3", t)
    assert reason3 is None and tok3 is not None

def test_dedup_failed_delivery_retry_allowed():
    """投递失败语义：预留后投递失败 → abort 回滚 → 重答（新 request_id）放行。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "投递失败后的重答不被指纹误拦的回复正文超二十字符"
    reason, tok = d.try_reserve("s1", "r1", t)
    assert reason is None
    d.abort(tok)  # 模拟投递失败：回滚预留，不 commit
    reason2, tok2 = d.try_reserve("s1", "r2-retry", t)
    assert reason2 is None and tok2 is not None

def test_dedup_no_ttl_renewal_on_hit():
    """TTL 不续期锁定（atomgit gw50-1）：命中重复不刷新时间戳，自首次记账起算过期。"""
    clock = _FakeClock()
    d = DeliveryDedup(ttl_seconds=300, min_chars=20, clock=clock)
    t = "命中重复不刷新时间戳的回复正文长度超过二十字符"
    reason, tok = d.try_reserve("s1", "r1", t)
    d.commit(tok)
    clock.advance(299)
    assert d.try_reserve("s1", "r2", t)[0] is not None  # TTL 内判重
    clock.advance(2)  # 距 commit 301s > TTL：命中未续期 → 过期放行
    assert d.try_reserve("s1", "r3", t)[0] is None

def test_dedup_reason_contains_both_request_ids():
    """原因串对账锁定（atomgit gw50-6 / issue #95）：携带 first_req 与 req 双方。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "原因串携带双方请求标识的回复正文长度超过二十字符"
    reason0, tok = d.try_reserve("s1", "r1", t)
    d.commit(tok)
    reason, _ = d.try_reserve("s1", "r2", t)
    assert reason is not None
    assert "first_req=r1" in reason
    assert "req=r2" in reason

def test_dedup_same_session_different_content_allowed():
    """维度锁定：同 session、内容不同（>=20 字符）两条都放行（不误杀后续回复）。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t1 = "第一条回复：任务已完成的完整答复正文长度超过阈值线"
    t2 = "第二条回复：另一个问题的完整答复正文同样超过阈值线"
    assert len(d.normalize(t1)) >= 20 and len(d.normalize(t2)) >= 20
    reason0, tok = d.try_reserve("s1", "r1", t1)
    d.commit(tok)
    assert d.try_reserve("s1", "r2", t2)[0] is None

def test_dedup_same_content_different_session_allowed():
    """维度锁定：同内容、不同 session 都放行（session 隔离，跨会话不误杀）。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "这是一条足够长的回复内容用于测试会话隔离维度abcdefg"
    reason0, tok = d.try_reserve("s1", "r1", t)
    d.commit(tok)
    assert d.try_reserve("s2", "r2", t)[0] is None

def test_dedup_min_chars_boundary():
    """维度锁定：19/20/21 字符边界（<20 不落库不判重，>=20 才判重）。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    r, tok = d.try_reserve("s1", "r1", "a" * 19)
    assert r is None and tok is None            # <20 短路：放行且不预留
    assert len(d._seen) == 0
    r, tok = d.try_reserve("s1", "r2", "a" * 19)
    assert r is None and tok is None
    r, tok = d.try_reserve("s1", "r3", "a" * 20)
    assert r is None and tok is not None
    d.commit(tok)
    assert d.try_reserve("s1", "r4", "a" * 20)[0] is not None
    r, tok = d.try_reserve("s1", "r5", "a" * 21)
    d.commit(tok)
    assert d.try_reserve("s1", "r6", "a" * 21)[0] is not None

def test_dedup_same_request_replay_exempt():
    """request_id 契约：同 request 重放豁免，不同 request 才去重。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "同一请求重放豁免的回复正文长度超过二十个字符xyz"
    reason0, tok = d.try_reserve("s1", "r1", t)
    d.commit(tok)
    assert d.try_reserve("s1", "r1", t)[0] is None  # 同 request 重放 → 豁免
    assert d.try_reserve("s1", "r2", t)[0] is not None  # 不同 request → 去重
    assert request_seen_before("r1", "r1") is True
    assert request_seen_before("r1", "r2") is False

def test_dedup_short_repeat_allowed():
    """characterization（D9 改写）：min_chars 短路恒放行＋≥阈值文本预留/提交后去重回归锁。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    # <min_chars 恒放行（不预留、不判重）
    r1, t1 = d.try_reserve("s1", "r1", "好的")
    assert r1 is None and t1 is None
    r2, t2 = d.try_reserve("s1", "r2", "好的")
    assert r2 is None and t2 is None
    # ≥min_chars：预留→提交→异 rid 重复抑制；同 rid 提交后重放豁免
    long_t = "短的合法重复不丢弃abc超长文本锁定回归"
    r3, t3 = d.try_reserve("s1", "r1", long_t)
    assert r3 is None and t3 is not None
    d.commit(t3)
    r4, t4 = d.try_reserve("s1", "r2", long_t)
    assert r4 is not None and t4 is None
    r5, t5 = d.try_reserve("s1", "r1", long_t)  # 同 request 重放豁免
    assert r5 is None

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
async def test_suppressed_frame_does_not_write_dedup_ledger():
    """顺序锁定（atomgit gw50-9）：被抑制帧先于去重记账——抑制后账本为空，
    后续合法同文回复（新 request_id）不被误拦。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    long_text = "被抑制帧不得写入去重表的顺序锁定回复正文超过二十字符"
    m1 = _msg(event_name="chat.final", content=long_text)
    m1.payload["output_suppressed"] = True
    await ch.send(m1)
    ch._send_feishu_message.assert_not_awaited()
    m2 = _msg(event_name="chat.final", content=long_text)
    m2.id = "req_t2"
    await ch.send(m2)
    ch._send_feishu_message.assert_awaited_once()
    assert ch._delivery_dedup.suppressed_total == 0
    # round-t（verdict 200160424）：账本为空契约同步锁「无在途预留残留」——
    # 抑制帧只计数不留 reserved，否则后续合法帧被幽灵 in-flight 误拦。
    assert not [e for e in ch._delivery_dedup._seen.values() if e.state == "reserved"]

@pytest.mark.asyncio
async def test_streaming_suppressed_frame_finalizes_cardkit():
    """流式抑制收尾（atomgit gw50-8 / gw50-4）：cardkit 流式路径（生产默认）下
    output_suppressed 帧不投递，cardkit 会话被 finalize 回收（卡片不卡「生成中」）。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._handle_cardkit_streaming_event = AsyncMock(return_value=True)
    session = MagicMock()
    session.finalize = AsyncMock()
    key = "req_t1\0oc_x\0chat_id"
    ch._cardkit_sessions[key] = session
    ch._cardkit_buffers[key] = "已流出的增量文本"
    m = _msg(event_name="chat.final", content="被抑制的最终回复")
    m.payload["output_suppressed"] = True
    await ch.send(m)
    ch._handle_cardkit_streaming_event.assert_not_awaited()
    ch._send_feishu_message.assert_not_awaited()
    session.finalize.assert_awaited_once()
    assert key not in ch._cardkit_sessions

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

# ── B1 RED：D1 中间帧收尾门（plan §1.3 D1）──

@pytest.mark.asyncio
async def test_cleanup_processing_intermediate_frame_keeps_card_open():
    """is_processing 门（RED→GREEN）：中间处理帧被抑制收尾时卡片保持打开；
    终态帧（is_processing is False）照常收尾。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    session = MagicMock()
    session.finalize = AsyncMock()
    key = "req_t1\0oc_x\0chat_id"
    ch._cardkit_sessions[key] = session
    ch._cardkit_buffers[key] = "已流出增量"
    mid = _msg(event_name="chat.processing_status", content="中间进度")
    mid.payload["is_processing"] = True
    mid.payload["output_suppressed"] = True
    await ch.send(mid)
    session.finalize.assert_not_awaited()  # RED：现码中间帧提前关卡
    assert key in ch._cardkit_sessions      # 卡片保持打开
    assert key in ch._cardkit_buffers
    # 终态反向锁：is_processing=False 的抑制帧照常收尾
    term = _msg(event_name="chat.processing_status", content="终态进度")
    term.payload["is_processing"] = False
    term.payload["output_suppressed"] = True
    await ch.send(term)
    session.finalize.assert_awaited_once()
    assert key not in ch._cardkit_sessions

# ── B1 RED：D4 reservation 状态机（plan §1.3 D4 六＋一项）──

@pytest.mark.asyncio
async def test_delivery_dedup_concurrent_same_content_single_pass():
    """通道级并发双放行锁（RED）：两协程同文并发投递现码双 await→恰一放行。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    async def _slow_send(*a, **k):
        await asyncio.sleep(0.01)  # 真挂起点（round-h：lambda 返回未 await 协程无效）

    send_mock = AsyncMock(side_effect=_slow_send)
    ch._send_feishu_message = send_mock
    t = "同一内容两个协程并发投递只应放行一次的正文超过二十字符"
    m1 = _msg(event_name="chat.final", content=t)
    m2 = _msg(event_name="chat.final", content=t)
    m2.id = "req_t2"
    await asyncio.gather(ch.send(m1), ch.send(m2))
    assert send_mock.await_count == 1  # 现码 check→await→record 非原子＝2＝RED

def test_delivery_dedup_abort_allows_retry():
    """reserve→abort→再 reserve 放行（RED＝AttributeError＋行为双保险）。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "预留回滚后换请求可重试的回复正文超过二十字符"
    reason, tok = d.try_reserve("s1", "r1", t)
    assert reason is None and tok is not None
    d.abort(tok)
    reason2, tok2 = d.try_reserve("s1", "r2", t)
    assert reason2 is None and tok2 is not None

def test_delivery_dedup_commit_then_duplicate_suppressed():
    """commit 后同文异 rid 抑制＋指标。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "提交后同文异请求被抑制的回复正文超过二十字符"
    reason, tok = d.try_reserve("s1", "r1", t)
    assert reason is None
    d.commit(tok)
    reason2, tok2 = d.try_reserve("s1", "r2", t)
    assert reason2 is not None and tok2 is None
    assert "first_req=r1" in reason2 and "req=r2" in reason2
    assert d.suppressed_total == 1

def test_delivery_dedup_same_rid_replay_exempt_inflight():
    """in-flight 同 rid 豁免：预留未提交期间同 rid 重放放行、零抑制。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "在途预留期间同请求重放豁免的正文超过二十字"
    reason, tok = d.try_reserve("s1", "r1", t)
    assert reason is None and tok is not None
    reason2, tok2 = d.try_reserve("s1", "r1", t)
    assert reason2 is None
    assert d.suppressed_total == 0

@pytest.mark.asyncio
async def test_delivery_dedup_send_exception_rolls_back():
    """通道级异常回滚锁（取代旧自抛异常 tautology）：send() 投递抛异常经外层
    finally abort 回滚预留后，同文异 rid 的第二笔照常投递（账本无预留残骸、
    零抑制）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    boom = RuntimeError("simulated transport failure")
    ch._send_feishu_message = AsyncMock(side_effect=[boom, None])
    t = "投递异常回滚后账本不留预留残骸同文异请求照常投递正文超二十字"
    m1 = _msg(event_name="chat.final", content=t)
    with pytest.raises(RuntimeError):
        await ch.send(m1)
    m2 = _msg(event_name="chat.final", content=t)
    m2.id = "req_t2"
    await ch.send(m2)
    assert ch._send_feishu_message.await_count == 2  # 首笔失败尝试＋第二笔放行
    assert ch._delivery_dedup.suppressed_total == 0

def test_delivery_dedup_expired_reservation_evicted():
    """TTL 过期 reservation＝清理后视为新（注入时钟零 sleep）。"""
    clock = _FakeClock()
    d = DeliveryDedup(ttl_seconds=300, min_chars=20, clock=clock)
    t = "过期预留在时钟推进后被视为全新的正文超二十"
    reason, tok = d.try_reserve("s1", "r1", t)
    assert reason is None
    clock.advance(301)
    reason2, tok2 = d.try_reserve("s1", "r2", t)
    assert reason2 is None and tok2 is not None

def test_delivery_dedup_capacity_eviction_preserves_suppressed_total():
    """容量迁入（R3-P2-1）：_MAX_ENTRIES=1024 弹最早项，reserved/delivered 同池计容，
    suppressed_total 指标不随淘汰变动。现码基线另见 logs/B1 红证档（legacy record 路径）。"""
    from agent_gateway.common import delivery_dedup as dd
    d = DeliveryDedup(ttl_seconds=100000, min_chars=5)
    texts = [f"容量淘汰测试回复正文序号{i}超过最短阈值" for i in range(dd._MAX_ENTRIES + 1)]
    for i, t in enumerate(texts):
        reason, tok = d.try_reserve("s1", f"r{i}", t)
        assert reason is None and tok is not None
        if i % 2 == 0:
            d.commit(tok)  # delivered 与 reserved 混池
    assert len(d._seen) == dd._MAX_ENTRIES       # 超限弹最早项
    # 最早序号 0 已被淘汰 → 同文异 rid 重新放行
    reason, tok = d.try_reserve("s1", "r-again", texts[0])
    assert reason is None
    # suppressed_total 语义不受淘汰影响：commit 序列未抑制任何条目
    assert d.suppressed_total == 0


# ── round-d2 复审整改（P2①②③）──

@pytest.mark.asyncio
async def test_cleanup_nonterminal_frame_keeps_buffer():
    """P2①：非终态事件收尾不得弹出增量缓冲——缓冲弹出唯一发生在终态集合内。"""
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

def test_dedup_empty_request_id_not_exempted():
    """P2②（still 项）：同 rid 豁免仅限非空 request_id；空 rid 不豁免、照常去重。"""
    d = DeliveryDedup(ttl_seconds=300, min_chars=20)
    t = "足够长的回复内容用于验证空请求号不参与同rid豁免逻辑abcdef"
    reason, tok = d.try_reserve("s1", "", t)
    assert reason is None and tok is not None
    d.commit(tok)
    reason2, tok2 = d.try_reserve("s1", "", t)
    assert reason2 is not None and tok2 is None
    assert request_seen_before("", "") is False


@pytest.mark.asyncio
async def test_dedup_same_content_different_routing_targets_both_pass():
    """round-e new P2：同文投递到不同 routing_target＝各自合法投递，双双放行。"""
    from types import SimpleNamespace
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    t = "同内容分别投递到两个不同群目标各应正常送达一次的正文超过二十字符"
    m1 = _msg(event_name="chat.final", content=t)
    m2 = _msg(event_name="chat.final", content=t)
    m2.id = "req_t2"
    rt_a = SimpleNamespace(delivery=SimpleNamespace(chat_id="oc_a", id_type="chat_id"),
                           mention_member_ids=[], mention_all=False, intent="mention")
    rt_b = SimpleNamespace(delivery=SimpleNamespace(chat_id="oc_b", id_type="chat_id"),
                           mention_member_ids=[], mention_all=False, intent="mention")
    await ch.send(m1, routing_target=rt_a)
    await ch.send(m2, routing_target=rt_b)
    assert ch._send_feishu_message.await_count == 2

@pytest.mark.asyncio
async def test_dedup_same_routing_target_second_suppressed():
    """反向锁：同目标同文第二笔照常抑制（指纹含目标≠取消去重）。"""
    from types import SimpleNamespace
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    t = "同目标同内容第二笔仍应被抑制的正文长度也超过二十字符"
    m1 = _msg(event_name="chat.final", content=t)
    m2 = _msg(event_name="chat.final", content=t)
    m2.id = "req_t2"
    rt = SimpleNamespace(delivery=SimpleNamespace(chat_id="oc_a", id_type="chat_id"),
                         mention_member_ids=[], mention_all=False, intent="mention")
    await ch.send(m1, routing_target=rt)
    await ch.send(m2, routing_target=rt)
    assert ch._send_feishu_message.await_count == 1


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
async def test_dedup_hit_finalizes_card_with_empty_text():
    """round-i P2（issue #95 延伸）：去重命中收尾不得以重复正文 finalize 卡片——
    空文本只关闭卡片，重复终文不落卡片快照，也不发文本消息。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._start_cardkit_session = AsyncMock(return_value=False)  # 零真实 CardKit HTTP
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    t = "去重命中后卡片只能空文本关闭重复终文不得再补写进卡片快照正文"
    first = _msg(event_name="chat.final", content=t)
    first.payload["is_complete"] = True
    await ch.send(first)  # 第一笔正常投递（无卡片会话，走文本消息）
    session = MagicMock()
    session.finalize = AsyncMock()
    key = "req_t2\0oc_x\0chat_id"
    ch._cardkit_sessions[key] = session
    ch._cardkit_buffers[key] = t
    dup = _msg(event_name="chat.final", content=t)
    dup.id = "req_t2"
    dup.payload["is_complete"] = True
    await ch.send(dup)
    session.finalize.assert_awaited_once()
    assert session.finalize.await_args.args[0] == ""  # 空文本收尾，非重复正文
    assert key not in ch._cardkit_sessions and key not in ch._cardkit_buffers

@pytest.mark.asyncio
async def test_output_suppressed_delta_never_enters_stream_buffers():
    """issue #95 验收项：被抑制（output_suppressed=True）的 chat.delta 不得进
    流式缓冲（_stream_text_buffers/_cardkit_buffers），否则抑制文本部分入卡。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    d = _msg(event_name="chat.delta", content="被抑制阶段的流式增量片段内容")
    d.payload["output_suppressed"] = True
    await ch.send(d)
    assert "req_t1" not in ch._stream_text_buffers
    assert not any(k.startswith("req_t1\x00") for k in ch._cardkit_buffers)


@pytest.mark.asyncio
async def test_dedup_inflight_reason_carries_state_tag():
    """round-n P3：预留期重复的原因串携带 in-flight 状态标记（消费端据此等待结算）。"""
    d = DeliveryDedup()
    text = "预留期重复原因串需要携带状态标记的正文超过二十字符"
    _, t1 = d.try_reserve("s", "rid1", text)
    assert t1
    r2, _ = d.try_reserve("s", "rid2", text)
    assert r2 and "in-flight" in r2

@pytest.mark.asyncio
async def test_dedup_inflight_wait_settle_releases_after_abort():
    """round-n P3：首条投递失败（abort）→wait_settle 放行→重试预留成功（消双丢）。"""
    d = DeliveryDedup()
    text = "首条投递失败后重复帧应可重新预留投递的正文超过二十字符"
    _, t1 = d.try_reserve("s", "rid1", text)
    r2, _ = d.try_reserve("s", "rid2", text)
    assert r2 and "in-flight" in r2
    waiter = asyncio.create_task(d.wait_settle("s", text, 2.0))
    await asyncio.sleep(0.01)
    d.abort(t1)
    assert await waiter is True
    _, t2 = d.try_reserve("s", "rid2", text)
    assert t2

@pytest.mark.asyncio
async def test_dedup_inflight_wait_settle_then_delivered_suppress():
    """round-n P3：首条成功（commit）→wait_settle 放行→重试预留仍按已投递抑制。"""
    d = DeliveryDedup()
    text = "首条投递成功后重复帧仍应按已投递语义抑制的正文超过二十字符"
    _, t1 = d.try_reserve("s", "rid1", text)
    waiter = asyncio.create_task(d.wait_settle("s", text, 2.0))
    await asyncio.sleep(0.01)
    d.commit(t1)
    assert await waiter is True
    r2, _ = d.try_reserve("s", "rid2", text)
    assert r2 and "in-flight" not in r2

@pytest.mark.asyncio
async def test_dedup_wait_settle_timeout_false_is_fail_open_signal():
    """round-o P3：首条迟迟不结算→wait_settle 超时 False＝消费端 fail-open
    放行信号（at-least-once 切换，禁继续抑制——verdict 200088875 契约）。"""
    d = DeliveryDedup()
    text = "首条长时间未结算时等待方应超时返回假值的正文超过二十字符"
    d.try_reserve("s", "rid1", text)
    assert await d.wait_settle("s", text, 0.05) is False

@pytest.mark.asyncio
async def test_dedup_inflight_first_fails_second_delivers():
    """round-n P3（channel 级）：并发同文两帧，首条投递失败→第二帧不再双丢，
    等结算后重新预留并投递（旧实现第二帧直接抑制＝双份丢失）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._start_cardkit_session = AsyncMock(return_value=False)
    t = "并发同文两帧首条投递失败时第二帧不得双份丢失的正文超过二十字符"
    m1 = _msg(event_name="chat.final", content=t)
    m2 = _msg(event_name="chat.final", content=t)
    m2.id = "req_t2"
    calls = {"n": 0}

    async def _first_fails(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            await asyncio.sleep(0.05)
            raise RuntimeError("first delivery failed")
        return None

    ch._send_feishu_message = AsyncMock(side_effect=_first_fails)
    results = await asyncio.gather(ch.send(m1), ch.send(m2), return_exceptions=True)
    assert isinstance(results[0], RuntimeError)
    assert results[1] is None
    assert ch._send_feishu_message.await_count == 2


@pytest.mark.asyncio
async def test_dedup_plain_dm_targets_isolated():
    """round-o P3：无 routing_target 的 DM/open_id 私发路径——指纹并入真实投递
    目标，同会话键不同收件人互不误杀（旧实现会话键同值＝跨用户抑制）。"""
    ch = _make_channel()
    def _recv(msg, *a, **k):
        return ("oc_u1", "open_id") if str(getattr(msg, "id", "")) == "req_t1" else ("oc_u2", "open_id")

    ch._extract_receive_info = MagicMock(side_effect=_recv)
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    t = "同会话键不同私发收件人各自合法投递的正文长度超过二十字符"
    m1 = _msg(event_name="chat.final", content=t)
    m2 = _msg(event_name="chat.final", content=t)
    m2.id = "req_t2"
    await ch.send(m1)
    await ch.send(m2)
    assert ch._send_feishu_message.await_count == 2

@pytest.mark.asyncio
async def test_dedup_settle_wait_timeout_fail_open_delivers():
    """round-o P3（channel 级）：等待超时→fail-open 放行投递（不丢回复）；
    病态慢投递下残余双投递风险劣于回复丢失（verdict 200088550）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._start_cardkit_session = AsyncMock(return_value=False)
    ch._dedup_settle_wait = 0.05
    t = "等待结算超时后第二帧应放行投递而不丢失回复的正文超过二十字符"
    m1 = _msg(event_name="chat.final", content=t)
    m2 = _msg(event_name="chat.final", content=t)
    m2.id = "req_t2"

    async def _slow(*a, **k):
        await asyncio.sleep(0.2)
        return None

    ch._send_feishu_message = AsyncMock(side_effect=_slow)
    results = await asyncio.gather(ch.send(m1), ch.send(m2), return_exceptions=True)
    assert results[0] is None and results[1] is None
    assert ch._send_feishu_message.await_count == 2


@pytest.mark.asyncio
async def test_dedup_fingerprint_same_source_as_merged_delivery():
    """round-q P2（verdict 200095286）：is_complete 时指纹按实投正文（流式缓冲合并正文）
    同源入账——仅帧身入账会让同合并文重复 final 帧漏拦双投。"""
    full = "流式缓冲拼终帧尾巴得到实际投递正文长度超过二十字符"
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m1 = _msg(event_name="chat.final", content="")
    m1.payload["is_complete"] = True
    ch._stream_text_buffers["req_t1"] = full
    await ch.send(m1)
    ch._send_feishu_message.assert_awaited_once()
    m2 = Message(id="req_t2", type="event", channel_id="feishu", session_id="sess_x",
                 params={}, timestamp=0, ok=True,
                 payload={"content": full, "event_type": "chat.final"},
                 event_type=None, metadata={}, app_id="app", agent_ref=None)
    await ch.send(m2)
    ch._send_feishu_message.assert_awaited_once()  # 重复帧须全抑制


@pytest.mark.asyncio
async def test_status_flush_delivery_recorded_in_dedup():
    """round-q P2（verdict 200095286）：终态冲刷投递入去重账——后到同文重复 final
    帧按账去重丢弃，双向对称。"""
    full = "终态冲刷投递出去的流式缓冲正文同样进入去重账本受保护超长"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    m1 = _msg(event_name="chat.processing_status", content="")
    m1.payload["is_processing"] = False
    ch._stream_text_buffers["req_t1"] = full
    await ch.send(m1)
    ch._send_feishu_message.assert_awaited_once()
    # 重复帧携独立请求号（同 rid 属重发豁免面，非去重面）
    m2 = Message(id="req_t2", type="event", channel_id="feishu", session_id="sess_x",
                 params={}, timestamp=0, ok=True,
                 payload={"content": full, "event_type": "chat.final"},
                 event_type=None, metadata={}, app_id="app", agent_ref=None)
    await ch.send(m2)
    ch._send_feishu_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_dedup_fingerprint_uses_cardkit_stream_buffer_preview():
    """round-r（verdict 200136409）：默认流式（cardkit）配置 delta 累积在
    _cardkit_buffers——final 帧实投合并正文入卡，指纹必须同源合并预览入账；
    后到同合并文重复 final 帧须被拦。"""
    full = "卡谈流式缓冲正文经终帧合并后实际写入卡片并须同源入账超过二十字符"
    key = "req_t1\0oc_x\0chat_id"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=True)
    session = MagicMock()
    session.replace = MagicMock()
    session.finalize = AsyncMock()
    ch._cardkit_sessions[key] = session
    ch._cardkit_buffers[key] = full
    m1 = _msg(event_name="chat.final", content="")
    await ch.send(m1)
    session.finalize.assert_awaited_once_with(full)
    m2 = Message(id="req_t2", type="event", channel_id="feishu", session_id="sess_x",
                 params={}, timestamp=0, ok=True,
                 payload={"content": full, "event_type": "chat.final"},
                 event_type=None, metadata={}, app_id="app", agent_ref=None)
    sup0 = ch._delivery_dedup.suppressed_total
    await ch.send(m2)
    # round-s（verdict 200147110）：去向须受保护——重复帧按去重抑制入账，
    # 卡片不再 finalize，投递零发生（断言非恒真）。
    assert ch._delivery_dedup.suppressed_total > sup0
    session.finalize.assert_awaited_once()
    ch._send_feishu_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_dedup_fingerprint_no_merge_when_no_terminal_source():
    """round-s（verdict 200147110）：is_complete 非 True 且无 cardkit 活动会话——
    实投＝帧正文本身，指纹不得多并增量缓冲；同帧文重复帧（独立 rid）仍须拦。"""
    frame = "无终态合并来源时实投即帧正文指纹同源重复帧仍须拦截超长文"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    ch._stream_text_buffers["req_t1"] = "增量缓冲里是另一段完全不同的流式文本内容不应并入指纹"
    m1 = _msg(event_name="chat.final", content=frame)
    await ch.send(m1)
    ch._send_feishu_message.assert_awaited_once()
    m2 = Message(id="req_t2", type="event", channel_id="feishu", session_id="sess_x",
                 params={}, timestamp=0, ok=True,
                 payload={"content": frame, "event_type": "chat.final"},
                 event_type=None, metadata={}, app_id="app", agent_ref=None)
    await ch.send(m2)
    ch._send_feishu_message.assert_awaited_once()  # 同帧文重复帧拦截


@pytest.mark.asyncio
async def test_status_flush_inflight_first_abort_redelivers():
    """round-r（verdict 200136409）：冲刷面 in-flight 时首条 abort 结算——
    结算后须重试预留，本帧照常投递并入账，不得双丢（镜像 final 路径形制）。"""
    text = "冲刷首条在飞中止结算后重算预留本帧须照常投递不得双丢超长文本"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._start_cardkit_session = AsyncMock(return_value=False)
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m = _msg(event_name="chat.processing_status", content="")
    m.payload["is_processing"] = False
    # P2（atomgit 2026-11-18）：键一律由单源 _p2_dedup_session 派生——手拼键形制
    # 在键演进（如新增发起人维度）时不会变化，正是账本缺陷被遮蔽的形态。
    sess = ch._p2_dedup_session(m.metadata or {}, m, None)
    reason0, tok0 = ch._delivery_dedup.try_reserve(sess, "req_first", text)
    assert reason0 is None and tok0
    seen_keys: list[str] = []
    _orig_reserve = ch._delivery_dedup.try_reserve

    def _spy(s, rid, t):
        seen_keys.append(s)
        return _orig_reserve(s, rid, t)

    ch._delivery_dedup.try_reserve = _spy

    async def fake_settle(s, t, w):
        ch._delivery_dedup.abort(tok0)  # 模拟首条投递失败结算（预留撤销）
        return True
    ch._delivery_dedup.wait_settle = fake_settle
    ch._stream_text_buffers["req_t1"] = text
    await ch.send(m)
    ch._send_feishu_message.assert_awaited_once()
    assert sess in seen_keys  # 冲刷以单源同键重算预留（结算后重试，不得双丢）


@pytest.mark.asyncio
async def test_final_and_terminal_flush_dedupe_same_request_id():
    """round-t（verdict 200160424）：final 与终态冲刷同 request_id——同 rid 重放
    豁免不得跨面豁免重复投递：final 已投递正文与冲刷缓冲同文时，冲刷侧须按账去重。"""
    text = "终帧已投递后同文终态冲刷不得以同请求号豁免造成二次投递超长文本"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._start_cardkit_session = AsyncMock(return_value=False)
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    m1 = _msg(event_name="chat.final", content=text)
    await ch.send(m1)
    ch._send_feishu_message.assert_awaited_once()
    m2 = _msg(event_name="chat.processing_status", content="")
    m2.payload["is_processing"] = False
    ch._stream_text_buffers["req_t1"] = text
    await ch.send(m2)
    ch._send_feishu_message.assert_awaited_once()  # 冲刷同文抑制


@pytest.mark.asyncio
async def test_echo_suppressed_partial_final_buffer_not_reflushed():
    """round-w（verdict 200184387）：回声拦截 is_complete=False 部分帧未清增量缓冲——
    回声记录超窗失效后，终态冲刷面把残留缓冲（回声文）再次投递。拦截即清缓冲。"""
    from tests.unit_tests.channel.test_feishu_p1_echo_guard import _record_input
    echo = "回声残留缓冲不得在记录超窗后被终态冲刷面再次投递出来造成复读"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    _record_input(ch, "sess_x", echo)
    ch._stream_text_buffers["req_t1"] = echo
    m = _msg(event_name="chat.final", content=echo)
    m.payload["is_complete"] = False
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()  # 守卫拦截生效
    assert "req_t1" not in ch._stream_text_buffers  # round-w：拦截即清缓冲
    ch._recent_user_inputs.clear()  # 模拟回声记录过期超窗（冲刷面回声守卫失配）
    ch._extract_message_content = MagicMock(return_value="")
    m2 = _msg(event_name="chat.processing_status", content="")
    m2.payload["is_processing"] = False
    await ch.send(m2)
    ch._send_feishu_message.assert_not_awaited()  # 冲刷面无残留正文可投


@pytest.mark.asyncio
async def test_dedup_suppressed_partial_final_buffer_not_reflushed():
    """round-w（verdict 200184387）：去重拦截部分帧未清增量缓冲——残留缓冲（含已
    投递正文的超集文）经终态冲刷面再次投递。"""
    text = "已投递正文被重复部分帧去重拦截后其增量缓冲残留不得再经终态冲刷面投递"
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    m1 = _msg(event_name="chat.final", content=text)  # rid=req_t1 正常投递并落账
    await ch.send(m1)
    ch._send_feishu_message.assert_awaited_once()
    # 重复流在新请求号下重新累积缓冲（正文超集：已投递正文＋增量尾巴）
    ch._stream_text_buffers["req_t2"] = text + "——补流增量残留尾巴不应被冲刷再投"
    m2 = Message(id="req_t2", type="event", channel_id="feishu", session_id="sess_x",
                 params={}, timestamp=0, ok=True,
                 payload={"content": text, "event_type": "chat.final",
                          "is_complete": False},
                 event_type=None, metadata={}, app_id="app", agent_ref=None)
    await ch.send(m2)  # 去重拦截（rid 异形成，正文已落账）
    ch._send_feishu_message.assert_awaited_once()
    assert "req_t2" not in ch._stream_text_buffers  # round-w：拦截即清本帧缓冲
    ch._extract_message_content = MagicMock(return_value="")
    m3 = Message(id="req_t2", type="event", channel_id="feishu", session_id="sess_x",
                 params={}, timestamp=0, ok=True,
                 payload={"content": "", "event_type": "chat.processing_status",
                          "is_processing": False},
                 event_type=None, metadata={}, app_id="app", agent_ref=None)
    await ch.send(m3)
    ch._send_feishu_message.assert_awaited_once()  # 冲刷面不得再投残留缓冲


def _lifecycle_data(create_time="1730000000000"):
    return {
        "header": {"event_id": "evt_1", "create_time": create_time},
        "event": {"chat_id": "oc_group", "message_id": "om_1",
                  "after_change": {"name": "群名"}},
    }


def _lifecycle_channel(disposition):
    ch = _make_channel()
    ch.config.agent_id = "agent_x"
    ch._persistent_group_fanout_callback = AsyncMock(return_value=disposition)
    return ch


@pytest.mark.asyncio
async def test_lifecycle_permanent_disposition_acks_without_raise():
    """P2③：REJECTED/UNCLAIMED＝永久结局→记日志 ACK（抛出＝无 ACK→无限重投）。"""
    from agent_gateway.channel_manager.persistent_events import InboundDisposition
    for disposition in (InboundDisposition.REJECTED, InboundDisposition.UNCLAIMED):
        ch = _lifecycle_channel(disposition)
        await ch._on_persistent_change(_lifecycle_data(), "chat.member_changed")
        ch._persistent_group_fanout_callback.assert_awaited_once()


@pytest.mark.asyncio
async def test_lifecycle_invalid_create_time_acks_without_raise():
    """P2③：create_time 非法＝畸形事件，重投不可能修复→ACK 丢弃并留日志。"""
    ch = _make_channel()
    ch.config.agent_id = "agent_x"
    ch._persistent_group_fanout_callback = AsyncMock()
    await ch._on_persistent_change(_lifecycle_data(create_time="not-a-time"),
                                   "chat.member_changed")
    ch._persistent_group_fanout_callback.assert_not_awaited()


@pytest.mark.asyncio
async def test_lifecycle_retryable_disposition_still_raises():
    """P2③ 反向锁：RETRYABLE＝瞬态，仍抛出交 SDK 重投（修复不得吞可重试失败）。"""
    from agent_gateway.channel_manager.persistent_events import InboundDisposition
    ch = _lifecycle_channel(InboundDisposition.RETRYABLE)
    with pytest.raises(RuntimeError):
        await ch._on_persistent_change(_lifecycle_data(), "chat.member_changed")

# ── 评审意见 #200634425（2026-11-13）：cardkit 终态帧绕账缺口 ──

_CK_LONG = "卡片流式最终正文需要超过二十个字符长度阈值才能进入去重指纹面判定验证"

@pytest.mark.asyncio
async def test_cardkit_terminal_status_frame_records_ledger():
    """cardkit 会话在席的 processing_status 终帧 finalize 后，最终正文必须入去重账
    （与 chat.final 面同形制：预留→投递成功 commit）。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    key = "req_t1\0oc_x\0chat_id"
    session = MagicMock()
    session.finalize = AsyncMock()
    ch._cardkit_sessions[key] = session
    ch._cardkit_buffers[key] = _CK_LONG
    m = _msg(event_name="chat.processing_status", content="")
    m.payload["is_processing"] = False
    await ch.send(m)
    session.finalize.assert_awaited_once()
    # 账本探针：同会话键（sess_x＋tgt 路由维度）同文不同 rid 必须判重复
    reason, _ = ch._delivery_dedup.try_reserve(
        "sess_x\x00tgt:chat_id:oc_x", "probe-rid", _CK_LONG)
    assert reason is not None, "cardkit 终帧正文未入去重账（verdict 200634425 缺口）"

@pytest.mark.asyncio
async def test_cardkit_terminal_then_replayed_final_not_redelivered():
    """终帧入账后，网关重投同文 chat.final：卡片会话已弹出，重投帧必须被账本抑制
    （不新建卡片、不降级普通投递全文）。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    key = "req_t1\0oc_x\0chat_id"
    session = MagicMock()
    session.finalize = AsyncMock()
    ch._cardkit_sessions[key] = session
    ch._cardkit_buffers[key] = _CK_LONG
    m1 = _msg(event_name="chat.processing_status", content="")
    m1.payload["is_processing"] = False
    await ch.send(m1)
    replay = _msg(event_name="chat.final", content=_CK_LONG)
    replay.id = "req_replay"
    replay.payload["is_complete"] = True
    await ch.send(replay)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._delivery_dedup.suppressed_total == 1


@pytest.mark.asyncio
async def test_cross_face_suppression_empty_delivery_chat_id():
    """评审意见 #200679984：routing_target 定向投递且 delivery.chat_id 为空——
    cardkit 终帧入账键与同文 chat.final 重投入账键必须同源（统一回退
    _extract_receive_info 入 tgt 维度），跨面同文重投被抑制。"""
    from types import SimpleNamespace
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("ou_ghost", "open_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    t = "当delivery无chat_id时cardkit入账与chat_final重投必须同键被抑制的正文超长部分"
    rt = SimpleNamespace(delivery=SimpleNamespace(chat_id="", id_type=""),
                         mention_member_ids=[], mention_all=False, intent="mention")
    m_ck = _msg(event_name="chat.final", content=t)
    sess = ch._p2_dedup_session(m_ck.metadata or {}, m_ck, rt.delivery)
    assert sess.endswith("\x00tgt:open_id:ou_ghost")  # 回退入键（cardkit 面形制）
    reason, tok = ch._delivery_dedup.try_reserve(sess, "ck-rid", t)
    assert reason is None and tok is not None
    ch._delivery_dedup.commit(tok)
    m2 = _msg(event_name="chat.final", content=t)
    m2.id = "req_t2"
    await ch.send(m2, routing_target=rt)
    ch._send_feishu_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_p2_replayed_final_suffix_fingerprint_suppressed():
    """评审意见 #200722011：cardkit 终帧在席时以 merge(缓冲,帧正文) 入账；重投帧
    （缓冲已被 finalize 清空、指纹退化为帧正文且仅为上次入账的尾部）不得漏拦——
    stream_key 级最近入账指纹兜底判定，第二张尾部卡片不再产生。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    tail = "这是仅携带尾部增量的终帧正文重投会被指纹兜底抑制的部分"
    fp = "首次入账的合并正文前段内容远超下限" + tail
    ch._p2_final_fingerprints["req_t1"] = fp
    m = _msg(event_name="chat.final", content=tail)
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()   # 修复前：帧正文未入过账→放行=红


@pytest.mark.asyncio
async def test_p2_replayed_final_equal_fingerprint_suppressed():
    """评审意见 #201580054：正文与上次入账指纹完全相等的重投帧是最强重投信号
    （重投时缓冲已弹出、入账文本退化为帧正文恰与指纹相等）——此前被
    _fp != _dedup_text 条件排除在兜底外而经同请求号重放豁免二次投递；现与
    suffix 形态同受抑制（endswith 对相等情形成立）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    same = "重投帧正文与上次入账指纹完全相等的终帧形态超过入账下限的正文"
    ch._p2_final_fingerprints["req_t1"] = same
    m = _msg(event_name="chat.final", content=same)
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()   # 修复前：相等被 != 条件排除→放行=红


@pytest.mark.asyncio
async def test_p2_fingerprint_guard_fail_open_on_unrelated_text():
    """反向锁：候选与最近入账指纹无尾部关系（新回复）→ 照常放行，不误杀。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    ch._p2_final_fingerprints["req_t1"] = "上一条完全不同内容的终帧入账文本超长部分"
    m = _msg(event_name="chat.final", content="新一轮回复内容与此前的终帧完全无关的正文")
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_awaited_once()


def test_p2_fingerprint_store_bounded_and_cleared_on_stop():
    """评审意见 #200722042：最近终帧入账指纹表容量上限 256（淘汰最旧）＋stop() 清理契约。"""
    ch = _make_channel()
    for i in range(300):
        ch._p2_final_fingerprints[f"req_{i}"] = f"正文-{i}-足够长度内容超过下限要求"
    ch._store_p2_final_fingerprint("req_300", "新正文内容同样满足长度下限要求数据")
    assert len(ch._p2_final_fingerprints) <= 256
    assert "req_0" not in ch._p2_final_fingerprints  # 最旧被淘汰
    assert ch._p2_final_fingerprints.get("req_300") is not None



@pytest.mark.asyncio
async def test_p2_cardkit_face_fingerprint_stored_and_replay_suppressed():
    """评审意见 #200833593：cardkit chat.final 投递面留存指纹——流式开启＋cardkit
    会话在席＋真 finalize 契约（弹会话/缓冲）形态：首帧经 cardkit 分支后指纹＝
    merge(缓冲,帧正文) 合并全文；清空后的同帧尾部重投被指纹兜底抑制。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    stub = MagicMock()
    stub.finalize = AsyncMock()
    _ck_key = "req_t1\x00oc_x\x00chat_id"
    ch._cardkit_sessions[_ck_key] = stub
    merged = "首次入账的合并正文前段内容远超下限要求内容足够长" + "尾部增量文本"
    ch._cardkit_buffers[_ck_key] = merged  # cardkit 缓冲在席＝首帧形态

    async def _finalize_like_real(key, session, text):
        await session.finalize(text)
        ch._cardkit_sessions.pop(key, None)
        ch._cardkit_buffers.pop(key, None)
        return True

    ch._finalize_cardkit_session = _finalize_like_real  # 真 finalize 契约（弹会话/缓冲）
    m = _msg(event_name="chat.final", content="尾部增量文本")
    m.payload["is_complete"] = True
    await ch.send(m)
    assert (ch._p2_final_fingerprints.get("req_t1") or ("", ""))[1] == merged   # (target, text) 形制：cardkit 面留存合并指纹
    stub.finalize.assert_awaited()                              # 卡片真 finalize
    assert _ck_key not in ch._cardkit_sessions and _ck_key not in ch._cardkit_buffers
    # 同帧重投（同 id、缓冲/会话已清、帧仅尾部正文）——同 rid 豁免形态亦拦
    m2 = _msg(event_name="chat.final", content="尾部增量文本")
    m2.payload["is_complete"] = True
    await ch.send(m2)
    ch._send_feishu_message.assert_not_awaited()                # 指纹兜底抑制


@pytest.mark.asyncio
async def test_p2_flush_face_fingerprint_replay_suppressed():
    """评审意见 #200833593：冲刷面接指纹兜底——processing_status 终态重投（缓冲已
    清、帧仅尾部增量）在此面被抑制（cardkit 终帧面写入的指纹有了消费点）。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    merged = "首次入账的合并正文前段内容远超下限要求内容足够长尾部增量文本"
    ch._p2_final_fingerprints["req_t1"] = merged
    # 评审意见 #202654733：冲刷面正文经流式缓冲 pop 入面（帧正文为空的真实重投形制）——
    # 仅携帧正文时该帧在「内容为空跳过发送」处自然不投递，not-awaited 恒真、兜底未达。
    ch._stream_text_buffers["req_t1"] = "尾部增量文本"
    m = _msg(event_name="chat.processing_status", content="尾部增量文本")
    m.payload["is_processing"] = False
    await ch.send(m)
    # 补正向计数断言真正锁定「被指纹兜底抑制」这一行为本身。
    assert ch._fingerprint_suppressed_total == 1
    ch._send_feishu_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_p2_fingerprint_cleared_on_stop():
    """评审意见 #200833594：指纹表纳入 stop() 清理契约——stop 后表为空。"""
    ch = _make_channel()
    ch._p2_final_fingerprints["req_t1"] = "某次成功投递的合并正文足够长内容数据"
    ch._shutdown_ws_client = AsyncMock()
    ch._websocket_client = None
    ch._ws_thread_loop = None
    ws_connected = asyncio.Event()
    ch._websocket_connected = ws_connected
    try:
        await ch.stop()
    except Exception:
        pass  # 只锁指纹表清理契约，stop 链上其他面 mock 缺省可容忍
    assert ch._p2_final_fingerprints == {}


def test_capacity_eviction_prefers_delivered_entries():
    """评审意见 #200835109：容量淘汰优先弹已结算（delivered）条目——
    reserved（in-flight）条目被误弹时 wait_settle 把「条目不存在」当已结算，
    消费端重试预留重新放行，首条仍在投递即出二次投递。"""
    dd = DeliveryDedup(ttl_seconds=300, min_chars=20)
    # 槽 0＝reserved（最旧），其余 delivered：淘汰必须弹 delivered 不弹 reserved。
    reason, token = dd.try_reserve("sess", "req-0", "首条在飞的长文本内容长度超过二十个字符阈值线")
    assert reason is None and token is not None
    for i in range(_MAX_ENTRIES_TEST):
        r, t = dd.try_reserve("sess", f"req-{i+1}", f"已结算投递的内容编号{i:04d}长度超过二十个字符阈值线")
        dd.commit(t)
    assert len(dd._seen) == _MAX_ENTRIES_TEST
    # 新预留触发容量压力：必须弹 delivered（编号 0000），reserved 的 req-0 存活。
    r2, t2 = dd.try_reserve("sess", "req-new", "新预留状态机的长文本内容长度同样超过二十个字符阈值线")
    assert r2 is None and t2 is not None
    assert ("sess", dd.fingerprint("sess", "首条在飞的长文本内容长度超过二十个字符阈值线")[1]) in dd._seen
    assert len(dd._seen) == _MAX_ENTRIES_TEST

_MAX_ENTRIES_TEST = 1024  # 与 delivery_dedup._MAX_ENTRIES 同值


def test_capacity_eviction_never_orphans_inflight_waiters():
    """全 reserved 满池时淘汰最旧 in-flight 条目：置位 settled 事件再弹出，
    已捕获事件的等待方随即释放重试预留（不孤儿化）。"""
    dd = DeliveryDedup(ttl_seconds=300, min_chars=20)
    text0 = "全池在飞场景的最旧预留文本内容长度超过二十个字符阈值线"
    _, token0 = dd.try_reserve("sess", "req-0", text0)
    entry0 = dd._seen[dd.fingerprint("sess", text0)]
    waiter = entry0.settled
    for i in range(_MAX_ENTRIES_TEST):
        r, t = dd.try_reserve("sess", f"req-{i+1}", f"其余在飞预留的内容编号{i:04d}长度超过二十个字符阈值线")
    assert len(dd._seen) == _MAX_ENTRIES_TEST
    # 新预留触发淘汰：无 delivered 可弹 → 弹最旧 reserved（req-0）并置位其事件。
    r2, t2 = dd.try_reserve("sess", "req-new", "触发淘汰的新预留文本内容长度超过二十个字符阈值线")
    assert r2 is None and t2 is not None
    assert waiter.is_set(), "in-flight 条目被淘汰时必须先置位 settled 再弹出"
    assert ("sess", dd.fingerprint("sess", text0)[1]) not in dd._seen


@pytest.mark.asyncio
async def test_flush_redelivery_suppressed_by_stored_flush_fingerprint():
    """评审意见 #200835803：首次投递走冲刷面（无 chat.final），重投帧正文
    退化为尾部片段时，此前指纹表空 → 尾缀兜底取不到指纹 → 尾部片段作为新消息
    二次投递。修复后冲刷正文入账指纹（=实际投递正文），重投帧被尾缀兜底拦截。"""
    ch = _make_channel()
    ch.config.enable_streaming = True
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    full = "自增量缓冲冲刷出的完整流式正文长度超过二十个字符阈值线"
    ch._stream_text_buffers["req_t1"] = full
    m1 = _msg(event_name="chat.processing_status", content="")
    m1.payload["is_processing"] = False
    await ch.send(m1)
    assert ch._send_feishu_message.await_count == 1, "首次冲刷投递应成功"
    # 契约锁：冲刷面投递成功后，指纹表留存的是**实际投递正文**（此前恒为空串不入账）。
    assert (ch._p2_final_fingerprints.get("req_t1") or ("", ""))[1] == full
    # 重投帧（缓冲已弹空，正文退化为尾部片段——生产形态由上游重投产生）：
    # 尾缀兜底须消费冲刷面留存的指纹拦截，不得二次投递。
    frag = full[-12:]
    ch._extract_message_content = MagicMock(return_value=frag)
    m2 = _msg(event_name="chat.processing_status", content="")
    m2.payload["is_processing"] = False
    await ch.send(m2)
    assert ch._send_feishu_message.await_count == 1, "重投帧的尾部片段必须被尾缀兜底拦截"



@pytest.mark.asyncio
async def test_dedup_session_key_unified_across_routing_shapes():
    """评审意见 #200900174：同目标、不同路由形制（delivery 在席 vs 回退解析）
    须产生同一去重会话键——键形制统一为 tgt:{id_type}:{rid}，id_type 缺失时
    默认 chat_id；跨分支同键后同文重投仍按已投递抑制，账本不静默漏判。"""
    from types import SimpleNamespace

    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    t = "同目标不同路由形制跨分支必须同键且同文重投仍被抑制的锁定正文"
    m = _msg(event_name="chat.final", content=t)
    k_present = ch._p2_dedup_session(
        m.metadata or {}, m, SimpleNamespace(chat_id="oc_x", id_type="chat_id"))
    k_fallback = ch._p2_dedup_session(
        m.metadata or {}, m, SimpleNamespace(chat_id="", id_type=""))
    assert k_present == k_fallback  # 跨分支同键（评审意见 #200900174）
    assert k_present.endswith("\x00tgt:chat_id:oc_x")
    reason, tok = ch._delivery_dedup.try_reserve(k_present, "rid-a", t)
    assert reason is None and tok is not None
    ch._delivery_dedup.commit(tok)
    reason2, _tok2 = ch._delivery_dedup.try_reserve(k_fallback, "rid-b", t)
    assert reason2 is not None and "delivered" in reason2  # 同文同键→已投递抑制
    # P2（atomgit 2026-11-18）：键形制补发起人维度——同群不同用户键必须分叉，
    # 甲乙同文互不抑制；无发起人字段帧维持无 who 的原形制。
    m_a = _msg(event_name="chat.final", content=t, metadata={"triggering_user_id": "ou_a"})
    k_a = ch._p2_dedup_session(m_a.metadata or {}, m_a,
                               SimpleNamespace(chat_id="oc_x", id_type="chat_id"))
    m_b = _msg(event_name="chat.final", content=t, metadata={"triggering_user_id": "ou_b"})
    k_b = ch._p2_dedup_session(m_b.metadata or {}, m_b,
                               SimpleNamespace(chat_id="oc_x", id_type="chat_id"))
    assert k_a.endswith("\x00who:ou_a") and k_b.endswith("\x00who:ou_b")
    assert k_a != k_b and k_a != k_present


@pytest.mark.asyncio
async def test_suppressed_replay_route_drift_reclaims_cardkit_session():
    """评审意见 #201604041：接收解析成功但精确（请求号，接收目标）键未命中 cardkit
    会话（路由目标漂移形态）——受抑帧收尾须按请求号前缀回收对应会话并以已流出
    正文收尾，卡片不悬挂「生成中」。修复前：session None 只清缓冲即半程返回=红。"""
    ch = _make_channel()
    ch._send_feishu_message = AsyncMock()
    stub = MagicMock()
    stub.finalize = AsyncMock()
    orig_key = "req_t1\x00oc_orig\x00chat_id"
    ch._cardkit_sessions[orig_key] = stub
    ch._cardkit_buffers[orig_key] = "原始会话已流出正文"
    ch._extract_receive_info = MagicMock(return_value=("oc_drift", "chat_id"))
    body = "路由目标与原始会话漂移的受抑终帧正文长度需要超过指纹与回声下限"
    ch._p2_final_fingerprints["req_t1"] = body
    m = _msg(event_name="chat.final", content=body)
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()   # 指纹兜底抑制
    stub.finalize.assert_awaited_once()            # 前缀回收并以已流出正文收尾
    assert stub.finalize.await_args.args[0] == "原始会话已流出正文"
    assert not any(k.startswith("req_t1\x00") for k in ch._cardkit_sessions)


@pytest.mark.asyncio
async def test_empty_final_not_suppressed_by_fingerprint_suffix_guard():
    """评审意见 #201614725：同请求号已有指纹入账时，空正文终帧（无缓冲可合并，
    _dedup_text 为空串）不得被尾缀兜底误判为重复——endswith("") 恒真缺陷。
    修复前：空终帧计数=抑制=红。"""
    ch = _make_channel()
    ch._send_feishu_message = AsyncMock()
    # atomgit 2026-11-18：补接收目标 stub——无接收信息时帧在投递解析前早退，
    # 尾缀兜底路径根本未走，空终帧断言恒真（取样缺陷修正）。
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._p2_final_fingerprints["req_t1"] = "先前已入账的终帧正文长度需超过指纹表最小门槛要求"
    m = _msg(event_name="chat.final", content="")
    m.payload["is_complete"] = True
    await ch.send(m)
    assert ch._fingerprint_suppressed_total == 0  # 空正文不进入尾缀兜底


@pytest.mark.asyncio
async def test_fingerprint_suppression_counter_final_and_flush():
    """评审意见 #201614734：指纹兜底面抑制须独立计数并纳入关停汇总口径——
    终帧面与冲刷面各命中一次，计数器逐次累加可观测。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    body = "终帧面指纹尾缀命中一次计数的正文需要超过去重与回声长度下限"
    ch._p2_final_fingerprints["req_t1"] = body
    m = _msg(event_name="chat.final", content=body)
    m.payload["is_complete"] = True
    await ch.send(m)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._fingerprint_suppressed_total == 1
    flush_text = "冲刷面镜像尾缀命中再计数一次的正文同样需要超过下限长度要求"
    ch._p2_final_fingerprints["req_t1"] = flush_text
    ch._stream_text_buffers["req_t1"] = flush_text
    s = _msg(event_name="chat.processing_status", content="")
    s.payload["is_processing"] = False
    await ch.send(s)
    ch._send_feishu_message.assert_not_awaited()
    assert ch._fingerprint_suppressed_total == 2


@pytest.mark.asyncio
async def test_same_group_text_different_initiators_not_cross_deduped():
    """P2 回归（atomgit 2026-11-18）：同群不同发起人同文不得跨用户去重。
    甲投递；乙同文不同键照常投递；乙同文重放（新请求号）照常被抑。
    修复前：乙被「同会话已投递」误吞＝红。"""
    text = "同群里两位不同用户先后发送的同一条长文本问答内容须超过去重长度下限"
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_g", "chat_id"))
    ch._send_feishu_message = AsyncMock()

    def _g(rid: str, uid: str) -> Message:
        return Message(id=rid, type="event", channel_id="feishu", session_id="sess_g",
                       params={}, timestamp=0, ok=True, user_id=uid,
                       payload={"content": text, "event_type": "chat.final",
                                "is_complete": True},
                       event_type=None, metadata={"feishu_chat_id": "oc_g"},
                       app_id="app", agent_ref=None)

    await ch.send(_g("req_a1", "ou_a"))
    assert ch._send_feishu_message.await_count == 1
    await ch.send(_g("req_b1", "ou_b"))   # 不同发起人同文：不得跨用户去重
    assert ch._send_feishu_message.await_count == 2
    await ch.send(_g("req_b2", "ou_b"))   # 同发起人同文异请求号：照常抑制
    assert ch._send_feishu_message.await_count == 2


@pytest.mark.asyncio
async def test_fingerprint_target_scoped_fanout_not_cross_suppressed():
    """评审意见 #201852873：尾缀兜底指纹键并入实际投递目标维度——同请求号同文
    跨目标扇出时，其他目标留存的指纹不得压制本目标（修复前键=request_id，第二
    目标被 endswith 兜底静默吞帧）；本目标自身指纹照旧抑制重投。"""
    t = "跨目标扇出场景下同一请求号同文回复正文长度超过二十字符门槛"
    # ① 其他目标留存的指纹 → 本目标放行投递
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._p2_final_fingerprints["req_t1"] = ("tgt:chat_id:oc_other_target", t)
    m = _msg(event_name="chat.final", content=t)
    await ch.send(m)
    assert ch._fingerprint_suppressed_total == 0, "跨目标指纹不得参与尾缀兜底"
    ch._send_feishu_message.assert_awaited_once()
    # ② 本目标留存的指纹 → 重投帧照旧抑制
    ch2 = _make_channel()
    ch2._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch2._send_feishu_message = AsyncMock()
    m2 = _msg(event_name="chat.final", content=t)
    cur = ch2._p2_dedup_session(dict(m2.metadata or {}), m2, None)
    ch2._p2_final_fingerprints["req_t1"] = (cur, t)
    await ch2.send(m2)
    ch2._send_feishu_message.assert_not_awaited()
    assert ch2._fingerprint_suppressed_total == 1


@pytest.mark.asyncio
async def test_third_party_refill_still_inflight_bounded_loop_delivers():
    """评审意见 #201852881：首条结算后第三方同文帧抢走预留、重试仍 in-flight——
    修复前该帧被直接丢弃（第三方再失败即双丢）；修复后有界循环继续等待该新预留
    结算后再试，按最终结果投递。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    seq = ["reserved in-flight-thirdparty-prev", "reserved in-flight-thirdparty", None]

    def _fake_reserve(sess, rid, text):
        r = seq.pop(0)
        return (r, None) if r else (None, "tok_t")

    ch._delivery_dedup.try_reserve = MagicMock(side_effect=_fake_reserve)
    waits = {"n": 0}

    async def _fake_settle(sess, text, w):
        waits["n"] += 1
        return True

    ch._delivery_dedup.wait_settle = _fake_settle
    m = _msg(event_name="chat.final",
             content="第三方抢预留场景下需二次等待其结算后再投递的正文长度达标")
    await ch.send(m)
    assert waits["n"] == 2, "两轮有界等待：每轮等结算后重试预留"
    ch._send_feishu_message.assert_awaited_once()
    assert not seq, "预留序列恰好消费完（第三次拿到 token 放行）"


# ── 评审意见 #201904588/#202434499/#201904595/#201904599 回归 ──────────────

def test_dedup_settle_cumulative_wait_within_outer_budget_invariant():
    """累计等待（轮数×单轮）须严格小于外层 10s 预算，且单轮值由预算推导——
    仅约束单轮时 2×7.0=14s 会越过外层超时，fail-open 分支不可达。"""
    ch = _make_channel()
    total = ch._dedup_settle_max_rounds * ch._dedup_settle_wait
    assert total < ch._dedup_outer_timeout, f"累计 {total}s 未严格小于外层 {ch._dedup_outer_timeout}s"
    assert ch._dedup_settle_wait == round(
        ch._dedup_outer_timeout / (ch._dedup_settle_max_rounds + 1), 2), \
        "单轮等待应由外层预算按预算/(轮数+1)推导"


@pytest.mark.asyncio
async def test_third_party_refill_two_round_cumulative_wait_below_outer_budget():
    """评审意见 #202434499：两轮有界等待的**墙钟累计**（各轮 wait 参数之和）
    必须严格小于外层调用方 10s 预算——把 round-u 口径从单轮钉到累计。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    seq = ["reserved in-flight-thirdparty-prev", "reserved in-flight-thirdparty", None]

    def _fake_reserve(sess, rid, text):
        r = seq.pop(0)
        return (r, None) if r else (None, "tok_t")

    ch._delivery_dedup.try_reserve = MagicMock(side_effect=_fake_reserve)
    seen_waits = []

    async def _fake_settle(sess, text, w):
        seen_waits.append(w)
        return True

    ch._delivery_dedup.wait_settle = _fake_settle
    m = _msg(event_name="chat.final",
             content="两轮等待累计时长必须钉在外层超时预算之内对应的正文长度达标")
    await ch.send(m)
    assert len(seen_waits) == 2
    assert sum(seen_waits) < ch._dedup_outer_timeout, \
        f"两轮累计 {sum(seen_waits)}s 越过外层预算 {ch._dedup_outer_timeout}s"
    ch._send_feishu_message.assert_awaited_once()


def test_dedup_same_rid_exemption_logs_warning_for_audit(monkeypatch):
    """评审意见 #201904595：同 rid 重放豁免放行（无预留凭证）须 warn 留痕，
    使「原帧成功投递后重放帧二次投递依赖上游 message-id 兜底」的窗口可对账。"""
    from agent_gateway.common import delivery_dedup as dd_mod
    dd = DeliveryDedup()
    text = "同rid豁免对账留痕回归所需的足够长度正文内容测试"
    reason1, tok1 = dd.try_reserve("sess_audit", "req-a", text)
    assert tok1 is not None
    warns = []
    monkeypatch.setattr(dd_mod.logger, "warning",
                        lambda msg, *a, **k: warns.append(msg % a if a else msg))
    reason2, tok2 = dd.try_reserve("sess_audit", "req-a", text)  # 同 rid 豁免
    assert reason2 is None and tok2 is None
    assert any("same-request replay exempted" in w for w in warns), \
        "豁免放行必须留下 warning 对账日志"


def test_p2_final_fingerprint_refresh_repositions_eviction_order():
    """评审意见 #201904599：已存在键的指纹更新须移动到插入序尾部——
    容量淘汰按最近更新时间滚动，先刷新的热键不得被先淘汰。"""
    ch = _make_channel()
    for i in range(256):
        ch._store_p2_final_fingerprint(f"sk_{i}", f"正文{i}内容与目标一致的入账文本")
    # 刷新最旧键 sk_0 → 应移动到尾部
    ch._store_p2_final_fingerprint("sk_0", "正文0内容与目标一致的入账文本")
    ch._store_p2_final_fingerprint("sk_256", "溢出触发淘汰的正文内容与目标一致入账")
    assert ch._p2_fingerprint_for_target("sk_0", "") == "正文0内容与目标一致的入账文本", \
        "刚刷新的热键不应被淘汰"
    assert ch._p2_fingerprint_for_target("sk_1", "") == "", \
        "淘汰应命中真正最久未更新的条目"


def test_dedup_outer_timeout_single_sourced_from_contract_constant():
    """评审意见 #202536818：消费端外层预算必须与 delivery_dedup 单源契约常量相等——
    若日后有人绕开常量重新硬编码字面量，本用例即联动告警。"""
    from agent_gateway.common import delivery_dedup
    ch = _make_channel()
    assert ch._dedup_outer_timeout == delivery_dedup.FRAME_DELIVERY_BUDGET_SECONDS, \
        "外层预算必须引用 delivery_dedup.FRAME_DELIVERY_BUDGET_SECONDS，禁止各自硬编码"


def test_frame_delivery_budget_env_validation(monkeypatch):
    """评审意见 #202580557：env 覆写非法值（非数字/非正/inf）必须在定义位抛出
    携带变量名的明确异常，合法覆写与缺省回退保持原行为。"""
    import importlib
    from agent_gateway.common import delivery_dedup
    try:
        monkeypatch.setenv("JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS", "abc")
        with pytest.raises(ValueError) as e1:
            importlib.reload(delivery_dedup)
        assert "JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS" in str(e1.value)
        monkeypatch.setenv("JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS", "-1")
        with pytest.raises(ValueError) as e2:
            importlib.reload(delivery_dedup)
        assert "JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS" in str(e2.value)
        monkeypatch.setenv("JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS", "inf")
        with pytest.raises(ValueError):
            importlib.reload(delivery_dedup)
        monkeypatch.setenv("JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS", "12.5")
        importlib.reload(delivery_dedup)
        assert delivery_dedup.FRAME_DELIVERY_BUDGET_SECONDS == 12.5
    finally:
        monkeypatch.delenv("JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS", raising=False)
        importlib.reload(delivery_dedup)
    assert delivery_dedup.FRAME_DELIVERY_BUDGET_SECONDS == 10.0


def test_tiny_frame_budget_settle_wait_is_rejected(monkeypatch):
    """评审意见 #202580557（边界钉死）＋#202649864：极小正预算使 settle_wait 舍入
    为 0 必须被拒绝——逐档预算实际调用生产初始化校验（settle_wait 推导与拒绝
    形态均由生产代码裁决，用例不再重算公式）。"""
    from agent_gateway.channel_manager.im_platforms.feishu import feishu_connect as fc
    for budget, expect_ok in [(10.0, True), (0.8, True), (0.01, False)]:
        monkeypatch.setattr(fc, "FRAME_DELIVERY_BUDGET_SECONDS", budget)
        if expect_ok:
            ch = _make_channel()
            assert ch._dedup_settle_wait > 0, f"budget={budget}"
        else:
            with pytest.raises(ValueError):
                _make_channel()


def test_init_budget_validation_is_valueerror_not_assert(monkeypatch):
    """评审意见 #202615213：预算边界校验不得依赖 assert（`python -O` 会剥离）——
    非法预算初始化必须无条件抛 ValueError。"""
    from agent_gateway.channel_manager.im_platforms.feishu import feishu_connect as fc
    monkeypatch.setattr(fc, "FRAME_DELIVERY_BUDGET_SECONDS", 0.01)
    with pytest.raises(ValueError):
        _make_channel()


def test_finalize_cardkit_swallows_unexpected_errors():
    """评审意见 #202615220＋#202644735：finalize 抛非 CardKitError 异常同样必须兜住、
    不得击穿调用方造成预留泄漏至 TTL。返回值精化：卡片已流出增量时按「内容已投递」
    返回 True——返回 False 会触发调用方回退再投全文，与卡内已实时展示内容二次投递；
    未流出任何增量时返回 False，由调用方回投递全文（恰好一次）。两分支均须弹清会话/缓冲。"""
    ch = _make_channel()

    class _Boom:
        async def finalize(self, text):
            raise RuntimeError("network down")

    key = "req-1\x00im\x00"
    boom = _Boom()
    ch._cardkit_sessions[key] = boom
    ch._cardkit_buffers[key] = "增量缓冲"
    assert asyncio.run(ch._finalize_cardkit_session(key, boom, "")) is True
    assert key not in ch._cardkit_sessions and key not in ch._cardkit_buffers

    boom2 = _Boom()
    ch._cardkit_sessions[key] = boom2
    assert asyncio.run(ch._finalize_cardkit_session(key, boom2, "")) is False
    assert key not in ch._cardkit_sessions and key not in ch._cardkit_buffers


def test_dedup_settle_retry_exhaustion_fail_open(monkeypatch):
    """评审意见 #202615216：帮助函数承接三面原语义——结算恒成功但预留恒被
    in-flight 抢回 → 轮次耗尽 → fail-open(None) 且透传最新 token。"""
    ch = _make_channel()

    async def _always_settled(*_a):
        return True

    monkeypatch.setattr(ch._delivery_dedup, "wait_settle", _always_settled)
    monkeypatch.setattr(ch._delivery_dedup, "try_reserve", lambda *_a: ("in-flight duplicate", "tok-x"))
    reason, token = asyncio.run(
        ch._dedup_settle_retry("sess", "rid", "正文" * 30, "in-flight duplicate", "tok", "final"))
    assert reason is None and token == "tok-x"


@pytest.mark.asyncio
async def test_p2_short_reply_not_swallowed_by_suffix_fallback():
    """评审意见 #202640934：短合法回复（「好的」）恰为上一终帧指纹尾缀时不得被
    endswith 兜底误判为重投整条吞掉——尾缀兜底与入站回声守卫 _ECHO_MIN_CHARS
    对称设最小长度门。修复前：2 字回复被抑制=红。"""
    ch = _make_channel()
    ch._extract_receive_info = MagicMock(return_value=("oc_x", "chat_id"))
    ch._send_feishu_message = AsyncMock()
    ch._start_cardkit_session = AsyncMock(return_value=False)
    ch._p2_final_fingerprints["req_t1"] = "上一轮助手完整回复的正文内容若干字，以好的收尾"
    m = _msg(event_name="chat.final", content="好的")
    m.payload["is_complete"] = True
    await ch.send(m)
    assert ch._send_feishu_message.await_count >= 1  # 修复前：短尾缀误抑制→零投递=红
