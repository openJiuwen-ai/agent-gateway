# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""P2 投递端精确重复兜底（2026-11-06 判例：同一回复投递两次）。

reservation 状态机契约（D4，atomgit gw50 复审轮裁定）：投递前 `try_reserve`
原子预留（消除旧两段式 check→await→record 的并发双放行窗口），投递成功
`commit(token)` 转 delivered（TTL 自提交起算），失败/异常 `abort(token)` 回滚。
命中重复不刷新时间戳（atomgit gw50-1）→ TTL 自 commit 起算，周期性相同回复
不会被无限续期静默丢弃。原因串携带双方 request_id，满足 issue #95 对账要求
（atomgit gw50-6）。clock 可注入，单测不依赖真实 sleep（atomgit gw50-10）。
同 request_id 重放豁免语义保持与 `request_seen_before` 一致（含 in-flight）。
"""
import asyncio
import logging
import os
from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import time
import uuid
from typing import Callable, Optional

_TTL_SECONDS = 300.0

# 评审意见 #202536818：外层帧投递预算（秒）——上游分发层对单帧 send 的超时契约
# （round-u 口径 10s），本仓无字面定义点，此前由消费端各自复制常量，上游一旦调整
# 即静默偏离。收敛为唯一来源：消费端（feishu settle 等待推导）与回归测试一律引用
# 本常量；上游预算不同的部署可用 JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS 覆写，
# 单点生效、处处一致，杜绝多处漂移。
# 评审意见 #202580557：env 覆写必须在定义位校验——裸 float() 遇非数字会让整包
# import 崩在 ValueError 且不指向变量名；此处统一抛携带变量名的明确异常。
def _frame_delivery_budget_seconds() -> float:
    raw = os.environ.get("JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS", "") or "10.0"
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(
            "JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS 必须是数字秒数，当前配置: %r" % raw
        ) from exc
    if not (value > 0.0) or value == float("inf"):
        raise ValueError(
            "JIUWEN_FRAME_DELIVERY_BUDGET_SECONDS 必须是正的有限秒数，当前配置: %r" % raw
        )
    return value


FRAME_DELIVERY_BUDGET_SECONDS: float = _frame_delivery_budget_seconds()

logger = logging.getLogger(__name__)
_MIN_CHARS = 20
_MAX_ENTRIES = 1024


@dataclass
class Entry:
    """账本值对象：ts=TTL 基准（reserved=预留时刻，delivered=commit 时刻）。"""

    ts: float
    request_id: str
    state: str  # "reserved" | "delivered"
    token: "str | None"
    # round-n：结算事件——commit/abort 置位；消费端等待后重试预留，消除
    # 「首条投递失败×重复帧已抑制」双丢角落（verdict 200075834）。
    settled: "object | None" = None


class DeliveryDedup:
    """同会话精确重复兜底：session+归一化 sha1 指纹，TTL 300s。

    契约：`try_reserve(session, request_id, text) -> (reason|None, token|None)`，
    放行且预留成功返回 (None, token)；短文本短路 (None, None)；同 rid 重放豁免
    (None, None)；重复抑制 (reason, None)。`commit(token)` 仅 token 匹配
    reserved→delivered（TTL 自此起算）；`abort(token)` 仅 token 匹配→pop。
    """

    def __init__(self, ttl_seconds: float = _TTL_SECONDS, min_chars: int = _MIN_CHARS,
                 clock: "Callable[[], float]" = time.monotonic):
        self._ttl = float(ttl_seconds)
        self._min = int(min_chars)
        self._clock = clock
        self._seen: "OrderedDict[tuple, Entry]" = OrderedDict()
        self.suppressed_total = 0

    def clear(self) -> None:
        """清空账本（round-e P3：纳入渠道 stop() 清理契约，与
        _recent_user_inputs 同语义——重启不得继承旧投递指纹）。"""
        self._seen.clear()

    @staticmethod
    def normalize(text: str) -> str:
        return " ".join((text or "").split())

    def fingerprint(self, session_id: str, text: str) -> tuple:
        n = self.normalize(text)
        h = hashlib.sha1(n.encode("utf-8")).hexdigest()
        return (str(session_id or ""), h)

    def _evict_expired(self, now: float) -> None:
        for k, entry in list(self._seen.items()):
            if now - entry.ts > self._ttl:
                self._seen.pop(k, None)

    def try_reserve(self, session_id: str, request_id: str,
                    text: str) -> "tuple[Optional[str], Optional[str]]":
        """原子预留：过期清理→min_chars 短路→同 rid 豁免→写 reserved 条目。

        返回 (reason, token)：reason 非 None＝抑制（携带对账原因串）；
        token 非 None＝放行且持有预留凭证（必须 commit 或 abort 了结）。
        """
        n = self.normalize(text)
        if len(n) < self._min:
            return (None, None)
        key = self.fingerprint(session_id, n)
        rid = str(request_id or "")
        now = self._clock()
        self._evict_expired(now)
        prev = self._seen.get(key)
        if prev is None:
            token = uuid.uuid4().hex
            self._seen[key] = Entry(ts=now, request_id=rid, state="reserved", token=token,
                                    settled=asyncio.Event())
            while len(self._seen) > _MAX_ENTRIES:
                # 评审意见 #200835109: prefer evicting delivered
                # (settled) entries; only when every slot is reserved, fall
                # back to the oldest entry — and never orphan a waiter:
                # set its settled event BEFORE popping, otherwise wait_settle
                # treats the missing key as already-settled and a duplicate
                # flows while the first delivery is still in flight.
                victim_key = next(
                    (k for k, e in self._seen.items() if e.state == "delivered"), None)
                if victim_key is None:
                    victim_key = next(iter(self._seen))
                victim = self._seen.pop(victim_key)
                if victim.state == "reserved" and victim.settled is not None:
                    victim.settled.set()
                    logger.info("[DeliveryDedup] evicted in-flight reservation under capacity pressure: req=%s", victim.request_id)
            return (None, token)
        if request_seen_before(prev.request_id, rid):
            # 评审意见 #201904595：同 rid 豁免放行不带预留凭证——原帧随后投递成功时，
            # 重放帧与原帧各投一次的窗口依赖上游 message-id 去重兜底；warn 留痕便于
            # 对账审计，使该残余面在去重层外可见。
            logger.warning(
                "[DeliveryDedup] same-request replay exempted without reservation: "
                "session=%s req=%s first_state=%s",
                str(session_id)[:16], rid, prev.state,
            )
            return (None, None)
        self.suppressed_total += 1
        # round-n：原因串携带首条状态标记——reserved（in-flight，未结算）＝消费端
        # 可等待结算后重试预留；delivered＝立即按已投递抑制。双方 rid 对账不变。
        _state = "in-flight" if prev.state == "reserved" else "delivered"
        return (
            f"duplicate ({_state}) within {int(self._ttl)}s "
            f"(first seen {int(now - prev.ts)}s ago, "
            f"first_req={prev.request_id or '-'}, req={rid or '-'})",
            None,
        )

    def commit(self, token: Optional[str]) -> None:
        """仅 token 匹配 reserved → delivered；TTL 自 commit 起算。"""
        if not token:
            return
        for entry in self._seen.values():
            if entry.token == token and entry.state == "reserved":
                entry.state = "delivered"
                entry.ts = self._clock()
                entry.token = None  # token 一次性：commit 后 abort 天然幂等
                if entry.settled is not None:
                    entry.settled.set()
                return

    def abort(self, token: Optional[str]) -> None:
        """仅 token 匹配 → pop（投递失败/异常回滚，账本不留残骸）。"""
        if not token:
            return
        for key, entry in list(self._seen.items()):
            if entry.token == token:
                if entry.settled is not None:
                    entry.settled.set()  # round-n：pop 前置位，等待方随即重试预留
                self._seen.pop(key, None)
                return

    async def wait_settle(self, session_id: str, text: str, timeout: float) -> bool:
        """round-n P3：等待该指纹预留结算（commit/abort 均置位）；条目不存在＝已结算。

        返回值契约（round-o P3，verdict 200088550/200088875 收敛）：True＝已结算，
        消费端重试预留（首条成功→按已投递抑制原语义；首条失败→本帧放行投递）；
        False＝有界等待超时未结算——消费端必须 fail-open 放行投递（at-most-once
        →at-least-once 切换），不得把 False 当作继续抑制的信号（回复丢失劣于
        病态慢投递下的残余双投递风险）。等待有界，禁无界阻塞。

        评审意见 #200900179：timeout 为必填参数（不设默认）——调用方必须显式传入
        严格小于其外层超时预算的值（生产值由 FRAME_DELIVERY_BUDGET_SECONDS
        按轮数推导≈3.3s，且全部轮次
        累计 轮数×单轮 < 外层预算由消费端初始化断言钉死），防默认值被遗漏时等待等于
        预算上限、外层先取消导致 fail-open 放行分支实际不可达。
        """
        key = self.fingerprint(session_id, self.normalize(text))
        entry = self._seen.get(key)
        ev = entry.settled if entry is not None else None
        if ev is None:
            return True
        try:
            await asyncio.wait_for(ev.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False


def request_seen_before(prev_request_id: str, request_id: str) -> bool:
    """豁免条件：同一**非空** request_id 的重放返回 True（豁免）；不同 request_id
    或任一为空都照常去重（P2② fail-closed：空 rid == 空 rid 不得互相豁免放行）。"""
    prev = str(prev_request_id or "")
    return bool(prev) and prev == str(request_id or "")
