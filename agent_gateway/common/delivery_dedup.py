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
from collections import OrderedDict
from dataclasses import dataclass
import hashlib
import time
import uuid
from typing import Callable, Optional

_TTL_SECONDS = 300.0
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
            while len(self._seen) > _MAX_ENTRIES:  # reserved/delivered 同池计容，弹最早
                self._seen.popitem(last=False)
            return (None, token)
        if request_seen_before(prev.request_id, rid):
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

    async def wait_settle(self, session_id: str, text: str, timeout: float = 10.0) -> bool:
        """round-n P3：等待该指纹预留结算（commit/abort 均置位）；条目不存在＝已结算。

        返回值契约（round-o P3，verdict 200088550/200088875 收敛）：True＝已结算，
        消费端重试预留（首条成功→按已投递抑制原语义；首条失败→本帧放行投递）；
        False＝有界等待超时未结算——消费端必须 fail-open 放行投递（at-most-once
        →at-least-once 切换），不得把 False 当作继续抑制的信号（回复丢失劣于
        病态慢投递下的残余双投递风险）。等待有界，禁无界阻塞。
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
