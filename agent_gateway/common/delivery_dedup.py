# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""P2 投递端精确重复兜底（2026-11-06 判例：同一回复投递两次）。

仅 chat.final 之间、归一化内容全同、长度>=20 且 request_id 不同才判重复丢弃；
同一 request_id 的重放豁免（仅刷新时间戳，不丢弃）。
"""
from collections import OrderedDict
import hashlib
import time
from typing import Optional

_TTL_SECONDS = 300.0
_MIN_CHARS = 20

class DeliveryDedup:
    """同会话精确重复兜底：session+归一化 sha1 指纹，TTL 300s。

    request_id 契约：request_id 存入 _seen value，命中时经 request_seen_before
    判定——同一 request 重放（prev==cur）豁免（刷新时间戳不丢弃）；
    不同 request_id 的全同内容在 TTL 内才判重复丢弃。
    """

    def __init__(self, ttl_seconds: float = _TTL_SECONDS, min_chars: int = _MIN_CHARS):
        self._ttl = float(ttl_seconds)
        self._min = int(min_chars)
        self._seen: "OrderedDict[tuple, tuple]" = OrderedDict()
        self.suppressed_total = 0

    @staticmethod
    def normalize(text: str) -> str:
        return " ".join((text or "").split())

    def fingerprint(self, session_id: str, text: str) -> tuple:
        n = self.normalize(text)
        h = hashlib.sha1(n.encode("utf-8")).hexdigest()
        return (str(session_id or ""), h)

    def check_and_record(self, session_id: str, request_id: str, text: str) -> Optional[str]:
        """返回 None=放行；返回 str=重复原因（丢弃）。"""
        n = self.normalize(text)
        if len(n) < self._min:
            return None
        key = self.fingerprint(session_id, n)
        rid = str(request_id or "")
        now = time.monotonic()
        for k, (ts, _prev_rid) in list(self._seen.items()):
            if now - ts > self._ttl:
                self._seen.pop(k, None)
        prev = self._seen.get(key)
        # 命中后无论如何都刷新时间戳与 request_id（保持 LRU 语义）；
        # 同 request 重放豁免：不判重不丢弃。
        self._seen.pop(key, None)
        self._seen[key] = (now, rid)
        if prev is not None:
            prev_ts, prev_rid = prev
            if not request_seen_before(prev_rid, rid):
                self.suppressed_total += 1
                return f"duplicate within {int(self._ttl)}s (first seen {int(now - prev_ts)}s ago)"
            return None
        return None

def request_seen_before(prev_request_id: str, request_id: str) -> bool:
    """豁免条件：同一 request_id 的重放返回 True（豁免）；不同 request_id 才去重。"""
    return str(prev_request_id or "") == str(request_id or "")
