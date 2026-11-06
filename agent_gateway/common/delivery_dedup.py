# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""P2 投递端精确重复兜底（2026-11-06 判例：同一回复投递两次）。

仅 chat.final 之间、不同 request_id、归一化内容全同、长度>=20 才判重复丢弃。
"""
from collections import OrderedDict
import hashlib
import time
from typing import Optional

_TTL_SECONDS = 300.0
_MIN_CHARS = 20

class DeliveryDedup:
    """同会话精确重复兜底：session+归一化 sha1 指纹，TTL 300s。"""

    def __init__(self, ttl_seconds: float = _TTL_SECONDS, min_chars: int = _MIN_CHARS):
        self._ttl = float(ttl_seconds)
        self._min = int(min_chars)
        self._seen: "OrderedDict[tuple, float]" = OrderedDict()
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
        now = time.monotonic()
        for k, t in list(self._seen.items()):
            if now - t > self._ttl:
                self._seen.pop(k, None)
        prev = self._seen.get(key)
        if prev is not None:
            self._seen.pop(key)
            self._seen[key] = now
            self.suppressed_total += 1
            return f"duplicate within {int(self._ttl)}s (first seen {int(now - prev)}s ago)"
        self._seen[key] = now
        return None

def request_seen_before(prev_request_id: str, request_id: str) -> bool:
    """豁免条件：不同 request_id 才生效；同请求重放不在此兜（属 P6 面）。"""
    return str(prev_request_id or "") == str(request_id or "")
