# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""gateway ST 的同步客户端：/ws 最小帧协议。"""

from __future__ import annotations

import json
from typing import Any

from websockets.sync.client import connect

_TERMINAL_EVENTS = frozenset({"chat.final", "chat.error"})


class GatewayWsClient:
    """/ws 长连接；连接建立后先收 ``connection.ack``。

    ``expect_ack=False`` 用于南向已掉线的场景：AgentServer 未就绪时 gateway
    不会下发 connection.ack。
    """

    def __init__(self, uri: str, *, expect_ack: bool = True) -> None:
        self._ws = connect(uri, open_timeout=10, close_timeout=2)
        self.ack: dict[str, Any] | None = (
            json.loads(self._ws.recv(timeout=10)) if expect_ack else None
        )

    def close(self) -> None:
        try:
            self._ws.close()
        except Exception:  # noqa: BLE001 - 关闭失败不影响用例结论
            pass

    def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        rid: str = "st-req",
        is_stream: bool = False,
        timeout: float = 15.0,
    ) -> list[dict[str, Any]]:
        """发一条最小帧请求，返回收到的帧（非流式到 ``res`` 为止，流式到终态事件）。"""
        frame: dict[str, Any] = {"type": "req", "id": rid, "method": method, "params": params or {}}
        if is_stream:
            frame["is_stream"] = True
        self._ws.send(json.dumps(frame))
        frames: list[dict[str, Any]] = []
        while True:
            data = json.loads(self._ws.recv(timeout=timeout))
            # 同一连接上可能残留上一条请求的 res（流式在终态事件提前返回）或
            # 服务端主动推送帧；按 id 过滤，避免把别的请求响应当成本次结果。
            if data.get("id") not in (None, rid):
                continue
            frames.append(data)
            if not is_stream and data.get("type") == "res":
                return frames
            if is_stream and data.get("type") == "event" and data.get("event") in _TERMINAL_EVENTS:
                return frames
            if is_stream and data.get("type") == "res" and data.get("ok") is False:
                return frames
