# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""/ws 最小帧的断言辅助：取响应、列事件名、生成唯一 request id。"""

from __future__ import annotations

import uuid
from typing import Any

TERMINAL_EVENTS = frozenset({"chat.final", "chat.error"})


def last_res(frames: list[dict[str, Any]]) -> dict[str, Any]:
    """取最后一条 ``type == "res"`` 帧（流式请求的受理/收尾响应）。"""
    for frame in reversed(frames):
        if frame.get("type") == "res":
            return frame
    raise AssertionError(f"未收到 res 帧: {frames[:3]}")


def event_names(frames: list[dict[str, Any]]) -> list[str]:
    return [str(frame.get("event")) for frame in frames if frame.get("type") == "event"]


def unique_rid(prefix: str) -> str:
    """AgentServer 按 request_id 幂等记账，同一进程内必须每次唯一。"""
    return f"{prefix}-{uuid.uuid4().hex[:12]}"
