# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""把本次请求按 Agent 模板槽展开的专家包写入出站请求。"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

from agent_gateway.common.expert_packages import (
    HEADER_NAME,
    METADATA_KEY,
    encode_expert_packages_header,
)

# 组 HTTP 头期间持有刚展开的专家包。None 表示这次发送没有绑定。
_OUTBOUND: ContextVar[list[dict[str, Any]] | None] = ContextVar(
    "expert_packages_outbound", default=None
)


@contextmanager
def bind_outbound_packages(packages: list[dict[str, Any]]) -> Iterator[None]:
    """Gateway 组 HTTP 头期间持有刚展开的专家包。"""
    token = _OUTBOUND.set(list(packages))
    try:
        yield
    finally:
        _OUTBOUND.reset(token)


def outbound_packages() -> list[dict[str, Any]] | None:
    """未绑定时为 None。调用方不得改用客户端自带的专家包。"""
    current = _OUTBOUND.get()
    if current is None:
        return None
    return list(current)


def apply_packages_to_envelope(envelope: Any, packages: list[dict[str, Any]]) -> None:
    """覆盖 channel_context 里的专家包，不沿用客户端字段。"""
    ctx = getattr(envelope, "channel_context", None)
    copied = dict(ctx) if isinstance(ctx, dict) else {}
    copied[METADATA_KEY] = list(packages)
    envelope.channel_context = copied


def attach_expert_packages_header(headers: dict[str, str]) -> None:
    """写入刚绑定的专家包。未绑定时按空列表发送。"""
    packages = outbound_packages()
    if packages is None:
        packages = []
    headers[HEADER_NAME] = encode_expert_packages_header(packages)
