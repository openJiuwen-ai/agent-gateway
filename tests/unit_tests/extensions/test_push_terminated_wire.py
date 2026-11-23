# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""``_emit_push_terminated`` 合成帧的 canonical E2A wire 契约锁定。

``HttpSseAgentServerClient._push_loop`` 在 events/stream 连续失败达到
``_PUSH_MAX_CONNECT_FAILURES`` 次后，调 ``_emit_push_terminated`` 合成一个 wire 帧
直接 ``await self._on_server_push(wire)``。该帧必须与同一 handler 路径上的真实
``server_push`` 帧（``build_server_push_wire`` / ``encode_agent_chunk_for_wire`` 产物）
走同一条 canonical E2A 解析路径，而非 deprecated legacy chunk 兜底，否则帧契约
不一致、解析噪音、且下游任何收紧 ``_deprecated_chunk_shape`` 的改动都会让合成帧
静默失效（issue #115 现象 1 修复的脆弱点）。

本组测试锁住以下跨端断言：

1. 合成帧是 canonical E2A 响应线（``is_e2a_response_wire_dict`` 命中），不再走
   ``_deprecated_chunk_shape`` 兜底；
2. 合成帧带 ``metadata[E2A_WIRE_SERVER_PUSH_KEY]=True``，与真实 ``server_push`` 帧
   一致；
3. 合成帧穿过 ``parse_agent_server_wire_chunk`` 不抛异常，且
   ``chunk.payload.get("event_type") == "gateway.push_terminated"``，下游
   ``_handle_agent_server_push_ordered`` 的 ``gateway.push_terminated`` 分支可命中；
4. ``_PUSH_MAX_CONNECT_FAILURES`` 可经环境变量 ``PUSH_MAX_CONNECT_FAILURES`` 覆盖。
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from e2a.constants import E2A_WIRE_SERVER_PUSH_KEY
from e2a.wire_codec import is_e2a_response_wire_dict, parse_agent_server_wire_chunk

from agent_gateway.extensions.runtime_management_extension import pod_http_client
from agent_gateway.extensions.runtime_management_extension.pod_http_client import (
    HttpSseAgentServerClient,
    _PUSH_TERMINATED_EVENT_TYPE,
)

# Loopback 测试地址：合成帧只做契约断言，不发起真实连接，避免硬编码 Pod IP。
_TEST_API_ROOT = "http://127.0.0.1:8766"


def _make_client(*, api_root: str = _TEST_API_ROOT, failures: int = 3) -> HttpSseAgentServerClient:
    """Construct a stub ``HttpSseAgentServerClient`` without running ``__init__``."""
    client = HttpSseAgentServerClient.__new__(HttpSseAgentServerClient)
    client._on_server_push = None  # set by each test
    client._api_root = api_root
    client._push_connect_failures = failures
    return client


async def _capture_synthesised_wire(*, reason: str = "connect_failures_exceeded") -> dict[str, Any]:
    client = _make_client()
    captured: list[dict[str, Any]] = []

    async def handler(wire: dict[str, Any]) -> None:
        captured.append(wire)

    client._on_server_push = handler
    await client._emit_push_terminated(reason)
    assert len(captured) == 1, "handler must be invoked exactly once"
    return captured[0]


@pytest.mark.asyncio
async def test_emit_push_terminated_produces_canonical_e2a_wire() -> None:
    """合成帧必须命中 ``is_e2a_response_wire_dict``，而非 deprecated legacy chunk。

    ``_deprecated_chunk_shape`` 是过渡期兜底分支，每次解析都打一条 WARNING；真实
    ``server_push`` 帧走 canonical E2A 路径。合成帧必须与真实帧走同一路径，否则
    帧契约不一致（review 反馈的核心问题）。
    """
    wire = await _capture_synthesised_wire()
    assert is_e2a_response_wire_dict(wire) is True, (
        "synthesised wire must be canonical E2A shape, not deprecated legacy chunk; "
        "otherwise it diverges from real server_push frames on the same handler path"
    )


@pytest.mark.asyncio
async def test_emit_push_terminated_stamps_server_push_metadata_marker() -> None:
    """合成帧必须带 ``metadata[E2A_WIRE_SERVER_PUSH_KEY]=True``。

    真实 ``server_push`` 帧由 ``build_server_push_wire`` 注入该标记；下游任何依赖
    该标记的过滤/路由逻辑（如 ``_push_loop`` 的 ``meta.get(E2A_WIRE_SERVER_PUSH_KEY)``
    过滤）对合成帧也必须成立。
    """
    wire = await _capture_synthesised_wire()
    metadata = wire.get("metadata")
    assert isinstance(metadata, dict), "wire must carry a metadata dict"
    assert metadata.get(E2A_WIRE_SERVER_PUSH_KEY) is True, (
        "synthesised wire must stamp metadata[E2A_WIRE_SERVER_PUSH_KEY]=True "
        "to match real server_push wire contract"
    )


@pytest.mark.asyncio
async def test_emit_push_terminated_payload_keeps_event_type_for_downstream_branch() -> None:
    """合成帧穿过 ``parse_agent_server_wire_chunk`` 后 ``chunk.payload.event_type``
    必须是 ``gateway.push_terminated``。

    ``_handle_agent_server_push_ordered`` 的 ``gateway.push_terminated`` 接收分支
    读的是 ``chunk.payload.get("event_type")``（不是顶层），合成帧的 canonical
    化不能丢掉这个字段。canonical E2A wire 把 ``AgentResponseChunk.payload`` 序列化
    到 ``body``，``parse_agent_server_wire_chunk`` 反序列化时还原回 ``payload``。
    """
    wire = await _capture_synthesised_wire()
    chunk = parse_agent_server_wire_chunk(wire)
    assert chunk.request_id == f"push_terminated:{_TEST_API_ROOT}"
    assert chunk.channel_id == "web"
    assert chunk.is_complete is False
    assert isinstance(chunk.payload, dict)
    assert chunk.payload.get("event_type") == _PUSH_TERMINATED_EVENT_TYPE
    assert chunk.payload.get("reason") == "connect_failures_exceeded"
    assert chunk.payload.get("api_root") == _TEST_API_ROOT
    assert chunk.payload.get("failures") == 3


@pytest.mark.asyncio
async def test_emit_push_terminated_skips_when_no_handler_registered() -> None:
    """``_on_server_push is None`` 时必须短路返回，不构造 wire 也不抛异常。"""
    client = _make_client()
    client._on_server_push = None
    # Must not raise.
    await client._emit_push_terminated("missing_api_root")


@pytest.mark.asyncio
async def test_emit_push_terminated_swallows_handler_exception() -> None:
    """handler 抛异常不能让 ``_push_loop`` 的退出路径崩掉。"""
    client = _make_client()

    async def boom(_wire: dict[str, Any]) -> None:
        raise RuntimeError("downstream handler exploded")

    client._on_server_push = boom
    # Must not propagate.
    await client._emit_push_terminated("connect_failures_exceeded")


@pytest.mark.asyncio
async def test_emit_push_terminated_forwards_failure_counter_and_reason() -> None:
    """合成帧必须把 ``failures`` 计数和 ``reason`` 透传给下游，便于前端告警/排查。"""
    client = _make_client(failures=7)
    captured: list[dict[str, Any]] = []

    async def handler(wire: dict[str, Any]) -> None:
        captured.append(wire)

    client._on_server_push = handler
    await client._emit_push_terminated("connect_failures_exceeded")
    chunk = parse_agent_server_wire_chunk(captured[0])
    assert chunk.payload.get("failures") == 7
    assert chunk.payload.get("reason") == "connect_failures_exceeded"


def test_push_max_connect_failures_is_env_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    """``PUSH_MAX_CONNECT_FAILURES`` 环境变量必须能覆盖默认 3。

    与 jiuwenswarm 生产副本对齐：运维调高/调低该阈值时只有走 ``_env_int`` 才生效。
    """
    monkeypatch.setenv("PUSH_MAX_CONNECT_FAILURES", "5")
    # Reload the module-level constant by re-importing the symbol fresh.
    import importlib

    reloaded = importlib.reload(pod_http_client)
    try:
        assert reloaded._PUSH_MAX_CONNECT_FAILURES == 5
    finally:
        monkeypatch.delenv("PUSH_MAX_CONNECT_FAILURES", raising=False)
        importlib.reload(pod_http_client)
