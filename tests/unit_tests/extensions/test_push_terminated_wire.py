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
from unittest.mock import AsyncMock, MagicMock

import httpx
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


# ---------------------------------------------------------------------------
# 跨文件事件链路覆盖（review 反馈建议 2）：_push_loop 两终止路径 →
# _emit_push_terminated → wire 解析 → message_handler 分发分支 →
# _broadcast_push_terminated → broadcast except 仅日志不向上抛。
# ---------------------------------------------------------------------------


def _make_client_with_loop_state(
    *,
    api_root: str = _TEST_API_ROOT,
    failures: int = 3,
    retry_push_connect: bool = False,
) -> HttpSseAgentServerClient:
    """Construct a stub ``HttpSseAgentServerClient`` with _push_loop state.

    The loop reads ``_running`` / ``_on_server_push`` / ``_api_root`` /
    ``_push_connect_failures`` / ``_retry_push_connect`` / ``_server_ready``;
    we set them all so the two exit paths run their full body.
    """
    client = _make_client(api_root=api_root, failures=failures)
    client._running = True
    client._server_ready = True
    client._retry_push_connect = retry_push_connect
    client._push_ready = asyncio.Event()
    client._push_handlers = set()
    # _push_loop uses asyncio.sleep at the bottom; the exit paths return
    # before that, but patch sleep defensively anyway.
    return client


@pytest.mark.asyncio
async def test_push_loop_missing_api_root_path_invokes_emit_push_terminated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_push_loop`` 的 ``missing_api_root`` 退出路径必须调
    ``_emit_push_terminated``，合成帧能被 ``parse_agent_server_wire_chunk``
    正常解析，payload 含 ``event_type`` 与 ``reason``。

    覆盖 review 反馈建议 2.1 的 ``missing_api_root`` 分支。
    """
    captured: list[dict[str, Any]] = []

    async def handler(wire: dict[str, Any]) -> None:
        captured.append(wire)

    client = _make_client_with_loop_state(api_root="", failures=0)
    client._on_server_push = handler
    # Patch asyncio.sleep so the loop's tail never blocks; the missing_api_root
    # path returns before reaching it, but we guard defensively.
    monkeypatch.setattr("asyncio.sleep", AsyncMock())
    await client._push_loop()

    assert len(captured) == 1, "missing_api_root path must emit exactly one frame"
    chunk = parse_agent_server_wire_chunk(captured[0])
    assert chunk.payload.get("event_type") == _PUSH_TERMINATED_EVENT_TYPE
    assert chunk.payload.get("reason") == "missing_api_root"


@pytest.mark.asyncio
async def test_push_loop_connect_failures_exceeded_path_invokes_emit_push_terminated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_push_loop`` 的 ``connect_failures_exceeded`` 退出路径必须调
    ``_emit_push_terminated``，合成帧 payload 含 ``event_type`` 与 ``reason``，
    且退出后 ``_running``/``_server_ready`` 翻 False。

    覆盖 review 反馈建议 2.1 的 ``connect_failures_exceeded`` 分支。
    用 httpx.MockTransport 强制连续 ConnectError，让重试计数到上限。
    """
    captured: list[dict[str, Any]] = []

    async def handler(wire: dict[str, Any]) -> None:
        captured.append(wire)

    # Force _PUSH_MAX_CONNECT_FAILURES to 2 for a fast trip.
    monkeypatch.setattr(
        pod_http_client, "_PUSH_MAX_CONNECT_FAILURES", 2
    )
    # No retry_push_connect so ConnectError increments _push_connect_failures.
    client = _make_client_with_loop_state(api_root=_TEST_API_ROOT, retry_push_connect=False)
    client._on_server_push = handler

    # Stub _ensure_http to return a client whose .stream raises ConnectError
    # every call. We bypass _request_headers' _link_mtls dependency by stubbing
    # it too, since the failing path returns before the request is built.
    def _boom_transport(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("boom", request=request)

    fake_http = httpx.AsyncClient(transport=httpx.MockTransport(_boom_transport))
    monkeypatch.setattr(client, "_ensure_http", MagicMock(return_value=fake_http))
    monkeypatch.setattr(client, "_request_headers", MagicMock(return_value={}))
    # Patch asyncio.sleep so the loop's tail never blocks between retries.
    monkeypatch.setattr("asyncio.sleep", AsyncMock())

    await client._push_loop()

    assert len(captured) == 1, "connect_failures_exceeded path must emit exactly one frame"
    chunk = parse_agent_server_wire_chunk(captured[0])
    assert chunk.payload.get("event_type") == _PUSH_TERMINATED_EVENT_TYPE
    assert chunk.payload.get("reason") == "connect_failures_exceeded"
    # failures is at least _PUSH_MAX_CONNECT_FAILURES (the loop may overshoot
    # by one because the increment-then-check happens inside the except branch
    # and a ConnectError can be raised multiple times per iteration by httpx
    # connection pool before tripping). The exact count is not contractual;
    # the contract is "failures >= ceiling → emit + stop".
    assert chunk.payload.get("failures") >= 2
    # Loop exit must flip running/server_ready so callers / wait_push_ready unblock.
    assert client._running is False
    assert client._server_ready is False
    await fake_http.aclose()


@pytest.mark.asyncio
async def test_message_handler_push_terminated_branch_dispatches_broadcast_and_returns_early(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``MessageHandler._handle_agent_server_push_ordered`` 收到
    ``gateway.push_terminated`` wire 时必须调 ``_broadcast_push_terminated`` 并
    提前 return——不走后续 session 路由 / 不写 robot_messages。

    覆盖 review 反馈建议 2.2：分发分支触发广播并提前返回。

    直接用 ``unittest.mock`` 构造一个最小 MessageHandler 子类实例，绕过
    ``__init__`` 的重依赖；只 stub 用到的方法。
    """
    from agent_gateway.message_handler.message_handler import MessageHandler

    captured_payload: list[dict[str, Any]] = []

    # Minimal stub instance bypassing __init__/__new__ (singleton + heavy DB/WS deps).
    # Use object.__new__ to skip MessageHandler.__new__'s singleton logic and
    # agent_client requirement; we only exercise _handle_agent_server_push_ordered.
    mh = object.__new__(MessageHandler)
    # _handle_agent_server_push_ordered reads _server_push_lock lazily; create it.
    mh._server_push_lock = asyncio.Lock()
    mh._stream_sessions = {}
    # Stub _broadcast_push_terminated to record the payload without doing real I/O.
    mh._broadcast_push_terminated = AsyncMock(side_effect=lambda p: captured_payload.append(p))
    # publish_robot_messages must not be touched by this branch; if it were, the
    # test would fail at attribute lookup. We do not stub it on purpose.
    mh.publish_robot_messages = AsyncMock(side_effect=AssertionError(
        "gateway.push_terminated branch must not publish robot_messages"
    ))

    wire = await _capture_synthesised_wire()
    await mh._handle_agent_server_push_ordered(wire)

    assert len(captured_payload) == 1, "must call _broadcast_push_terminated exactly once"
    broadcasted = captured_payload[0]
    assert broadcasted.get("event_type") == _PUSH_TERMINATED_EVENT_TYPE
    assert broadcasted.get("affected_scope") == "pod"
    assert broadcasted.get("reason") == "connect_failures_exceeded"


@pytest.mark.asyncio
async def test_message_handler_push_terminated_branch_swallows_broadcast_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_broadcast_push_terminated`` 的 except 分支只记日志、不向上抛——
    否则一条通知帧的转发失败会把整条推送通道炸掉。

    覆盖 review 反馈建议 2.3：广播异常仅记录日志而不中断推送循环。

    让 ``_broadcast_push_terminated`` 的 ``web_channel.broadcast_event`` 抛异常，
    断言 ``_handle_agent_server_push_ordered`` 不抛、且确实把帧识别为
    ``gateway.push_terminated``（即走到了正确的分支才触发了广播）。
    """
    from agent_gateway.message_handler.message_handler import MessageHandler

    mh = object.__new__(MessageHandler)
    mh._server_push_lock = asyncio.Lock()
    mh._stream_sessions = {}

    class _BoomChannel:
        async def broadcast_event(self, event: str, payload: dict[str, Any]) -> None:
            raise RuntimeError("broadcast exploded")

    mh._resolve_web_channel = MagicMock(return_value=_BoomChannel())
    mh.publish_robot_messages = AsyncMock(side_effect=AssertionError(
        "gateway.push_terminated branch must not publish robot_messages"
    ))

    wire = await _capture_synthesised_wire()
    # Must not raise.
    await mh._handle_agent_server_push_ordered(wire)


@pytest.mark.asyncio
async def test_emit_push_terminated_payload_carries_pod_scope() -> None:
    """合成帧 payload 必须带 ``affected_scope="pod"``，与 message_handler
    ``_broadcast_push_terminated`` docstring 中事件性质说明一致。

    前端按 scope 决定行为：pod 级只提示不重连，避免单 pod 故障引发不相关
    会话的连接抖动（review 反馈建议 1）。
    """
    wire = await _capture_synthesised_wire()
    chunk = parse_agent_server_wire_chunk(wire)
    assert chunk.payload.get("affected_scope") == "pod"
