# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""反向 RPC 状态与流保活生命周期的回归测试。

两类都是「跑久了才出问题」的缺陷：origin 表随工具调用线性增长，保活失败
无限重路由放大下游压力。这里用真实类 + 内存替身把边界钉住。
"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.extensions.runtime_management_extension import runtime_routed_client as routed
from agent_gateway.extensions.runtime_management_extension.runtime_routed_client import (
    RuntimeRoutedAgentClient,
)
from agent_gateway.southbound.orchestration import delegated_router as routed_impl
from agent_gateway.southbound.orchestration.delegated_router import _RpcOrigin


@pytest.mark.asyncio
async def test_startup_is_not_pod_connection_and_shutdown_preserves_borrowed_clients():
    """Dynamic startup is lazy; injected transport/control clients are borrowed."""
    route, http = AsyncMock(), AsyncMock()
    instance = RuntimeRoutedAgentClient(route_client=route, http_client=http)
    await instance.connect("unused")
    assert instance.server_ready
    assert not instance.agent_ready
    route.route.assert_not_awaited()
    http.connect.assert_not_awaited()
    await instance.disconnect()
    await instance.disconnect()
    assert not instance.server_ready
    assert not instance.agent_ready
    route.aclose.assert_not_awaited()
    http.disconnect.assert_not_awaited()


@pytest.fixture()
def client(monkeypatch):
    monkeypatch.setattr(routed, "authorize_agent", AsyncMock(return_value=None))
    http = AsyncMock()
    instance = RuntimeRoutedAgentClient(route_client=AsyncMock(), http_client=http)
    instance._connected = True
    return instance, http


# --------------------------------------------------------------------------- #
# 12：反向 RPC origin 有界
# --------------------------------------------------------------------------- #


def test_origin_entries_are_pruned_after_ttl(client, monkeypatch):
    instance, _ = client
    instance._remember_rpc_origin(("s1", "1"), "http://pod1")
    instance._remember_rpc_origin(("s2", "2"), "http://pod2")

    # 时间推进到只有第二条还活着
    later = time.monotonic() + routed_impl._RPC_ORIGIN_TTL_SECONDS + 1
    monkeypatch.setattr(routed_impl.time, "monotonic", lambda: later)

    assert instance._remember_rpc_origin(("s3", "3"), "http://pod3") is not None
    assert list(instance._rpc_origins) == [("s3", "3")]


def test_expired_origin_is_not_used_for_reply(client, monkeypatch):
    instance, http = client
    instance._remember_rpc_origin(("s1", "1"), "http://pod1")

    later = time.monotonic() + routed_impl._RPC_ORIGIN_TTL_SECONDS + 1
    monkeypatch.setattr(routed_impl.time, "monotonic", lambda: later)

    envelope = routed_impl.E2AEnvelope(
        method="acp.tool_response", request_id="r", session_id="s1", params={"jsonrpc_id": 1}
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        asyncio.run(instance.send_request(envelope))
    http.send_request.assert_not_awaited()


def test_origin_table_is_capped(client):
    instance, _ = client
    for index in range(routed_impl._MAX_RPC_ORIGINS + 10):
        instance._remember_rpc_origin(("s", str(index)), "http://pod")
    assert len(instance._rpc_origins) == routed_impl._MAX_RPC_ORIGINS
    # 保留的是最近的条目
    assert ("s", str(routed_impl._MAX_RPC_ORIGINS - 1)) in instance._rpc_origins


def test_stale_reply_does_not_consume_other_session_origin(client):
    """显式 session 的回程不命中时，不能吃掉别的 session 同 ID 的 origin。"""
    instance, http = client
    instance._remember_rpc_origin(("other", "7"), "http://pod-other")

    envelope = routed_impl.E2AEnvelope(
        method="acp.tool_response", request_id="r", session_id="mine", params={"jsonrpc_id": 7}
    )
    with pytest.raises(RuntimeError, match="unavailable"):
        asyncio.run(instance.send_request(envelope))
    http.send_request.assert_not_awaited()
    assert ("other", "7") in instance._rpc_origins


# --------------------------------------------------------------------------- #
# 15：保活单飞与退避
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_concurrent_streams_share_one_keepalive(client, monkeypatch):
    instance, _ = client
    instance._touch_interval_seconds = 0.01
    monkeypatch.setattr(instance, "_ensure_pod_push", AsyncMock())

    started: list[object] = []
    real_create = asyncio.create_task

    def spy_create(coro, **kwargs):
        started.append(coro)
        return real_create(coro, **kwargs)

    monkeypatch.setattr(routed_impl.asyncio, "create_task", spy_create)
    monkeypatch.setattr(instance, "_touch_quiet", AsyncMock(return_value=True))

    async def fake_stream(*_args, **_kwargs):
        await asyncio.sleep(0.05)
        yield object()

    monkeypatch.setattr(instance._http, "send_request_stream", fake_stream)

    async def consume():
        return [chunk async for chunk in instance._stream_with_touch(
            routed_impl.E2AEnvelope(method="chat.send", request_id="r"),
            base_url="http://pod1", session_id="s1", route_id="r1",
            group_id="g", bot_id="b", user_id="u",
        )]

    await asyncio.gather(consume(), consume(), consume())
    assert len(started) == 1, "同一会话的并发流应共享一个保活任务"
    assert instance._stream_keepalives == {}


@pytest.mark.asyncio
async def test_deferred_team_stream_keeps_busy_affinity_after_short_stream(client, monkeypatch):
    """Team deferred short stream must keep one session keepalive after HTTP ends."""
    instance, _ = client
    instance._touch_interval_seconds = 0.01
    monkeypatch.setattr(instance, "_ensure_pod_push", AsyncMock())
    monkeypatch.setattr(instance, "_touch_quiet", AsyncMock(return_value=True))

    deferred = SimpleNamespace(
        payload={"event_type": "chat.processing_status_deferred", "session_id": "s1"},
    )
    done = SimpleNamespace(payload=None, is_complete=True)

    async def fake_stream(*_args, **_kwargs):
        yield deferred
        yield done

    monkeypatch.setattr(instance._http, "send_request_stream", fake_stream)

    chunks = [
        chunk
        async for chunk in instance._stream_with_touch(
            routed_impl.E2AEnvelope(method="chat.send", request_id="r"),
            base_url="http://pod1",
            session_id="s1",
            route_id="r1",
            group_id="g",
            bot_id="b",
            user_id="u",
        )
    ]
    assert chunks == [deferred, done]
    assert "s1" in instance._busy_holds
    assert len(instance._stream_keepalives) == 1
    key = ("s1", "g", "b", "u")
    assert instance._stream_keepalives[key].users == 1

    # Second deferred refreshes the deadline without doubling the slot/hold.
    first_deadline = instance._busy_holds["s1"].expires_at
    instance.hold_busy_session_affinity(
        session_id="s1",
        route_id="r1",
        base_url="http://pod1",
        group_id="g",
        bot_id="b",
        user_id="u",
    )
    assert instance._stream_keepalives[key].users == 1
    assert instance._busy_holds["s1"].expires_at >= first_deadline

    await instance.release_busy_session_affinity("s1")
    assert instance._busy_holds == {}
    assert instance._stream_keepalives == {}
    await instance.release_busy_session_affinity("s1")  # idempotent


@pytest.mark.asyncio
async def test_ordinary_stream_without_deferred_does_not_busy_hold(client, monkeypatch):
    """Enterprise ordinary chat (no deferred marker) must not leave a busy hold."""
    instance, _ = client
    instance._touch_interval_seconds = 0.01
    monkeypatch.setattr(instance, "_ensure_pod_push", AsyncMock())
    monkeypatch.setattr(instance, "_touch_quiet", AsyncMock(return_value=True))

    async def fake_stream(*_args, **_kwargs):
        yield SimpleNamespace(payload={"event_type": "chat.delta", "content": "hi"})
        yield SimpleNamespace(payload=None, is_complete=True)

    monkeypatch.setattr(instance._http, "send_request_stream", fake_stream)

    _ = [
        chunk
        async for chunk in instance._stream_with_touch(
            routed_impl.E2AEnvelope(method="chat.send", request_id="r"),
            base_url="http://pod1",
            session_id="s1",
            route_id="r1",
            group_id="g",
            bot_id="b",
            user_id="u",
        )
    ]
    assert instance._busy_holds == {}
    assert instance._stream_keepalives == {}


@pytest.mark.asyncio
async def test_busy_hold_times_out_and_releases(client, monkeypatch):
    """Leaked holds must self-expire instead of touching forever."""
    instance, _ = client
    instance._touch_interval_seconds = 0.01
    instance._busy_hold_max_seconds = 0.05
    monkeypatch.setattr(instance, "_ensure_pod_push", AsyncMock())
    monkeypatch.setattr(instance, "_touch_quiet", AsyncMock(return_value=True))

    async def fake_stream(*_args, **_kwargs):
        yield SimpleNamespace(
            payload={"event_type": "chat.processing_status_deferred", "session_id": "s1"},
        )
        yield SimpleNamespace(payload=None, is_complete=True)

    monkeypatch.setattr(instance._http, "send_request_stream", fake_stream)

    _ = [
        chunk
        async for chunk in instance._stream_with_touch(
            routed_impl.E2AEnvelope(method="chat.send", request_id="r"),
            base_url="http://pod1",
            session_id="s1",
            route_id="r1",
            group_id="g",
            bot_id="b",
            user_id="u",
        )
    ]
    assert "s1" in instance._busy_holds
    # Wait past ttl + a few keepalive ticks; timeout releases without self-cancel.
    for _ in range(30):
        if not instance._busy_holds and not instance._stream_keepalives:
            break
        await asyncio.sleep(0.02)
    assert instance._busy_holds == {}
    assert instance._stream_keepalives == {}


@pytest.mark.asyncio
async def test_keepalive_backs_off_and_keeps_probing(client, monkeypatch):
    instance, _ = client
    instance._touch_interval_seconds = 1.0
    sleeps: list[float] = []
    touches = 0

    async def fake_sleep(delay):
        sleeps.append(delay)
        if len(sleeps) >= 4:
            raise asyncio.CancelledError

    async def always_fail(*_args, **_kwargs):
        nonlocal touches
        touches += 1
        return False

    monkeypatch.setattr(routed_impl.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(instance, "_touch_quiet", always_fail)
    monkeypatch.setattr(instance, "_restore_stream_affinity", AsyncMock(return_value=None))

    with pytest.raises(asyncio.CancelledError):
        await instance._keep_stream_affinity(("s1", "g", "b", "u"), "r1", "http://pod1")

    # 退避递增，且有上限（shift 最多 3 → 8x）
    assert sleeps[:4] == [1.0, 2.0, 4.0, 8.0]
    assert touches >= 3


@pytest.mark.asyncio
async def test_touch_success_resets_backoff(client, monkeypatch):
    instance, _ = client
    instance._touch_interval_seconds = 1.0
    sleeps: list[float] = []
    results = iter([False, False, True, False])

    async def fake_sleep(delay):
        sleeps.append(delay)
        if len(sleeps) >= 4:
            raise asyncio.CancelledError

    async def flaky(*_args, **_kwargs):
        return next(results)

    monkeypatch.setattr(routed_impl.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(instance, "_touch_quiet", flaky)
    monkeypatch.setattr(instance, "_restore_stream_affinity", AsyncMock(return_value=None))

    with pytest.raises(asyncio.CancelledError):
        await instance._keep_stream_affinity(("s1", "g", "b", "u"), "r1", "http://pod1")

    # 第 3 次 touch 成功 → 退避归零，回到基础间隔
    assert sleeps[2] == 4.0
    assert sleeps[3] == 1.0


def test_interrupt_targets_cover_every_active_stream(client):
    instance, _ = client
    instance._session_stream_bases["s1"] = {"a": "http://pod-a", "b": "http://pod-b"}
    targets = asyncio.run(instance._interrupt_target_bases("s1", "http://routed"))
    assert targets == ["http://pod-b", "http://pod-a", "http://routed"]


def _release_probe_handler(agent_client: object):
    from agent_gateway.message_handler.message_handler import MessageHandler

    handler = object.__new__(MessageHandler)
    handler.agent_client = agent_client
    handler._team_busy_affinity_tasks = set()
    return handler


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload", "should_release"),
    [
        ({"event_type": "team.completed"}, True),
        ({"event_type": "chat.error", "error": "boom"}, True),
        (
            {
                "event_type": "chat.processing_status",
                "is_processing": False,
                "is_complete": True,
            },
            True,
        ),
        (
            {
                "event_type": "chat.processing_status",
                "is_processing": False,
                "is_complete": True,
                "awaiting_user_input": True,
            },
            False,
        ),
        (
            {
                "event_type": "chat.processing_status",
                "is_processing": False,
            },
            False,
        ),
        (
            {
                "event_type": "chat.processing_status",
                "is_processing": True,
                "is_complete": True,
            },
            False,
        ),
    ],
)
async def test_maybe_release_team_busy_affinity_round_terminal_only(payload, should_release):
    """Release only on round terminals; bare false / ask-user pause keep the hold."""
    release = AsyncMock()
    agent = SimpleNamespace(release_busy_session_affinity=release)
    handler = _release_probe_handler(agent)

    handler._maybe_release_team_busy_affinity("s1", payload)
    await asyncio.sleep(0)

    if should_release:
        release.assert_awaited_once_with("s1")
    else:
        release.assert_not_awaited()
