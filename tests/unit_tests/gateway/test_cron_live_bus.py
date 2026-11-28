# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""定时任务执行会话的实时流：晚到的页面还能补上已经产生的结果。"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi import FastAPI
from starlette.responses import JSONResponse

from agent_gateway.cron.live_bus import CronLiveBus


def test_live_window_evicts_oldest_distinct_frame_for_all_viewers() -> None:
    bus = CronLiveBus(replay_size=1)
    sid = "cron_fifo"
    viewers = [bus.subscribe(sid, replay=False) for _ in range(2)]
    frames = [
        {"event": "chat.delta", "payload": {"live_seq": seq, "content": "same token"}}
        for seq in range(bus._seen_size + 1)
    ]

    for frame in frames[:-1]:
        bus.publish(sid, frame)
    bus.publish(sid, frames[0])  # Still in the window; a duplicate must not refresh its position.
    bus.publish(sid, frames[-1])
    bus.publish(sid, frames[1])  # Still remembered.
    bus.publish(sid, frames[0])  # Evicted; deliver again.

    for queue in viewers:
        assert [queue.get_nowait() for _ in range(queue.qsize())] == [*frames, frames[0]]
        bus.unsubscribe(sid, queue)


def test_late_subscriber_replays_cron_final() -> None:
    async def _run() -> None:
        bus = CronLiveBus()
        sid = "__cron___1a1640a452f_40c17fbf6426"
        bus.publish(sid, {"event": "chat.final", "payload": {"content": "你好", "session_id": sid}})
        queue = bus.subscribe(sid, replay=True)
        frame = await asyncio.wait_for(queue.get(), timeout=1)
        bus.unsubscribe(sid, queue)
        assert frame["event"] == "chat.final"
        assert frame["payload"]["content"] == "你好"
        assert frame["replay"] is True

    asyncio.run(_run())


def test_resubscribe_replays_terminal_frame_seen_by_previous_viewer() -> None:
    """上个订阅者已收过终态帧，重订阅仍要拿到回放。

    会话级去重只该挡住「同一批订阅者的重复投递」。若回放也被它挡住，
    effect 重建 / 导航竞态下终态帧就会丢，前端「任务执行 Xs」再也停不下来。
    """

    async def _run() -> None:
        bus = CronLiveBus()
        sid = "__cron___1a1640a452f_resubscribe"
        first = bus.subscribe(sid, replay=False)
        try:
            bus.publish(
                sid,
                {
                    "event": "chat.processing_status",
                    "payload": {"is_processing": False, "live_seq": 7, "session_id": sid},
                },
            )
            delivered = await asyncio.wait_for(first.get(), timeout=1)
            assert delivered["event"] == "chat.processing_status"
        finally:
            bus.unsubscribe(sid, first)

        second = bus.subscribe(sid, replay=True)
        try:
            replayed = await asyncio.wait_for(second.get(), timeout=1)
        finally:
            bus.unsubscribe(sid, second)
        assert replayed["event"] == "chat.processing_status"
        assert replayed["payload"]["is_processing"] is False
        assert replayed["replay"] is True

    asyncio.run(_run())


def test_http_stream_marks_replay_but_not_new_frames(monkeypatch) -> None:
    from agent_gateway.channel_manager.web.web_http_routes import register_web_http_api_routes

    async def _run() -> None:
        bus = CronLiveBus()
        monkeypatch.setattr("agent_gateway.cron.live_bus._redis_client", lambda: None)
        monkeypatch.setattr("agent_gateway.cron.live_bus.cron_live_bus", bus)
        lookup = AsyncMock(return_value=JSONResponse({"ok": True, "data": {"cron_id": "job"}}))
        monkeypatch.setitem(register_web_http_api_routes.__globals__, "_unary", lookup)
        app = FastAPI()
        channel = object()
        register_web_http_api_routes(app, channel, enterprise=True)
        endpoint = next(route.endpoint for route in app.routes if route.path == "/api/v1/sessions/{session_id}/live")
        sid = "cron_http_replay_marker"
        bus.publish(sid, {"event": "chat.delta", "payload": {"content": "old", "live_seq": 1}})
        request = SimpleNamespace(is_disconnected=AsyncMock(return_value=False))
        response = await endpoint(sid, request)
        assert response.status_code == 200
        lookup.assert_awaited_once_with(request, channel, "session.get_metadata", {"session_id": sid})
        try:
            replay = await anext(response.body_iterator)
            assert '"live_replay": true' in replay
            bus.publish(sid, {"event": "chat.final", "payload": {"content": "new", "live_seq": 2}})
            live = await anext(response.body_iterator)
            assert '"live_replay": false' in live
        finally:
            await response.body_iterator.aclose()
            bus.clear_session(sid)

    asyncio.run(_run())


def test_replay_marker_does_not_leak_into_live_fanout_or_stored_frames() -> None:
    async def _run() -> None:
        bus = CronLiveBus()
        sid = "cron_replay_marker"
        first = {"event": "chat.delta", "payload": {"content": "first", "live_seq": 1}}
        bus.publish(sid, first)
        queue = bus.subscribe(sid)
        assert (await queue.get())["replay"] is True
        assert "replay" not in first
        second = {"event": "chat.final", "payload": {"content": "done", "live_seq": 2}}
        bus.publish(sid, second)
        assert await queue.get() == second
        assert "replay" not in second
        bus.unsubscribe(sid, queue)
        bus.clear_session(sid)

    asyncio.run(_run())


def test_redis_backlog_also_marks_replayed_frames(monkeypatch) -> None:
    from agent_gateway.cron.live_bus import _encode_wire

    async def _run() -> None:
        frame = {"event": "chat.final", "payload": {"content": "remote answer", "live_seq": 7}}
        client = AsyncMock()
        client.lrange.return_value = [_encode_wire(origin="other-pod", frame=frame)]
        monkeypatch.setattr("agent_gateway.cron.live_bus._redis_client", lambda: client)
        bus = CronLiveBus()
        queue = asyncio.Queue()
        await bus._redis_replay_into("cron_remote_replay", queue, set())
        assert await queue.get() == {**frame, "replay": True}

    asyncio.run(_run())


def test_redis_fallback_does_not_requeue_live_frames(monkeypatch) -> None:
    """Redis 回放失败时不能把订阅后已实时投递的帧再补一遍。"""

    from agent_gateway.cron.live_bus import _frame_fingerprint

    async def _run() -> None:
        client = AsyncMock()
        client.lrange.side_effect = RuntimeError("redis down")
        monkeypatch.setattr("agent_gateway.cron.live_bus._redis_client", lambda: client)
        bus = CronLiveBus()
        sid = "cron_redis_fallback"
        replayed = {"event": "chat.final", "payload": {"content": "old", "live_seq": 1}}
        live = {"event": "chat.delta", "payload": {"content": "new", "live_seq": 2}}
        queue = asyncio.Queue()
        # 订阅时同步回放与订阅后的实时投递已经入队。
        await queue.put({**replayed, "replay": True})
        await queue.put(live)

        await bus._redis_replay_into(sid, queue, {_frame_fingerprint(replayed)})

        assert queue.qsize() == 2
        assert await queue.get() == {**replayed, "replay": True}
        assert await queue.get() == live
        assert queue.empty()

    asyncio.run(_run())


def test_redis_replay_skips_frames_already_live_delivered(monkeypatch) -> None:
    """订阅建立后、Redis 列表读完前实时投递的帧不能再带 replay 标记补一遍。"""

    from agent_gateway.cron.live_bus import _encode_wire

    async def _run() -> None:
        frame = {"event": "chat.delta", "payload": {"content": "live", "live_seq": 3}}
        remote = {"event": "chat.final", "payload": {"content": "done", "live_seq": 4}}
        release = asyncio.Event()

        async def backlog(*args):
            await release.wait()
            return [_encode_wire(origin="other-pod", frame=item) for item in (frame, remote)]

        client = AsyncMock()
        client.lrange.side_effect = backlog
        monkeypatch.setattr("agent_gateway.cron.live_bus._redis_client", lambda: client)
        bus = CronLiveBus()
        monkeypatch.setattr(bus, "_ensure_redis_bridge", AsyncMock())
        sid = "cron_replay_double_delivery"
        queue = bus.subscribe(sid)
        try:
            bus.publish(sid, frame)
            assert queue.get_nowait() == frame
            release.set()
            assert await asyncio.wait_for(queue.get(), timeout=1) == {**remote, "replay": True}
            assert queue.empty()
            await asyncio.sleep(0)  # Let the replay task's completion callback run.
            assert queue not in bus._queue_seen
        finally:
            bus.unsubscribe(sid, queue)

    asyncio.run(_run())


def test_redis_replay_still_delivers_frames_seen_by_previous_queue(monkeypatch) -> None:
    """上个订阅者已收过的帧仍要回放：per-queue 去重不能退化成会话级去重。"""

    from agent_gateway.cron.live_bus import _encode_wire

    async def _run() -> None:
        frame = {"event": "chat.final", "payload": {"content": "done", "live_seq": 9}}
        client = AsyncMock()
        client.lrange.return_value = [_encode_wire(origin="other-pod", frame=frame)]
        monkeypatch.setattr("agent_gateway.cron.live_bus._redis_client", lambda: client)
        bus = CronLiveBus()
        sid = "cron_replay_previous_queue"
        first: asyncio.Queue = asyncio.Queue()
        bus._subscribers[sid] = {first}
        bus.publish(sid, frame)
        assert await first.get() == frame
        bus.unsubscribe(sid, first)

        second: asyncio.Queue = asyncio.Queue()
        bus._subscribers[sid] = {second}
        await bus._redis_replay_into(sid, second, set())

        assert await asyncio.wait_for(second.get(), timeout=1) == {**frame, "replay": True}
        bus.unsubscribe(sid, second)

    asyncio.run(_run())
