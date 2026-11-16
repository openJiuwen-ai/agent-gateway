# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

import asyncio
import base64
import io
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import httpx
from fastapi import FastAPI
from e2a.message import ReqMethod
from websockets.legacy.client import connect

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig


def test_web_channel_import_keeps_listener_framework_lazy(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["JIUWENSWARM_HOME"] = str(tmp_path)
    env["JIUWENSWARM_DATA_DIR"] = str(tmp_path / "data")
    result = subprocess.run(
        [sys.executable, "-c", "import sys; import agent_gateway.channel_manager.web.web_connect; assert 'uvicorn' not in sys.modules"],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr


async def test_listener_preserves_complete_trajectory_archive_over_http() -> None:
    from agent_gateway.channel_manager.web.trajectory_http import attach_trajectory_routes
    from agent_gateway.transport.uvicorn_listener import UvicornListener

    archive_buffer = io.BytesIO()
    expected_file = bytes(range(256)) * 512
    with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("trajectory.jsonl", expected_file)
    body = archive_buffer.getvalue()
    calls: list[ReqMethod] = []

    async def metadata(session_id: str, user_id: str | None = None) -> dict[str, str]:
        assert user_id == "alice"
        return {"session_id": session_id, "mode": "agent.work.normal", "team_name": ""}

    async def fetch(method: ReqMethod, params: dict, user_id: str | None = None) -> tuple[bool, dict]:
        assert user_id == "alice"
        calls.append(method)
        if method == ReqMethod.TRAJECTORY_ARCHIVE_OPEN:
            return True, {"chunked": True, "export_id": "export-1", "size": len(body), "content_type": "application/zip"}
        if method == ReqMethod.TRAJECTORY_BLOB_READ:
            offset = int(params["offset"])
            chunk = body[offset:offset + 16384]
            next_offset = offset + len(chunk)
            return True, {"data": base64.b64encode(chunk).decode("ascii"), "next_offset": next_offset, "done": next_offset >= len(body)}
        assert method == ReqMethod.TRAJECTORY_BLOB_CLOSE
        return True, {}

    app = FastAPI()
    attach_trajectory_routes(app, SimpleNamespace(), metadata_loader=metadata, fetch=fetch)
    listener = UvicornListener(app, host="127.0.0.1", port=0, ws_per_message_deflate=False)
    try:
        await asyncio.wait_for(listener.start(), 5)
        host, port = listener.bound_addresses[0]
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"http://{host}:{port}/api/trajectory/sessions/session-1/archive", params={"user_id": "alice"})
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/zip"
        assert int(response.headers["content-length"]) == len(body)
        assert response.content == body
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            assert archive.read("trajectory.jsonl") == expected_file
        assert calls[-1] == ReqMethod.TRAJECTORY_BLOB_CLOSE
        assert calls.count(ReqMethod.TRAJECTORY_BLOB_READ) > 1
    finally:
        await asyncio.wait_for(listener.stop(), 5)


async def test_web_start_waits_until_stop_and_preserves_connection_cleanup() -> None:
    channel = WebChannel(WebChannelConfig(enabled=True, host="127.0.0.1", port=0), RobotMessageRouter())
    task = asyncio.create_task(channel.start())
    try:
        await asyncio.wait_for(channel.wait_ready(), 5)
        assert not task.done()
        listener = channel._listener
        host, port = listener.bound_addresses[0]
        async with connect(f"ws://{host}:{port}/ws") as ws:
            await asyncio.wait_for(ws.ping(), 5)
            await asyncio.wait_for(channel.stop(), 5)
            await ws.wait_closed()
            assert ws.close_code == 1001
        await asyncio.wait_for(task, 5)
        assert not channel._running
        assert not channel.clients
        assert not channel._clients_by_key
        rebound = await asyncio.start_server(lambda reader, writer: writer.close(), host, port)
        rebound.close()
        await rebound.wait_closed()
    finally:
        await channel.stop()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_web_channel_can_start_again_with_new_listener() -> None:
    channel = WebChannel(WebChannelConfig(enabled=True, host="127.0.0.1", port=0), RobotMessageRouter())
    previous = None
    for _ in range(2):
        task = asyncio.create_task(channel.start())
        try:
            await asyncio.sleep(0)
            await asyncio.wait_for(channel.wait_ready(), 5)
            assert channel._listener is not previous
            previous = channel._listener
            await channel.stop()
            await asyncio.wait_for(task, 5)
        finally:
            await channel.stop()
            await asyncio.gather(task, return_exceptions=True)


async def test_cancelled_web_start_cleans_listener_and_port() -> None:
    channel = WebChannel(WebChannelConfig(enabled=True, host="127.0.0.1", port=0), RobotMessageRouter())
    task = asyncio.create_task(channel.start())
    try:
        await asyncio.wait_for(channel.wait_ready(), 5)
        host, port = channel._listener.bound_addresses[0]
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 5)
        await channel.stop()
        rebound = await asyncio.start_server(lambda reader, writer: writer.close(), host, port)
        rebound.close()
        await rebound.wait_closed()
    finally:
        await channel.stop()
        await asyncio.gather(task, return_exceptions=True)


async def test_web_app_construction_failure_wakes_ready_waiter() -> None:
    channel = WebChannel(WebChannelConfig(enabled=True, host="127.0.0.1", port=0), RobotMessageRouter())
    with patch("agent_gateway.channel_manager.web.web_channel_app.build_web_channel_app", side_effect=ValueError("app build probe")):
        ready = asyncio.create_task(channel.wait_ready())
        task = asyncio.create_task(channel.start())
        try:
            with pytest.raises(ValueError, match="app build probe"):
                await task
            with pytest.raises(ValueError, match="app build probe"):
                await asyncio.wait_for(ready, 5)
        finally:
            await channel.stop()
            await asyncio.gather(task, ready, return_exceptions=True)


async def test_gateway_server_start_returns_and_closes_active_tui_connection() -> None:
    from agent_gateway.app_gateway import GatewayServer, GatewayServerConfig, RouteConfig

    server = GatewayServer(
        GatewayServerConfig(enabled=True, host="127.0.0.1", port=0, routes={"/tui": RouteConfig(path="/tui", channel_id="tui")}),
        RobotMessageRouter(),
    )
    try:
        await asyncio.wait_for(server.start(), 5)
        host, port = server._listener.bound_addresses[0]
        waiter = asyncio.create_task(server.wait_until_closed())
        async with connect(f"ws://{host}:{port}/tui") as ws:
            assert json.loads(await ws.recv())["event"] == "connection.ack"
            assert not waiter.done()
            await asyncio.wait_for(server.stop(), 5)
            await ws.wait_closed()
            assert ws.close_code == 1001
        await asyncio.wait_for(waiter, 5)
        await server.stop()
        assert not server._clients
    finally:
        await server.stop()


@pytest.mark.parametrize("mode", ["ready", "early_exit", "error", "timeout", "cancel"])
async def test_ready_helper_uses_public_readiness_without_cancelling_owner(mode: str) -> None:
    from agent_gateway.app_gateway import _wait_for_web_channel_listening

    ready = asyncio.Event()
    keep_running = asyncio.Event()

    async def start() -> None:
        if mode == "early_exit":
            return
        if mode == "error":
            raise ValueError("start probe")
        await keep_running.wait()

    channel = SimpleNamespace(wait_ready=ready.wait, config=SimpleNamespace(host="127.0.0.1", port=0, path="/ws"))
    owner = asyncio.create_task(start())
    helper = asyncio.create_task(_wait_for_web_channel_listening(channel, owner, timeout=0.05 if mode == "timeout" else 5))
    try:
        if mode == "ready":
            ready.set()
            await asyncio.wait_for(helper, 5)
            assert not owner.done()
        elif mode == "early_exit":
            with pytest.raises(RuntimeError, match="stopped before"):
                await helper
        elif mode == "error":
            with pytest.raises(ValueError, match="start probe"):
                await helper
        elif mode == "timeout":
            with pytest.raises(TimeoutError, match="did not bind"):
                await helper
            assert not owner.done()
        else:
            await asyncio.sleep(0)
            helper.cancel()
            with pytest.raises(asyncio.CancelledError):
                await helper
            assert not owner.done()
    finally:
        keep_running.set()
        if not helper.done():
            helper.cancel()
        await asyncio.gather(owner, helper, return_exceptions=True)
