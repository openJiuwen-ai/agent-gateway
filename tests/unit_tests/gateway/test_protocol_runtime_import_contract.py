# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Import compatibility and live protocol checks for the extracted runtime."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from websockets.exceptions import InvalidStatusCode
from websockets.legacy.client import connect

from agent_gateway.channel_manager.base import RobotMessageRouter


def _isolated_env(tmp_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env["JIUWENSWARM_HOME"] = str(tmp_path / "home")
    env["JIUWENSWARM_DATA_DIR"] = str(tmp_path / "data")
    env["AGENTOS_GATEWAY_LOG_DIR"] = str(tmp_path / "logs")
    return env


@pytest.mark.parametrize("runtime_first", [True, False])
def test_gateway_runtime_preserves_legacy_class_identity(
    tmp_path: Path,
    runtime_first: bool,
) -> None:
    first, second = (
        ("from agent_gateway.routing import protocol_runtime as runtime", "from agent_gateway import app_gateway as legacy")
        if runtime_first
        else ("from agent_gateway import app_gateway as legacy", "from agent_gateway.routing import protocol_runtime as runtime")
    )
    script = f"""
{first}
{second}
from agent_gateway.routing import gateway_server as compatibility
assert legacy.GatewayServer is runtime.ProtocolRuntime
assert legacy.ProtocolRuntime is runtime.ProtocolRuntime
assert compatibility.GatewayServer is runtime.ProtocolRuntime
assert compatibility.ProtocolRuntime is runtime.ProtocolRuntime
assert legacy.RouteConfig is compatibility.RouteConfig is runtime.RouteConfig
assert legacy.GatewayServerConfig is runtime.ProtocolRuntimeConfig
assert legacy.ProtocolRuntimeConfig is runtime.ProtocolRuntimeConfig
assert compatibility.GatewayServerConfig is runtime.ProtocolRuntimeConfig
assert compatibility.ProtocolRuntimeConfig is runtime.ProtocolRuntimeConfig
assert runtime.ProtocolRuntime.__name__ == 'ProtocolRuntime'
assert runtime.ProtocolRuntimeConfig.__name__ == 'ProtocolRuntimeConfig'
assert runtime.ProtocolRuntime.__module__ == 'agent_gateway.routing.protocol_runtime'
config = legacy.GatewayServerConfig(path='/cli', channel_id='tui')
assert isinstance(config.routes['/cli'], runtime.RouteConfig)
assert legacy._build_event_frame is runtime._build_event_frame
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=_isolated_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("first_module", ["protocol_runtime", "gateway_server"])
def test_gateway_runtime_cold_import_does_not_run_entrypoint(
    tmp_path: Path,
    first_module: str,
) -> None:
    dotenv = tmp_path / "import-probe.env"
    dotenv.write_text("P2B_DOTENV_PROBE=loaded\n", encoding="utf-8")
    script = f"from agent_gateway.routing import {first_module}\n" + """
import os
import sys
from pathlib import Path
from agent_gateway.routing import protocol_runtime
from agent_gateway.routing import gateway_server
assert gateway_server.GatewayServer is protocol_runtime.ProtocolRuntime
assert 'agent_gateway.app_gateway' not in sys.modules
assert 'agent_gateway.startup.bootstrap' not in sys.modules
assert 'agent_gateway.channel_manager.protocol.acp.acp_connect' not in sys.modules
assert 'uvicorn' not in sys.modules
assert os.environ.get('P2B_DOTENV_PROBE') is None
assert not Path(os.environ['JIUWENSWARM_DATA_DIR']).exists()
assert not Path(os.environ['JIUWENSWARM_HOME']).exists()
assert not Path(os.environ['AGENTOS_GATEWAY_LOG_DIR']).exists()
"""
    env = _isolated_env(tmp_path)
    env.pop("P2B_DOTENV_PROBE", None)
    result = subprocess.run(
        [sys.executable, "-c", script, "--dotenv", str(dotenv)],
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr


def test_channel_package_preserves_lazy_acp_exports(tmp_path: Path) -> None:
    script = """
import agent_gateway.channel_manager as channels
from agent_gateway.channel_manager import AcpChannel, AcpChannelConfig
from agent_gateway.channel_manager.protocol.acp.acp_connect import (
    AcpChannel as DirectChannel, AcpChannelConfig as DirectConfig,
)
assert AcpChannel is DirectChannel
assert AcpChannelConfig is DirectConfig
assert channels.AcpChannel is DirectChannel
assert channels.AcpChannelConfig is DirectConfig
exports = {}
exec('from agent_gateway.channel_manager import *', exports)
assert exports['AcpChannel'] is DirectChannel
assert exports['AcpChannelConfig'] is DirectConfig
try:
    channels.missing_export
except AttributeError:
    pass
else:
    raise AssertionError('missing exports must raise AttributeError')
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        env=_isolated_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr


async def test_extracted_runtime_handles_live_requests_and_disconnect() -> None:
    from agent_gateway.routing.protocol_runtime import ProtocolRuntime, ProtocolRuntimeConfig

    disconnected = asyncio.Event()
    server = ProtocolRuntime(
        ProtocolRuntimeConfig(host="127.0.0.1", port=0, path="/cli", channel_id="tui"),
        RobotMessageRouter(),
    )

    async def echo(
        ws: object,
        req_id: str,
        params: dict,
        session_id: str,
        user_id: str | None = None,
    ) -> None:
        await server.send_response(ws, req_id, ok=True, payload={"user_id": user_id, "session_id": session_id, "text": params["text"]})

    async def on_disconnect(ws: object, message: object) -> None:
        disconnected.set()

    server.register_local_handler("/cli", "probe.echo", echo)
    server.on_disconnect(on_disconnect)
    try:
        await asyncio.wait_for(server.start(), 5)
        host, port = server._listener.bound_addresses[0]
        async with connect(f"ws://{host}:{port}/cli?user_id=alice") as ws:
            ack = json.loads(await asyncio.wait_for(ws.recv(), 5))
            assert ack == {"type": "event", "event": "connection.ack", "payload": {"protocol_version": "1.0", "transport": "tui"}}
            await ws.send("not json")
            invalid = json.loads(await asyncio.wait_for(ws.recv(), 5))
            assert invalid == {"type": "res", "id": "", "ok": False, "error": "invalid json"}
            await ws.send(json.dumps({"type": "req", "id": "echo-1", "method": "probe.echo", "params": {"session_id": "session-1", "text": "hello"}}))
            response = json.loads(await asyncio.wait_for(ws.recv(), 5))
            assert response == {"type": "res", "id": "echo-1", "ok": True, "payload": {"user_id": "alice", "session_id": "session-1", "text": "hello"}}
        await asyncio.wait_for(disconnected.wait(), 5)
        assert not server._clients
        assert not server._request_to_client
        assert not server._session_to_client
        assert not server._pending_session_clients
        await server.stop()
        rebound = await asyncio.start_server(lambda reader, writer: writer.close(), host, port)
        rebound.close()
        await rebound.wait_closed()
    finally:
        await server.stop()


async def test_extracted_runtime_keeps_forward_response_on_original_connection() -> None:
    from agent_gateway.routing.protocol_runtime import ProtocolRuntime, ProtocolRuntimeConfig, RouteConfig

    forwarded = asyncio.Queue()
    server = ProtocolRuntime(
        ProtocolRuntimeConfig(host="127.0.0.1", port=0, routes={
            "/cli": RouteConfig(path="/cli", channel_id="tui", forward_methods=frozenset({"chat.send"}), forward_no_local_handler_methods=frozenset({"chat.send"})),
        }),
        RobotMessageRouter(),
    )
    server.on_message(forwarded.put)
    try:
        await asyncio.wait_for(server.start(), 5)
        host, port = server._listener.bound_addresses[0]
        async with connect(f"ws://{host}:{port}/cli", extra_headers={"X-User-Id": "alice"}) as ws:
            await asyncio.wait_for(ws.recv(), 5)
            await ws.send(json.dumps({"type": "req", "id": "chat-1", "method": "chat.send", "params": {"session_id": "session-1", "query": "hello"}}))
            msg = await asyncio.wait_for(forwarded.get(), 5)
            assert msg.id.startswith("tui-forward-")
            assert (msg.session_id, msg.channel_id, msg.user_id) == ("session-1", "tui", "alice")
            assert msg.params["query"] == "hello"
            from e2a.message import Message

            await server.send(Message(id=msg.id, type="res", channel_id="tui", session_id="session-1", params={}, timestamp=0.0, ok=True, payload={"content": "reply"}))
            response = json.loads(await asyncio.wait_for(ws.recv(), 5))
            assert response == {"type": "res", "id": "chat-1", "ok": True, "payload": {"content": "reply"}}
    finally:
        await server.stop()


async def test_extracted_runtime_keeps_upgrade_auth_rejection() -> None:
    from agent_gateway.routing.protocol_runtime import ProtocolRuntime, ProtocolRuntimeConfig

    server = ProtocolRuntime(
        ProtocolRuntimeConfig(host="127.0.0.1", port=0, path="/cli", channel_id="tui"),
        RobotMessageRouter(),
    )

    def reject(*, path: str, headers: object, remote: str, channel: str) -> SimpleNamespace:
        return SimpleNamespace(success=False)

    from agent_gateway.channel_manager.tui.tui_channel import TuiChannel, TuiChannelConfig

    channel = TuiChannel(TuiChannelConfig(enabled=True), RobotMessageRouter())
    channel.set_handshake_auth(reject)
    server.config.routes["/cli"].ws_channel = channel
    try:
        await asyncio.wait_for(server.start(), 5)
        host, port = server._listener.bound_addresses[0]
        with pytest.raises(InvalidStatusCode) as exc:
            async with connect(f"ws://{host}:{port}/cli"):
                pytest.fail("unauthorized connection upgraded")
        assert exc.value.status_code == 401
        assert not server._clients
    finally:
        await server.stop()
