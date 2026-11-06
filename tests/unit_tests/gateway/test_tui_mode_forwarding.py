"""Mode requests remain usable in the standalone Gateway process."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest


def test_standalone_import_registers_mode_methods_without_swarm() -> None:
    root = Path(__file__).resolve().parents[3]
    script = (
        "import sys\n"
        f"sys.path.insert(0, {str(root)!r})\n"
        "import agent_gateway\n"
        "from openjiuwen_protocol.message import ReqMethod\n"
        "assert ReqMethod('mode.set') is ReqMethod.MODE_SET\n"
        "assert ReqMethod('mode.get') is ReqMethod.MODE_GET\n"
        "assert not any(name == 'jiuwenswarm' or name.startswith('jiuwenswarm.') for name in sys.modules)\n"
    )
    subprocess.run([sys.executable, "-I", "-c", script], check=True, capture_output=True, text=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["mode.set", "mode.get"])
async def test_tui_mode_frame_forwards_without_local_ack(method: str) -> None:
    from agent_gateway.app_gateway import GatewayServer, GatewayServerConfig, RouteConfig
    from agent_gateway.channel_manager.tui.tui_connect import CliRouteBindParams, build_cli_route_binding
    from openjiuwen_protocol.message import ReqMethod

    binding = build_cli_route_binding(CliRouteBindParams(path="/tui"))
    route = RouteConfig(
        path=binding.path,
        channel_id=binding.channel_id,
        forward_methods=binding.forward_methods,
        forward_no_local_handler_methods=binding.forward_no_local_handler_methods,
    )
    server = GatewayServer(GatewayServerConfig(routes={"/tui": route}), object())
    forwarded = []
    server.on_message(forwarded.append)

    class Socket:
        closed = False
        _gateway_user_id = "user-1"

        def __init__(self) -> None:
            self.sent = []

        async def send(self, data: str) -> None:
            self.sent.append(json.loads(data))

    socket = Socket()
    params = {"session_id": "saved-session", "mode": "team.work.normal", "expected_mode": "agent.code.normal"}
    await server._handle_raw_message(
        socket, json.dumps({"type": "req", "id": "mode-1", "method": method, "params": params}),
        "/tui", route,
    )
    assert len(forwarded) == 1
    assert forwarded[0].req_method is ReqMethod(method)
    assert forwarded[0].session_id == "saved-session"
    assert {key: forwarded[0].params[key] for key in params} == params
    assert forwarded[0].params["agent_type"] == "jiuwenswarm"
    assert socket.sent == []
