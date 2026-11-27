"""ACP session catalogue lifecycle tests.

The gateway bridge is shared by every websocket connection, and the stdio
channel outlives a single client. Session ids therefore have to be scoped to
the connection that created them and dropped when that connection ends, or a
closed IDE session stays visible to other clients and pinned in memory for the
lifetime of the process.
"""

import json

import pytest

from agent_gateway.channel_manager.protocol.acp import acp_connect
from agent_gateway.channel_manager.protocol.acp.acp_connect import (
    AcpChannel,
    AcpChannelConfig,
    AcpGatewayBridge,
)


class _DummyBus:
    @staticmethod
    async def publish_user_messages(msg):
        return None


class _FakeWebSocket:
    def __init__(self) -> None:
        self.sent_frames: list[dict] = []

    async def send(self, data: str) -> None:
        self.sent_frames.append(json.loads(data))


class _FakeStdoutBuffer:
    def __init__(self) -> None:
        self.parts: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.parts.append(data)

    def flush(self) -> None:
        return None

    def json_lines(self) -> list[dict]:
        raw = b"".join(self.parts).decode("utf-8")
        return [json.loads(line) for line in raw.splitlines() if line.strip()]


class _FakeStdout:
    def __init__(self) -> None:
        self.buffer = _FakeStdoutBuffer()


async def _request(
    bridge: AcpGatewayBridge,
    ws: _FakeWebSocket,
    rpc_id: int,
    method: str,
    params: dict,
) -> dict:
    await bridge.handle_jsonrpc_request(
        ws,
        {"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params},
    )
    return ws.sent_frames[-1]


def _listed_sessions(frame: dict) -> list[dict]:
    return frame["result"]["sessions"]


@pytest.mark.asyncio
async def test_bridge_session_list_only_returns_sessions_of_the_calling_connection():
    bridge = AcpGatewayBridge(None)
    ws_a = _FakeWebSocket()
    ws_b = _FakeWebSocket()

    await _request(bridge, ws_a, 1, "session/new", {"sessionId": "sess-a"})
    await _request(bridge, ws_b, 1, "session/new", {"sessionId": "sess-b"})

    assert _listed_sessions(await _request(bridge, ws_a, 2, "session/list", {})) == [
        {"sessionId": "sess-a"}
    ]
    assert _listed_sessions(await _request(bridge, ws_b, 2, "session/list", {})) == [
        {"sessionId": "sess-b"}
    ]


@pytest.mark.asyncio
async def test_bridge_cleanup_releases_catalogue_of_the_disconnected_connection():
    bridge = AcpGatewayBridge(None)
    ws_a = _FakeWebSocket()
    ws_b = _FakeWebSocket()

    await _request(bridge, ws_a, 1, "session/new", {"sessionId": "sess-a"})
    await _request(bridge, ws_b, 1, "session/new", {"sessionId": "sess-b"})

    bridge.cleanup(ws_a)

    assert _listed_sessions(await _request(bridge, ws_a, 2, "session/list", {})) == []
    assert _listed_sessions(await _request(bridge, ws_b, 2, "session/list", {})) == [
        {"sessionId": "sess-b"}
    ]
    assert set(bridge._known_sessions_by_ws) == {id(ws_b)}


@pytest.mark.asyncio
async def test_bridge_prompt_after_disconnect_rebinds_the_session():
    bridge = AcpGatewayBridge(None)
    ws = _FakeWebSocket()

    await _request(bridge, ws, 1, "session/new", {"sessionId": "sess-a"})
    bridge.cleanup(ws)
    await _request(
        bridge,
        ws,
        2,
        "session/prompt",
        {
            "sessionId": "sess-a",
            "prompt": [{"type": "text", "text": "resume"}],
        },
    )

    assert _listed_sessions(await _request(bridge, ws, 3, "session/list", {})) == [
        {"sessionId": "sess-a"}
    ]


@pytest.mark.asyncio
async def test_channel_stop_releases_known_sessions_and_active_prompts(monkeypatch):
    fake_stdout = _FakeStdout()
    monkeypatch.setattr(acp_connect, "_ACP_STDOUT", fake_stdout)
    channel = AcpChannel(AcpChannelConfig(enabled=True), _DummyBus())

    await channel._handle_raw_line(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "session/new",
                "params": {"sessionId": "sess-stale"},
            }
        )
    )
    await channel._handle_raw_line(
        json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session/prompt",
                "params": {
                    "sessionId": "sess-active",
                    "prompt": [{"type": "text", "text": "hello"}],
                },
            }
        )
    )
    assert channel._known_sessions == {"sess-stale", "sess-active"}
    assert set(channel._active_prompt_request_by_session) == {"sess-active"}

    await channel.stop()

    assert channel._known_sessions == set()
    assert channel._active_prompt_request_by_session == {}
    await channel._handle_raw_line(
        json.dumps({"jsonrpc": "2.0", "id": 3, "method": "session/list", "params": {}})
    )
    assert fake_stdout.buffer.json_lines()[-1] == {
        "jsonrpc": "2.0",
        "id": 3,
        "result": {"sessions": []},
    }
