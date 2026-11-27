"""ACP session/new snapshot release tests.

AcpChannel merges the params captured by ``session/new`` into every later
``session/prompt`` for that session. The channel is a long-lived object, so
the snapshot has to be dropped when the channel stops; otherwise the last
client's session params stay reachable for the lifetime of the process.
"""

import json

import pytest

from agent_gateway.channel_manager.protocol.acp import acp_connect
from agent_gateway.channel_manager.protocol.acp.acp_connect import (
    AcpChannel,
    AcpChannelConfig,
)


class _DummyBus:
    @staticmethod
    async def publish_user_messages(msg):
        return None


class _FakeStdoutBuffer:
    def __init__(self) -> None:
        self.parts: list[bytes] = []

    def write(self, data: bytes) -> None:
        self.parts.append(data)

    def flush(self) -> None:
        return None


class _FakeStdout:
    def __init__(self) -> None:
        self.buffer = _FakeStdoutBuffer()


async def _send_line(channel: AcpChannel, payload: dict) -> None:
    await channel._handle_raw_line(json.dumps(payload, ensure_ascii=False))


@pytest.mark.asyncio
async def test_channel_stop_releases_captured_session_new_context(monkeypatch):
    fake_stdout = _FakeStdout()
    monkeypatch.setattr(acp_connect, "_ACP_STDOUT", fake_stdout)
    channel = AcpChannel(AcpChannelConfig(enabled=True), _DummyBus())
    dispatched = []
    channel.on_message(dispatched.append)

    await _send_line(
        channel,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "session/new",
            "params": {
                "sessionId": "sess-ctx",
                "cwd": "/workspace/project",
                "mcpServers": [{"name": "repo", "command": "server"}],
            },
        },
    )
    await _send_line(
        channel,
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/prompt",
            "params": {
                "sessionId": "sess-ctx",
                "prompt": [{"type": "text", "text": "before stop"}],
            },
        },
    )
    assert dispatched[-1].params["cwd"] == "/workspace/project"
    assert channel._session_ctx["sess-ctx"]["cwd"] == "/workspace/project"

    await channel.stop()

    assert channel._session_ctx == {}
    await _send_line(
        channel,
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/prompt",
            "params": {
                "sessionId": "sess-ctx",
                "prompt": [{"type": "text", "text": "after stop"}],
            },
        },
    )
    assert "cwd" not in dispatched[-1].params
    assert "mcpServers" not in dispatched[-1].params
