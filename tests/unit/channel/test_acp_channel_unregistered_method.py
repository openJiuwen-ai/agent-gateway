"""未登记的方法名要从 ACP stdio 适配层原样透传出去。

方法枚举归 AgentServer；网关只回答"是不是我的、要不要拦"。适配层如果仍然
要求方法必须落在本地 ``ReqMethod`` 枚举里，那么 AgentServer 新增方法时，
ACP 客户端仍然要先等网关发版，回到方法清单耦合的老问题。
"""
from __future__ import annotations

import json
from typing import Any

import pytest

from agent_gateway.channel_manager.protocol.acp.acp_connect import (
    AcpChannel,
    AcpChannelConfig,
)


class _NoopBus:
    @staticmethod
    async def publish_user_messages(_msg: Any) -> None:
        return None


class _CapturingChannel(AcpChannel):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.dispatched: list[Any] = []

    async def _dispatch_message(self, msg: Any) -> None:
        self.dispatched.append(msg)


class _FakeWs:
    def __init__(self) -> None:
        self.frames: list[dict[str, Any]] = []

    async def send(self, payload: str) -> None:
        self.frames.append(json.loads(payload))


def _build_channel() -> _CapturingChannel:
    return _CapturingChannel(
        AcpChannelConfig(
            enabled=True,
            channel_id="acp",
            default_session_id="acp_cli_session",
            metadata={},
        ),
        router=_NoopBus(),
        gateway_url=None,
    )


def _envelope(method: str, *, request_id: str = "req-unknown") -> str:
    return json.dumps({
        "protocol_version": "1.0",
        "request_id": request_id,
        "method": method,
        "params": {"session_id": "sess-1"},
        "channel": "acp",
        "session_id": "sess-1",
        "is_stream": False,
        "jsonrpc_id": request_id,
    })


@pytest.mark.asyncio
async def test_unregistered_method_keeps_raw_string() -> None:
    channel = _build_channel()

    await channel._handle_raw_line(_envelope("team.brand.new"))

    assert len(channel.dispatched) == 1
    msg = channel.dispatched[0]
    assert msg.req_method == "team.brand.new"
    assert msg.metadata["acp"]["method"] == "team.brand.new"


@pytest.mark.asyncio
async def test_unregistered_method_is_forwarded_verbatim(monkeypatch: pytest.MonkeyPatch) -> None:
    channel = _build_channel()
    state = _FakeWs()
    monkeypatch.setattr(channel, "_ensure_gateway_connection", _noop_connect)
    channel._gateway_ws = state

    await channel._handle_raw_line(_envelope("team.brand.new"))
    assert channel.dispatched, "envelope must reach dispatch"
    await channel._send_to_gateway(channel.dispatched[0])

    assert state.frames == [{
        "type": "req",
        "id": "req-unknown",
        "method": "team.brand.new",
        # mode 来自 Message 的默认值，与本次改动无关，这里保持整帧断言。
        "params": {"session_id": "sess-1", "mode": "agent.work.normal"},
    }]


@pytest.mark.asyncio
async def test_empty_method_is_still_rejected() -> None:
    channel = _build_channel()

    with pytest.raises(ValueError):
        await channel._handle_raw_line(_envelope(""))

    assert channel.dispatched == []


async def _noop_connect() -> None:
    return None
