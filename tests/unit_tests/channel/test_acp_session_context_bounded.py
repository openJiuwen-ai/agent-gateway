# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""ACP ``session/new`` 上下文快照必须有界。

``AcpChannel._session_ctx`` 按 sessionId 缓存 ``session/new`` 的完整参数
（cwd、mcpServers 等），供后续 ``session/prompt`` 补全缺省字段。缓存原本
只写不删，长驻网关里每个新会话都会留下约 1~2 KB，会话数增长即线性上涨。
"""
from __future__ import annotations

from typing import Any

import pytest

from agent_gateway.channel_manager.protocol.acp import acp_connect
from agent_gateway.channel_manager.protocol.acp.acp_connect import (
    AcpChannel,
    AcpChannelConfig,
)


class _NoopBus:
    @staticmethod
    async def publish_user_messages(_msg: Any) -> None:
        return None


class _Recorder(AcpChannel):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.dispatched: list[Any] = []
        self.results: list[tuple[Any, dict[str, Any]]] = []

    async def _dispatch_message(self, msg: Any) -> None:
        self.dispatched.append(msg)

    async def _write_jsonrpc_result(self, rpc_id: Any, result: dict[str, Any]) -> None:
        self.results.append((rpc_id, result))


def _build_channel() -> _Recorder:
    return _Recorder(
        AcpChannelConfig(
            enabled=True,
            channel_id="acp",
            default_session_id="acp_cli_session",
        ),
        _NoopBus(),
    )


def _params(session_id: str) -> dict[str, Any]:
    return {
        "sessionId": session_id,
        "cwd": "D:/workspace/demo",
        "mcpServers": [
            {
                "name": "filesystem",
                "command": "npx",
                "args": [
                    "-y",
                    "@modelcontextprotocol/server-filesystem",
                    "D:/workspace",
                ],
                "env": {"TOKEN": "t" * 64},
            }
        ],
    }


@pytest.mark.asyncio
async def test_session_context_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(acp_connect, "_SESSION_CTX_LIMIT", 3)
    channel = _build_channel()

    for index in range(5):
        await channel._handle_jsonrpc_session_new(index, _params(f"sess-{index}"))

    assert list(channel._session_ctx) == ["sess-2", "sess-3", "sess-4"]


@pytest.mark.asyncio
async def test_session_context_refreshes_recency_on_prompt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(acp_connect, "_SESSION_CTX_LIMIT", 3)
    channel = _build_channel()

    await channel._handle_jsonrpc_session_new(1, _params("sess-a"))
    await channel._handle_jsonrpc_session_new(2, _params("sess-b"))
    await channel._handle_jsonrpc_session_prompt(
        3,
        {"sessionId": "sess-a", "prompt": [{"type": "text", "text": "hi"}]},
    )
    await channel._handle_jsonrpc_session_new(4, _params("sess-c"))
    await channel._handle_jsonrpc_session_new(5, _params("sess-d"))

    assert list(channel._session_ctx) == ["sess-a", "sess-c", "sess-d"]


@pytest.mark.asyncio
async def test_prompt_merges_context_for_retained_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(acp_connect, "_SESSION_CTX_LIMIT", 2)
    channel = _build_channel()

    params = _params("sess-ctx")
    params["query"] = "hello"
    await channel._handle_jsonrpc_session_new(1, params)
    await channel._handle_jsonrpc_session_prompt(
        2,
        {"sessionId": "sess-ctx", "prompt": [{"type": "text", "text": "hi"}]},
    )

    assert channel.dispatched
    msg = channel.dispatched[-1]
    assert msg.params.get("cwd") == "D:/workspace/demo"
    assert msg.params.get("query") == "hello"
    assert msg.params.get("session_id") == "sess-ctx"
