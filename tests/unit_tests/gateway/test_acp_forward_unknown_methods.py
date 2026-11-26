# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""ACP 通道只做“是不是我的、要不要拦”，其余方法按不透明字符串透传。"""

from __future__ import annotations

import dataclasses
import json
import time
from types import SimpleNamespace

import pytest
from e2a.message import Message, ReqMethod

from agent_gateway.app_gateway import _normalize_gateway_message
from agent_gateway.message_handler.message_handler import MessageHandler
from tests.unit_tests.gateway.test_app_gateway_acp import (
    FakeWebSocket,
    build_server,
)


def _request_frame(method: str, *, request_id: str = "req-opaque") -> str:
    return json.dumps(
        {
            "type": "req",
            "id": request_id,
            "method": method,
            "params": {"session_id": "sess-opaque"},
        },
        ensure_ascii=False,
    )


@pytest.mark.asyncio
async def test_acp_unknown_method_is_forwarded_as_opaque_string(monkeypatch):
    monkeypatch.delenv("AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS", raising=False)
    server = build_server()
    ws = FakeWebSocket()
    seen = []

    async def on_message(msg):
        seen.append(msg)

    server.on_message(on_message)

    await server.handle_raw_message_public(ws, _request_frame("team.run"))

    assert ws.sent_frames == []
    assert len(seen) == 1
    assert seen[0].channel_id == "acp"
    assert seen[0].req_method == "team.run"
    assert isinstance(seen[0].req_method, str)


@pytest.mark.asyncio
async def test_acp_known_method_still_reaches_message_handler(monkeypatch):
    monkeypatch.delenv("AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS", raising=False)
    server = build_server()
    ws = FakeWebSocket()
    seen = []

    async def on_message(msg):
        seen.append(msg)

    server.on_message(on_message)

    await server.handle_raw_message_public(ws, _request_frame("chat.send"))

    assert ws.sent_frames == []
    assert len(seen) == 1
    assert seen[0].req_method == ReqMethod.CHAT_SEND


@pytest.mark.asyncio
async def test_acp_denied_prefix_is_rejected(monkeypatch):
    monkeypatch.delenv("AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS", raising=False)
    server = build_server()
    ws = FakeWebSocket()
    seen = []

    async def on_message(msg):
        seen.append(msg)

    server.on_message(on_message)

    await server.handle_raw_message_public(ws, _request_frame("gateway.internal.ping"))

    assert seen == []
    assert len(ws.sent_frames) == 1
    frame = ws.sent_frames[0]
    assert frame["ok"] is False
    assert frame["code"] == "METHOD_NOT_ALLOWED"
    assert "gateway.internal.ping" in frame["error"]


@pytest.mark.asyncio
async def test_acp_enterprise_write_is_rejected_on_passthrough(monkeypatch):
    monkeypatch.delenv("AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS", raising=False)
    from agent_gateway.channel_manager.web import invoke as invoke_module

    monkeypatch.setattr(invoke_module, "is_enterprise", lambda: True)
    server = build_server()
    ws = FakeWebSocket()
    seen = []

    async def on_message(msg):
        seen.append(msg)

    server.on_message(on_message)

    await server.handle_raw_message_public(ws, _request_frame("agents.create"))

    assert seen == []
    assert len(ws.sent_frames) == 1
    frame = ws.sent_frames[0]
    assert frame["ok"] is False
    assert frame["code"] == "FORBIDDEN"
    assert frame["error"] == "企业版配置由管理面统一下发"


@pytest.mark.asyncio
async def test_acp_enterprise_write_is_rejected_for_registered_method(monkeypatch):
    """已登记的转发方法同样要过企业版禁写，不能只在透传分支判定。

    ``build_server`` 的 /acp 路由只登记了 chat/history 两个方法，覆盖不到生产
    集合；这里换成 ``_FORWARD_REQ_METHODS``，确认 ``agents.create`` 这类
    ``ROUTE_FORWARD`` 方法也被拦住。
    """
    monkeypatch.delenv("AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS", raising=False)
    from agent_gateway.channel_manager.web import invoke as invoke_module
    from agent_gateway.channel_manager.web.app_web_handlers import _FORWARD_REQ_METHODS

    assert "agents.create" in _FORWARD_REQ_METHODS
    monkeypatch.setattr(invoke_module, "is_enterprise", lambda: True)
    server = build_server()
    server.config.routes["/acp"] = dataclasses.replace(
        server.config.routes["/acp"], forward_methods=_FORWARD_REQ_METHODS,
    )
    ws = FakeWebSocket()
    seen = []

    async def on_message(msg):
        seen.append(msg)

    server.on_message(on_message)

    await server.handle_raw_message_public(ws, _request_frame("agents.create"))

    assert seen == []
    assert len(ws.sent_frames) == 1
    frame = ws.sent_frames[0]
    assert frame["ok"] is False
    assert frame["code"] == "FORBIDDEN"
    assert frame["error"] == "企业版配置由管理面统一下发"


@pytest.mark.asyncio
async def test_acp_passthrough_switch_off_restores_unknown_method_error(monkeypatch):
    monkeypatch.setenv("AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS", "0")
    server = build_server()
    ws = FakeWebSocket()
    seen = []

    async def on_message(msg):
        seen.append(msg)

    server.on_message(on_message)

    await server.handle_raw_message_public(ws, _request_frame("team.run"))

    assert seen == []
    assert len(ws.sent_frames) == 1
    assert ws.sent_frames[0]["error"] == "unknown method: team.run"


@pytest.mark.asyncio
async def test_tui_unknown_method_is_still_rejected(monkeypatch):
    monkeypatch.delenv("AGENT_GATEWAY_FORWARD_UNKNOWN_METHODS", raising=False)
    server = build_server()
    ws = FakeWebSocket()
    seen = []

    async def on_message(msg):
        seen.append(msg)

    server.on_message(on_message)

    await server.handle_raw_message_public(ws, _request_frame("team.run"), path="/tui")

    assert seen == []
    assert len(ws.sent_frames) == 1
    assert ws.sent_frames[0]["error"] == "unknown method: team.run"


def test_normalize_gateway_message_accepts_opaque_string_method():
    msg = Message(
        id="req-opaque",
        type="req",
        channel_id="acp",
        session_id="sess-opaque",
        params={"session_id": "sess-opaque"},
        timestamp=time.time(),
        ok=True,
        req_method="team.run",
    )

    normalized = _normalize_gateway_message(msg)

    assert normalized.req_method == "team.run"
    assert normalized.is_stream is False


@pytest.mark.asyncio
async def test_acp_opaque_method_survives_dispatch_into_e2a_envelope():
    handler = MessageHandler(SimpleNamespace())
    handler._acp_session_aliases["sess-opaque"] = "internal-sess"
    msg = Message(
        id="req-opaque",
        type="req",
        channel_id="acp",
        session_id="sess-opaque",
        params={"session_id": "sess-opaque"},
        timestamp=time.time(),
        ok=True,
        req_method="team.run",
    )

    agent_msg = await handler._prepare_agent_dispatch_message(msg)
    env = handler.message_to_e2a(agent_msg)

    # 方法名对网关是不透明字符串：原样进信封、原样上线。
    assert env.method == "team.run"
    assert env.to_dict()["method"] == "team.run"
    assert env.channel == "acp"
    # ACP 会话别名仍然生效，透传不影响既有 session 重映射。
    assert env.params["session_id"] == "internal-sess"
    # 非 chat 方法走后台 RPC 分支，不占聊天串行队列。
    assert MessageHandler._non_stream_rpc_may_run_parallel(env) is True
