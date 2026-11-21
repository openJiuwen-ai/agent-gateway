# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""HTTP entry forwards agent display identity into message metadata."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web import web_http_dispatch as dispatch_mod
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig


def test_http_dispatch_injects_agent_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dispatch_mod, "is_enterprise", lambda: True)
    captured: dict[str, Any] = {}
    real_message = dispatch_mod.Message

    def _capture_message(**kwargs: Any):
        captured.update(kwargs)
        return real_message(**kwargs)

    async def _noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(dispatch_mod, "Message", _capture_message)
    monkeypatch.setattr(dispatch_mod, "invoke_web_request", _noop)

    channel = WebChannel(WebChannelConfig(host="127.0.0.1", port=0), RobotMessageRouter())

    async def _run() -> None:
        outbound, _rid, _sid = await dispatch_mod.dispatch_http_request(
            channel,
            method="chat.send",
            params={"content": "hi"},
            headers={
                "X-Request-Id": "req-1",
                "X-User-Id": "u",
                "X-Group-Id": "g",
                "X-Bot-Id": "b",
                "X-Agent-Name": "agent_svc_agent",
                "X-Agent-Identity-Id": "svc_agent",
                "X-Runtime-Template-Id": "svc",
                "X-Agent-Template-Id": "agent",
            },
            client_host="localhost",
        )
        await channel.unregister_request_outbound(outbound)

    asyncio.run(_run())

    assert captured["metadata"]["agent_identity"] == {
        "agent_name": "agent_svc_agent",
        "agent_id": "svc_agent",
        "runtime_template_id": "svc",
        "agent_template_id": "agent",
    }
