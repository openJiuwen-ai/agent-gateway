# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Exercise multimodal write policy through the shared Web dispatch boundary."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from e2a.message import Message, ReqMethod

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web.app_web_handlers import WebHandlersBindParams, _register_web_handlers
from agent_gateway.channel_manager.web.invoke import dispatch_web_request
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
from agent_gateway.channel_manager.web.web_http_dispatch import dispatch_http_request
from agent_gateway.routing.agent_client import WebSocketAgentServerClient


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["ws", "http"])
@pytest.mark.parametrize("enterprise", [False, True])
@pytest.mark.parametrize("method,write", [
    ("multimodal.models.list", False),
    ("multimodal.models.get", False),
    ("multimodal.models.upsert", True),
    ("multimodal.models.delete", True),
    ("multimodal.tools.update", True),
    ("multimodal.config.apply", True),
])
async def test_multimodal_dispatch_respects_enterprise_write_policy(monkeypatch, transport, enterprise, method, write):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise" if enterprise else "personal")
    client = WebSocketAgentServerClient()
    client._apply_connection_ack({"payload": {"readiness": "AGENT_READY"}})
    send = AsyncMock(return_value=SimpleNamespace(ok=True, payload={"forwarded": True}))
    monkeypatch.setattr(client, "send_request", send)
    channel = WebChannel(WebChannelConfig(host="127.0.0.1", port=0), RobotMessageRouter(), client)
    _register_web_handlers(WebHandlersBindParams(channel=channel, agent_client=client))
    response = AsyncMock()
    monkeypatch.setattr(channel, "send_response", response)
    callback = AsyncMock(return_value=False)
    channel.on_message(callback)

    if transport == "http":
        outbound, _, _ = await dispatch_http_request(
            channel, method=method, params={}, request_id="multimodal-policy",
            headers={"X-User-Id": "owner", "X-Bot-Id": "bot"}, client_host="127.0.0.1",
        )
        await channel.unregister_request_outbound(outbound)
    else:
        message = Message(
            id="multimodal-policy", type="req", channel_id="web", session_id="settings",
            params={}, timestamp=1.0, ok=True, req_method=ReqMethod(method),
            metadata={"routing": {"bot_id": "bot"}},
        )
        await dispatch_web_request(
            channel, method=method, params={}, request_id=message.id, session_id=message.session_id,
            outbound=SimpleNamespace(_web_connection_user_id="owner"), user_message=message,
        )

    response.assert_awaited_once()
    result = response.await_args.kwargs
    if enterprise and write:
        assert result == {"ok": False, "error": "企业版配置由管理面统一下发", "code": "FORBIDDEN"}
        send.assert_not_awaited()
        callback.assert_not_awaited()
    else:
        assert result["ok"] is True
        assert result["payload"] == {"forwarded": True}
        send.assert_awaited_once()
        assert send.await_args.args[0].method == method
        callback.assert_awaited_once()
