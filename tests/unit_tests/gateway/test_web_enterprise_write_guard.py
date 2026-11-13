# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig


@pytest.mark.asyncio
async def test_ws_rejects_enterprise_write_before_local_handler_lookup(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.send_response = AsyncMock()
    callback = AsyncMock(return_value=True)
    channel.on_message(callback)

    ws = SimpleNamespace()
    await channel._handle_request_message(
        ws,
        {"is_stream": False},
        {},
        "req-forbidden",
        "plugins.install",
        {},
    )

    channel.send_response.assert_awaited_once_with(
        ws,
        "req-forbidden",
        ok=False,
        error="企业版配置由管理面统一下发",
        code="FORBIDDEN",
    )
    callback.assert_not_awaited()
