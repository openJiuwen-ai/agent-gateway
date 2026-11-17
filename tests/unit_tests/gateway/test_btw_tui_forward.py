# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""TUI forwards unregistered methods and preserves registered local handlers."""

import json
from unittest.mock import AsyncMock

import pytest

try:  # pysbd 0.3.4 (latest on PyPI) ships invalid escape sequences that are a
      # SyntaxError on newer interpreters; skip collection instead of reddening UT.
    import pysbd  # noqa: F401
except Exception:  # noqa: BLE001
    pytest.skip("pysbd unusable on this interpreter (0.3.4 invalid escapes)",
                allow_module_level=True)

from tests.unit_tests.gateway.test_app_gateway_acp import build_server, FakeWebSocket


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["command.btw", "command.compact", "future.capability.execute"])
@pytest.mark.parametrize("stream", [False, True])
async def test_tui_default_forward(method, stream):
    server = build_server()
    ws = FakeWebSocket()
    upstream = AsyncMock()
    server.on_message(upstream)
    params = {"session_id": "s1", "content": {"nested": [False, None, 1]}}
    await server.handle_raw_message_public(ws, json.dumps({
        "type": "req", "id": "r1", "method": method, "params": params, "is_stream": stream,
    }), path="/tui")
    upstream.assert_awaited_once()
    message = upstream.await_args.args[0]
    assert type(message.req_method) is str
    assert message.req_method == method
    assert message.params == params
    assert message.is_stream is stream
    assert not ws.sent_frames


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["tui.disconnect", "plugin.local.action"])
async def test_tui_registered_handler_stays_local(method):
    server = build_server()
    local = AsyncMock()
    upstream = AsyncMock()
    server.register_local_handler("/tui", method, local)
    server.on_message(upstream)
    await server.handle_raw_message_public(FakeWebSocket(), json.dumps({
        "type": "req", "id": "local", "method": method, "params": {},
    }), path="/tui")
    local.assert_awaited_once()
    upstream.assert_not_awaited()


@pytest.mark.asyncio
async def test_tui_missing_transport_reports_unavailable():
    server = build_server()
    ws = FakeWebSocket()
    await server.handle_raw_message_public(ws, json.dumps({
        "type": "req", "id": "offline", "method": "future.execute", "params": {},
    }), path="/tui")
    assert len(ws.sent_frames) == 1
    assert ws.sent_frames[0]["code"] == "SERVICE_UNAVAILABLE"
