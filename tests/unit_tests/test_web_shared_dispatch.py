# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.channel_manager.web import invoke


@pytest.fixture
def channel(monkeypatch):
    monkeypatch.setattr(invoke, "is_enterprise_write_forbidden", lambda method: False)
    return SimpleNamespace(
        _method_handlers={},
        _local_only_methods=set(),
        _on_message_cb=None,
        bus=SimpleNamespace(publish_user_messages=AsyncMock()),
        send_response=AsyncMock(),
        invoke_method_handler=AsyncMock(return_value=True),
    )


async def dispatch(channel, method, websocket):
    await invoke.dispatch_web_message(
        channel, method=method, params={}, request_id="request",
        outbound=object(), session_id="session",
        user_message=SimpleNamespace(metadata={}), websocket=websocket,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket", [False, True])
async def test_callback_and_local_handler_dispatch(channel, websocket):
    channel._method_handlers["custom.method"] = object()
    channel._on_message_cb = AsyncMock(return_value=True)
    await dispatch(channel, "custom.method", websocket)
    channel._on_message_cb.assert_awaited_once()
    channel.invoke_method_handler.assert_not_awaited()

    channel._on_message_cb.return_value = False
    await dispatch(channel, "custom.method", websocket)
    channel.invoke_method_handler.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket", [False, True])
async def test_local_only_and_unknown_method(channel, websocket):
    channel._local_only_methods.add("custom.local")
    channel._method_handlers["custom.local"] = object()
    channel._on_message_cb = AsyncMock(return_value=True)
    await dispatch(channel, "custom.local", websocket)
    channel.invoke_method_handler.assert_awaited_once()
    channel._on_message_cb.assert_not_awaited()

    channel._on_message_cb.return_value = False
    await dispatch(channel, "custom.unknown", websocket)
    assert channel.send_response.await_args.kwargs["code"] == "METHOD_NOT_FOUND"


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket", [False, True])
async def test_handler_before_callback_and_bus_fallback(channel, websocket):
    channel._method_handlers["chat.send"] = object()
    await dispatch(channel, "chat.send", websocket)
    channel.invoke_method_handler.assert_awaited_once()
    channel.bus.publish_user_messages.assert_awaited_once()
    channel.send_response.assert_not_awaited()

    channel.invoke_method_handler.return_value = False
    await dispatch(channel, "chat.send", websocket)
    assert channel.bus.publish_user_messages.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("websocket", [False, True])
async def test_enterprise_forbidden_precedes_dispatch(channel, websocket, monkeypatch):
    monkeypatch.setattr(invoke, "is_enterprise_write_forbidden", lambda method: True)
    channel._method_handlers["config.set"] = object()
    await dispatch(channel, "config.set", websocket)
    assert channel.send_response.await_args.kwargs["code"] == "FORBIDDEN"
    channel.invoke_method_handler.assert_not_awaited()
    channel.bus.publish_user_messages.assert_not_awaited()
