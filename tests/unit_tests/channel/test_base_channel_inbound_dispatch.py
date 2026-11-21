# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from typing import Any

import pytest

from e2a.message import Message

from agent_gateway.channel_manager.base import BaseChannel, RobotMessageRouter


class _TestChannel(BaseChannel):
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


def _message() -> Message:
    return Message(
        id="inbound-1",
        type="req",
        channel_id="test",
        session_id="session-1",
        params={"content": "hello"},
        timestamp=0,
        ok=True,
    )


@pytest.mark.asyncio
async def test_dispatch_uses_sync_callback_without_router_fallback() -> None:
    router = RobotMessageRouter()
    channel = _TestChannel({}, router)
    received: list[Message] = []

    await channel._dispatch_inbound_message(_message(), received.append)

    assert len(received) == 1
    assert router.pending_incoming_count == 0


@pytest.mark.asyncio
async def test_dispatch_awaits_async_callback_without_router_fallback() -> None:
    router = RobotMessageRouter()
    channel = _TestChannel({}, router)
    received: list[Message] = []

    async def callback(message: Message) -> None:
        received.append(message)

    await channel._dispatch_inbound_message(_message(), callback)

    assert len(received) == 1
    assert router.pending_incoming_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("callback_result", "should_enqueue"),
    [(None, True), (False, True), (True, False)],
)
async def test_dispatch_falls_back_when_callback_reports_unhandled(
    callback_result: Any, should_enqueue: bool
) -> None:
    router = RobotMessageRouter()
    channel = _TestChannel({}, router)
    message = _message()

    await channel._dispatch_inbound_message(
        message, lambda _: callback_result, fallback_on_unhandled=True
    )

    assert router.pending_incoming_count == int(should_enqueue)
    if should_enqueue:
        assert await router.wait_for_user_message() is message


@pytest.mark.asyncio
async def test_dispatch_falls_back_to_router_without_callback() -> None:
    router = RobotMessageRouter()
    channel = _TestChannel({}, router)
    message = _message()

    await channel._dispatch_inbound_message(message, None)

    assert await router.wait_for_user_message() is message


@pytest.mark.asyncio
async def test_dispatch_awaits_async_unhandled_callback_before_fallback() -> None:
    router = RobotMessageRouter()
    channel = _TestChannel({}, router)
    message = _message()
    callback_messages: list[Message] = []

    async def callback(inbound: Message) -> bool:
        callback_messages.append(inbound)
        return False

    await channel._dispatch_inbound_message(
        message, callback, fallback_on_unhandled=True
    )

    assert callback_messages == [message]
    assert await router.wait_for_user_message() is message
