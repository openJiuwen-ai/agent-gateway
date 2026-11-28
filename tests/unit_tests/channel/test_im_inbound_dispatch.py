from __future__ import annotations

import asyncio
import inspect
import types

import pytest
from e2a.message import Message

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.inbound_dispatch import (
    CallbackAwaitMode,
    dispatch_inbound_message,
)

pytestmark = pytest.mark.asyncio


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


async def test_dispatch_uses_sync_callback_without_bus_fallback() -> None:
    router = RobotMessageRouter()
    message = _message()
    received: list[Message] = []

    handled = await dispatch_inbound_message(
        message,
        router=router,
        callback=received.append,
    )

    assert handled is True
    assert received == [message]
    assert router.pending_incoming_count == 0


async def test_dispatch_awaits_async_callback_without_bus_fallback() -> None:
    router = RobotMessageRouter()
    message = _message()
    received: list[Message] = []

    async def callback(value: Message) -> None:
        received.append(value)

    handled = await dispatch_inbound_message(
        message,
        router=router,
        callback=callback,
    )

    assert handled is True
    assert received == [message]
    assert router.pending_incoming_count == 0


async def test_dispatch_routes_to_bus_without_callback() -> None:
    router = RobotMessageRouter()
    message = _message()

    handled = await dispatch_inbound_message(
        message,
        router=router,
        callback=None,
    )

    assert handled is False
    assert router.pending_incoming_count == 1
    assert await router.wait_for_user_message() is message


async def test_dispatch_can_fall_back_when_callback_declines_message() -> None:
    router = RobotMessageRouter()
    message = _message()

    handled = await dispatch_inbound_message(
        message,
        router=router,
        callback=lambda _: False,
        fallback_on_false=True,
    )

    assert handled is False
    assert router.pending_incoming_count == 1
    assert await router.wait_for_user_message() is message


async def test_dispatch_can_fall_back_when_async_callback_declines_message() -> None:
    router = RobotMessageRouter()
    message = _message()

    async def callback(_: Message) -> bool:
        return False

    handled = await dispatch_inbound_message(
        message,
        router=router,
        callback=callback,
        fallback_on_false=True,
    )

    assert handled is False
    assert router.pending_incoming_count == 1
    assert await router.wait_for_user_message() is message


async def test_dispatch_keeps_message_when_callback_accepts_it() -> None:
    router = RobotMessageRouter()
    message = _message()

    handled = await dispatch_inbound_message(
        message,
        router=router,
        callback=lambda _: True,
        fallback_on_false=True,
    )

    assert handled is True
    assert router.pending_incoming_count == 0


async def test_dispatch_can_preserve_sync_callback_behavior() -> None:
    router = RobotMessageRouter()
    callback_messages: list[Message] = []

    def callback(message: Message) -> None:
        callback_messages.append(message)

    message = _message()
    await dispatch_inbound_message(
        message,
        router=router,
        callback=callback,
        await_mode=CallbackAwaitMode.NONE,
    )

    assert callback_messages == [message]


async def test_dispatch_none_mode_does_not_await_callback_result() -> None:
    router = RobotMessageRouter()
    callback_result = asyncio.sleep(0)

    await dispatch_inbound_message(
        _message(),
        router=router,
        callback=lambda _: callback_result,
        await_mode=CallbackAwaitMode.NONE,
    )

    assert inspect.getcoroutinestate(callback_result) == inspect.CORO_CREATED
    callback_result.close()


async def test_dispatch_coroutine_mode_awaits_coroutine() -> None:
    router = RobotMessageRouter()
    callback_completed = False

    async def callback(_: Message) -> None:
        nonlocal callback_completed
        callback_completed = True

    await dispatch_inbound_message(
        _message(),
        router=router,
        callback=callback,
        await_mode=CallbackAwaitMode.COROUTINE,
    )

    assert callback_completed is True


async def test_dispatch_coroutine_mode_keeps_legacy_generator_coroutine_behavior() -> None:
    router = RobotMessageRouter()
    callback_completed = []

    @types.coroutine
    def callback_result():
        yield from asyncio.sleep(0).__await__()
        callback_completed.append(True)
        return True

    result = callback_result()

    await dispatch_inbound_message(
        _message(),
        router=router,
        callback=lambda _: result,
        await_mode=CallbackAwaitMode.COROUTINE,
    )

    assert callback_completed == []
    result.close()


async def test_dispatch_coroutine_mode_does_not_await_future() -> None:
    router = RobotMessageRouter()
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    await dispatch_inbound_message(
        _message(),
        router=router,
        callback=lambda _: future,
        await_mode=CallbackAwaitMode.COROUTINE,
    )

    assert future.done() is False
    future.cancel()


async def test_dispatch_awaitable_mode_awaits_future() -> None:
    router = RobotMessageRouter()
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    future.set_result(True)

    handled = await dispatch_inbound_message(
        _message(),
        router=router,
        callback=lambda _: future,
        await_mode=CallbackAwaitMode.AWAITABLE,
        fallback_on_false=True,
    )

    assert handled is True
