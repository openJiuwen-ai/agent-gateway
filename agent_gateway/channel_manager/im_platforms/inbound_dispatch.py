"""Shared inbound message dispatch for IM platform channels."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from enum import Enum
from typing import Any

from e2a.message import Message

from agent_gateway.channel_manager.base import RobotMessageRouter


class CallbackAwaitMode(Enum):
    NONE = "none"
    COROUTINE = "coroutine"
    AWAITABLE = "awaitable"


async def dispatch_inbound_message(
    message: Message,
    *,
    router: RobotMessageRouter,
    callback: Callable[[Message], Any] | None,
    fallback_on_false: bool = False,
    await_mode: CallbackAwaitMode = CallbackAwaitMode.AWAITABLE,
) -> bool:
    """Dispatch a message, optionally falling back when a callback declines it."""
    if callback is not None:
        result = callback(message)
        should_await = (
            await_mode is CallbackAwaitMode.COROUTINE and inspect.iscoroutine(result)
        ) or (
            await_mode is CallbackAwaitMode.AWAITABLE and inspect.isawaitable(result)
        )
        if should_await:
            result = await result
        if not fallback_on_false or bool(result):
            return True

    await router.route_user_message(message)
    return False


__all__ = ["CallbackAwaitMode", "dispatch_inbound_message"]
