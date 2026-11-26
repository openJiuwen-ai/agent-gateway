"""入站归一化不能假设 req_method 一定是本地枚举。

方法枚举归 AgentServer：未登记方法从 ACP 适配层进来时是纯字符串。归一化、
路由、投递都得按字符串处理，否则 ``req_method.value`` 会在赋值机上抛
``AttributeError``。
"""
from __future__ import annotations

from typing import Any

import pytest

from e2a.message import Message

from agent_gateway.application.inbound import (
    MessagePreprocessor,
    _normalize_gateway_message,
)
from agent_gateway.common.protocol_methods import ReqMethod


def _message(req_method: Any) -> Message:
    return Message(
        id="req-1",
        type="req",
        channel_id="acp",
        session_id="sess-1",
        params={"session_id": "sess-1"},
        timestamp=0.0,
        ok=True,
        req_method=req_method,
    )


class _ChannelManager:
    def __init__(self) -> None:
        self.delivered: list[Message] = []

    async def deliver_to_message_handler(self, msg: Message) -> None:
        self.delivered.append(msg)


async def _noop(_msg: Any) -> None:
    return None


def test_normalize_keeps_unknown_method_string() -> None:
    normalized = _normalize_gateway_message(_message("team.brand.new"))

    assert normalized.req_method == "team.brand.new"
    assert normalized.params["session_id"] == "sess-1"


def test_normalize_still_accepts_enum_method() -> None:
    normalized = _normalize_gateway_message(_message(ReqMethod.CHAT_SEND))

    assert normalized.req_method is ReqMethod.CHAT_SEND


@pytest.mark.asyncio
async def test_forward_delivers_unknown_method_string() -> None:
    preprocessor = MessagePreprocessor(
        refresh_credentials=_noop,
        normalize=_normalize_gateway_message,
        inject_work_mode=lambda _msg: None,
    )
    manager = _ChannelManager()

    handled = await preprocessor.forward(_message("team.brand.new"), manager)

    assert handled is False
    assert len(manager.delivered) == 1
    assert manager.delivered[0].req_method == "team.brand.new"


@pytest.mark.asyncio
async def test_for_channel_routes_unknown_method_string_as_passthrough() -> None:
    preprocessor = MessagePreprocessor(
        refresh_credentials=_noop,
        normalize=_normalize_gateway_message,
        inject_work_mode=lambda _msg: None,
    )
    manager = _ChannelManager()
    handle = preprocessor.for_channel(
        manager,
        forward_methods=frozenset(),
        no_local_methods=frozenset(),
        source_label="ACP",
        local_methods=frozenset({"session.create"}),
    )

    forwarded = await handle(_message("team.brand.new"))

    assert forwarded is True
    assert len(manager.delivered) == 1
    assert manager.delivered[0].req_method == "team.brand.new"
