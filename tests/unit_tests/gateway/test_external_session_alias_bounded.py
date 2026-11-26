# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A2A 外部会话别名映射必须有上界。

``_external_session_aliases`` 以 ``(channel_id, external_session_id)`` 为键，
每个外部会话首次出现时写入一次，此前既没有删除也没有上限，a2a 客户端可以
自造 external id，长跑网关下线性增长。
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
from e2a.message import Message

from agent_gateway.message_handler import message_handler as mh
from agent_gateway.message_handler.message_handler import MessageHandler
from tests._support.null_session_login_bindings import NullSessionLoginBindings


def _handler() -> MessageHandler:
    handler = MessageHandler(
        SimpleNamespace(),
        session_login_bindings=NullSessionLoginBindings(),
    )
    handler._external_session_aliases.clear()
    return handler


def _a2a_message(session_id: str) -> Message:
    return Message(
        id="req-a2a",
        type="req",
        channel_id="a2a",
        session_id=session_id,
        params={"mode": "agent"},
        timestamp=time.time(),
        ok=True,
        req_method="chat.send",
    )


def test_alias_map_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mh, "_EXTERNAL_SESSION_ALIAS_LIMIT", 3)
    handler = _handler()

    for index in range(5):
        handler._remember_external_session_alias(("a2a", f"ext-{index}"), f"int-{index}")

    assert list(handler._external_session_aliases) == [
        ("a2a", "ext-2"),
        ("a2a", "ext-3"),
        ("a2a", "ext-4"),
    ]


def test_rewriting_alias_refreshes_its_position() -> None:
    handler = _handler()
    handler._remember_external_session_alias(("a2a", "ext-a"), "int-a")
    handler._remember_external_session_alias(("a2a", "ext-b"), "int-b")
    handler._remember_external_session_alias(("a2a", "ext-a"), "int-a2")

    assert list(handler._external_session_aliases) == [("a2a", "ext-b"), ("a2a", "ext-a")]
    assert handler._external_session_aliases[("a2a", "ext-a")] == "int-a2"


def test_empty_values_are_not_recorded() -> None:
    handler = _handler()

    handler._remember_external_session_alias(("a2a", ""), "int-a")
    handler._remember_external_session_alias(("a2a", "ext-a"), "")

    assert not handler._external_session_aliases


@pytest.mark.asyncio
async def test_resolve_reuses_cached_alias(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = _handler()
    calls: list[str] = []

    async def fake_allocate(msg, state, **kwargs):
        calls.append("allocate")
        return "int-allocated"

    monkeypatch.setattr(handler, "_allocate_channel_session", fake_allocate)

    first = _a2a_message("ext-1")
    await handler._resolve_external_channel_session(first)
    second = _a2a_message("ext-1")
    await handler._resolve_external_channel_session(second)

    assert calls == ["allocate"]
    assert first.session_id == "int-allocated"
    assert second.session_id == "int-allocated"
    assert second.metadata["external_session_id"] == "ext-1"
    assert handler._external_session_aliases[("a2a", "ext-1")] == "int-allocated"

