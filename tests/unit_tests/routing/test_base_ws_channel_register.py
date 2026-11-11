# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""register_ws 幂等：重复注册不重建状态、不打 INFO；evict_previous 仍然生效."""

from __future__ import annotations

import importlib
import logging

import pytest

pytest.importorskip("e2a")

from e2a.agent_models import AgentRef

# Load the package first so web_connect -> base_ws_channel resolve in the same
# order the runtime uses; importing base_ws_channel directly hits a pre-existing
# circular import through channel_manager/__init__.
importlib.import_module("agent_gateway.channel_manager")
from agent_gateway.routing.base_ws_channel import BaseWsChannel
from agent_gateway.routing.keys import RoutingKey


class _FakeWs:
    closed = False


class _Channel(BaseWsChannel):
    channel_id = "test"

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def _serialize_frame(self, msg, routing_target, *, member_names=None):
        return {}


def _key(session_id: str) -> RoutingKey:
    return RoutingKey(
        user_id="u1",
        channel_id="web",
        app_id="default",
        agent_ref=AgentRef(mode="agent", id="a1"),
        session_id=session_id,
    )


async def test_register_ws_is_idempotent(caplog) -> None:
    channel = _Channel(None, None)
    ws = _FakeWs()
    key = _key("s1")
    await channel.register_ws(ws, key)
    ws_id = channel._id_by_ws[id(ws)]

    caplog.clear()
    caplog.set_level(logging.DEBUG)
    await channel.register_ws(ws, key)

    assert channel._id_by_ws[id(ws)] == ws_id
    assert channel._clients_by_key[key] == [ws]
    assert caplog.records == [], "re-registration must be silent"

    await channel.unregister_ws(ws)


async def test_evict_previous_still_unlinks_old_keys() -> None:
    channel = _Channel(None, None)
    ws = _FakeWs()
    first, second = _key("s1"), _key("s2")
    await channel.register_ws(ws, first)
    await channel.register_ws(ws, second, evict_previous=True)

    assert first not in channel._clients_by_key
    assert channel._clients_by_key[second] == [ws]
    assert channel._id_by_ws[id(ws)] == getattr(ws, "_jiuwen_ws_id")

    await channel.unregister_ws(ws)
