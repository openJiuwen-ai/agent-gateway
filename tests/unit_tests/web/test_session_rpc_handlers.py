# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.channel_manager.web.session_rpc_handlers import (
    register_session_handlers,
)


EXPECTED_ROUTES = [
    "session.list",
    "session.create",
    "session.get_metadata",
    "session.message.continue_queued",
    "session.plan_status",
    "session.rename",
    "session.pin",
]


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


def _register(agent_client=None, _resolve=lambda ref: ref):
    channel = _FakeChannel()
    register_session_handlers(channel, agent_client, _resolve)
    return channel


def test_session_routes_are_registered_in_original_order():
    channel = _register(object())

    assert list(channel.methods) == EXPECTED_ROUTES


@pytest.mark.asyncio
async def test_session_create_rejects_non_object_params():
    channel = _register(object())

    await channel.methods["session.create"](
        object(), "rpc-1", [], "session-1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-1",
            "ok": False,
            "error": "params must be object",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_session_create_prepares_unary_request_params(monkeypatch):
    from agent_gateway.channel_manager.web import session_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    monkeypatch.setattr(session_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    channel = _register(object())

    await channel.methods["session.create"](
        object(),
        "rpc-2",
        {"session_id": "session-2", "title": "example"},
        "session-2",
        user_id="u2",
    )

    assert len(forwarded) == 1
    call = forwarded[0]
    assert call["params"] == {
        "session_id": "session-2",
        "title": "example",
        "create_token": call["params"]["create_token"],
    }
    assert call["session_id"] is None
    assert call["user_id"] == "u2"
    assert call["req_method"].value == "session.create"
    assert call["label"] == "session.create"
    assert call["drop_params"] == ("session_id",)
    assert call["default_error_code"] == "SESSION_CREATE_FAILED"
    assert channel.responses == []


@pytest.mark.asyncio
async def test_session_list_forwards_unary_request(monkeypatch):
    from agent_gateway.channel_manager.web import session_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    monkeypatch.setattr(session_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    channel = _register(object())

    params = {"project_id": "p1"}
    await channel.methods["session.list"](
        object(), "rpc-3", params, "session-3", user_id="u3"
    )

    assert len(forwarded) == 1
    call = forwarded[0]
    assert call["params"] is params
    assert call["session_id"] == "session-3"
    assert call["user_id"] == "u3"
    assert call["req_method"].value == "session.list"
    assert call["label"] == "session.list"
    assert channel.responses == []

