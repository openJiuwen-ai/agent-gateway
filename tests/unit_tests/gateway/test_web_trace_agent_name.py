# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from e2a.gateway_normalize import message_to_e2a

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web import invoke, web_connect
from agent_gateway.common.request_ext import (
    INTERNAL_HEADER_NAME,
    METADATA_KEY,
    TRACE_AGENT_NAME_KEY,
    build_ext_from_source,
    decode_internal_header,
    set_forward_headers,
)
from agent_gateway.enterprise.config.manager_config.core.instance import instance_service
from agent_gateway.southbound.data.rest_map import assemble_rest_request

_SCOPE = {"jiuwenclaw_id": "cluster", "user_id": "user", "group_id": "group", "bot_id": "bot"}
_ROUTING = {key: value for key, value in _SCOPE.items() if key != "jiuwenclaw_id"}
_NAME = "需求澄清助手"


def _install_manager(monkeypatch, handler):
    client = httpx.AsyncClient
    monkeypatch.setattr(instance_service, "resolve_manager_http_base", lambda: "https://manager.test")
    monkeypatch.setattr(
        web_connect.httpx, "AsyncClient",
        lambda **kwargs: client(transport=httpx.MockTransport(handler), **kwargs),
    )


def _ws(credential):
    headers = {"cookie": "jiuwenclaw_id=cluster"}
    if credential == "browser":
        headers["cookie"] += "; openjiuwen_user_access_token=browser-token"
    elif credential == "insight":
        headers["authorization"] = "Bearer insight-token"
    return SimpleNamespace(
        request_headers=headers,
        _web_connection_user_id="user",
        _web_routing=dict(_ROUTING),
        _web_agent_identity={"agent_name": "unverified"},
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("credential,mode", [("browser", "agent"), ("insight", "code")])
async def test_first_web_turn_carries_verified_name_over_rest(monkeypatch, credential, mode):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    calls = []

    async def manager(request):
        assert request.url.path == "/api/v1/user-console/agent-contexts"
        assert request.headers["authorization"] == f"Bearer {credential}-token"
        calls.append(request)
        await asyncio.sleep(0)
        return httpx.Response(200, json={"data": {"contexts": [
            {**_SCOPE, "jiuwenclaw_id": "different-cluster", "agent_name": "other cluster"},
            {**_SCOPE, "user_id": "different-user", "agent_name": "other user"},
            {**_SCOPE, "group_id": "different-group", "agent_name": "other group"},
            {**_SCOPE, "bot_id": "different-bot", "agent_name": "other bot"},
            {**_SCOPE, "agent_name": _NAME},
        ]}})

    _install_manager(monkeypatch, manager)
    channel = web_connect.WebChannel(web_connect.WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.ensure_extensions = AsyncMock()
    channel._process_files = AsyncMock(side_effect=lambda params, **_: params)
    dispatch = AsyncMock()
    monkeypatch.setattr(invoke, "dispatch_web_request", dispatch)
    ws = _ws(credential)

    await asyncio.gather(*(
        channel._handle_request_message(
            ws, {}, {"agent_name": ["unverified"]}, request_id, "chat.send",
            {"mode": mode, "query": "hello", "agent_name": "also unverified"},
        )
        for request_id in ("first", "second")
    ))

    assert len(calls) == 1
    assert dispatch.await_count == 2
    for call in dispatch.await_args_list:
        message = call.kwargs["user_message"]
        rest = assemble_rest_request(message_to_e2a(message), base_url="http://agent.test")
        assert decode_internal_header(rest.headers[INTERNAL_HEADER_NAME]) == {
            TRACE_AGENT_NAME_KEY: {"bot_id": "bot", "name": _NAME},
        }


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["forbidden", "timeout", "no_exact_match"])
async def test_lookup_failures_keep_existing_metadata(monkeypatch, failure):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    monkeypatch.setattr(web_connect, "_TRACE_AGENT_NAME_TIMEOUT_S", 0.01)
    calls = []

    async def manager(request):
        calls.append(request)
        if failure == "timeout":
            await asyncio.sleep(10)
        if failure == "forbidden":
            return httpx.Response(403)
        return httpx.Response(200, json={"data": {"contexts": [
            {**_SCOPE, "group_id": "different-group", "agent_name": "other group"},
        ]}})

    _install_manager(monkeypatch, manager)
    ws = _ws("browser")
    metadata = {METADATA_KEY: {"custom": "unchanged"}}
    for _ in range(2):
        result = await web_connect._attach_trace_agent_name(ws, {}, metadata, dict(_ROUTING))
        assert result is metadata
    assert len(calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("edition,mode,team,credential", [
    ("personal", "agent", False, "browser"),
    ("enterprise", "team", False, "browser"),
    ("enterprise", "agent", True, "browser"),
    ("enterprise", "agent", False, None),
])
async def test_ineligible_turn_does_not_query_manager(monkeypatch, edition, mode, team, credential):
    monkeypatch.setenv("JIUWENSWARM_EDITION", edition)
    lookup = AsyncMock()
    monkeypatch.setattr(web_connect, "_query_trace_agent_name", lookup)

    result = await web_connect._attach_trace_agent_name(
        _ws(credential), {"mode": mode, "team": team}, {}, dict(_ROUTING),
    )

    assert result == {}
    lookup.assert_not_awaited()


def test_reserved_name_is_ignored_by_client_header_forwarding():
    set_forward_headers([TRACE_AGENT_NAME_KEY.upper(), "x-custom"])
    try:
        assert build_ext_from_source({TRACE_AGENT_NAME_KEY: "unverified", "x-custom": "ok"}) == {
            "x-custom": "ok",
        }
    finally:
        set_forward_headers(None)
