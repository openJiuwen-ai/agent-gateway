# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from e2a.gateway_normalize import message_to_e2a

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web import invoke, trace_agent_name, web_connect, web_http_dispatch
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
        trace_agent_name.httpx, "AsyncClient",
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
async def test_first_ws_turn_carries_verified_name_over_rest(monkeypatch, credential, mode):
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
@pytest.mark.parametrize("failure", [
    "forbidden", "timeout", "no_exact_match", "ambiguous", "overlong", "unexpected",
])
async def test_lookup_failures_keep_existing_metadata(monkeypatch, failure):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    monkeypatch.setattr(trace_agent_name, "_MANAGER_TIMEOUT_S", 0.01)
    calls = []

    async def manager(request):
        calls.append(request)
        if failure == "timeout":
            await asyncio.sleep(10)
        if failure == "forbidden":
            return httpx.Response(403)
        if failure == "unexpected":
            raise RuntimeError("Manager client failed")
        if failure == "ambiguous":
            contexts = [{**_SCOPE, "agent_name": name} for name in (_NAME, "duplicate")]
        elif failure == "overlong":
            contexts = [{**_SCOPE, "agent_name": "a" * 129}]
        else:
            contexts = [{**_SCOPE, "group_id": "different-group", "agent_name": "other group"}]
        return httpx.Response(200, json={"data": {"contexts": contexts}})

    _install_manager(monkeypatch, manager)
    lookup = trace_agent_name.TraceAgentNameLookup()
    metadata = {METADATA_KEY: {"custom": "unchanged"}}
    for _ in range(2):
        result = await lookup.attach(
            params={}, metadata=metadata, routing=dict(_ROUTING),
            cluster_id="cluster", authorization="", access_token="browser-token",
        )
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
    monkeypatch.setattr(trace_agent_name, "_query_agent_name", lookup)

    result = await trace_agent_name.TraceAgentNameLookup().attach(
        params={"mode": mode, "team": team}, metadata={}, routing=dict(_ROUTING),
        cluster_id="cluster", authorization="", access_token=credential or "",
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


@pytest.mark.asyncio
@pytest.mark.parametrize("use_sse,auth_header", [(True, True), (False, False)])
async def test_http_dispatch_adds_verified_name_to_existing_ext(monkeypatch, use_sse, auth_header):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    captured = AsyncMock()
    monkeypatch.setattr(web_http_dispatch, "invoke_web_request", captured)
    credentials = []

    async def manager(request):
        credentials.append(request.headers["authorization"])
        await asyncio.sleep(0)
        return httpx.Response(200, json={"data": {"contexts": [
            {**_SCOPE, "group_id": None, "bot_id": 42, "agent_name": "助" * 128},
        ]}})

    _install_manager(monkeypatch, manager)
    channel = web_connect.WebChannel(web_connect.WebChannelConfig(enabled=True), RobotMessageRouter())
    headers = {
        "Cookie": "jiuwenclaw_id=cluster; openjiuwen_user_access_token=browser-token",
        "X-User-Id": "user",
        "X-Bot-Id": "42",
        "X-Agent-Name": "unverified",
        "X-User-Token": "opaque-value",
        TRACE_AGENT_NAME_KEY: "unverified",
    }
    if auth_header:
        headers["Authorization"] = "Bearer header-token"

    async def dispatch():
        outbound, _, _ = await web_http_dispatch.dispatch_http_request(
            channel, method="chat.send", params={"mode": "code", "session_id": "session"},
            headers=headers, client_host="127.0.0.1", use_sse=use_sse, is_stream=use_sse,
        )
        await channel.unregister_request_outbound(outbound)

    await asyncio.gather(dispatch(), dispatch())

    assert credentials == ["Bearer header-token" if auth_header else "Bearer browser-token"]
    assert captured.await_count == 2
    for call in captured.await_args_list:
        message = call.kwargs["user_message"]
        rest = assemble_rest_request(message_to_e2a(message), base_url="http://agent.test")
        assert decode_internal_header(rest.headers[INTERNAL_HEADER_NAME]) == {
            "X-User-Token": "opaque-value",
            TRACE_AGENT_NAME_KEY: {"bot_id": "42", "name": "助" * 128},
        }


@pytest.mark.asyncio
async def test_http_ignores_untrusted_routing_headers(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    monkeypatch.setenv("GATEWAY_WEB_HTTP_TRUST_CLIENT_HEADERS", "false")
    captured = AsyncMock()
    monkeypatch.setattr(web_http_dispatch, "invoke_web_request", captured)
    lookup = AsyncMock()
    monkeypatch.setattr(trace_agent_name, "_query_agent_name", lookup)
    channel = web_connect.WebChannel(web_connect.WebChannelConfig(enabled=True), RobotMessageRouter())

    outbound, _, _ = await web_http_dispatch.dispatch_http_request(
        channel, method="chat.send", params={"mode": "agent", "session_id": "session"},
        headers={
            "Authorization": "Bearer browser-token",
            "Cookie": "jiuwenclaw_id=cluster",
            "X-User-Id": "user",
            "X-Bot-Id": "bot",
        },
        client_host="192.0.2.20",
    )
    await channel.unregister_request_outbound(outbound)
    lookup.assert_not_awaited()
    assert TRACE_AGENT_NAME_KEY not in captured.await_args.kwargs["user_message"].metadata.get(METADATA_KEY, {})


@pytest.mark.asyncio
async def test_lookup_expires_and_keeps_credentials_separate(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    now = 0.0
    monkeypatch.setattr(trace_agent_name, "monotonic", lambda: now)
    manager = AsyncMock(side_effect=[_NAME, None, "更名后的 Agent"])
    monkeypatch.setattr(trace_agent_name, "_query_agent_name", manager)
    lookup = trace_agent_name.TraceAgentNameLookup()

    async def resolve(token):
        return await lookup.attach(
            params={}, metadata={}, routing=dict(_ROUTING), cluster_id="cluster",
            authorization="", access_token=token,
        )

    first = await resolve("first")
    assert first[METADATA_KEY][TRACE_AGENT_NAME_KEY]["name"] == _NAME
    assert await resolve("second") == {}
    assert await resolve("first") == first
    assert manager.await_count == 2

    now = 31.0
    refreshed = await resolve("first")
    assert refreshed[METADATA_KEY][TRACE_AGENT_NAME_KEY]["name"] == "更名后的 Agent"
    assert manager.await_count == 3
