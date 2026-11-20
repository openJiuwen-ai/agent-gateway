# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Regression tests for Runtime routing errors and reverse RPC affinity."""

from unittest.mock import AsyncMock

import httpx
import pytest
from e2a.models import E2AEnvelope

from agent_gateway.extensions.runtime_management_extension import runtime_routed_client as routed
from agent_gateway.extensions.runtime_management_extension.session_route_client import (
    FatalRouteError,
    RetryableRouteError,
    RuntimeSessionRouteClient,
    _error_from_response,
)


@pytest.mark.parametrize(
    ("status", "body", "expected_type", "expected_code"),
    [
        (429, {}, RetryableRouteError, "TRANSPORT"),
        (429, {"error_code": "TRANSPORT"}, RetryableRouteError, "TRANSPORT"),
        (429, {"error_code": "VALIDATION"}, FatalRouteError, "VALIDATION"),
        (429, {"error_code": "CONFIG_NOT_FOUND"}, FatalRouteError, "CONFIG_NOT_FOUND"),
        (429, {"error_code": "UNKNOWN"}, RetryableRouteError, "UNKNOWN"),
        (503, {}, RetryableRouteError, "TRANSPORT"),
        (400, {}, FatalRouteError, "VALIDATION"),
    ],
)
def test_route_error_preserves_explicit_business_codes(status, body, expected_type, expected_code):
    response = httpx.Response(status, headers={"Retry-After": "2"})
    error = _error_from_response(response, body)
    assert isinstance(error, expected_type)
    assert error.code == expected_code
    assert error.retry_after == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b"", b"not-json", b"{}"])
async def test_http_429_without_business_code_retries_route(body):
    requests = []

    def respond(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(429, content=body, headers={"Retry-After": "0"})
        return httpx.Response(200, json={
            "ok": True, "rawdata": {"pod_id": "pod-1", "pod_sse_url": "http://pod1/sse"},
        })

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as http:
        route = RuntimeSessionRouteClient(http_client=http)
        client = routed.RuntimeRoutedAgentClient(route_client=route, http_client=AsyncMock())
        base_url, _ = await client._route_with_retry(
            session_id="s1", group_id="g", bot_id="b", request_id="r", user_id="u",
        )
    assert base_url == "http://pod1"
    assert len(requests) == 2


def _reply(session_id):
    return E2AEnvelope(
        method="acp.tool_response",
        request_id="reply-1",
        session_id=session_id,
        params={"jsonrpc_id": 1},
    )


def _client(monkeypatch):
    monkeypatch.setattr(routed, "authorize_agent", AsyncMock(return_value=None))
    http = AsyncMock()
    client = routed.RuntimeRoutedAgentClient(route_client=AsyncMock(), http_client=http)
    client._connected = True
    return client, http


@pytest.mark.asyncio
async def test_reverse_rpc_ambiguous_fallback_preserves_other_sessions(monkeypatch):
    client, http = _client(monkeypatch)
    origins = {("s1", "1"): "http://pod1", ("s2", "1"): "http://pod2"}
    for key, base in origins.items():
        client._remember_rpc_origin(key, base)
    with pytest.raises(RuntimeError, match="ambiguous"):
        await client.send_request(_reply("missing"))
    http.send_request.assert_not_awaited()
    assert {key: origin.base_url for key, origin in client._rpc_origins.items()} == origins


@pytest.mark.asyncio
async def test_reverse_rpc_exact_match_keeps_same_id_in_other_session(monkeypatch):
    client, http = _client(monkeypatch)
    client._remember_rpc_origin(("s1", "1"), "http://pod1")
    client._remember_rpc_origin(("s2", "1"), "http://pod2")
    envelope = _reply("s2")
    await client.send_request(envelope)
    http.send_request.assert_awaited_once_with(envelope, base_url="http://pod2", timeout=None)
    assert list(client._rpc_origins) == [("s1", "1")]


@pytest.mark.asyncio
async def test_reverse_rpc_unique_fallback_remains_compatible(monkeypatch):
    client, http = _client(monkeypatch)
    client._remember_rpc_origin(("synthetic-session", "1"), "http://pod1")
    envelope = _reply(None)
    await client.send_request(envelope)
    http.send_request.assert_awaited_once_with(envelope, base_url="http://pod1", timeout=None)
    assert client._rpc_origins == {}


@pytest.mark.asyncio
async def test_reverse_rpc_missing_origin_does_not_send(monkeypatch):
    client, http = _client(monkeypatch)
    with pytest.raises(RuntimeError, match="unavailable"):
        await client.send_request(_reply("missing"))
    http.send_request.assert_not_awaited()
