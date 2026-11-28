# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""AgentServer capability gating on the live southbound socket.

A restricted personal-compatibility request is only sent when the
``connection.ack`` of the socket that would carry it advertised the required
capability. A miss is never a write and is reported as
``UNSUPPORTED_DATA_PLANE``.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from e2a.message import ReqMethod
from e2a.models import E2AEnvelope
from websockets.exceptions import ConnectionClosedError

from agent_gateway.routing.agent_client import (
    AgentCapabilityUnavailable,
    DuplicateRequestIdError,
    WebSocketAgentServerClient,
)
from agent_gateway.routing.agent_request_timeout import send_agent_request_with_timeout
from agent_gateway.routing.e2a_proxy import (
    ProxyResponseValidationError,
    fetch_agent_unary,
    proxy_unary_request,
)
from agent_gateway.southbound.contracts import ConnectionTarget, DeliveryState, TransportKind, TransportError
from agent_gateway.southbound.data.transport import StaticWebSocketTransport
from agent_gateway.southbound.data.websocket import RequestNotSentError, WebSocketConnection
from agent_gateway.southbound.orchestration.client import build_static_client

CAPABILITY = "personal_project_git_v1"


class Socket:
    def __init__(self, capabilities=("personal_project_git_v1",)):
        self.sent: list[dict] = []
        self.ack = {"type": "event", "event": "connection.ack",
                    "payload": {"status": "ready", "readiness": "AGENT_READY",
                                "capabilities": list(capabilities)}}
        self._ack = True

    async def recv(self):
        if self._ack:
            self._ack = False
            import json

            return json.dumps(self.ack)
        await asyncio.Event().wait()

    async def send(self, raw):
        self.sent.append(raw)

    async def close(self):
        pass


def envelope(request_id="cap-request"):
    return E2AEnvelope(request_id=request_id, channel="web", method="project.git.commit", params={})


def test_connection_ack_advertises_only_string_capabilities():
    client = WebSocketAgentServerClient()
    client._apply_connection_ack({"payload": {"capabilities": ["one_v1", 2, None, "two_v1"]}})
    assert client._capabilities == frozenset({"one_v1", "two_v1"})
    client._apply_connection_ack({"payload": {"capabilities": "one_v1"}})
    assert client._capabilities == frozenset()
    client._apply_connection_ack({"payload": {}})
    assert client._capabilities == frozenset()


async def test_advisory_check_needs_a_ready_socket_and_is_reset_when_it_retires():
    client = WebSocketAgentServerClient()
    assert client.advertises_capability(CAPABILITY) is False  # never connected
    client._apply_connection_ack({"payload": {"capabilities": [CAPABILITY]}})
    assert client.advertises_capability(CAPABILITY) is True
    assert client.advertises_capability("other_v1") is False

    await client._stop_receiver_after_fatal_error(ConnectionClosedError(None, None))
    assert client._capabilities == frozenset()
    assert client.advertises_capability(CAPABILITY) is False

    client._apply_connection_ack({"payload": {"capabilities": [CAPABILITY]}})
    assert client.advertises_capability(CAPABILITY) is True
    await client.disconnect()
    assert client.advertises_capability(CAPABILITY) is False

    client._apply_connection_ack({"payload": {"capabilities": [CAPABILITY]}})
    client._uri, client._ws = "ws://127.0.0.1:1/agent/ws", Socket()
    client._reconnect_enabled = True
    # A new connect() starts from an unqualified socket until its own ack arrives.
    monkey = pytest.MonkeyPatch()
    monkey.setattr("websockets.legacy.client.connect", _hanging_connect)
    try:
        connecting = asyncio.create_task(client.connect(client._uri))
        await asyncio.sleep(0.01)
        assert client.advertises_capability(CAPABILITY) is False
        connecting.cancel()
        await asyncio.gather(connecting, return_exceptions=True)
    finally:
        monkey.undo()
        await client.disconnect()


async def _hanging_connect(uri, **_kwargs):
    await asyncio.Event().wait()


@pytest.mark.parametrize("capabilities", [(), ("something_else_v1",)])
async def test_missing_capability_never_reaches_the_socket(capabilities):
    client = WebSocketAgentServerClient()
    socket = Socket(capabilities)
    client._ws = socket
    client._server_ready = True
    client._apply_connection_ack({"payload": {"capabilities": list(capabilities)}})
    with pytest.raises(AgentCapabilityUnavailable, match="does not support"):
        await client.send_request(envelope(), required_capability=CAPABILITY)
    assert socket.sent == []
    assert client._message_queues == {}


async def test_advertised_capability_is_sent_and_unqualified_requests_stay_unchanged():
    client = WebSocketAgentServerClient()
    socket = Socket()
    client._ws = socket
    client._apply_connection_ack({"payload": {"capabilities": [CAPABILITY]}})
    waiting = asyncio.create_task(client.send_request(envelope("qualified"), required_capability=CAPABILITY))
    plain = asyncio.create_task(client.send_request(envelope("plain")))
    try:
        for _ in range(200):
            if len(socket.sent) == 2:
                break
            await asyncio.sleep(0.001)
        assert len(socket.sent) == 2
    finally:
        waiting.cancel()
        plain.cancel()
        await asyncio.gather(waiting, plain, return_exceptions=True)


async def test_closed_connection_still_reports_not_sent():
    client = WebSocketAgentServerClient()
    client._ws = Socket()
    client._apply_connection_ack({"payload": {"capabilities": [CAPABILITY]}})
    async with client._lock:
        task = asyncio.create_task(client.send_request(envelope(), required_capability=CAPABILITY))
        await asyncio.sleep(0)
        client._ws = None
    with pytest.raises(RequestNotSentError):
        await asyncio.wait_for(task, 1)


async def test_static_client_qualifies_its_own_connection(monkeypatch):
    target = ConnectionTarget(TransportKind.WEBSOCKET, "ws://127.0.0.1:1/agent/ws")
    client = build_static_client(target.endpoint)
    transport = client._transport
    connection = transport._connection
    assert transport.advertises_capability(CAPABILITY) is False  # never connected
    connection._server_ready = True
    connection._apply_connection_ack({"payload": {"capabilities": [CAPABILITY]}})
    assert client.advertises_capability(CAPABILITY) is True
    assert client.advertises_capability("other_v1") is False

    seen = []

    async def send_request(self, envelope, *, timeout=None, on_written=None, required_capability=None):
        seen.append(required_capability)
        return "response"

    monkeypatch.setattr(WebSocketConnection, "send_request", send_request)
    monkeypatch.setattr(transport, "_get_for_request", AsyncMock(return_value=connection))
    await client.send_request(envelope())
    await client.send_request(envelope(), required_capability=CAPABILITY)
    assert seen == [None, CAPABILITY]

    monkeypatch.setattr(WebSocketConnection, "send_request",
                        AsyncMock(side_effect=AgentCapabilityUnavailable()))
    with pytest.raises(TransportError) as error:
        await transport.send_request(target, envelope(), required_capability=CAPABILITY)
    assert error.value.delivery is DeliveryState.NOT_SENT
    assert isinstance(error.value.__cause__, AgentCapabilityUnavailable)


async def test_capability_requirement_is_never_carried_by_the_sse_link(monkeypatch):
    from agent_gateway.southbound.data.http_sse import HttpSseConnection

    target = ConnectionTarget(TransportKind.HTTP_SSE, "http://127.0.0.1:1")
    transport = StaticWebSocketTransport(target)
    sse = HttpSseConnection.__new__(HttpSseConnection)
    sse.send_request = AsyncMock()
    monkeypatch.setattr(transport, "_get_for_request", AsyncMock(return_value=sse))
    with pytest.raises(TransportError) as error:
        await transport.send_request(target, envelope(), required_capability=CAPABILITY)
    assert error.value.delivery is DeliveryState.NOT_SENT
    assert isinstance(error.value.__cause__, AgentCapabilityUnavailable)
    sse.send_request.assert_not_awaited()


async def test_unqualified_requests_do_not_add_a_keyword():
    client = SimpleNamespace(send_request=AsyncMock(return_value="response"))
    assert await send_agent_request_with_timeout(client, envelope(), label="plain") == "response"
    assert client.send_request.await_args.kwargs == {}
    await send_agent_request_with_timeout(client, envelope(), label="plain", timeout_seconds=1)
    assert client.send_request.await_args.kwargs == {}


class Channel:
    channel_id = "web"

    def __init__(self):
        self.responses = []

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
        self.responses.append({"id": req_id, "ok": ok, "payload": payload, "error": error, "code": code})


async def test_proxy_answers_unsupported_plane_before_sending_anything():
    channel, client = Channel(), SimpleNamespace(server_ready=True, send_request=AsyncMock())
    ws = SimpleNamespace()
    assert await proxy_unary_request(
        channel=channel, agent_client=client, ws=ws, req_id="req-1", params={},
        session_id=None, user_id=None, req_method=ReqMethod.PROJECT_GIT_COMMIT,
        required_capability=CAPABILITY,
    )
    assert channel.responses == [{"id": "req-1", "ok": False, "payload": None,
                                  "error": "This AgentServer does not support the requested operation",
                                  "code": "UNSUPPORTED_DATA_PLANE"}]
    client.send_request.assert_not_awaited()


async def test_proxy_passes_the_requirement_to_a_qualifying_client():
    from agent_gateway.southbound.data.websocket import WebSocketConnection

    class Client(WebSocketConnection):
        def __init__(self):
            super().__init__()
            self._server_ready = True

        async def send_request(self, envelope, *, timeout=None, on_written=None, required_capability=None):
            self.required = required_capability
            return SimpleNamespace(ok=True, payload={"done": True})

    client, channel = Client(), Channel()
    assert await proxy_unary_request(
        channel=channel, agent_client=client, ws=SimpleNamespace(), req_id="req-2", params={},
        session_id=None, user_id=None, req_method=ReqMethod.PROJECT_GIT_COMMIT,
        required_capability=CAPABILITY,
    )
    assert client.required == CAPABILITY
    assert channel.responses[-1]["ok"] is True and channel.responses[-1]["payload"] == {"done": True}


async def test_proxy_socket_level_capability_miss_is_a_fixed_public_failure():
    from agent_gateway.southbound.data.websocket import WebSocketConnection

    class Client(WebSocketConnection):
        def __init__(self):
            super().__init__()
            self._ws = Socket(capabilities=())
            self._server_ready = True
            self._apply_connection_ack({"payload": {"capabilities": []}})

    client, channel = Client(), Channel()
    assert await proxy_unary_request(
        channel=channel, agent_client=Client(), ws=SimpleNamespace(), req_id="req-3", params={},
        session_id=None, user_id=None, req_method=ReqMethod.PROJECT_GIT_COMMIT,
        required_capability=CAPABILITY,
    )
    assert channel.responses[-1]["code"] == "UNSUPPORTED_DATA_PLANE"
    assert channel.responses[-1]["ok"] is False
    assert client._ws.sent == [] and client._message_queues == {}


async def test_fetch_reports_the_miss_instead_of_raising_to_a_poller():
    class Client:
        server_ready = True

        async def send_request(self, envelope, **_kwargs):
            raise AssertionError("must not send")

    assert await fetch_agent_unary(
        agent_client=Client(), req_method=ReqMethod.PROJECT_GIT_COMMIT, params={},
        session_id=None, user_id=None, channel_id="web", required_capability=CAPABILITY,
    ) == (False, {"error": "This AgentServer does not support the requested operation",
                  "code": "UNSUPPORTED_DATA_PLANE"})


async def test_fetch_keeps_the_single_retry_for_a_duplicate_request_id():
    sent = []

    class Client(WebSocketAgentServerClient):
        def __init__(self):
            super().__init__()
            self._server_ready = True
            self._apply_connection_ack({"payload": {"capabilities": [CAPABILITY]}})

        async def send_request(self, envelope, *, timeout=None, on_written=None, required_capability=None):
            sent.append(required_capability)
            if len(sent) == 1:
                raise DuplicateRequestIdError(envelope.request_id)
            return SimpleNamespace(ok=True, payload={"value": 1})

    ok, payload = await fetch_agent_unary(
        agent_client=Client(), req_method=ReqMethod.PROJECT_GIT_COMMIT, params={},
        session_id=None, user_id=None, channel_id="web", required_capability=CAPABILITY,
    )
    assert (ok, payload, sent) == (True, {"value": 1}, [CAPABILITY, CAPABILITY])


async def test_response_validator_runs_before_callbacks_and_the_reply():
    channel = Channel()
    client = SimpleNamespace(server_ready=True, advertises_capability=lambda _capability: True,
                             send_request=AsyncMock(return_value=SimpleNamespace(ok=True, payload={})))
    seen = []

    async def reject(_response):
        raise ProxyResponseValidationError("CONFLICT", "stale version")

    async def subscribe(response):
        seen.append(("validator", response.ok))

    def on_done(ok, _payload):
        seen.append(("on_done", ok))

    ws = SimpleNamespace()
    await proxy_unary_request(
        channel=channel, agent_client=client, ws=ws, req_id="stale", params={}, session_id=None,
        user_id=None, req_method=ReqMethod.PROJECT_GIT_COMMIT, required_capability=CAPABILITY,
        response_validator=reject, on_done=on_done,
    )
    assert channel.responses == [{"id": "stale", "ok": False, "payload": None,
                                  "error": "stale version", "code": "CONFLICT"}]
    assert seen == []
    await proxy_unary_request(
        channel=channel, agent_client=client, ws=ws, req_id="fresh", params={}, session_id=None,
        user_id=None, req_method=ReqMethod.PROJECT_GIT_COMMIT, required_capability=CAPABILITY,
        response_validator=subscribe, on_done=on_done,
    )
    assert seen == [("validator", True), ("on_done", True)]
    assert channel.responses[-1]["id"] == "fresh" and channel.responses[-1]["ok"] is True

    async def crash(_response):
        raise RuntimeError("unexpected")

    await proxy_unary_request(
        channel=channel, agent_client=client, ws=ws, req_id="crash", params={}, session_id=None,
        user_id=None, req_method=ReqMethod.PROJECT_GIT_COMMIT, response_validator=crash,
    )
    assert channel.responses[-1] == {"id": "crash", "ok": False, "payload": None,
                                     "error": "AgentServer response validation failed",
                                     "code": "SERVICE_UNAVAILABLE"}
