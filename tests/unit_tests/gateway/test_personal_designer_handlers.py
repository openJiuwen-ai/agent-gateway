# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Personal Web Designer requests use the guarded, capability-checked unary path."""

import asyncio
import inspect
from types import SimpleNamespace

import pytest
from e2a.agent_models import AgentResponseChunk
from e2a.constants import E2A_WIRE_SERVER_PUSH_KEY
from e2a.message import ReqMethod
from e2a.wire_codec import encode_agent_chunk_for_wire
from starlette.datastructures import Headers

from agent_gateway.application.personal_web_operations import PERSONAL_WEB_OPERATIONS
from agent_gateway.application.worker_data import PERSONAL_WEB_WORKER_METHODS, is_data_frame
from agent_gateway.channel_manager.web import app_web_handlers
from agent_gateway.channel_manager.web.designer_handlers import (
    _DESIGNER_METHODS, _MAX_ID_LENGTH, _MAX_PARAMS_BYTES, PERSONAL_DESIGNER_CAPABILITY,
    PERSONAL_DESIGNER_METHODS, DesignerSubscriptions, register_personal_designer_handlers,
)
from agent_gateway.routing import agent_request_timeout
from agent_gateway.routing.agent_client import WebSocketAgentServerClient
from agent_gateway.routing.agent_request_timeout import AgentRequestTimeoutError
from tests.unit_tests.gateway.test_personal_settings import Channel as _Channel
from tests.unit_tests.gateway.test_personal_web_operations import ORIGIN, bind

MARKER = "_personal_designer_v1"
PAGE_METHODS = sorted(PERSONAL_DESIGNER_METHODS)
ALL_METHODS = sorted(method.value for method, _ in _DESIGNER_METHODS)


class Channel(_Channel):
    def __init__(self):
        super().__init__()
        self.events: list = []

    async def send_event(self, ws, event, payload):
        self.events.append((ws, event, payload))


def register(client, channel=None):
    """Register on a channel directly, the way the personal Worker does."""
    channel = channel or Channel()
    register_personal_designer_handlers(channel, lambda: client)
    return channel


def page(origin="http://localhost:5173"):
    headers = Headers({"Origin": origin}) if origin is not None else Headers()
    return SimpleNamespace(request_headers=headers, closed=False)


def ready_client():
    client = WebSocketAgentServerClient()
    client._server_ready = True
    return client


@pytest.fixture
def southbound(monkeypatch):
    """Record what the Gateway hands to the southbound send, and answer for the AgentServer."""
    requests = []

    async def send(client, request, *, label, timeout_seconds, required_capability=None):
        requests.append((request, timeout_seconds, {"required_capability": required_capability}))
        return SimpleNamespace(ok=True, payload={"method": request.method, "params": request.params})

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.send_agent_request_with_timeout", send)
    return requests


def designer_methods(channel):
    return sorted(method for method in channel.methods if method.startswith("designer."))


def test_the_guarded_registration_covers_what_the_page_calls():
    channel = register(None)
    assert sorted(channel.methods) == PAGE_METHODS
    assert "designer.run.pause" not in channel.methods
    assert PAGE_METHODS == sorted(set(ALL_METHODS) - {"designer.run.pause"})
    # Sockets are told about a canvas only through what the registration installed.
    assert isinstance(channel.designer_subscriptions, DesignerSubscriptions)
    assert channel.disconnect_handlers == [channel.designer_subscriptions.disconnect]


@pytest.mark.parametrize("method,params", [
    ("designer.graph.save", {"graph": {"graph_id": "g1", "nodes": [], "edges": []}, "base_updated_at": 7}),
    ("designer.graph.patch", {"graph_id": "g1", "patch": {"title": "Renamed"}, "base_updated_at": 7}),
    ("designer.graph.get", {"graph_id": "g1"}),
    ("designer.graph.list", {"project_id": "default"}),
    ("designer.workspace.create", {"prompt": "A short film", "create_token": "token-1", "model_name": "m"}),
    ("designer.workspace.get", {"project_id": "default", "session_id": "design_1"}),
    ("designer.workspace.session.create", {"project_id": "default"}),
    ("designer.workspace.session.compose", {"project_id": "default", "session_id": "design_1", "prompt": "A film"}),
    ("designer.media.capabilities", {"graph_id": "g1"}),
    ("designer.run.start", {"graph_id": "g1", "session_id": "design_1"}),
    ("designer.run.get", {"run_id": "run_1"}),
    ("designer.run.cancel", {"run_id": "run_1"}),
])
async def test_each_request_carries_the_gateway_marker_and_needs_the_capability(southbound, method, params):
    channel = register(ready_client())
    await channel.methods[method](page(), "req-1", params, "design_1", "user-42")

    [(request, _, options)] = southbound
    assert request.method == method
    assert request.params == {**params, MARKER: True}
    assert request.user_id == "user-42"
    assert request.session_id == "design_1"
    assert request.is_stream is False
    assert options == {"required_capability": PERSONAL_DESIGNER_CAPABILITY}
    assert channel.responses == [{
        "id": "req-1", "ok": True, "payload": {"method": method, "params": {**params, MARKER: True}},
        "error": None, "code": None,
    }]


async def test_the_caller_cannot_supply_the_identity_or_vouch_for_itself(southbound):
    channel = register(ready_client())
    forged = {
        "graph_id": "g1", "user_id": "another", MARKER: False,
        "graph": {"user_id": "another"}, "patch": {MARKER: True, "user_id": "another"},
    }

    await channel.methods["designer.graph.patch"](page(), "forged", forged, "design_1", "user-42")

    [(request, _, _)] = southbound
    assert request.user_id == "user-42"
    assert request.params == {
        "graph_id": "g1", MARKER: True,
        "graph": {"user_id": "another"}, "patch": {MARKER: True, "user_id": "another"},
    }


@pytest.mark.parametrize("params", [
    [], None, "graph", 5,
    {"graph_id": ""}, {"graph_id": " g1"}, {"graph_id": "g1 "}, {"graph_id": "g1\n"},
    {"graph_id": "x" * (_MAX_ID_LENGTH + 1)}, {"graph_id": 5}, {"graph_id": True},
    {"session_id": "../other"}, {"session_id": "a/b"}, {"session_id": ".."}, {"session_id": "C:\\x"},
    {"graph_id": "g:1"}, {"run_id": "a\\b"}, {"project_id": "a/b"}, {"run_id": "x" * 300}, {"node_id": "\x00"},
    {"create_token": "token\x00"},
    {"graph_id": "g1", "base_updated_at": "5"}, {"graph_id": "g1", "base_updated_at": -1},
    {"graph_id": "g1", "base_updated_at": True}, {"graph_id": "g1", "base_updated_at": 1.5},
    {"graph_id": "g1", "pad": "x" * _MAX_PARAMS_BYTES},
])
async def test_a_malformed_request_never_reaches_the_agentserver(southbound, params):
    channel = register(ready_client())

    for method in PAGE_METHODS:
        await channel.methods[method](page(), f"bad-{method}", params, "design_1", "user-42")

    assert southbound == []
    assert [response["code"] for response in channel.responses] == ["BAD_REQUEST"] * len(PAGE_METHODS)
    assert all(response["ok"] is False and response["payload"] is None for response in channel.responses)
    # The refusal names the field, never the value.
    assert "x" * 300 not in str(channel.responses)


@pytest.mark.parametrize("origin", [None, "https://desk.example.evil", "null"])
async def test_a_page_without_an_allowed_origin_is_refused_before_anything_is_read(southbound, origin):
    channel = register(ready_client())

    await channel.methods["designer.graph.save"](page(origin), "origin", [], "design_1", "user-42")

    assert channel.responses == [{
        "id": "origin", "ok": False, "payload": None,
        "error": "This operation requires an allowed Web Origin", "code": "FORBIDDEN",
    }]
    assert southbound == []


async def test_an_agentserver_without_the_capability_is_written_nothing(monkeypatch):
    """Checked on the real client: the request never reaches the socket."""
    for advertised in (frozenset(), frozenset({PERSONAL_DESIGNER_CAPABILITY})):
        client = WebSocketAgentServerClient()
        client._server_ready = True
        client._capabilities = advertised
        written = []

        async def connected():
            return None

        async def send_payload(payload):
            written.append(payload)

        monkeypatch.setattr(client, "_ensure_connected_for_request", connected)
        monkeypatch.setattr(client, "_check_request_connection", lambda generation: None)
        monkeypatch.setattr(client, "_send_wire_payload", send_payload)
        real_send = agent_request_timeout.send_agent_request_with_timeout

        async def send(agent_client, envelope, *, label, timeout_seconds=None, required_capability=None):
            return await real_send(
                agent_client, envelope, label=label, timeout_seconds=0.05,
                required_capability=required_capability,
            )

        monkeypatch.setattr("agent_gateway.routing.e2a_proxy.send_agent_request_with_timeout", send)
        channel = register(client)
        params = {"graph": {"graph_id": "g1", "nodes": [], "edges": []}, "base_updated_at": 0}

        await channel.methods["designer.graph.save"](page(), "cap", params, "design_1", "user-42")

        [(response)] = channel.responses
        if advertised:
            # Written with the marker; nothing answered before the short test timeout.
            assert [payload["params"] for payload in written] == [{**params, MARKER: True}]
            assert written[0]["user_id"] == "user-42"
            assert (response["ok"], response["code"]) == (False, "AGENT_SERVER_TIMEOUT")
        else:
            assert written == []
            assert (response["ok"], response["code"]) == (False, "UNSUPPORTED_DATA_PLANE")


async def test_a_refusal_is_passed_back_with_its_code_and_details(monkeypatch):
    async def send(client, request, *, label, timeout_seconds, **options):
        return SimpleNamespace(
            ok=False,
            payload={
                "error": "canvas was updated elsewhere; reload the latest version before saving again",
                "code": "CONFLICT",
                "current_updated_at": 1795699102917,
            },
        )

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.send_agent_request_with_timeout", send)
    channel = register(ready_client())

    await channel.methods["designer.graph.save"](
        page(), "stale", {"graph": {"graph_id": "g1"}, "base_updated_at": 1}, "design_1", "user-42",
    )

    assert channel.responses == [{
        "id": "stale", "ok": False,
        "payload": {
            "error": "canvas was updated elsewhere; reload the latest version before saving again",
            "code": "CONFLICT", "current_updated_at": 1795699102917,
        },
        "error": "canvas was updated elsewhere; reload the latest version before saving again",
        "code": "CONFLICT",
    }]


@pytest.mark.parametrize("failure,code", [("timeout", "AGENT_SERVER_TIMEOUT"), ("unavailable", "SERVICE_UNAVAILABLE")])
async def test_a_failed_request_is_not_replayed(southbound, monkeypatch, failure, code):
    channel = register(ready_client())

    async def send(client, request, *, label, timeout_seconds, **options):
        southbound.append(request)
        raise AgentRequestTimeoutError()

    if failure == "timeout":
        monkeypatch.setattr("agent_gateway.routing.e2a_proxy.send_agent_request_with_timeout", send)
    else:
        channel = register(None)
    await channel.methods["designer.workspace.session.create"](page(), "once", {"project_id": "default"}, "", "u")

    assert len(southbound) == (1 if failure == "timeout" else 0)
    assert len(channel.responses) == 1 and channel.responses[-1]["code"] == code


def test_the_personal_worker_registers_the_guarded_handlers_and_nothing_else():
    """Wired: in the personal Worker every Designer RPC the page calls is the guarded one."""
    assert "register_personal_designer_handlers" in inspect.getsource(app_web_handlers)
    worker = bind(personal=True, agent_client=ready_client())
    assert designer_methods(worker) == PAGE_METHODS
    # Registered like the other personal Web operations: local and behind their guard.
    assert set(PAGE_METHODS) <= worker.local_only
    assert all(worker.methods[method].__name__ == "guarded" for method in PAGE_METHODS)
    assert "designer.run.pause" not in worker.methods
    assert set(PAGE_METHODS) <= PERSONAL_WEB_OPERATIONS
    # No Designer RPC sits in a Worker allowlist, so none can be registered as the plain proxy.
    assert not [method for method in PERSONAL_WEB_WORKER_METHODS if method.startswith("designer.")]

    def frame(method):
        return f'{{"type": "req", "id": "1", "method": "{method}", "params": {{}}}}'

    for method in sorted(ALL_METHODS):
        assert is_data_frame(frame(method), web=True, personal=True, compat=True) is (method in PAGE_METHODS), method
        # Where the personal compatibility is not installed, nothing of Designer is admitted.
        assert not is_data_frame(frame(method), web=True)
        assert not is_data_frame(frame(method), web=True, personal=True)
        # ...on the Worker's other listener either.
        assert not is_data_frame(frame(method), web=True, personal=True, compat=True, acp=True), method


async def test_the_listing_no_longer_takes_the_unmarked_legacy_proxy(southbound):
    """`designer.graph.list` used to be a whitelisted read that reached the AgentServer unmarked."""
    worker = bind(personal=True, agent_client=ready_client())

    await worker.methods["designer.graph.list"](page(), "list", {"project_id": "default"}, "", "user-42")

    [(request, _, options)] = southbound
    assert request.method == "designer.graph.list"
    assert request.params == {"project_id": "default", MARKER: True}
    assert options == {"required_capability": PERSONAL_DESIGNER_CAPABILITY}


async def test_the_wired_handlers_refuse_before_the_agentserver(southbound):
    """The same two refusals through the registration the Worker uses."""
    worker = bind(personal=True, agent_client=ready_client())
    await worker.methods["designer.graph.save"](SimpleNamespace(request_headers=Headers()), "origin", [], "s", "u")
    await worker.methods["designer.graph.save"](page(), "bad", {"graph_id": "../other"}, "s", "u")
    assert [(r["ok"], r["code"]) for r in worker.responses] == [(False, "FORBIDDEN"), (False, "BAD_REQUEST")]
    assert southbound == []


def test_the_default_profile_keeps_the_plain_proxy_it_always_had():
    """No personal profile: the shared canvases are served as before."""
    channel = bind(personal=False, agent_client=ready_client())
    assert designer_methods(channel) == ALL_METHODS
    assert not any(method.startswith("designer.") for method in channel.local_only)
    assert all(getattr(channel.methods[method], "__name__", "") != "guarded" for method in ALL_METHODS)
    # Nothing subscribes a socket to a canvas there.
    assert not hasattr(channel, "designer_subscriptions")
    assert not [
        hook for hook in channel.disconnect_handlers
        if getattr(hook, "__self__", None).__class__ is DesignerSubscriptions
    ]


def push_wire(*, channel_id="web", session_id="design_1", payload=None, personal=True):
    """The frame the AgentServer emits for a canvas event (``build_server_push_wire``)."""
    payload = payload if payload is not None else {"event_type": "designer.node.updated", "node_id": "n1"}
    chunk = AgentResponseChunk(request_id="designer-push", channel_id=channel_id, payload=payload, is_complete=False)
    wire = encode_agent_chunk_for_wire(chunk, response_id="designer-push", sequence=0)
    metadata = dict(wire.get("metadata") or {})
    if personal:
        metadata["_personal_designer_push"] = True
    metadata[E2A_WIRE_SERVER_PUSH_KEY] = True
    wire["metadata"] = metadata
    if session_id is not None:
        wire["session_id"] = session_id
    return wire


def push_handler(web):
    """A MessageHandler with only what the push route reads."""
    from agent_gateway.message_handler.message_handler import MessageHandler

    handler = object.__new__(MessageHandler)
    handler._server_push_lock = asyncio.Lock()
    handler._stream_sessions = {}
    handler._stream_metadata = {}
    handler._resolve_web_channel = lambda: web
    return handler


class RegularPath(Exception):
    """Raised where the regular session route of a server push begins."""


@pytest.fixture
def regular_path():
    calls = []

    async def regular(chunk):
        calls.append(chunk)
        raise RegularPath()

    return calls, regular


def route(handler, regular):
    handler._handle_login_credential_refresh_push = regular


async def test_a_canvas_push_reaches_the_subscribed_sockets_and_no_regular_route(regular_path):
    channel = register(ready_client())
    first, second, closed = page(), page(), page()
    closed.closed = True
    subscriptions = channel.designer_subscriptions
    subscriptions.add(first, "design_1")
    subscriptions.add(closed, "design_1")
    subscriptions.add(second, "design_2")
    calls, regular = regular_path
    handler = push_handler(channel)
    route(handler, regular)

    await handler._handle_agent_server_push_ordered(push_wire())
    await handler._handle_agent_server_push_ordered(
        push_wire(session_id="design_2", payload={"event_type": "designer.run.updated"}),
    )
    assert channel.events == [
        (first, "designer.node.updated", {"event_type": "designer.node.updated", "node_id": "n1", "session_id": "design_1"}),
        (second, "designer.run.updated", {"event_type": "designer.run.updated", "session_id": "design_2"}),
    ]
    # A push without its canvas session delivers nothing and goes no further.
    await handler._handle_agent_server_push_ordered(push_wire(session_id=None))
    assert len(channel.events) == 2 and calls == []
    # A push on another channel is not the subscriptions' to deliver.
    with pytest.raises(RegularPath):
        await handler._handle_agent_server_push_ordered(push_wire(channel_id="tui"))
    assert len(calls) == 1 and len(channel.events) == 2


@pytest.mark.parametrize("personal, reaches_regular_path", [(True, False), (False, True)])
async def test_a_marked_push_is_dropped_where_nothing_subscribes(regular_path, personal, reaches_regular_path):
    """The single process has no subscriptions: an unmarked push is handled exactly as before."""
    calls, regular = regular_path
    handler = push_handler(SimpleNamespace())
    route(handler, regular)

    if reaches_regular_path:
        with pytest.raises(RegularPath):
            await handler._handle_agent_server_push_ordered(push_wire(personal=personal))
    else:
        await handler._handle_agent_server_push_ordered(push_wire(personal=personal))
    assert len(calls) == int(reaches_regular_path)


@pytest.mark.parametrize("change", [
    {"payload": {"event_type": "session.status", "node_id": "n1"}},
    {"channel_id": "tui"},
])
async def test_other_pushes_keep_their_route_with_subscriptions_installed(regular_path, change):
    channel = register(ready_client())
    channel.designer_subscriptions.add(page(), "design_1")
    calls, regular = regular_path
    handler = push_handler(channel)
    route(handler, regular)

    with pytest.raises(RegularPath):
        await handler._handle_agent_server_push_ordered(push_wire(**change))

    assert len(calls) == 1 and channel.events == []


async def test_the_socket_subscribes_to_the_canvas_the_agentserver_verified(monkeypatch):
    answers, sent = {"session": "design_1"}, []

    async def send(client, request, *, label, timeout_seconds, required_capability=None):
        sent.append(request)
        session = answers.get("session")
        return SimpleNamespace(
            ok=answers.get("ok", True), payload={},
            metadata={"_designer_subscribed_session": session} if session else None,
        )

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.send_agent_request_with_timeout", send)
    channel = register(ready_client())
    subscriptions = channel.designer_subscriptions
    first, other, closed = page(), page(), page()
    closed.closed = True

    async def read(ws, request_id, **extra):
        await channel.methods["designer.graph.get"](
            ws, request_id, {"graph_id": "g1", **extra}, "design_1", "user-42",
        )

    # Only the AgentServer's answer says which canvas a socket may be told about.
    await read(first, "read")
    assert subscriptions.by_session == {"design_1": {id(first): first}}
    # Not the caller's: a session_id in the request, or the connection's, subscribes nothing.
    answers["session"] = None
    await read(other, "unverified", session_id="design_1")
    # Nor a refused answer, nor one for a socket that is already closed.
    answers.update(session="design_9", ok=False)
    await read(other, "refused")
    answers.update(session="design_9", ok=True)
    await read(closed, "closed")
    assert subscriptions.by_session == {"design_1": {id(first): first}}
    # The same socket is subscribed once the AgentServer vouches for it.
    answers["session"] = "design_1"
    await read(other, "verified")
    assert subscriptions.by_session == {"design_1": {id(first): first, id(other): other}}
    assert len(sent) == 5
    # Leaving the page ends its subscription.
    await channel.disconnect_handlers[0](first, [])
    assert subscriptions.by_session == {"design_1": {id(other): other}}
    await channel.disconnect_handlers[0](other, [])
    assert subscriptions.by_session == {} and subscriptions.by_socket == {}


def test_a_socket_keeps_its_latest_32_canvases():
    subscriptions = DesignerSubscriptions(Channel())
    ws = page()
    for index in range(33):
        subscriptions.add(ws, f"design_{index}")
    assert sorted(subscriptions.by_session, key=lambda sid: int(sid.split("_")[1])) == [
        f"design_{index}" for index in range(1, 33)
    ]
    assert list(subscriptions.by_socket[id(ws)]) == [f"design_{index}" for index in range(1, 33)]
    # Asking again for a canvas keeps it, as the most recent one.
    subscriptions.add(ws, "design_1")
    subscriptions.add(ws, "design_33")
    assert "design_1" in subscriptions.by_session and "design_2" not in subscriptions.by_session
    assert len(subscriptions.by_socket[id(ws)]) == 32


async def test_a_push_skips_a_socket_that_closed_before_the_disconnect_hook_ran():
    channel = Channel()
    subscriptions = DesignerSubscriptions(channel)
    ws = page()
    subscriptions.add(ws, "design_1")
    ws.closed = True
    await subscriptions.send("design_1", "designer.node.updated", {"node_id": "n1"})
    assert channel.events == []
