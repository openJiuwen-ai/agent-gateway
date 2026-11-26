"""Web mode requests reach AgentServer once and each reply returns to its own request."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from e2a.agent_compat import e2a_to_agent_request
from e2a.agent_models import AgentResponse
from e2a.message import ReqMethod
from e2a.models import E2AEnvelope
from e2a.wire_codec import encode_agent_response_for_wire

from agent_gateway import app_gateway
from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.channel_manager import ChannelManager
from agent_gateway.channel_manager.web.app_web_handlers import (
    _FORWARD_NO_LOCAL_HANDLER_METHODS,
    _FORWARD_REQ_METHODS,
)
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.routing.agent_client import WebSocketAgentServerClient
from tests._support.null_session_login_bindings import NullSessionLoginBindings


class _ClientSocket:
    """A browser tab: records every frame the Gateway writes to it."""

    closed = False

    def __init__(self, port: int) -> None:
        self.remote_address = ("127.0.0.1", port)
        self.sent: list[dict] = []

    async def send(self, value: str) -> None:
        self.sent.append(json.loads(value))


class _AgentServerSocket:
    """AgentServer's end of the E2A connection: records requests, answers only when told."""

    closed = False

    def __init__(self) -> None:
        self.envelopes: list[E2AEnvelope] = []

    async def send(self, value: str) -> None:
        self.envelopes.append(E2AEnvelope.from_dict(json.loads(value)))


class _RecordingChannelManager:
    def __init__(self) -> None:
        self.delivered = []

    async def deliver_to_message_handler(self, msg) -> None:
        self.delivered.append(msg)


@pytest.fixture(autouse=True)
def _isolated_gateway(monkeypatch, tmp_path):
    monkeypatch.setenv("JIUWENSWARM_DATA_DIR", str(tmp_path / ".jiuwenswarm"))
    monkeypatch.setattr(MessageHandler, "_instance", None)

    async def no_hook(_self, _msg) -> None:
        return None

    monkeypatch.setattr(MessageHandler, "_trigger_before_chat_request_hook", no_hook)


def _mode_frame(request_id: str, method: str, params: dict) -> str:
    return json.dumps({"type": "req", "id": request_id, "method": method, "params": params})


async def _wait_for(predicate) -> None:
    async def poll() -> None:
        while not predicate():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), 5)


async def _flush_writes(channel: WebChannel, *tabs: _ClientSocket) -> None:
    """Write out every frame queued for these tabs; the channel sends through per-socket writers."""
    for tab in tabs:
        ws_id = getattr(tab, "_jiuwen_ws_id", "")
        if ws_id:
            await channel._drain_and_cleanup_writer(tab, ws_id)


def _web_forwarder(manager):
    """The Web inbound callback as _run builds it: the production method lists and normalization."""
    async def forward(msg) -> bool:
        method = getattr(getattr(msg, "req_method", None), "value", None) or ""
        if method not in _FORWARD_REQ_METHODS:
            return False
        await manager.deliver_to_message_handler(app_gateway._normalize_gateway_message(msg))
        return method in _FORWARD_NO_LOCAL_HANDLER_METHODS

    return forward


def _recording_web_channel() -> tuple[WebChannel, _RecordingChannelManager]:
    """The Web forwarder in front of a manager that only records what reaches it."""
    manager = _RecordingChannelManager()
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.on_message(_web_forwarder(manager))
    return channel, manager


def _connect(channel: WebChannel, port: int, user_id: str = "alice") -> _ClientSocket:
    tab = _ClientSocket(port)
    channel._resolve_connection_user_id({"user_id": user_id}, tab)
    return tab


@asynccontextmanager
async def _gateway_with_agent_server():
    """The Web forwarder, the production MessageHandler and E2A client, and an AgentServer the test answers."""
    client = WebSocketAgentServerClient()
    agent = _AgentServerSocket()
    client._ws = agent
    client._server_ready = True
    handler = MessageHandler(client, session_login_bindings=NullSessionLoginBindings())
    manager = ChannelManager(handler, {})
    handler.set_channel_manager(manager)
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    manager.register_channel_with_inbound(channel, _web_forwarder(manager))
    await handler.start_forwarding()
    await manager.start_dispatch()
    try:
        yield SimpleNamespace(client=client, agent=agent, channel=channel)
    finally:
        await asyncio.wait_for(manager.stop_dispatch(), 5)
        await asyncio.wait_for(handler.stop_forwarding(), 5)
        await asyncio.wait_for(channel._shutdown_all_writers(), 5)


def _answer(gateway: SimpleNamespace, env: E2AEnvelope, *, ok: bool, payload: dict) -> None:
    """Deliver AgentServer's reply to one forwarded request."""
    reply = AgentResponse(request_id=env.request_id, channel_id="web", ok=ok, payload=payload)
    gateway.client._message_queues[env.request_id].put_nowait(
        encode_agent_response_for_wire(reply, response_id=env.request_id),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["mode.set", "mode.get"])
async def test_web_mode_frame_forwards_once_without_local_reply(method: str) -> None:
    """The Web forwarder hands the frame to MessageHandler unchanged and the channel stays silent."""
    channel, manager = _recording_web_channel()
    tab = _connect(channel, 1001)
    params = {"session_id": "saved-session", "mode": "team.work.normal", "expected_mode": "agent.work.normal"}

    await channel._handle_raw_message(tab, _mode_frame("mode-1", method, params), {})
    await _flush_writes(channel, tab)

    assert len(manager.delivered) == 1
    forwarded = manager.delivered[0]
    assert forwarded.req_method is ReqMethod(method)
    assert (forwarded.id, forwarded.channel_id, forwarded.session_id) == ("mode-1", "web", "saved-session")
    assert forwarded.user_id == "alice"
    # Pass-through: the Gateway adds, removes and rewrites nothing in a mode request's params.
    assert forwarded.params == params
    # Neither a local acknowledgement nor METHOD_NOT_FOUND: AgentServer owns the reply.
    assert tab.sent == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [3, ["team.work.normal"]])
async def test_non_string_mode_is_refused_locally_and_not_forwarded(mode) -> None:
    """The frame-level check answers a non-string mode itself; AgentServer never sees it."""
    channel, manager = _recording_web_channel()
    tab = _connect(channel, 1001)
    # A real connection gets its writer at handshake; this refusal comes before any session registration.
    await channel.register_ws(tab, None)
    params = {"session_id": "saved-session", "mode": mode, "expected_mode": "agent.work.normal"}

    await channel._handle_raw_message(tab, _mode_frame("bad-type", "mode.set", params), {})
    await _flush_writes(channel, tab)

    assert manager.delivered == []
    assert [(f["type"], f["id"], f["ok"], f.get("code")) for f in tab.sent] == [
        ("res", "bad-type", False, "BAD_REQUEST"),
    ]


@pytest.mark.asyncio
async def test_mode_request_carries_the_connection_identity_not_frame_params() -> None:
    """AgentServer authorizes against the connection's user and channel; frame params cannot change them."""
    async with _gateway_with_agent_server() as gateway:
        tab = _connect(gateway.channel, 1001, user_id="alice")
        params = {
            "session_id": "sess-1", "mode": "team.work.normal", "expected_mode": "agent.work.normal",
            "user_id": "mallory", "channel_id": "tui",
        }
        await gateway.channel._handle_raw_message(tab, _mode_frame("set-1", "mode.set", params), {})
        await _wait_for(lambda: len(gateway.agent.envelopes) == 1)

        env = gateway.agent.envelopes[0]
        request = e2a_to_agent_request(env)
        assert (request.user_id, request.channel_id) == ("alice", "web")

        _answer(gateway, env, ok=False, payload={"error": "session not found", "code": "NOT_FOUND"})
        await _wait_for(lambda: tab.sent)


@pytest.mark.asyncio
@pytest.mark.parametrize("method, params, code", [
    ("mode.set", {"session_id": "sess-1", "mode": "team.design", "expected_mode": "agent.work.normal"}, "BAD_REQUEST"),
    ("mode.set", {"session_id": "sess-1", "mode": "team", "expected_mode": "agent.work.normal"}, "BAD_REQUEST"),
    ("mode.set", {"session_id": "sess-1", "mode": "agent.work.bogus", "expected_mode": "agent.work.normal"},
     "BAD_REQUEST"),
    ("mode.set", {"session_id": "sess-1", "mode": "team.work.normal"}, "BAD_REQUEST"),
    ("mode.get", {"session_id": "no-such-session"}, "NOT_FOUND"),
])
async def test_rejected_mode_request_is_forwarded_unchanged_and_its_error_returns_to_the_tab(
    method: str, params: dict, code: str,
) -> None:
    """The Gateway does not judge mode values or Sessions: it forwards them and relays AgentServer's refusal."""
    async with _gateway_with_agent_server() as gateway:
        tab, other = _connect(gateway.channel, 1001), _connect(gateway.channel, 1002)
        await gateway.channel._handle_raw_message(tab, _mode_frame("bad-1", method, params), {})
        await _wait_for(lambda: len(gateway.agent.envelopes) == 1)

        env = gateway.agent.envelopes[0]
        assert (env.method, env.session_id, env.params) == (method, params["session_id"], params)

        _answer(gateway, env, ok=False, payload={"error": "refused", "code": code})
        await _wait_for(lambda: tab.sent)
        await _flush_writes(gateway.channel, tab, other)

        assert [(f["type"], f["id"], f["ok"], f.get("code")) for f in tab.sent] == [("res", "bad-1", False, code)]
        assert other.sent == []
        assert not gateway.client._message_queues


@pytest.mark.asyncio
async def test_concurrent_web_mode_sets_return_commit_and_conflict_to_their_own_tabs() -> None:
    """Two tabs switch one Session at once from the same view.

    AgentServer serializes them: the first commits and the second, whose
    expected_mode is now stale, gets MODE_CONFLICT. The Gateway neither queues
    nor decides; it returns each answer, even out of order, to the tab that asked.
    """
    async with _gateway_with_agent_server() as gateway:
        tab_a, tab_b = _connect(gateway.channel, 1001), _connect(gateway.channel, 1002)
        await gateway.channel._handle_raw_message(tab_a, _mode_frame("set-a", "mode.set", {
            "session_id": "sess-1", "mode": "team.work.normal", "expected_mode": "agent.work.normal",
        }), {})
        await gateway.channel._handle_raw_message(tab_b, _mode_frame("set-b", "mode.set", {
            "session_id": "sess-1", "mode": "agent.code.normal", "expected_mode": "agent.work.normal",
        }), {})
        await _wait_for(lambda: len(gateway.agent.envelopes) == 2)
        envelopes = {env.request_id: env for env in gateway.agent.envelopes}
        assert set(envelopes) == {"set-a", "set-b"}
        assert {env.method for env in envelopes.values()} == {"mode.set"}
        assert {env.session_id for env in envelopes.values()} == {"sess-1"}

        # AgentServer answers the conflicting request first, then the one it committed.
        _answer(gateway, envelopes["set-b"], ok=False, payload={
            "error": "session mode changed from 'agent.work.normal' to 'team.work.normal'", "code": "MODE_CONFLICT",
        })
        _answer(gateway, envelopes["set-a"], ok=True, payload={"mode": "team.work.normal", "changed": True})
        await _wait_for(lambda: tab_a.sent and tab_b.sent)
        # Write out anything else queued (a local reply or a misrouted copy) before comparing.
        await _flush_writes(gateway.channel, tab_a, tab_b)

        assert [(f["type"], f["id"], f["ok"], f["payload"].get("mode")) for f in tab_a.sent] == [
            ("res", "set-a", True, "team.work.normal"),
        ]
        assert [(f["type"], f["id"], f["ok"], f.get("code")) for f in tab_b.sent] == [
            ("res", "set-b", False, "MODE_CONFLICT"),
        ]
        assert not gateway.client._message_queues
