# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""The shared northbound handlers of a static Worker serve one named user per frame."""

import asyncio
from types import SimpleNamespace

import pytest
from e2a.agent_models import AgentResponse
from e2a.message import EventType, Message, ReqMethod

from agent_gateway.application.worker_routed_northbound import bind_routed_delivery
from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.protocol.acp.acp_connect import AcpGatewayBridge
from agent_gateway.channel_manager.tui.tui_channel import TuiChannel
from agent_gateway.routing.protocol_runtime import ProtocolRuntime, ProtocolRuntimeConfig
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.routing.owner_delivery import OwnerDelivery
from tests._support.null_session_login_bindings import NullSessionLoginBindings

pytestmark = pytest.mark.asyncio


class _RecordingClient:
    """Answers every request locally and records what the handler dispatched."""

    def __init__(self, answers: dict[str, dict] | None = None) -> None:
        self.requests: list = []
        self.answers = answers or {}

    async def send_request(self, envelope, **_kwargs):
        self.requests.append(envelope)
        payload = self.answers.get(envelope.method, {"ok": True})
        return AgentResponse(
            request_id=envelope.request_id, channel_id=envelope.channel, payload=dict(payload), ok=True,
        )

    def methods(self) -> list[str]:
        return [str(item.method) for item in self.requests]


@pytest.fixture(autouse=True)
def _fresh_handler_singleton():
    # MessageHandler is a process singleton: every test owns a fresh one.
    _reset_handler()
    yield
    _reset_handler()


def _reset_handler() -> None:
    setattr(MessageHandler, "_instance", None)
    if "_singleton_initialized" in MessageHandler.__dict__:
        del MessageHandler._singleton_initialized


def _handler(client=None) -> MessageHandler:
    _reset_handler()
    return MessageHandler(client or _RecordingClient(), session_login_bindings=NullSessionLoginBindings())


def _request(user, *, channel="acp", request="r1", session="s1", content="hello") -> Message:
    return Message(id=request, type="req", channel_id=channel, session_id=session, params={"content": content},
                   timestamp=0, ok=True, req_method=ReqMethod.CHAT_SEND, user_id=user)


def _response(user=None, *, request="r1", channel="acp", event=EventType.CHAT_FINAL) -> Message:
    return Message(id=request, type="event", channel_id=channel, session_id="s1", params={}, timestamp=0, ok=True,
                   req_method=ReqMethod.CHAT_SEND, user_id=user, event_type=event, payload={"event_type": event.value})


def _components(routed=None):
    web = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel = TuiChannel()
    # The TUI/ACP assembly owns the channel and the protocol runtime.
    tui = SimpleNamespace(tui_channel=channel, protocol_runtime=ProtocolRuntime(
        ProtocolRuntimeConfig(enabled=True), RobotMessageRouter(),
    ))
    handler = _handler()
    return SimpleNamespace(routed=routed, handler=handler, web=web, tui=tui, channel=channel)


def _routed(**users: str):
    return SimpleNamespace(
        user_routing=True, request_owner=lambda rid: users.get(rid), session_owner=lambda _sid: None,
    )


async def test_binding_requires_every_shared_handler_and_a_routed_client() -> None:
    parts = _components(_routed())
    with pytest.raises(ValueError, match="routed Worker client"):
        bind_routed_delivery(SimpleNamespace(user_routing=False), handler=parts.handler, web=parts.web, tui=parts.tui)
    for name in ("handler", "web", "tui"):
        with pytest.raises(ValueError, match=name):
            bind_routed_delivery(
                parts.routed, handler=None if name == "handler" else parts.handler,
                web=None if name == "web" else parts.web, tui=None if name == "tui" else parts.tui,
            )
    with pytest.raises(ValueError, match="protocol runtime"):
        bind_routed_delivery(parts.routed, handler=parts.handler, web=parts.web, tui=SimpleNamespace())
    with pytest.raises(ValueError, match="TUI channel"):
        bind_routed_delivery(parts.routed, handler=parts.handler, web=parts.web, tui=SimpleNamespace(
            protocol_runtime=parts.tui.protocol_runtime,
        ))


async def test_one_gate_reaches_every_outbound_layer() -> None:
    parts = _components(_routed(r1="alice"))
    gate = bind_routed_delivery(parts.routed, handler=parts.handler, web=parts.web, tui=parts.tui)
    assert isinstance(gate, OwnerDelivery)
    assert parts.web.owner_delivery is gate
    assert parts.channel.owner_delivery is gate
    assert parts.tui.protocol_runtime.owner_delivery is gate
    assert parts.tui.protocol_runtime._acp_bridge.bind_request_user is True
    assert parts.handler._owner_delivery is True
    # Unbound handlers keep the single-AgentServer behaviour.
    assert _components().web.owner_delivery is None
    assert _handler()._owner_delivery is False


async def test_handler_names_the_user_of_the_request_on_every_response() -> None:
    handler = _handler()
    untouched = _response()
    # Single-AgentServer mode: the very same Message object is queued.
    assert handler._bind_response_owner(untouched) is untouched
    handler.set_owner_delivery(True, request_owner={"r1": "alice", "r-stream": "bob"}.get)
    assert handler._bind_response_owner(_response(None)) .user_id == "alice"
    # A declared user never overrides the route the request was admitted for.
    assert handler._bind_response_owner(_response("mallory")).user_id == "alice"
    # The live stream record names the user while the request runs.
    handler._stream_user_ids["r-stream"] = "bob"
    assert handler._bind_response_owner(_response(None, request="r-stream")).user_id == "bob"
    # A response to an unknown request keeps what it declares, or nobody.
    assert handler._bind_response_owner(_response(None, request="unknown")).user_id is None
    declared = _response("carol", request="unknown")
    assert handler._bind_response_owner(declared) is declared
    with pytest.raises(ValueError):
        handler.set_owner_delivery(True, request_owner=None)
    with pytest.raises(ValueError):
        handler.set_owner_delivery(1, request_owner=dict.get)
    assert handler.request_owner(" ") is None


async def test_published_responses_carry_the_owner_and_late_ones_keep_it() -> None:
    handler = _handler()
    routes = {"web-1": "alice"}
    handler.set_owner_delivery(True, request_owner=routes.get)
    await handler.publish_robot_messages(_response(None, request="web-1", channel="web"))
    handler.publish_robot_messages_nowait(_response(None, request="web-1", channel="web"))
    # The stream record is gone once a request finished; the route still names the user.
    first = await handler.consume_robot_messages()
    second = await handler.consume_robot_messages()
    assert first.user_id == "alice" and second.user_id == "alice"


async def test_another_users_request_never_cancels_a_stream() -> None:
    client = _RecordingClient(answers={ReqMethod.SESSION_CREATE.value: {"sessionId": "alice-internal"}})
    handler = _handler(client)
    handler.set_owner_delivery(True, request_owner=lambda _rid: None)

    async def stream() -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(stream())
    handler._stream_tasks["acp-alice"] = task
    handler._stream_channels["acp-alice"] = "acp"
    handler._stream_sessions["acp-alice"] = "alice-session"
    handler._stream_user_ids["acp-alice"] = "alice"
    try:
        # ACP replaces orphan work of any session: only for the same user now.
        assert await handler._cancel_stream_tasks_for_channel(
            _request("bob", request="acp-bob", session="bob-s"),
        ) == 0
        assert not task.done() and client.requests == []
        assert await handler._cancel_stream_tasks_for_channel(_request("alice", request="acp-alice-2")) == 1
        await asyncio.gather(task, return_exceptions=True)
        assert task.cancelled()
        # The interrupt goes south with the owner of the cancelled work.
        assert [str(item.method) for item in client.requests] == [
            ReqMethod.SESSION_CREATE.value, ReqMethod.CHAT_CANCEL.value,
        ]
        assert all(item.user_id == "alice" for item in client.requests)
        assert "acp-alice" not in handler._stream_user_ids
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_acp_session_is_created_for_the_user_who_prompted() -> None:
    client = _RecordingClient(answers={ReqMethod.SESSION_CREATE.value: {"sessionId": "internal-1"}})
    handler = _handler(client)
    internal, aliased = await handler._resolve_acp_internal_session_id("acp_external", "alice")
    assert internal == "internal-1" and aliased
    assert client.requests[0].user_id == "alice"
    dispatch = await handler._prepare_agent_dispatch_message(_request("alice", session="acp_external"))
    assert dispatch.session_id == "internal-1"
    assert dispatch.params["session_id"] == "internal-1"
    assert dispatch.metadata["acp_original_session_id"] == "acp_external"
    assert dispatch.user_id == "alice"
    # A single-AgentServer gateway keeps creating the session without a user.
    handler.agent_client = plain_client = _RecordingClient(
        answers={ReqMethod.SESSION_CREATE.value: {"sessionId": "internal-2"}},
    )
    handler._acp_session_aliases.clear()
    plain = handler
    await plain._resolve_acp_internal_session_id("acp_external")
    assert plain_client.requests[0].user_id is None


async def test_acp_prompt_message_names_the_user_of_its_connection() -> None:
    async def capture(msg):
        captured.append(msg)

    captured: list[Message] = []
    bridge = AcpGatewayBridge(capture)
    alice = SimpleNamespace(_gateway_user_id="alice")

    async def send(_data: str) -> None:
        return None

    alice.send = send
    prompt = {"jsonrpc": "2.0", "id": 7, "method": "session/prompt",
              "params": {"sessionId": "acp_one", "prompt": [{"type": "text", "text": "hi"}]}}
    assert await bridge.handle_jsonrpc_request(alice, prompt)
    assert len(captured) == 1
    # Unbound: the single-AgentServer Message is unchanged.
    assert captured[0].user_id is None
    bridge.bind_request_user = True
    assert await bridge.handle_jsonrpc_request(alice, prompt)
    assert captured[1].user_id == "alice"
    assert captured[1].channel_id == "acp" and captured[1].session_id == "acp_one"
