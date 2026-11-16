"""Review commands must use the routed AgentServer locale in enterprise edition."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from e2a.message import Message, ReqMethod

from agent_gateway.message_handler.message_handler import ChannelControlState, MessageHandler
from agent_gateway.message_handler.prompts.review_prompt import build_review_prompt
from agent_gateway.message_handler.prompts.security_review_prompt import build_security_review_prompt


class _LocaleClient:
    def __init__(self):
        self.requests = []
        self.language = "en"
        self.response = None
        self.error = None

    async def send_request(self, envelope):
        self.requests.append(envelope)
        assert envelope.method == "locale.get_conf"
        if self.error is not None:
            raise self.error
        return self.response or SimpleNamespace(ok=True, payload={"preferred_language": self.language})


@pytest.fixture
def handler(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    monkeypatch.setattr("agent_gateway.common.config.get_config", lambda: {})
    monkeypatch.setattr(MessageHandler, "_instance", None)
    instance = MessageHandler(_LocaleClient())
    instance._get_config_raw = lambda: {}
    instance._gateway_hook_handler = None
    monkeypatch.setattr(instance, "_ensure_inbound_session", AsyncMock())
    monkeypatch.setattr(instance, "_maybe_register_godview", AsyncMock())
    monkeypatch.setattr(instance, "_trigger_before_chat_request_hook", AsyncMock())
    monkeypatch.setattr(instance, "_publish_hook_params_update_if_changed", AsyncMock())
    instance.sent_chats = []

    async def dispatch(_message, envelope):
        instance.sent_chats.append(envelope)

    monkeypatch.setattr(instance, "_start_non_stream_chat_task", dispatch)
    monkeypatch.setattr(instance, "publish_robot_messages", AsyncMock())
    return instance


def _message(command="/review 42", channel="web", user="alice", group="group-a", app="bot-a"):
    return Message(
        id=f"review-{user}-{app}", type="req", channel_id=channel,
        session_id="external-session", params={"query": command, "agent_type": "jiuwenswarm"},
        timestamp=0, ok=True, req_method=ReqMethod.CHAT_SEND, is_stream=False,
        user_id=user, app_id=app, agent_ref={"mode": "single", "id": "expert-a"},
        metadata={"group_id": group, "bot_id": app, "ws_id": "ws-a"},
    )


async def _forward(handler, monkeypatch, *messages):
    pending = list(messages)

    async def consume(timeout=None):
        if pending:
            return pending.pop(0)
        handler._running = False
        return None

    monkeypatch.setattr(handler, "consume_user_messages", consume)
    handler._running = True
    await asyncio.wait_for(handler._forward_loop(), timeout=5)


def _forbid_local_config(monkeypatch):
    def fail_local():
        pytest.fail("Enterprise review must not read Gateway preferred_language")
    monkeypatch.setattr("agent_gateway.message_handler.prompts.get_config", fail_local)


@pytest.mark.parametrize("builder", [build_review_prompt, build_security_review_prompt])
@pytest.mark.parametrize("language", ["en", "zh"])
def test_explicit_review_language_does_not_read_local_config(monkeypatch, builder, language):
    _forbid_local_config(monkeypatch)
    prompt = builder("42", preferred_language=language)
    expected = "Respond in English." if language == "en" else "Respond in Chinese (simplified)."
    assert expected in prompt


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["/review 42", "/security-review focus on auth"])
@pytest.mark.parametrize("channel", ["web", "dingtalk"])
@pytest.mark.parametrize("language", ["en", "zh"])
async def test_enterprise_review_uses_resolved_route_language(handler, monkeypatch, command, channel, language):
    _forbid_local_config(monkeypatch)
    handler.agent_client.language = language
    message = _message(command, channel)
    handler._channel_states[handler._get_channel_state_key(channel, message.session_id)] = (
        ChannelControlState(session_id="resolved-session")
    )

    await _forward(handler, monkeypatch, message)

    assert handler.publish_robot_messages.await_count == 0
    assert len(handler.sent_chats) == 1
    assert len(handler.agent_client.requests) == 1
    query, chat = handler.agent_client.requests[0], handler.sent_chats[0]
    assert query.user_id == chat.user_id == "alice"
    assert query.channel == chat.channel == channel
    assert query.session_id == chat.session_id
    if channel == "dingtalk":
        assert query.session_id == "resolved-session"
    assert query.channel_context == chat.channel_context
    assert query.agent_ref == chat.agent_ref
    assert query.params == {"agent_type": "jiuwenswarm"}
    assert query.request_id != chat.request_id
    assert query.is_stream is False
    expected = "Respond in English." if language == "en" else "Respond in Chinese (simplified)."
    assert expected in chat.params["query"]
    opposite = "Respond in Chinese (simplified)." if language == "en" else "Respond in English."
    assert opposite not in chat.params["query"]


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["/review 42", "/security-review focus on auth"])
@pytest.mark.parametrize("channel", ["web", "dingtalk"])
@pytest.mark.parametrize("language", ["en", "zh"])
async def test_personal_review_preserves_local_language(handler, monkeypatch, command, channel, language):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    monkeypatch.setattr("agent_gateway.message_handler.prompts.get_config", lambda: {"preferred_language": language})
    await _forward(handler, monkeypatch, _message(command, channel))
    assert handler.agent_client.requests == []
    assert handler.publish_robot_messages.await_count == 0
    assert len(handler.sent_chats) == 1
    expected = "Respond in English." if language == "en" else "Respond in Chinese (simplified)."
    assert expected in handler.sent_chats[0].params["query"]


@pytest.mark.asyncio
async def test_review_language_isolated_by_user_and_routing_metadata(handler, monkeypatch):
    _forbid_local_config(monkeypatch)
    messages = [
        _message(user="alice", group="group-a", app="bot-a"),
        _message(user="bob", group="group-a", app="bot-a"),
        _message(user="alice", group="group-b", app="bot-b"),
    ]
    expected = {
        ("alice", "group-a", "bot-a"): "en",
        ("bob", "group-a", "bot-a"): "zh",
        ("alice", "group-b", "bot-b"): "zh",
    }

    async def send(envelope):
        handler.agent_client.requests.append(envelope)
        await asyncio.sleep(0)  # Interleave requests on the same client.
        identity = (envelope.user_id, envelope.channel_context["group_id"], envelope.channel_context["bot_id"])
        return SimpleNamespace(ok=True, payload={"preferred_language": expected[identity]})

    monkeypatch.setattr(handler.agent_client, "send_request", send)
    languages = await asyncio.gather(*(handler._resolve_review_prompt_language(message) for message in messages))
    assert languages == ["en", "zh", "zh"]
    for query, message in zip(handler.agent_client.requests, messages):
        original = handler.message_to_e2a(message)
        assert query.channel_context == original.channel_context
        assert query.agent_ref == original.agent_ref
        assert query.user_id == original.user_id
        assert query.session_id == original.session_id


@pytest.mark.asyncio
async def test_review_reads_language_again_after_target_config_changes(handler, monkeypatch):
    _forbid_local_config(monkeypatch)
    message = _message()
    assert await handler._resolve_review_prompt_language(message) == "en"
    handler.agent_client.language = "zh"
    assert await handler._resolve_review_prompt_language(message) == "zh"
    requests = handler.agent_client.requests
    assert len(requests) == 2
    assert requests[0].request_id != requests[1].request_id
    assert message.params["query"] == "/review 42"


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["/review 42", "/security-review"])
@pytest.mark.parametrize("failure", ["offline", "rejected", "missing", "invalid", "not-string"])
async def test_enterprise_locale_failure_stops_review_without_local_fallback(
    handler, monkeypatch, command, failure,
):
    _forbid_local_config(monkeypatch)
    if failure == "offline":
        handler.agent_client.error = ConnectionError("AgentServer offline")
    else:
        payload = {
            "rejected": {"error": "not ready"}, "missing": {},
            "invalid": {"preferred_language": "fr"}, "not-string": {"preferred_language": 123},
        }[failure]
        handler.agent_client.response = SimpleNamespace(ok=failure != "rejected", payload=payload)
    message = _message(command)
    await _forward(handler, monkeypatch, message)
    assert handler.sent_chats == []
    assert message.params["query"] == command
    handler.publish_robot_messages.assert_awaited_once()
    error = handler.publish_robot_messages.await_args.args[0]
    assert error.ok is False
    assert error.payload["code"] == "SERVICE_UNAVAILABLE"
    assert "AgentServer" in error.payload["error"]


@pytest.mark.asyncio
async def test_acp_locale_query_preserves_internal_session_alias(handler):
    handler._acp_session_aliases["external-session"] = "internal-session"
    message = _message(channel="acp")
    assert await handler._resolve_review_prompt_language(message) == "en"
    query = handler.agent_client.requests[0]
    assert query.session_id == "internal-session"
    assert query.channel_context["acp_original_session_id"] == "external-session"
    assert message.session_id == "external-session"


@pytest.mark.asyncio
async def test_ordinary_enterprise_chat_does_not_query_locale(handler, monkeypatch):
    await _forward(handler, monkeypatch, _message("hello"))
    assert handler.agent_client.requests == []
    assert len(handler.sent_chats) == 1
    assert handler.sent_chats[0].params["query"] == "hello"


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["/review 42", "/security-review"])
async def test_busy_enterprise_im_review_still_injects_target_language(handler, monkeypatch, command):
    _forbid_local_config(monkeypatch)
    monkeypatch.setattr(handler, "_session_has_streams_blocking_processing_false", lambda _sid: True)
    handler._channel_states[handler._get_channel_state_key("dingtalk", "external-session")] = (
        ChannelControlState(session_id="resolved-session")
    )
    await _forward(handler, monkeypatch, _message(command, "dingtalk"))
    assert handler.publish_robot_messages.await_count == 0
    assert len(handler.sent_chats) == 1
    assert len(handler.agent_client.requests) == 1
    assert handler.agent_client.requests[0].session_id == "resolved-session"
    assert handler.sent_chats[0].params["input_mode"] == "steer"
    assert "Respond in English." in handler.sent_chats[0].params["query"]
