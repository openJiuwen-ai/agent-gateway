"""DingTalk lifecycle transport through the existing internal host route."""
import asyncio
import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from dingtalk_stream import EventMessage

from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect import DingTalkGroupEventHandler
from agent_gateway.channel_manager.im_platforms.dingtalk.group_events import DingTalkGroupEvent
from agent_gateway.channel_manager.persistent_events import InboundDisposition
from agent_gateway.message_handler.message_handler import MessageHandler
from tests.unit_tests.gateway.test_persistent_dingtalk_transport import _channel

def _lifecycle_message(kind="chat_update_title"):
    return SimpleNamespace(data={"eventType": kind, "eventId": "lifecycle-1", "eventCorpId": "corp",
        "data": {"timeStamp": 1793750400000, "openConversationId": "cid-one", "corpId": "corp", "title": "New group"}},
        headers=None)


def _sdk_lifecycle_message(kind="chat_disband"):
    return EventMessage.from_dict({
        "specVersion": "1.0",
        "type": "EVENT",
        "headers": {
            "eventType": kind, "eventId": "sdk-event", "eventCorpId": "sdk-corp",
            "eventBornTime": str(int(datetime.now(timezone.utc).timestamp() * 1000)),
        },
        "data": json.dumps({"openConversationId": "sdk-group", "title": "SDK group"}),
    })


def _sdk_payload():
    message = _sdk_lifecycle_message()
    return DingTalkGroupEvent.from_stream(
        message.data, app_id="app-test", agent_id="agent-dingtalk", headers=message.headers,
    ).to_payload()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", [
    "chat_add_member", "chat_remove_member", "chat_quit", "chat_update_title", "chat_disband",
])
async def test_real_sdk_headers_deliver_all_group_events(kind):
    channel = _channel()
    channel._persistent_inbound_callback = AsyncMock(return_value=InboundDisposition.HANDLED)
    message = _sdk_lifecycle_message(kind)

    assert "eventType" not in message.data
    assert (await DingTalkGroupEventHandler(channel).process(message))[0] == 200
    channel._persistent_inbound_callback.assert_awaited_once()
    event = channel._persistent_inbound_callback.await_args.args[0]
    assert (event.event_type, event.event_id, event.corp_id) == (kind, "sdk-event", "sdk-corp")
    assert event.conversation_id == "sdk-group"
    assert event.changed_at == datetime.fromtimestamp(message.headers.event_born_time / 1000, timezone.utc)
    assert DingTalkGroupEvent.from_payload(json.loads(json.dumps(event.to_payload()))) == event


@pytest.mark.asyncio
@pytest.mark.parametrize("disposition", ["handled", "rejected", "unclaimed", "retryable", None])
async def test_host_callback_contract_controls_lifecycle_ack(disposition):
    host = SimpleNamespace(agent_client=SimpleNamespace(send_request=AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={"disposition": disposition}),
    )))
    channel = _channel()

    async def callback(event):
        return await MessageHandler.handle_persistent_dingtalk_event(host, event)

    channel.set_persistent_inbound_callback(callback)
    code, _ = await DingTalkGroupEventHandler(channel).process(_sdk_lifecycle_message())

    assert code == (200 if disposition == "handled" else 500)
    host.agent_client.send_request.assert_awaited_once()
    payload = host.agent_client.send_request.await_args.args[0].params["event"]
    assert payload["event_kind"] == "group_lifecycle"
    assert payload["expected_agent_id"] == "agent-dingtalk"


@pytest.mark.asyncio
async def test_missing_lifecycle_callback_is_not_acknowledged_as_success():
    channel = _channel()
    channel.set_persistent_inbound_callback(None)
    assert (await DingTalkGroupEventHandler(channel).process(_sdk_lifecycle_message()))[0] == 500


@pytest.mark.parametrize("field", [
    "receiving_app_id", "expected_agent_id", "event_type", "event_id",
    "conversation_id", "changed_at", "received_at", "corp_id",
])
def test_lifecycle_payload_missing_required_fields_raises_value_error(field):
    payload = _sdk_payload()
    payload.pop(field)
    with pytest.raises(ValueError, match="invalid DingTalk lifecycle payload"):
        DingTalkGroupEvent.from_payload(payload)


@pytest.mark.parametrize("field", ["changed_at", "received_at"])
@pytest.mark.parametrize("value", [None, {}, "invalid-time", "2026-11-10T00:00:00"])
def test_lifecycle_payload_invalid_timestamp_raises_value_error(field, value):
    payload = {**_sdk_payload(), field: value}
    with pytest.raises(ValueError, match="invalid DingTalk lifecycle payload"):
        DingTalkGroupEvent.from_payload(payload)


@pytest.mark.parametrize("changes", [{"unexpected": True}, {"event_type": []}])
def test_lifecycle_payload_invalid_schema_raises_value_error(changes):
    payload = {**_sdk_payload(), **changes}
    with pytest.raises(ValueError, match="invalid DingTalk lifecycle payload"):
        DingTalkGroupEvent.from_payload(payload)


@pytest.mark.asyncio
async def test_lifecycle_uses_existing_internal_route_and_does_not_apply_human_allowlist():
    channel = _channel()
    channel._persistent_inbound_callback = AsyncMock(return_value="handled")
    code, _ = await DingTalkGroupEventHandler(channel).process(_lifecycle_message())
    assert code == 200
    event = channel._persistent_inbound_callback.await_args.args[0]
    assert event.receiving_app_id == "app-test" and event.expected_agent_id == "agent-dingtalk"
    assert event.to_payload()["event_kind"] == "group_lifecycle"


@pytest.mark.asyncio
async def test_lifecycle_failure_is_not_acknowledged_as_success_and_unknown_events_are_ignored():
    channel = _channel()
    channel._persistent_inbound_callback = AsyncMock(side_effect=RuntimeError("private upstream detail"))
    code, message = await DingTalkGroupEventHandler(channel).process(_lifecycle_message())
    assert code == 500 and "private" not in message
    channel._persistent_inbound_callback.reset_mock()
    assert (await DingTalkGroupEventHandler(channel).process(_lifecycle_message("user_add_org")))[0] == 200
    channel._persistent_inbound_callback.assert_not_awaited()


@pytest.mark.parametrize("bound", [True, False])
def test_stream_registers_lifecycle_handler_only_for_jiuwenmate(monkeypatch, bound):
    from agent_gateway.channel_manager.im_platforms.dingtalk import dingtalk_connect

    client = MagicMock()
    monkeypatch.setattr(dingtalk_connect, "DingTalkStreamClient", MagicMock(return_value=client))
    channel = _channel()
    channel.config.agent_id = "agent-dingtalk" if bound else ""
    channel._initialize_stream_client()
    client.register_callback_handler.assert_called_once()
    assert client.register_all_event_handler.call_count == int(bound)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chat_add_member", "chat_remove_member", "chat_quit", "chat_update_title", "chat_disband"])
async def test_all_subscribed_events_keep_bound_identity_and_json_contract(kind):
    import json
    from agent_gateway.channel_manager.im_platforms.dingtalk.group_events import DingTalkGroupEvent
    c = _channel()
    c._persistent_inbound_callback = AsyncMock(return_value="handled")
    assert (await DingTalkGroupEventHandler(c).process(_lifecycle_message(kind)))[0] == 200
    event = c._persistent_inbound_callback.await_args.args[0]
    payload = json.loads(json.dumps(event.to_payload()))
    assert payload["event_type"] == kind
    assert payload["receiving_app_id"] == "app-test"
    assert payload["expected_agent_id"] == "agent-dingtalk"
    assert DingTalkGroupEvent.from_payload(payload) == event


@pytest.mark.asyncio
@pytest.mark.parametrize("broken", ["missing_id", "conflicting_corp", "bad_time", "conflicting_type"])
async def test_malformed_lifecycle_never_reaches_host(broken):
    c = _channel()
    c._persistent_inbound_callback = AsyncMock()
    message = _lifecycle_message()
    if broken == "missing_id":
        message.data.pop("eventId")
    elif broken == "conflicting_corp":
        message.data["data"]["corpId"] = "other-corp"
    elif broken == "bad_time":
        message.data["data"]["timeStamp"] = True
    else:
        message.headers = SimpleNamespace(event_type="chat_disband")
    assert (await DingTalkGroupEventHandler(c).process(message))[0] == 400
    c._persistent_inbound_callback.assert_not_awaited()


@pytest.mark.asyncio
async def test_unbound_channel_does_not_forward_lifecycle():
    c = _channel()
    c.config.agent_id = ""
    c._persistent_inbound_callback = AsyncMock()
    assert (await DingTalkGroupEventHandler(c).process(_lifecycle_message()))[0] == 200
    c._persistent_inbound_callback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["user_add_org", "chat_future_event", None])
async def test_ignored_stream_events_keep_safe_diagnostic_identity(caplog, kind):
    channel = _channel()
    channel._persistent_inbound_callback = AsyncMock()
    message = _sdk_lifecycle_message(kind) if kind else SimpleNamespace(data="private payload", headers=None)
    with caplog.at_level(logging.DEBUG):
        assert (await DingTalkGroupEventHandler(channel).process(message))[0] == 200

    channel._persistent_inbound_callback.assert_not_awaited()
    records = [record.getMessage() for record in caplog.records if "ignore stream event" in record.getMessage()]
    assert len(records) == 1
    assert f"kind={kind}" in records[0] and "app_id=app-test" in records[0]
    expected_event_id = "sdk-event" if kind else "None"
    assert f"event_id={expected_event_id}" in records[0]
    assert "private payload" not in records[0]


@pytest.mark.asyncio
async def test_invalid_lifecycle_logs_sdk_identity_without_payload(caplog):
    channel = _channel()
    channel._persistent_inbound_callback = AsyncMock()
    message = _sdk_lifecycle_message()
    message.data["timeStamp"] = True
    message.data["private"] = "private payload"
    with caplog.at_level(logging.WARNING):
        assert (await DingTalkGroupEventHandler(channel).process(message))[0] == 400

    channel._persistent_inbound_callback.assert_not_awaited()
    record, = [record.getMessage() for record in caplog.records if "invalid group lifecycle" in record.getMessage()]
    assert "kind=chat_disband" in record and "event_id=sdk-event" in record and "app_id=app-test" in record
    assert "private payload" not in record


@pytest.mark.asyncio
async def test_lifecycle_host_timeout_cancels_delivery_and_returns_retryable_ack(monkeypatch, caplog):
    from agent_gateway.channel_manager.im_platforms.dingtalk import dingtalk_connect

    monkeypatch.setattr(dingtalk_connect, "_LIFECYCLE_ACK_TIMEOUT_SECONDS", 0.01, raising=False)
    channel = _channel()
    cancelled = asyncio.Event()

    async def slow_host(event):
        try:
            await asyncio.sleep(0.15)
        except asyncio.CancelledError:
            cancelled.set()
            raise
        return InboundDisposition.HANDLED

    channel._persistent_inbound_callback = slow_host
    with caplog.at_level(logging.WARNING):
        code, reason = await asyncio.wait_for(
            DingTalkGroupEventHandler(channel).process(_sdk_lifecycle_message()), timeout=1,
        )

    assert code == 500 and "timed out" in reason
    assert cancelled.is_set()
    record, = [record.getMessage() for record in caplog.records if "group lifecycle delivery timed out" in record.getMessage()]
    assert "kind=chat_disband" in record and "event_id=sdk-event" in record and "app_id=app-test" in record


@pytest.mark.asyncio
async def test_timeout_cleans_real_agent_request_and_redelivery_keeps_host_contract(monkeypatch):
    from e2a.agent_models import AgentResponse
    from e2a.wire_codec import encode_agent_response_for_wire
    from agent_gateway.channel_manager.im_platforms.dingtalk import dingtalk_connect
    from agent_gateway.routing.agent_client import WebSocketAgentServerClient

    monkeypatch.setattr(dingtalk_connect, "_LIFECYCLE_ACK_TIMEOUT_SECONDS", 0.01)
    client = WebSocketAgentServerClient()
    # Skip the two-second tombstone cleanup delay; queue removal remains real.
    monkeypatch.setattr(client, "_delayed_cleanup_cancelled_request_id", AsyncMock())
    sent = []
    reply = False

    async def send(data):
        payload = json.loads(data)
        sent.append(payload)
        if reply:
            rid = payload["request_id"]
            response = AgentResponse(request_id=rid, channel_id="dingtalk", ok=True,
                                     payload={"disposition": "handled"})
            await client._message_queues[rid].put(encode_agent_response_for_wire(response, response_id=rid))

    client._ws = SimpleNamespace(send=send)
    host = SimpleNamespace(agent_client=client)
    channel = _channel()

    async def callback(event):
        return await MessageHandler.handle_persistent_dingtalk_event(host, event)

    channel.set_persistent_inbound_callback(callback)
    message = _sdk_lifecycle_message()
    assert (await DingTalkGroupEventHandler(channel).process(message))[0] == 500
    assert not client._message_queues
    monkeypatch.setattr(dingtalk_connect, "_LIFECYCLE_ACK_TIMEOUT_SECONDS", 1.0)
    reply = True
    assert (await DingTalkGroupEventHandler(channel).process(message))[0] == 200
    assert not client._message_queues
    assert len(sent) == 2 and sent[0]["request_id"] != sent[1]["request_id"]
    for payload in sent:
        event = payload["params"]["event"]
        assert payload["method"] == "jiuwenmate.dingtalk.inbound"
        assert (event["event_id"], event["expected_agent_id"], event["event_kind"]) == (
            "sdk-event", "agent-dingtalk", "group_lifecycle",
        )


@pytest.mark.asyncio
async def test_host_confirmation_within_budget_still_gets_success_ack(monkeypatch):
    from agent_gateway.channel_manager.im_platforms.dingtalk import dingtalk_connect

    monkeypatch.setattr(dingtalk_connect, "_LIFECYCLE_ACK_TIMEOUT_SECONDS", 0.5)
    channel = _channel()

    async def host(event):
        await asyncio.sleep(0.01)
        return InboundDisposition.HANDLED

    channel.set_persistent_inbound_callback(host)
    assert (await DingTalkGroupEventHandler(channel).process(_sdk_lifecycle_message()))[0] == 200
