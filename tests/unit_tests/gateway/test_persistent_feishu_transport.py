"""JM-08: route bound Feishu events into the persistent request path only."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent_gateway.channel_manager.channel_manager import ChannelManager
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel,
    FeishuConfig,
)
from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.channel_manager.persistent_events import FeishuInboundEvent, InboundDisposition

NOW = datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc)

def _feishu_channel(app_id="app-dev", agent_id="") -> FeishuChannel:
    channel = FeishuChannel(
        FeishuConfig(enabled=True, app_id=app_id, agent_id=agent_id, channel_id="test-feishu"),
        MagicMock(),
    )
    channel._add_reaction = AsyncMock()
    channel._parse_message_content_with_file = AsyncMock(return_value=("legacy text", None))
    channel._process_batched_message = AsyncMock()
    return channel


@pytest.mark.asyncio
async def test_delivery_feishu_result_replies_to_source_with_stable_uuid_and_receipt():
    channel = _feishu_channel()
    response = SimpleNamespace(
        success=lambda: True,
        data=SimpleNamespace(message=SimpleNamespace(message_id="om_sent")),
    )
    api_method = MagicMock(return_value=response)
    channel._api_client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(reply=api_method)))
    )

    result = await channel.send_persistent_result(
        chat_id="oc_project",
        text="已完成",
        source_message_id="om_source",
        idempotency_key="delivery-key",
    )

    request = api_method.call_args.args[0]
    assert request.message_id == "om_source"
    assert request.request_body.uuid == "delivery-key"
    assert request.request_body.msg_type == "interactive"
    card = json.loads(request.request_body.content)
    assert any(
        element.get("text", {}).get("content") == "已完成"
        and element.get("text", {}).get("tag") == "lark_md"
        for element in card["elements"]
    )
    assert result == {"outcome": "delivered", "external_message_id": "om_sent"}


@pytest.mark.asyncio
async def test_delivery_feishu_result_only_contains_the_answer():
    channel = _feishu_channel(agent_id="mate_dev")
    channel.config.bot_open_id = "ou_self"
    response = SimpleNamespace(
        success=lambda: True,
        data=SimpleNamespace(message=SimpleNamespace(message_id="om_sent")),
    )
    api_method = MagicMock(return_value=response)
    channel._api_client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(reply=api_method)))
    )

    await channel.send_persistent_result(
        chat_id="oc_project",
        text="请核对这个数字",
        source_message_id="om_source",
        idempotency_key="delivery-key",
    )

    content = json.loads(api_method.call_args.args[0].request_body.content)
    assert content["elements"][0]["text"]["content"] == "请核对这个数字"
    assert content["elements"][0]["text"]["tag"] == "lark_md"


@pytest.mark.asyncio
async def test_delivery_feishu_result_renders_a_native_bot_mention_for_group_handoff():
    channel = _feishu_channel(agent_id="agent-sender")
    response = SimpleNamespace(
        success=lambda: True,
        data=SimpleNamespace(message=SimpleNamespace(message_id="om_sent")),
    )
    api_method = MagicMock(return_value=response)
    channel._api_client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(reply=api_method)))
    )

    await channel.send_persistent_result(
        chat_id="oc_project",
        text="请继续 [[agent:agent-target]]",
        source_message_id="om_source",
        idempotency_key="delivery-key",
        mention_targets=[{"agent_id": "agent-target", "name": "研究同事", "open_id": "ou_target"}],
    )

    content = json.loads(api_method.call_args.args[0].request_body.content)
    assert content["elements"][0]["text"]["content"] == "请继续 <at id=ou_target></at>"


@pytest.mark.asyncio
async def test_delivery_feishu_result_keeps_a_readable_name_when_target_bot_is_unavailable():
    channel = _feishu_channel(agent_id="agent-sender")
    response = SimpleNamespace(
        success=lambda: True,
        data=SimpleNamespace(message=SimpleNamespace(message_id="om_sent")),
    )
    api_method = MagicMock(return_value=response)
    channel._api_client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(reply=api_method)))
    )

    await channel.send_persistent_result(
        chat_id="oc_project",
        text="请继续 [[agent:agent-target]]",
        source_message_id="om_source",
        idempotency_key="delivery-key",
        mention_targets=[{"agent_id": "agent-target", "name": "研究同事"}],
    )

    content = json.loads(api_method.call_args.args[0].request_body.content)
    assert content["elements"][0]["text"]["content"] == "请继续 @研究同事"


@pytest.mark.asyncio
async def test_delivery_feishu_result_uses_chat_create_when_no_source_message():
    channel = _feishu_channel()
    response = SimpleNamespace(
        success=lambda: True,
        data=SimpleNamespace(message=SimpleNamespace(message_id="om_new")),
    )
    api_method = MagicMock(return_value=response)
    channel._api_client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message=SimpleNamespace(create=api_method)))
    )

    result = await channel.send_persistent_result(
        chat_id="oc_project",
        text="已完成",
        source_message_id=None,
        idempotency_key="delivery-key",
    )

    request = api_method.call_args.args[0]
    assert request.receive_id_type == "chat_id"
    assert request.request_body.receive_id == "oc_project"
    assert request.request_body.uuid == "delivery-key"
    assert request.request_body.msg_type == "interactive"
    card = json.loads(request.request_body.content)
    assert card["elements"][0]["text"]["content"] == "已完成"
    assert card["elements"][0]["text"]["tag"] == "lark_md"
    assert result == {"outcome": "delivered", "external_message_id": "om_new"}


def _raw_feishu_event(
    *, sender_type="user", message_type="text", content=None, message_id="om_001", mention_ids=("ou_dev",)
):
    message = SimpleNamespace(
        message_id=message_id,
        chat_id="oc_project",
        chat_type="group",
        create_time=str(int(NOW.timestamp() * 1000)),
        message_type=message_type,
        content=content if content is not None else json.dumps({"text": "请看一下"}),
        mentions=[SimpleNamespace(id=SimpleNamespace(open_id=item), name="研发同事") for item in mention_ids],
        parent_id="om_parent",
        root_id="om_root",
    )
    sender = SimpleNamespace(
        sender_type=sender_type,
        sender_id=SimpleNamespace(open_id="ou_alice", user_id="u_alice", union_id="un_alice"),
        app_id=None,
    )
    return SimpleNamespace(event=SimpleNamespace(message=message, sender=sender))


@pytest.mark.asyncio
async def test_bound_attachment_event_is_claimed_before_personal_download_and_dedup():
    channel = _feishu_channel()
    channel.config.bot_open_id = "ou_dev"
    received = []

    async def handle(event):
        received.append(event)
        return InboundDisposition.HANDLED

    channel.set_persistent_group_fanout_callback(handle)
    raw = _raw_feishu_event(message_type="image", content=json.dumps({"image_key": "img_key_1"}))

    await channel._on_message(raw)
    await channel._on_message(raw)

    channel._parse_message_content_with_file.assert_not_awaited()
    channel._process_batched_message.assert_not_awaited()
    assert len(received) == 2  # JM-03, not the personal in-memory cache, owns persistent dedup.
    assert received[0].receiving_app_id == "app-dev"
    assert received[0].message_id == "om_001"
    assert received[0].chat_type == "group"
    assert received[0].sender_type == "user"
    assert received[0].sender_id == "ou_alice"
    assert received[0].sender_name == "ou_alice"
    assert received[0].sender_app_id is None
    assert received[0].received_at == NOW
    assert received[0].text is None
    assert received[0].mention_ids == ("ou_dev",)
    assert received[0].receiver_bot_mentioned is True
    assert received[0].reply_to == "om_parent"
    assert received[0].attachment_refs == ("feishu:attachment:om_001:img_key_1",)


@pytest.mark.asyncio
async def test_persistent_route_resolves_bot_open_id_before_normalizing_mention(monkeypatch):
    channel = _feishu_channel(agent_id="mate_dev")
    order = []

    def fetch_bot_open_id():
        order.append("resolve")
        channel.config.bot_open_id = "ou_dev"

    async def handle(event):
        order.append("callback")
        assert event.receiver_bot_mentioned is True
        return InboundDisposition.HANDLED

    monkeypatch.setattr(channel, "_fetch_bot_open_id", fetch_bot_open_id)
    channel.set_persistent_group_fanout_callback(handle)

    await channel._on_message(_raw_feishu_event())

    assert order == ["resolve", "callback"]


@pytest.mark.asyncio
async def test_mention_of_another_bot_does_not_mark_receiving_bot_mentioned():
    channel = _feishu_channel(agent_id="mate_dev")
    channel.config.bot_open_id = "ou_dev"
    received = []

    async def handle(event):
        received.append(event)
        return InboundDisposition.HANDLED

    channel.set_persistent_group_fanout_callback(handle)
    await channel._on_message(_raw_feishu_event(mention_ids=("ou_other",)))

    assert received[0].receiver_bot_mentioned is False


@pytest.mark.asyncio
async def test_persistent_text_shows_the_mentioned_display_name():
    channel = _feishu_channel(agent_id="mate_study")
    channel.config.bot_open_id = "ou_study"
    received = []

    async def handle(event):
        received.append(event)
        return InboundDisposition.HANDLED

    channel.set_persistent_group_fanout_callback(handle)
    raw = _raw_feishu_event(
        content=json.dumps({"text": "@_user_1 理我一下"}),
        mention_ids=("ou_life",),
    )
    raw.event.message.mentions[0].key = "@_user_1"
    raw.event.message.mentions[0].name = "生活小助手"

    await channel._on_message(raw)

    assert received[0].text == "@生活小助手 理我一下"
    assert received[0].mention_ids == ("ou_life",)
    assert received[0].receiver_bot_mentioned is False


@pytest.mark.asyncio
async def test_persistent_post_keeps_the_at_node_name_in_the_body():
    channel = _feishu_channel(agent_id="mate_study")
    channel.config.bot_open_id = "ou_study"
    received = []

    async def handle(event):
        received.append(event)
        return InboundDisposition.HANDLED

    channel.set_persistent_group_fanout_callback(handle)
    raw = _raw_feishu_event(
        message_type="post",
        content=json.dumps(
            {
                "title": "",
                "content": [
                    [
                        {"tag": "at", "user_id": "@_user_1", "user_name": "ai员工生活小助手"},
                        {"tag": "text", "text": " 请完成一次客服工单运营分析"},
                    ]
                ],
            }
        ),
        mention_ids=("ou_life",),
    )
    raw.event.message.mentions[0].key = "@_user_1"
    raw.event.message.mentions[0].name = "ai员工生活小助手"

    await channel._on_message(raw)

    assert received[0].text == "@ai员工生活小助手 请完成一次客服工单运营分析"
    assert received[0].mention_ids == ("ou_life",)
    assert received[0].receiver_bot_mentioned is False


@pytest.mark.asyncio
async def test_persistent_text_normalizes_an_all_mention_without_a_mention_object():
    channel = _feishu_channel(agent_id="mate_study")
    channel.config.bot_open_id = "ou_study"
    received = []

    async def handle(event):
        received.append(event)
        return InboundDisposition.HANDLED

    channel.set_persistent_group_fanout_callback(handle)
    raw = _raw_feishu_event(
        content=json.dumps({"text": "@_all 大家都自我介绍下"}),
        mention_ids=(),
    )
    raw.event.message.mentions = []

    await channel._on_message(raw)

    assert received[0].text == "@all 大家都自我介绍下"
    assert received[0].mention_ids == ()
    assert received[0].receiver_bot_mentioned is False


@pytest.mark.asyncio
async def test_unclaimed_human_continues_personal_path_and_keeps_local_dedup():
    channel = _feishu_channel()
    calls = []

    async def unclaimed(_event):
        calls.append("route")
        return InboundDisposition.UNCLAIMED

    channel.set_persistent_group_fanout_callback(unclaimed)
    raw = _raw_feishu_event()

    await channel._on_message(raw)
    await channel._on_message(raw)

    assert calls == ["route", "route"]
    channel._parse_message_content_with_file.assert_awaited_once()
    channel._process_batched_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_unclaimed_bot_stays_ignored_and_rejected_or_failed_routes_never_fall_back():
    bot_channel = _feishu_channel()
    bot_route = AsyncMock(return_value=InboundDisposition.UNCLAIMED)
    bot_channel.set_persistent_group_fanout_callback(bot_route)
    await bot_channel._on_message(_raw_feishu_event(sender_type="bot"))
    bot_channel._parse_message_content_with_file.assert_not_awaited()
    bot_channel._process_batched_message.assert_not_awaited()

    rejected_channel = _feishu_channel()
    rejected_channel.set_persistent_group_fanout_callback(
        AsyncMock(return_value=InboundDisposition.REJECTED)
    )
    await rejected_channel._on_message(_raw_feishu_event())
    rejected_channel._parse_message_content_with_file.assert_not_awaited()
    rejected_channel._process_batched_message.assert_not_awaited()

    failed_channel = _feishu_channel()

    async def fail_closed(_event):
        raise RuntimeError("sensitive event detail")

    failed_channel.set_persistent_group_fanout_callback(fail_closed)
    await failed_channel._on_message(_raw_feishu_event())
    failed_channel._parse_message_content_with_file.assert_not_awaited()
    failed_channel._process_batched_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_channel_manager_wires_feishu_to_message_handler_and_unconfigured_handler_is_unclaimed():
    handler = object.__new__(MessageHandler)
    channel = _feishu_channel()
    manager = ChannelManager(handler)

    manager.register_channel(channel)
    await channel._on_message(_raw_feishu_event())

    channel._parse_message_content_with_file.assert_awaited_once()
    channel._process_batched_message.assert_awaited_once()


def test_channel_manager_wires_only_feishu_apps_with_a_persistent_agent_binding():
    handler = object.__new__(MessageHandler)
    manager = ChannelManager(handler)
    personal_channel = _feishu_channel(agent_id="")
    bound_channel = _feishu_channel(agent_id="mate_bound")

    manager.register_channel(personal_channel)
    assert personal_channel._persistent_group_fanout_callback is None

    manager.register_channel(bound_channel)
    assert bound_channel._persistent_group_fanout_callback == handler.handle_persistent_feishu_event



@pytest.mark.asyncio
async def test_group_channel_checks_live_bot_membership_and_fails_closed():
    channel = FeishuChannel(FeishuConfig(enabled=True, app_id="cli_bound", agent_id="mate_1"), MagicMock())
    response = SimpleNamespace(success=lambda: True, data=SimpleNamespace(is_in_chat=False))
    channel._api_client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(chat_members=SimpleNamespace(
        ais_in_chat=AsyncMock(return_value=response)))))
    assert (await channel.inspect_persistent_group("oc_group"))["status"] == "left"
    response.data.is_in_chat = True
    assert (await channel.inspect_persistent_group("oc_group"))["status"] == "active"
    response.data.is_in_chat = None
    assert (await channel.inspect_persistent_group("oc_group"))["status"] == "unknown"

@pytest.mark.asyncio
async def test_group_caches_group_name_but_not_membership(monkeypatch):
    channel = FeishuChannel(FeishuConfig(enabled=True, app_id="cli_bound", agent_id="mate_1"), MagicMock())
    now = [0.0]
    monkeypatch.setattr("agent_gateway.channel_manager.im_platforms.feishu.feishu_connect.time.monotonic",
                        lambda: now[0])
    membership = SimpleNamespace(success=lambda: True, data=SimpleNamespace(is_in_chat=True))
    member_api = AsyncMock(return_value=membership)
    name_api = AsyncMock(return_value=SimpleNamespace(success=lambda: True, data=SimpleNamespace(name="Project")))
    channel._api_client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(
        chat_members=SimpleNamespace(ais_in_chat=member_api), chat=SimpleNamespace(aget=name_api))))

    assert (await channel.inspect_persistent_group("oc_group"))["group_name"] == "Project"
    assert (await channel.inspect_persistent_group("oc_group"))["group_name"] == "Project"
    assert member_api.await_count == 2
    assert name_api.await_count == 1
    now[0] = 301.0
    assert (await channel.inspect_persistent_group("oc_group"))["group_name"] == "Project"
    assert name_api.await_count == 2
    membership.data.is_in_chat = False
    assert (await channel.inspect_persistent_group("oc_group")) == {"status": "left"}
    assert member_api.await_count == 4


@pytest.mark.asyncio
async def test_group_gateway_refreshes_edited_and_recalled_bodies_in_the_target_group():
    channel = FeishuChannel(FeishuConfig(enabled=True, app_id="cli_bound", agent_id="mate_1"), MagicMock())
    membership = SimpleNamespace(success=lambda: True, data=SimpleNamespace(is_in_chat=True))
    edited = SimpleNamespace(message_id="m1", chat_id="oc_group", deleted=False, updated=True,
        update_time=int(NOW.timestamp() * 1000), create_time=int(NOW.timestamp() * 1000),
        body=SimpleNamespace(content='{"text":"new body"}'))
    recalled = SimpleNamespace(message_id="m2", chat_id="oc_group", deleted=True)

    async def read(request):
        item = edited if request.message_id == "m1" else recalled
        return SimpleNamespace(success=lambda: True, data=SimpleNamespace(items=[item]))

    channel._api_client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(
        chat_members=SimpleNamespace(ais_in_chat=AsyncMock(return_value=membership)),
        message=SimpleNamespace(aget=read))))
    result = await channel.inspect_persistent_group("oc_group", ("m1", "m2"))
    assert result["status"] == "ok"
    channel._api_client.im.v1.chat_members.ais_in_chat.assert_not_awaited()
    assert result["messages"]["m1"]["text"] == "new body"
    assert result["messages"]["m2"] == {"status": "recalled"}
    edited.chat_id = "other-group"
    assert (await channel.inspect_persistent_group("oc_group", ("m1",)))["messages"]["m1"]["status"] == "unavailable"


@pytest.mark.asyncio
async def test_group_message_read_failure_stays_unknown_instead_of_unreadable():
    channel = FeishuChannel(FeishuConfig(enabled=True, app_id="cli_bound", agent_id="mate_1"), MagicMock())

    async def read(_request):
        return SimpleNamespace(success=lambda: False, code=99991403, msg="quota exceeded", data=None)

    channel._api_client = SimpleNamespace(im=SimpleNamespace(v1=SimpleNamespace(
        message=SimpleNamespace(aget=read))))

    result = await channel.inspect_persistent_group("oc_group", ("m1",))

    assert result == {"status": "ok", "messages": {"m1": {"status": "unknown"}}}


@pytest.mark.asyncio
async def test_group_platform_lifecycle_event_enters_only_the_bound_persistent_route():
    channel = FeishuChannel(FeishuConfig(enabled=True, app_id="cli_bound", agent_id="mate_1"), MagicMock())
    events = []

    async def receive(event):
        events.append(event)
        return InboundDisposition.HANDLED

    channel.set_persistent_group_fanout_callback(receive)
    payload = {"header": {"event_id": "recall-1", "create_time": str(int(NOW.timestamp() * 1000))},
               "event": {"chat_id": "oc_group", "message_id": "m1"}}
    await channel._on_persistent_change(payload, "recalled")
    assert events[0].event_kind == "recalled" and events[0].message_id == "m1"
    assert events[0].changed_at == NOW


def _event(**overrides) -> FeishuInboundEvent:
    values = {
        "receiving_app_id": "cli_bound",
        "expected_agent_id": "mate_1",
        "message_id": "om_1",
        "chat_id": "oc_group",
        "chat_type": "group",
        "sender_type": "user",
        "sender_id": "ou_alice",
        "sender_name": "Alice",
        "sender_app_id": None,
        "received_at": NOW,
        "text": "hello",
        "mention_ids": ("ou_mate",),
        "reply_to": "om_parent",
        "attachment_refs": ("feishu:attachment:om_1:file_1",),
    }
    values.update(overrides)
    return FeishuInboundEvent(**values)

@pytest.mark.asyncio
async def test_message_handler_forwards_bound_event_over_e2a_and_maps_disposition():
    client = MagicMock()
    client.send_request = AsyncMock(
        return_value=SimpleNamespace(ok=True, payload={"disposition": "handled"})
    )
    handler = object.__new__(MessageHandler)
    handler.agent_client = client

    result = await handler.handle_persistent_feishu_event(_event())

    assert result is InboundDisposition.HANDLED
    envelope = client.send_request.await_args.args[0]
    assert envelope.method == "jiuwenmate.feishu.inbound"
    assert envelope.channel == "feishu"
    assert envelope.params["event"] == _event().to_payload()

@pytest.mark.asyncio
async def test_unbound_feishu_event_does_not_make_an_e2a_request():
    client = MagicMock()
    client.send_request = AsyncMock()
    handler = object.__new__(MessageHandler)
    handler.agent_client = client

    result = await handler.handle_persistent_feishu_event(_event(expected_agent_id=None))

    assert result is InboundDisposition.UNCLAIMED
    client.send_request.assert_not_awaited()
@pytest.mark.asyncio
async def test_channel_manager_routes_result_delivery_only_to_exact_feishu_app():
    handler = object.__new__(MessageHandler)
    manager = ChannelManager(handler)
    wrong_app = FeishuChannel(FeishuConfig(enabled=True, app_id="cli_other"), MagicMock())
    right_app = FeishuChannel(FeishuConfig(enabled=True, app_id="cli_bound"), MagicMock())
    wrong_app.send_persistent_result = AsyncMock(return_value={"outcome": "delivered"})
    right_app.send_persistent_result = AsyncMock(
        return_value={"outcome": "delivered", "external_message_id": "om_sent"}
    )
    manager.register_channel(wrong_app)
    manager.register_channel(right_app)

    result = await manager.deliver_persistent_result(
        {
            "app_id": "cli_bound",
            "chat_id": "oc_group",
            "source_message_id": "om_source",
            "idempotency_key": "stable-key",
            "text": "final answer",
        }
    )

    right_app.send_persistent_result.assert_awaited_once_with(
        chat_id="oc_group",
        text="final answer",
        source_message_id="om_source",
        idempotency_key="stable-key",
    )
    wrong_app.send_persistent_result.assert_not_awaited()
    assert result["outcome"] == "delivered"


@pytest.mark.asyncio
async def test_channel_manager_maps_handoff_target_to_its_native_bot_identity():
    handler = object.__new__(MessageHandler)
    manager = ChannelManager(handler)
    sender = FeishuChannel(
        FeishuConfig(enabled=True, app_id="cli_sender", agent_id="agent-sender"), MagicMock()
    )
    target = FeishuChannel(
        FeishuConfig(enabled=True, app_id="cli_target", agent_id="agent-target", bot_open_id="ou_target"), MagicMock()
    )
    sender.send_persistent_result = AsyncMock(return_value={"outcome": "delivered"})
    target.send_persistent_result = AsyncMock(return_value={"outcome": "delivered"})
    manager.register_channel(sender)
    manager.register_channel(target)

    await manager.deliver_persistent_result(
        {
            "app_id": "cli_sender",
            "chat_id": "oc_group",
            "source_message_id": "om_source",
            "idempotency_key": "stable-key",
            "text": "请接手 [[agent:agent-target]]",
            "mention_targets": [{"agent_id": "agent-target", "name": "接手同事", "app_ids": ["cli_target"]}],
        }
    )

    sender.send_persistent_result.assert_awaited_once_with(
        chat_id="oc_group",
        text="请接手 [[agent:agent-target]]",
        source_message_id="om_source",
        idempotency_key="stable-key",
        mention_targets=[
            {"agent_id": "agent-target", "name": "接手同事", "app_ids": ["cli_target"], "open_id": "ou_target"}
        ],
    )


@pytest.mark.asyncio
async def test_channel_manager_loads_bot_open_id_before_rendering_a_native_mention():
    handler = object.__new__(MessageHandler)
    manager = ChannelManager(handler)
    sender = FeishuChannel(
        FeishuConfig(enabled=True, app_id="cli_sender", agent_id="agent-sender"), MagicMock()
    )
    target = FeishuChannel(
        FeishuConfig(enabled=True, app_id="cli_target", agent_id="agent-target", bot_open_id=""), MagicMock()
    )
    sender.send_persistent_result = AsyncMock(return_value={"outcome": "delivered"})
    target.send_persistent_result = AsyncMock(return_value={"outcome": "delivered"})

    def remember_open_id() -> None:
        target.config.bot_open_id = "ou_learned"

    target._fetch_bot_open_id = remember_open_id
    manager.register_channel(sender)
    manager.register_channel(target)

    await manager.deliver_persistent_result(
        {
            "app_id": "cli_sender",
            "chat_id": "oc_group",
            "source_message_id": "om_source",
            "idempotency_key": "stable-key",
            "text": "请接手 [[agent:agent-target]]",
            "mention_targets": [{"agent_id": "agent-target", "name": "接手同事", "app_ids": ["cli_target"]}],
        }
    )

    sender.send_persistent_result.assert_awaited_once_with(
        chat_id="oc_group",
        text="请接手 [[agent:agent-target]]",
        source_message_id="om_source",
        idempotency_key="stable-key",
        mention_targets=[
            {"agent_id": "agent-target", "name": "接手同事", "app_ids": ["cli_target"], "open_id": "ou_learned"}
        ],
    )


@pytest.mark.asyncio
async def test_channel_manager_keeps_the_handoff_when_bot_identity_cannot_be_loaded():
    handler = object.__new__(MessageHandler)
    manager = ChannelManager(handler)
    sender = FeishuChannel(
        FeishuConfig(enabled=True, app_id="cli_sender", agent_id="agent-sender"), MagicMock()
    )
    target = FeishuChannel(
        FeishuConfig(enabled=True, app_id="cli_target", agent_id="agent-target", bot_open_id=""), MagicMock()
    )
    sender.send_persistent_result = AsyncMock(return_value={"outcome": "delivered"})
    target.send_persistent_result = AsyncMock(return_value={"outcome": "delivered"})

    def fail_open_id() -> None:
        raise RuntimeError("quota")

    target._fetch_bot_open_id = fail_open_id
    manager.register_channel(sender)
    manager.register_channel(target)

    result = await manager.deliver_persistent_result(
        {
            "app_id": "cli_sender",
            "chat_id": "oc_group",
            "source_message_id": "om_source",
            "idempotency_key": "stable-key",
            "text": "请接手 [[agent:agent-target]]",
            "mention_targets": [{"agent_id": "agent-target", "name": "接手同事", "app_ids": ["cli_target"]}],
        }
    )

    assert result["outcome"] == "delivered"
    sent = sender.send_persistent_result.await_args.kwargs
    assert "open_id" not in sent["mention_targets"][0]


@pytest.mark.asyncio
async def test_channel_manager_sends_direct_message_without_quoting_source():
    handler = object.__new__(MessageHandler)
    manager = ChannelManager(handler)
    channel = FeishuChannel(FeishuConfig(enabled=True, app_id="cli_bound"), MagicMock())
    channel.send_persistent_result = AsyncMock(return_value={"outcome": "delivered"})
    manager.register_channel(channel)

    await manager.deliver_persistent_result(
        {
            "app_id": "cli_bound",
            "chat_id": "oc_dm",
            "source_message_id": "om_source",
            "is_direct_message": True,
            "idempotency_key": "stable-key",
            "text": "你好",
        }
    )

    channel.send_persistent_result.assert_awaited_once_with(
        chat_id="oc_dm",
        text="你好",
        source_message_id=None,
        idempotency_key="stable-key",
    )


@pytest.mark.asyncio
async def test_channel_manager_keeps_delivery_pending_while_bound_app_is_disconnected():
    handler = object.__new__(MessageHandler)
    manager = ChannelManager(handler)

    result = await manager.deliver_persistent_result(
        {
            "app_id": "cli_bound",
            "chat_id": "oc_group",
            "source_message_id": "om_source",
            "idempotency_key": "stable-key",
            "text": "final answer",
        }
    )

    assert result == {"outcome": "pending", "error_code": "FEISHU_APP_NOT_CONNECTED"}


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type,method", [
    ("jiuwenmate.delivery.send", "jiuwenmate.delivery.ack"),
    ("jiuwenmate.thinking_notice", "jiuwenmate.thinking_notice.ack"),
    ("jiuwenmate.group.inspect", "jiuwenmate.group.response"),
])
async def test_gateway_push_returns_correlated_platform_receipt(event_type, method):
    from e2a.agent_models import AgentResponseChunk
    from e2a.wire_codec import encode_agent_chunk_for_wire

    handler = object.__new__(MessageHandler)
    handler.agent_client = SimpleNamespace(send_request=AsyncMock(return_value=SimpleNamespace(ok=True)))
    payload = {"event_type": event_type, "app_id": "cli", "chat_id": "oc", "channel": "feishu",
               "delivery_id": "del", "stage": "send", "query_id": "query", "idempotency_key": "key"}
    handler.set_feishu_group_handler(AsyncMock(return_value={"status": "active"}))
    sender = AsyncMock(return_value={"outcome": "delivered", "external_message_id": "om"})
    handler.set_result_delivery_handler(sender)
    handler.set_thinking_notice_handler(sender)
    wire = encode_agent_chunk_for_wire(
        AgentResponseChunk(request_id="req", channel_id="feishu", payload=payload),
        response_id="req", sequence=0,
    )
    await handler._handle_agent_server_push(wire)
    if event_type == "jiuwenmate.group.inspect":
        await asyncio.gather(*handler._feishu_group_inspect_tasks)
    ack = handler.agent_client.send_request.await_args.args[0]
    assert ack.method == method
    if event_type == "jiuwenmate.group.inspect":
        assert ack.params == {"status": "active", "query_id": "query", "chat_id": "oc", "app_id": "cli"}
    else:
        sender.assert_awaited_once_with(payload)
        assert ack.params["outcome"] == "delivered"
        assert ack.params["external_message_id"] == "om"


@pytest.mark.asyncio
@pytest.mark.parametrize("share_mode", ["attachment", "upload"])
@pytest.mark.parametrize("uploaded", [False, True])
async def test_gateway_feishu_file_reaches_real_sender(tmp_path, share_mode, uploaded):
    source = tmp_path / "workspaces/local/agent/sessions/session/output/report.txt"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"test123")
    channel = FeishuChannel(FeishuConfig(enabled=True, app_id="cli-test"), MagicMock())
    channel._api_client = MagicMock()
    channel._upload_file = MagicMock(return_value="file-test")
    channel._send_persistent_message = AsyncMock(return_value={
        "outcome": "delivered", "external_message_id": "om-test",
    })
    manager = ChannelManager(SimpleNamespace())
    manager.set_persistent_output_file_resolver(lambda key, file_ref, size: source)
    manager.register_channel(channel)
    file = {
        "key": "local/agent/sessions/session/output/report.txt", "name": "report.txt",
        "size": 7, "file_ref": "sha256:" + "test-file-digest",
        "file_key": "file-test" if uploaded else None, "share_mode": share_mode,
    }
    result = await manager.deliver_persistent_result({
        "channel": "feishu", "app_id": "cli-test", "chat_id": "oc-test",
        "source_message_id": "om-source", "idempotency_key": "delivery-test", "file": file,
    })
    assert result["outcome"] == "delivered", result
    assert result["file_key"] == "file-test"
    channel._send_persistent_message.assert_awaited_once_with(
        "oc-test", "file", '{"file_key": "file-test"}', "om-source", "delivery-test",
    )
    if uploaded:
        channel._upload_file.assert_not_called()
    else:
        channel._upload_file.assert_called_once_with(str(source))
