"""DingTalk JM transport contracts; no server, model, credentials or real network."""

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_connect import (
    DingTalkChannel,
    DingTalkConfig,
    DingTalkHandler,
)
from agent_gateway.channel_manager.persistent_events import DingTalkInboundEvent
from agent_gateway.channel_manager.im_platforms.dingtalk.dingtalk_file_service import DingTalkFileService


def _callback(**changes):
    return {
        "conversationType": "1", "conversationId": "conversation-test", "msgtype": "text",
        "msgId": "message-test", "senderStaffId": "staff-test", "senderId": "opaque-test",
        "senderNick": "Test sender", "robotCode": "robot-test", "text": {"content": "test task"},
        "sessionWebhook": "https://example.invalid/temporary", **changes,
    }


def _channel():
    channel = DingTalkChannel(DingTalkConfig(
        client_id="app-test", client_secret="fake-secret", robot_code="robot-test", agent_id="agent-dingtalk",
        is_default=False, api_base="https://api.example.invalid", allow_from=["staff-test"],
    ), MagicMock(spec=RobotMessageRouter))
    channel._http = AsyncMock(spec=httpx.AsyncClient)
    channel._access_token = "fake-token"
    channel._token_expiry = time.time() + 3600
    return channel


async def _send(channel, *, chat_id="staff-test", **changes):
    return await channel.send_persistent_result(
        chat_id=chat_id, text="test result", source_message_id="message-test", idempotency_key="delivery-test",
        **changes,
    )


def test_event_preserves_business_identity_and_excludes_temporary_webhook():
    event = DingTalkInboundEvent.from_callback(_callback(), app_id="trusted-app", agent_id="agent-dingtalk")
    payload = event.to_payload()
    assert payload["receiving_app_id"] == "trusted-app"
    assert payload["message_id"] == "message-test"
    assert payload["sender_staff_id"] == "staff-test"
    assert "sessionWebhook" not in payload and "senderId" not in payload
    assert DingTalkInboundEvent.from_payload(payload) == event


def test_event_accepts_a_mentioned_group_message_and_preserves_group_identity():
    callback = _callback(
        conversationType="2", isInAtList=True, chatbotUserId="bot-test",
        atUsers=[{"dingtalkId": "bot-test"}], conversationTitle="Test group",
    )
    event = DingTalkInboundEvent.from_callback(callback, app_id="trusted-app", agent_id="agent-dingtalk")

    assert event.conversation_type == "2"
    assert event.receiver_bot_mentioned is True
    assert event.conversation_id == "conversation-test"
    assert event.mention_ids == ("bot-test",)
    payload = event.to_payload()
    assert payload["conversation_type"] == "2" and payload["receiver_bot_mentioned"] is True
    assert "sessionWebhook" not in payload and "chatbotUserId" not in payload
    assert DingTalkInboundEvent.from_payload(payload) == event


def test_group_bot_stream_callback_is_authoritative_when_at_metadata_is_inconsistent():
    callback = _callback(
        conversationType="2", isInAtList=False, chatbotUserId="bot-test",
        atUsers=[{"dingtalkId": "another-user"}],
    )

    event = DingTalkInboundEvent.from_callback(callback, app_id="trusted-app", agent_id="agent-dingtalk")

    assert event.receiver_bot_mentioned is True


def test_group_broadcast_is_not_treated_as_an_explicit_bot_mention():
    callback = _callback(
        conversationType="2", isInAtList=True, chatbotUserId="bot-test",
        atUsers=[{"dingtalkId": "all"}],
    )

    event = DingTalkInboundEvent.from_callback(callback, app_id="trusted-app", agent_id="agent-dingtalk")

    assert event.mention_ids == ("all",)
    assert event.receiver_bot_mentioned is False


@pytest.mark.parametrize("changes", [
    {"senderStaffId": None}, {"conversationId": ""}, {"msgId": None},
    {"conversationType": None}, {"conversationType": "3"}, {"msgtype": "file"}, {"text": {}},
])
def test_event_never_guesses_missing_identity_or_unsupported_input(changes):
    with pytest.raises(ValueError):
        DingTalkInboundEvent.from_callback(_callback(**changes), app_id="app-test", agent_id="agent-dingtalk")


@pytest.mark.asyncio
async def test_persistent_group_result_uses_the_group_send_api():
    channel = _channel()
    channel._http.post = AsyncMock(return_value=httpx.Response(200, json={"processQueryKey": "group-handle"}))

    result = await _send(channel, chat_id="conversation-test", is_direct_message=False)

    assert result == {"outcome": "delivered", "external_message_id": "group-handle"}
    url, kwargs = channel._http.post.await_args.args[0], channel._http.post.await_args.kwargs
    assert url == "https://api.example.invalid/v1.0/robot/groupMessages/send"
    assert kwargs["json"]["openConversationId"] == "conversation-test"
    assert kwargs["json"]["robotCode"] == "robot-test"
    assert json.loads(kwargs["json"]["msgParam"])["text"] == "test result"


@pytest.mark.asyncio
async def test_success_requires_real_receipt_and_correct_robot_target():
    channel = _channel()
    channel._http.post.return_value = httpx.Response(200, json={"processQueryKey": "opaque+/send=handle"})
    result = await _send(channel)
    assert result == {"outcome": "delivered", "external_message_id": "opaque+/send=handle"}
    call = channel._http.post.await_args
    assert call.args[0].endswith("/v1.0/robot/oToMessages/batchSend")
    assert call.kwargs["json"]["robotCode"] == "robot-test"
    assert call.kwargs["json"]["userIds"] == ["staff-test"]
    assert json.loads(call.kwargs["json"]["msgParam"])["text"] == "test result"
    assert "idempotency_key" not in call.kwargs["json"]


@pytest.mark.parametrize("status,body,outcome", [
    (200, {}, "unknown"),
    (200, {"processQueryKey": "key", "invalidStaffIdList": ["staff-test"]}, "failed"),
    (200, {"processQueryKey": "key", "flowControlledStaffIdList": ["staff-test"]}, "failed"),
    (200, {"processQueryKey": "key", "filteredStaffIdList": ["staff-test"]}, "failed"),
    (200, {"processQueryKey": "key", "invalidStaffIdList": "invalid shape"}, "unknown"),
    (400, {"code": "unknown.send.result"}, "unknown"),
    (400, {"code": "invalidParameter.robotCode.invalid"}, "failed"),
    (400, {}, "unknown"), (429, {}, "failed"), (503, {}, "unknown"),
])
@pytest.mark.asyncio
async def test_platform_receipt_classification_is_conservative(status, body, outcome):
    channel = _channel()
    channel._http.post.return_value = httpx.Response(status, json=body)
    assert (await _send(channel))["outcome"] == outcome
    assert channel._http.post.await_count == 1


@pytest.mark.parametrize("error,outcome", [
    (httpx.ReadTimeout("test timeout"), "unknown"),
    (httpx.ConnectTimeout("test timeout"), "pending"),
    (httpx.ConnectError("test connection refused"), "pending"),
])
@pytest.mark.asyncio
async def test_transport_failure_does_not_retry_an_ambiguous_send(error, outcome):
    channel = _channel()
    channel._http.post.side_effect = error
    assert (await _send(channel))["outcome"] == outcome
    assert channel._http.post.await_count == 1


@pytest.mark.asyncio
async def test_unreachable_token_service_waits_before_asking_again():
    channel = _channel()
    channel._access_token = None
    channel._http.post.side_effect = httpx.ConnectError("test connection refused")
    assert await _send(channel) == {"outcome": "pending", "error_code": "DINGTALK_TOKEN_UNAVAILABLE"}
    assert (await _send(channel))["outcome"] == "pending"
    assert channel._http.post.await_count == 1


@pytest.mark.asyncio
async def test_refused_credentials_fail_instead_of_retrying_every_poll():
    channel = _channel()
    channel._access_token = None
    channel._http.post.return_value = httpx.Response(400, json={"code": "invalidClientIdOrSecret"})
    assert await _send(channel) == {"outcome": "failed", "error_code": "DINGTALK_CREDENTIAL_REJECTED"}
    assert channel._http.post.await_count == 1


@pytest.mark.asyncio
async def test_revoked_cached_token_is_replaced_once_but_a_fresh_one_is_not_retried():
    channel = _channel()
    channel._http.post.return_value = httpx.Response(401, json={})
    assert await _send(channel) == {"outcome": "pending", "error_code": "DINGTALK_TOKEN_EXPIRED"}
    assert channel._access_token is None
    channel._http.post.side_effect = [
        httpx.Response(200, json={"accessToken": "fake-new-token", "expireIn": 7200}),
        httpx.Response(401, json={}),
    ]
    assert (await _send(channel))["outcome"] == "failed"


@pytest.mark.asyncio
async def test_unsupported_file_never_requests_a_token_or_sends():
    channel = _channel()
    channel._access_token = None
    assert (await _send(channel, file_key="file-test"))["outcome"] == "failed"
    channel._http.post.assert_not_awaited()


@pytest.mark.parametrize("file_name", ["slides.pptx", "notes.txt", "report.docx"])
@pytest.mark.asyncio
async def test_group_file_result_uploads_the_original_file_to_the_group_space(tmp_path, file_name):
    channel = _channel()
    source = tmp_path / file_name
    source.write_bytes(b"file contents")
    channel._file_service = SimpleNamespace(
        upload_group_file=AsyncMock(return_value={"dentryId": "dentry-test"}),
    )

    result = await channel.send_persistent_result(
        chat_id="conversation-test",
        text=None,
        source_message_id="message-test",
        idempotency_key="delivery-test",
        file_path=str(source),
        is_direct_message=False,
        sender_staff_id="staff-test",
        share_mode="upload",
    )

    assert result == {
        "outcome": "delivered", "external_message_id": "dentry-test", "file_key": "dentry-test",
    }
    channel._file_service.upload_group_file.assert_awaited_once_with(
        conversation_id="conversation-test",
        sender_staff_id="staff-test",
        file_path=source,
        access_token="fake-token",
    )


@pytest.mark.asyncio
async def test_attachment_mode_keeps_the_file_in_the_group_chat(tmp_path):
    channel = _channel()
    source = tmp_path / "slides.pptx"
    source.write_bytes(b"pptx bytes")
    channel._file_service = SimpleNamespace(upload_media=AsyncMock(return_value="media-test"))
    channel._http.post.return_value = httpx.Response(200, json={"processQueryKey": "message-test"})

    result = await channel.send_persistent_result(
        chat_id="conversation-test",
        text=None,
        source_message_id="source-test",
        idempotency_key="delivery-test",
        file_path=str(source),
        file_name="slides.pptx",
        share_mode="attachment",
        is_direct_message=False,
    )

    assert result == {
        "outcome": "delivered", "external_message_id": "message-test", "file_key": "media-test",
    }
    channel._file_service.upload_media.assert_awaited_once_with(str(source), "file")
    request = channel._http.post.await_args
    assert request.args[0] == "https://api.example.invalid/v1.0/robot/groupMessages/send"
    assert request.kwargs["json"]["msgKey"] == "sampleFile"
    assert '"fileType": "pptx"' in request.kwargs["json"]["msgParam"]


@pytest.mark.parametrize("has_text", [False, True])
@pytest.mark.parametrize("field,error_code", [
    ("invalidStaffIdList", "DINGTALK_TARGET_INVALID"),
    ("flowControlledStaffIdList", "DINGTALK_RATE_LIMITED"),
    ("filteredStaffIdList", "DINGTALK_TARGET_FILTERED"),
])
@pytest.mark.asyncio
async def test_attachment_rejected_targets_are_not_reported_as_delivered(field, error_code, has_text):
    channel = _channel()
    channel._http.post.return_value = httpx.Response(200, json={
        "processQueryKey": "receipt-test", field: ["staff-test"],
    })

    result = await channel.send_persistent_result(
        chat_id="staff-test", text="caption" if has_text else None, source_message_id=None,
        idempotency_key="delivery-test", file_key="media-test", share_mode="attachment",
    )

    assert result == {"outcome": "failed", "error_code": error_code, "file_key": "media-test"}
    assert channel._http.post.await_count == 1


@pytest.mark.parametrize("retry_status", [200, 401])
@pytest.mark.asyncio
async def test_attachment_replaces_a_revoked_cached_token_once(retry_status):
    channel = _channel()
    channel._http.post.return_value = httpx.Response(401, json={})
    arguments = {
        "chat_id": "staff-test", "text": None, "source_message_id": None,
        "idempotency_key": "delivery-test", "file_key": "media-test", "share_mode": "attachment",
    }

    result = await channel.send_persistent_result(**arguments)

    assert result == {"outcome": "pending", "error_code": "DINGTALK_TOKEN_EXPIRED", "file_key": "media-test"}
    assert channel._access_token is None
    assert channel._http.post.await_count == 1
    channel._http.post.side_effect = [
        httpx.Response(200, json={"accessToken": "fake-new-token", "expireIn": 7200}),
        httpx.Response(retry_status, json={"processQueryKey": "receipt-test"} if retry_status == 200 else {}),
    ]

    result = await channel.send_persistent_result(**arguments)

    expected = {"outcome": "delivered", "external_message_id": "receipt-test"} if retry_status == 200 else {
        "outcome": "failed", "error_code": "DINGTALK_REJECTED",
    }
    assert result == {**expected, "file_key": "media-test"}
    assert channel._http.post.await_count == 3
    request = channel._http.post.await_args
    assert request.kwargs["headers"]["x-acs-dingtalk-access-token"] == "fake-new-token"


@pytest.mark.asyncio
async def test_claimed_input_precedes_media_download_and_never_falls_back():
    channel = _channel()
    channel.handle_file_message = AsyncMock()
    channel.handle_incoming_message = AsyncMock()
    callback = AsyncMock()
    channel.set_persistent_inbound_callback(callback)
    handler = DingTalkHandler(channel)
    await handler.process(SimpleNamespace(data=_callback(msgtype="file")))
    channel.handle_file_message.assert_not_awaited()
    channel.handle_incoming_message.assert_not_awaited()
    callback.assert_not_awaited()
    await handler.process(SimpleNamespace(data=_callback(
        conversationType="2", isInAtList=True, chatbotUserId="bot-test",
        atUsers=[{"dingtalkId": "bot-test"}],
    )))
    await asyncio.gather(*tuple(channel._background_tasks))
    group_event = callback.await_args.args[0]
    assert group_event.conversation_type == "2" and group_event.receiver_bot_mentioned is True
    assert channel.handle_file_message.await_count == 0
    assert channel.handle_incoming_message.await_count == 0
    await handler.process(SimpleNamespace(data=_callback()))
    await asyncio.gather(*channel._background_tasks)
    assert callback.await_count == 2
    event = callback.await_args_list[-1].args[0]
    assert event.expected_agent_id == "agent-dingtalk" and event.receiving_app_id == "app-test"
    channel.handle_incoming_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_claimed_route_rejects_whitelist_and_robot_identity_mismatch():
    channel = _channel()
    callback = AsyncMock()
    channel.set_persistent_inbound_callback(callback)
    channel.enqueue_persistent_event(_callback(senderStaffId="not-allowed"))
    channel.enqueue_persistent_event(_callback(robotCode="other-robot"))
    assert not channel._background_tasks
    callback.assert_not_awaited()


def test_routing_keys_keep_the_default_app_on_the_legacy_key():
    router = MagicMock(spec=RobotMessageRouter)
    ordinary = DingTalkChannel(DingTalkConfig(client_id="ordinary"), router)
    converted = DingTalkChannel(DingTalkConfig(client_id="converted", agent_id="agent-a", is_default=True), router)
    other = DingTalkChannel(DingTalkConfig(client_id="other", agent_id="agent-b", is_default=False), router)
    assert ordinary.app_id == converted.app_id == "default" and other.app_id == "other"
    assert converted.persistent_app_id == "converted"

@pytest.mark.asyncio
@pytest.mark.parametrize("custom_signed_upload", [False, True])
async def test_group_space_upload_runs_precheck_signed_put_and_commit(tmp_path, custom_signed_upload):
    source = tmp_path / "slides.pptx"
    content = b"pptx bytes"
    source.write_bytes(content)
    http = AsyncMock(spec=httpx.AsyncClient)
    http.put.return_value = httpx.Response(200)
    http.post.side_effect = [
        httpx.Response(200, json={"result": {"unionid": "union-test"}}),
        httpx.Response(200, json={"space": {"spaceId": "space-test"}}),
        httpx.Response(200, json={
            "uploadKey": "upload-test",
            "headerSignatureInfo": {
                "resourceUrls": ["https://oss.aliyuncs.com/signed-upload"],
                "headers": {"x-oss-signature": "signed"},
            },
        }),
        httpx.Response(200, json={"dentry": {"id": "dentry-test", "uuid": "uuid-test"}}),
    ]
    signed_upload = AsyncMock()
    service = DingTalkFileService(
        client_id="robot-test",
        get_token_func=AsyncMock(return_value="token-test"),
        http_client=http,
        api_base="https://api.dingtalk.com",
        oapi_base="https://oapi.dingtalk.com",
        signed_upload=signed_upload if custom_signed_upload else None,
    )

    result = await service.upload_group_file(
        conversation_id="conversation-test",
        sender_staff_id="staff-test",
        file_path=source,
    )

    assert result == {"dentryId": "dentry-test", "dentryUuid": "uuid-test"}
    upload_call = http.post.await_args_list[2]
    upload_body = upload_call.kwargs["json"]
    assert upload_body["option"]["preCheckParam"] == {
        "md5": "57711f9ddb417149cf1c69fd210fbd71",
        "size": len(content),
        "parentId": "0",
        "name": "slides.pptx",
    }
    if custom_signed_upload:
        signed_upload.assert_awaited_once_with(
            "https://oss.aliyuncs.com/signed-upload",
            {"x-oss-signature": "signed"},
            content,
        )
        http.put.assert_not_awaited()
    else:
        http.put.assert_awaited_once_with(
            "https://oss.aliyuncs.com/signed-upload",
            content=content,
            headers={"x-oss-signature": "signed"},
        )


@pytest.mark.asyncio
async def test_group_space_upload_uses_the_group_root_dentry_protocol(tmp_path):
    source = tmp_path / "deck.pptx"
    content = b"pptx bytes"
    source.write_bytes(content)
    http = AsyncMock(spec=httpx.AsyncClient)
    http.post.side_effect = [
        httpx.Response(200, json={"result": {"unionid": "union-test"}}),
        httpx.Response(200, json={"space": {"spaceId": "space-test", "rootDentryUuid": "root-test"}}),
        httpx.Response(200, json={
            "uploadKey": "upload-test",
            "headerSignatureInfo": {
                "resourceUrls": ["https://oss.aliyuncs.com/signed-upload"],
                "headers": {"x-oss-signature": "signed"},
            },
        }),
        httpx.Response(200, json={"dentry": {"id": "dentry-test", "uuid": "uuid-test"}}),
    ]
    service = DingTalkFileService(
        client_id="robot-test",
        get_token_func=AsyncMock(return_value="token-test"),
        http_client=http,
        api_base="https://api.dingtalk.com",
        oapi_base="https://oapi.dingtalk.com",
        signed_upload=AsyncMock(),
    )

    result = await service.upload_group_file(
        conversation_id="conversation-test", sender_staff_id="staff-test", file_path=source,
    )

    upload_call = http.post.await_args_list[2]
    assert upload_call.args[0] == "https://api.dingtalk.com/v2.0/storage/spaces/files/root-test/uploadInfos/query"
    assert upload_call.kwargs["json"]["option"]["preCheckParam"] == {
        "size": len(content), "name": "deck.pptx",
    }
    commit_call = http.post.await_args_list[3]
    assert commit_call.args[0] == "https://api.dingtalk.com/v2.0/storage/spaces/files/root-test/commit"
    assert result["dentryId"] == "dentry-test"
