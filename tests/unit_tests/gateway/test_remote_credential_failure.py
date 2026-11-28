# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Typed failures for remote credential and login-binding access."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import websockets
from e2a.message import Message, ReqMethod


@pytest.fixture(autouse=True)
def isolated_gateway_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    data = tmp_path / "data"
    logs = tmp_path / "logs"
    temporary = tmp_path / "tmp"
    for path in (home, data, logs, temporary):
        path.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_DATA_DIR", str(data))
    monkeypatch.setenv("JIUWENSWARM_LOG_DIR", str(logs))
    monkeypatch.setenv("AGENTOS_GATEWAY_LOG_DIR", str(logs))
    monkeypatch.setenv("TMPDIR", str(temporary))
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[3]))
    monkeypatch.setenv("JIUWENSWARM_CONFIG_URL", "http://127.0.0.1:9/isolated-test-config")


def _message(request_id: str = "request-a") -> Message:
    return Message(
        type="request",
        timestamp=1.0,
        ok=True,
        id=request_id,
        channel_id="web",
        session_id="session-a",
        req_method=ReqMethod.CHAT_SEND,
        user_id="business-a",
        params={
            "content": "hello",
            "model_name": "login-model",
            "_model_auth": {"api_key": "client-secret"},
        },
        metadata={"auth_session": "login-a"},
    )


def _handler(port: SimpleNamespace, *, model: str = "login-model"):
    from agent_gateway.message_handler.message_handler import MessageHandler

    handler = object.__new__(MessageHandler)
    handler.agent_client = SimpleNamespace(send_request=AsyncMock())
    handler._effective_session_model = AsyncMock(return_value=model)
    handler.set_session_login_bindings(port)
    return handler


def _env(channel: str = "feishu", params: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        channel=channel,
        session_id="session-a",
        method="chat.send",
        params=dict(params or {}),
        user_id="business-user",
        is_stream=False,
        request_id="request-a",
    )


@pytest.mark.parametrize(
    "reason",
    ["unavailable", "invalid", "protocol", "not_enabled"],
)
def test_credential_access_error_reasons_are_whitelisted(reason: str) -> None:
    from agent_gateway.auth.credential_access import CredentialAccessError

    error = CredentialAccessError(reason)
    assert error.reason == reason
    assert str(error) == "Remote credential access failed"
    assert CredentialAccessError().reason == "unavailable"


def test_credential_access_error_rejects_non_whitelisted_reason() -> None:
    from agent_gateway.auth.credential_access import CredentialAccessError

    with pytest.raises(ValueError, match="Unsupported credential failure reason"):
        CredentialAccessError("client-secret")


async def test_preprocessor_propagates_typed_failure_without_agent_dispatch() -> None:
    from agent_gateway.application import inbound
    from agent_gateway.auth.credential_access import CredentialAccessError

    error = CredentialAccessError("invalid")
    access = SimpleNamespace(request_model_auth=AsyncMock(side_effect=error))
    processor = inbound.MessagePreprocessor(
        AsyncMock(), lambda msg: msg, lambda msg: None
    ).with_credential_access(access)
    manager = SimpleNamespace(deliver_to_message_handler=AsyncMock())
    message = _message()

    with pytest.raises(CredentialAccessError) as caught:
        await processor.forward(message, manager)

    assert caught.value is error
    assert "_model_auth" not in message.params
    manager.deliver_to_message_handler.assert_not_awaited()


async def test_preprocessor_keeps_ordinary_exception_fallback() -> None:
    from agent_gateway.application import inbound

    access = SimpleNamespace(
        request_model_auth=AsyncMock(side_effect=ConnectionError("offline"))
    )
    processor = inbound.MessagePreprocessor(
        AsyncMock(), lambda msg: msg, lambda msg: None
    ).with_credential_access(access)
    manager = SimpleNamespace(deliver_to_message_handler=AsyncMock())
    message = _message()

    await processor.forward(message, manager)

    sent = manager.deliver_to_message_handler.await_args.args[0]
    assert "_model_auth" not in message.params
    assert "_model_auth" not in sent.params
    manager.deliver_to_message_handler.assert_awaited_once()


@pytest.mark.parametrize("rejection", [False, asyncio.CancelledError()])
async def test_preprocessor_releases_credential_for_unaccepted_request(rejection) -> None:
    from unittest.mock import Mock

    from agent_gateway.application import inbound
    from agent_gateway.auth.credential_access import ModelCredential

    access = SimpleNamespace(
        request_model_auth=AsyncMock(return_value=ModelCredential("https://test.invalid", "key", "ref")),
        release_request=Mock(),
    )
    processor = inbound.MessagePreprocessor(
        AsyncMock(), lambda msg: msg, lambda msg: None,
    ).with_credential_access(access)
    delivery = AsyncMock(side_effect=rejection) if isinstance(rejection, BaseException) else AsyncMock(return_value=False)
    manager = SimpleNamespace(deliver_to_message_handler=delivery)
    if isinstance(rejection, BaseException):
        with pytest.raises(asyncio.CancelledError):
            await processor.forward(_message(), manager)
    else:
        await processor.forward(_message(), manager)
    access.release_request.assert_called_once_with(delivery.await_args.args[0])


async def test_handler_propagates_typed_remember_failure() -> None:
    from agent_gateway.auth.credential_access import CredentialAccessError
    from e2a.constants import E2A_MODEL_AUTH_PARAM_KEY

    error = CredentialAccessError("protocol")
    port = SimpleNamespace(remember=AsyncMock(side_effect=error))
    handler = _handler(port)
    env = _env(params={E2A_MODEL_AUTH_PARAM_KEY: {"credential_ref": "ref"}})

    with pytest.raises(CredentialAccessError) as caught:
        await handler._apply_session_login_owner(env)

    assert caught.value is error


async def test_handler_keeps_ordinary_remember_fallback() -> None:
    from e2a.constants import E2A_MODEL_AUTH_PARAM_KEY

    port = SimpleNamespace(
        remember=AsyncMock(side_effect=ConnectionError("offline"))
    )
    handler = _handler(port)
    env = _env(params={E2A_MODEL_AUTH_PARAM_KEY: {"credential_ref": "ref"}})

    await handler._apply_session_login_owner(env)

    assert E2A_MODEL_AUTH_PARAM_KEY in env.params


async def test_handler_propagates_typed_binding_failure_without_agent_dispatch() -> None:
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.auth.session_login_bindings import SessionLoginBinding
    from e2a.constants import (
        E2A_LOGIN_REQUIRED_HINT_PARAM_KEY,
        E2A_MODEL_AUTH_PARAM_KEY,
    )

    error = CredentialAccessError("unavailable")
    binding = SessionLoginBinding("a" * 32, "login-model")
    port = SimpleNamespace(
        remember=AsyncMock(),
        lookup=AsyncMock(return_value=binding),
        model_auth_for_binding=AsyncMock(side_effect=error),
    )
    handler = _handler(port)
    env = _env()

    with pytest.raises(CredentialAccessError) as caught:
        await handler._send_non_stream_agent_request(env)

    assert caught.value is error
    assert E2A_LOGIN_REQUIRED_HINT_PARAM_KEY not in env.params
    assert E2A_MODEL_AUTH_PARAM_KEY not in env.params
    handler.agent_client.send_request.assert_not_awaited()


async def test_handler_propagates_typed_lookup_failure() -> None:
    from agent_gateway.auth.credential_access import CredentialAccessError
    from e2a.constants import (
        E2A_LOGIN_REQUIRED_HINT_PARAM_KEY,
        E2A_MODEL_AUTH_PARAM_KEY,
    )

    error = CredentialAccessError("not_enabled")
    port = SimpleNamespace(
        remember=AsyncMock(),
        lookup=AsyncMock(side_effect=error),
        model_auth_for_binding=AsyncMock(),
    )
    handler = _handler(port)
    env = _env()

    with pytest.raises(CredentialAccessError) as caught:
        await handler._apply_session_login_owner(env)

    assert caught.value is error
    assert E2A_LOGIN_REQUIRED_HINT_PARAM_KEY not in env.params
    assert E2A_MODEL_AUTH_PARAM_KEY not in env.params
    port.model_auth_for_binding.assert_not_awaited()


async def test_handler_keeps_ordinary_binding_failure_fallback() -> None:
    from agent_gateway.auth.session_login_bindings import SessionLoginBinding
    from agent_gateway.message_handler import message_handler as module
    from e2a.constants import (
        E2A_LOGIN_REQUIRED_HINT_PARAM_KEY,
        E2A_MODEL_AUTH_PARAM_KEY,
    )

    binding = SessionLoginBinding("a" * 32, "login-model")
    port = SimpleNamespace(
        remember=AsyncMock(),
        lookup=AsyncMock(return_value=binding),
        model_auth_for_binding=AsyncMock(
            side_effect=ConnectionError("offline")
        ),
    )
    handler = _handler(port)
    env = _env()

    await handler._apply_session_login_owner(env)

    assert env.params[E2A_LOGIN_REQUIRED_HINT_PARAM_KEY] == module._LOGIN_HINT_RETRY
    assert E2A_MODEL_AUTH_PARAM_KEY not in env.params


class _RecordingSocket:
    def __init__(self) -> None:
        self.frames: list[dict] = []

    async def send(self, frame: str) -> None:
        self.frames.append(json.loads(frame))


async def test_web_dispatch_returns_fixed_credential_service_failure() -> None:
    from agent_gateway.auth.credential_access import CredentialAccessError
    from agent_gateway.channel_manager.web.invoke import dispatch_web_request
    from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig

    ws = _RecordingSocket()
    channel = WebChannel(WebChannelConfig(), SimpleNamespace())
    channel._enqueue_send = lambda target, frame: target.frames.append(frame)
    channel.on_message(AsyncMock(side_effect=CredentialAccessError("invalid")))
    message = _message()
    await dispatch_web_request(channel, method="chat.send", params=message.params,
                               request_id=message.id, outbound=ws, session_id=message.session_id,
                               user_message=message, authorization=None, jiuwenclaw_id=None)

    assert ws.frames == [
        {
            "type": "res",
            "id": "request-a",
            "ok": False,
            "payload": {},
            "error": "Remote credential access failed",
            "code": "CREDENTIAL_SERVICE_FAILED",
        }
    ]


async def test_inbound_queue_error_callback_receives_message_and_error() -> None:
    from agent_gateway.application.inbound import InboundQueue
    from agent_gateway.auth.credential_access import CredentialAccessError

    error = CredentialAccessError("protocol")
    message = _message("queue-error")
    callback_error = asyncio.Event()
    received: list[tuple[Message, Exception]] = []

    async def inbound_handler(received_message: Message) -> None:
        raise error

    async def error_callback(received_message: Message, received_error: Exception) -> None:
        received.append((received_message, received_error))
        callback_error.set()

    queue = InboundQueue(inbound_handler, error_callback=error_callback)
    await queue.start()
    try:
        await queue.handle_message(message)
        await asyncio.wait_for(callback_error.wait(), 1)
    finally:
        await queue.stop()

    assert received == [(message, error)]


async def test_inbound_queue_without_error_callback_keeps_original_behavior() -> None:
    from agent_gateway.application.inbound import InboundQueue
    from agent_gateway.auth.credential_access import CredentialAccessError

    second_message_handled = asyncio.Event()

    async def inbound_handler(received_message: Message) -> None:
        if received_message.id == "queue-first":
            raise CredentialAccessError("unavailable")
        second_message_handled.set()

    queue = InboundQueue(inbound_handler)
    await queue.start()
    try:
        await queue.handle_message(_message("queue-first"))
        await queue.handle_message(_message("queue-second"))
        await asyncio.wait_for(second_message_handled.wait(), 1)
    finally:
        await queue.stop()


async def test_remote_owned_deadline_timeout_is_typed_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.auth.credential_access import (
        CredentialAccessError,
        OwnsResolveDeadline,
    )
    from agent_gateway.auth.session_login_bindings import SessionLoginBinding
    from agent_gateway.message_handler import message_handler as module
    from e2a.constants import (
        E2A_LOGIN_REQUIRED_HINT_PARAM_KEY,
        E2A_MODEL_AUTH_PARAM_KEY,
    )

    class SlowRemoteBindings(OwnsResolveDeadline):
        resolve_deadline_s = 0.03

        async def remember(self, session_id: str, credential_ref: str, model_name: str) -> None:
            del session_id, credential_ref, model_name

        async def lookup(self, session_id: str) -> SessionLoginBinding | None:
            return SessionLoginBinding("a" * 32, "login-model")

        async def model_auth_for_binding(self, binding: SessionLoginBinding) -> None:
            del binding
            await asyncio.sleep(self.resolve_deadline_s)
            raise CredentialAccessError("unavailable")

    monkeypatch.setattr(module, "_SESSION_OWNER_AUTH_WAIT_S", 0.01)
    handler = _handler(SlowRemoteBindings())
    env = _env()

    with pytest.raises(CredentialAccessError) as caught:
        await handler._send_non_stream_agent_request(env)

    assert caught.value.reason == "unavailable"
    assert E2A_LOGIN_REQUIRED_HINT_PARAM_KEY not in env.params
    assert E2A_MODEL_AUTH_PARAM_KEY not in env.params
    handler.agent_client.send_request.assert_not_awaited()


async def test_acp_typed_failure_returns_jsonrpc_error_without_agent_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application import inbound, tui_acp
    from agent_gateway.auth.credential_access import CredentialAccessError

    error = CredentialAccessError("unavailable")
    access = SimpleNamespace(request_model_auth=AsyncMock(side_effect=error))
    preprocessor = inbound.MessagePreprocessor(
        AsyncMock(), lambda message: message, lambda message: None
    ).with_credential_access(access)
    manager = SimpleNamespace(
        register_channel_with_inbound=lambda channel, handler: None,
        register_external_channel=lambda channel_id, channel: None,
        deliver_to_message_handler=AsyncMock(),
    )
    monkeypatch.setenv("GATEWAY_HOST", "127.0.0.1")
    monkeypatch.setenv("GATEWAY_PORT", "0")

    assembly = await tui_acp.assemble_tui_acp(
        agent_client=SimpleNamespace(),
        message_handler=SimpleNamespace(),
        channel_manager=manager,
        third_agent=None,
        on_config_saved=lambda *args, **kwargs: None,
        cron_controller=None,
        heartbeat_controller=None,
        preprocessor=preprocessor,
        log_stage=lambda stage: None,
    )
    try:
        await assembly.protocol_runtime.start()
        host, port = assembly.protocol_runtime._listener.bound_addresses[0]
        async with websockets.connect(f"ws://{host}:{port}/acp") as client:
            acknowledgement = json.loads(await asyncio.wait_for(client.recv(), 1))
            assert acknowledgement["event"] == "connection.ack"
            await client.send(
                json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": "acp-error",
                        "method": "session/prompt",
                        "params": {
                            "sessionId": "session-a",
                            "prompt": [{"type": "text", "text": "hello"}],
                        },
                    }
                )
            )
            response = json.loads(await asyncio.wait_for(client.recv(), 2))

        assert response == {
            "jsonrpc": "2.0",
            "id": "acp-error",
            "error": {
                "code": -32603,
                "message": "Remote credential access failed",
            },
        }
        manager.deliver_to_message_handler.assert_not_awaited()
    finally:
        await assembly.stop()


async def test_tui_typed_failure_returns_protocol_error_without_agent_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application import inbound, tui_acp
    from agent_gateway.auth.credential_access import CredentialAccessError

    error = CredentialAccessError("invalid")
    access = SimpleNamespace(request_model_auth=AsyncMock(side_effect=error))
    preprocessor = inbound.MessagePreprocessor(
        AsyncMock(), lambda message: message, lambda message: None
    ).with_credential_access(access)
    manager = SimpleNamespace(
        register_channel_with_inbound=lambda channel, handler: None,
        register_external_channel=lambda channel_id, channel: None,
        deliver_to_message_handler=AsyncMock(),
    )
    monkeypatch.setenv("GATEWAY_HOST", "127.0.0.1")
    monkeypatch.setenv("GATEWAY_PORT", "0")

    assembly = await tui_acp.assemble_tui_acp(
        agent_client=SimpleNamespace(),
        message_handler=SimpleNamespace(cancel_agent_sessions_on_disconnect=AsyncMock()),
        channel_manager=manager,
        third_agent=None,
        on_config_saved=lambda *args, **kwargs: None,
        cron_controller=None,
        heartbeat_controller=None,
        preprocessor=preprocessor,
        log_stage=lambda stage: None,
    )
    try:
        await assembly.protocol_runtime.start()
        host, port = assembly.protocol_runtime._listener.bound_addresses[0]
        async with websockets.connect(f"ws://{host}:{port}/tui") as client:
            acknowledgement = json.loads(await asyncio.wait_for(client.recv(), 1))
            assert acknowledgement["event"] == "connection.ack"
            await client.send(
                json.dumps(
                    {
                        "type": "req",
                        "id": "tui-error",
                        "method": "chat.send",
                        "params": {
                            "session_id": "session-a",
                            "content": "hello",
                            "model_name": "login-model",
                        },
                    }
                )
            )
            response = json.loads(await asyncio.wait_for(client.recv(), 2))
            while (
                response.get("ok") is True
                and response.get("payload", {}).get("accepted") is True
            ):
                response = json.loads(await asyncio.wait_for(client.recv(), 2))

        assert response == {
            "type": "res",
            "id": "tui-error",
            "ok": False,
            "payload": {
                "error": "Remote credential access failed",
                "code": "CREDENTIAL_SERVICE_FAILED",
            },
            "error": "Remote credential access failed",
            "code": "CREDENTIAL_SERVICE_FAILED",
        }
        manager.deliver_to_message_handler.assert_not_awaited()
    finally:
        await assembly.stop()
