from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.application.worker_cron_commands import WorkerCronRouteUnknown
from agent_gateway.processes.control_service import (
    ControlServiceBusy,
    ControlServiceOperationError,
)


def make_ownership() -> CronOwnership:
    return CronOwnership(WorkerManifest((0, 1)), LocalIdentityPolicy())


def owner_slot(user_id: str = "owner") -> int:
    return make_ownership().owner_slot(user_id)


def make_command(
    *,
    request_id: str = "request-1",
    session_id: str | None = "session-1",
    command_id: str = "command-1",
    owner_user_id: str = "owner",
    action: str = "list",
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "request_id": request_id,
        "session_id": session_id,
        "channel_id": "web",
        "owner_user_id": owner_user_id,
        "owner_group_id": "default",
        "owner_bot_id": "default",
        "payload": {
            "event_type": "cron.response",
            "action": action,
            "command_id": command_id,
            "data": {} if data is None else data,
        },
    }


def owner_metadata(user_id: str = "owner") -> dict[str, str]:
    return {
        "_jiuwenswarm_cron_owner_user_id": user_id,
        "_jiuwenswarm_cron_owner_group_id": "default",
        "_jiuwenswarm_cron_owner_bot_id": "default",
    }


class RecordingHandler:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.command_acks: list[dict[str, Any]] = []
        self.run_now_acks: list[dict[str, Any]] = []
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.block = False

    async def _handle_cron_push_payload(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)
        self.started.set()
        if self.block:
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled.set()
                raise

    async def _push_cron_command_ack(self, **kwargs: Any) -> None:
        self.command_acks.append(kwargs)

    async def _push_cron_run_now_ack(self, **kwargs: Any) -> None:
        self.run_now_acks.append(kwargs)


class FailingHandler(RecordingHandler):
    async def _handle_cron_push_payload(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)
        self.started.set()
        raise RuntimeError("cron controller failed")


class SlowAckHandler(RecordingHandler):
    async def _push_cron_command_ack(self, **kwargs: Any) -> None:
        await asyncio.sleep(10)
        self.command_acks.append(kwargs)


class UncancellableHandler(RecordingHandler):
    async def _handle_cron_push_payload(self, **kwargs: Any) -> None:
        self.calls.append(kwargs)
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            await self.release.wait()


class RecordingControlClient:
    def __init__(
        self,
        result: Any = {"status": "accepted"},
        *,
        error: BaseException | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.calls: list[dict[str, Any]] = []
        self.disconnect_called = False

    async def call(self, operation: str, arguments: dict[str, Any], *, timeout: float) -> Any:
        self.calls.append(
            {"operation": operation, "arguments": arguments, "timeout": timeout}
        )
        if self.error is not None:
            raise self.error
        return self.result

    async def disconnect(self) -> None:
        self.disconnect_called = True


def make_runtime(
    *,
    slot_id: int | None = None,
    handler: RecordingHandler | None = None,
    client: RecordingControlClient | None = None,
    enabled: Any = None,
) -> Any:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    return WorkerCronCommands(
        client=client or RecordingControlClient(),
        message_handler=handler or RecordingHandler(),
        ownership=make_ownership(),
        slot_id=slot_id if slot_id is not None else owner_slot(),
        **({} if enabled is None else {"enabled": enabled}),
    )


def chat_envelope(
    *,
    request_id: str = "request-1",
    user_id: str = "owner",
    session_id: str | None = "session-1",
    mode: str = "work",
) -> Any:
    return SimpleNamespace(
        method="chat.send",
        request_id=request_id,
        user_id=user_id,
        session_id=session_id,
        params={
            "mode": mode,
            "request_id": "inner-request-id",
            "auth_session": "forged",
            "user_id": "forged",
        },
    )


async def wait_until(predicate) -> None:
    for _ in range(200):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition was not met")


@pytest.mark.asyncio
async def test_accepted_command_uses_local_chat_context_and_sanitizes_llm_identity() -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)

    commands.remember(
        chat_envelope(),
        {
            "auth_session": "trusted-session",
            "mode": "forged-mode",
            "user_id": "forged-user",
        },
    )
    commands.complete("request-1")

    await commands.accept(
        make_command(
            data={
                "description": "nightly report",
                "user_id": "llm-user",
                "group_id": "llm-group",
                "bot_id": "llm-bot",
                "nested": {"user_id": "nested-user", "group_id": "nested-group"},
            }
        )
    )
    await asyncio.wait_for(handler.started.wait(), 1)
    await commands.stop()

    assert len(handler.calls) == 1
    call = handler.calls[0]
    assert call["channel_id"] == ""
    assert call["session_id"] == "session-1"
    assert call["user_id"] == "owner"
    assert call["_trusted_cron_metadata"] == {
        "auth_session": "trusted-session",
        "mode": "work",
    }
    assert call["payload"]["data"] == {
        "description": "nightly report",
        "group_id": "",
        "bot_id": "",
        "nested": {},
    }
    assert "_auth_session" not in call["payload"]["data"]
    assert handler.command_acks == []


@pytest.mark.asyncio
async def test_non_chat_requests_do_not_use_context_cache() -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)

    commands.remember(
        SimpleNamespace(
            method="config.get",
            request_id="request-1",
            user_id="owner",
            session_id="session-1",
            params={"mode": "work", "auth_session": "forged"},
        ),
        {"auth_session": "trusted-session", "mode": "forged"},
    )
    commands.complete("request-1")
    await commands.accept(make_command())
    await asyncio.wait_for(handler.started.wait(), 1)
    await commands.stop()

    assert handler.calls[0]["_trusted_cron_metadata"] == {}


def test_remember_rejects_identity_reuse_and_reports_active_capacity(monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_gateway.application.worker_cron_commands import (
        WorkerCronCommands,
        WorkerCronContextCapacityError,
    )

    commands = make_runtime()
    commands.remember(
        chat_envelope(),
        {"auth_session": "session-a"},
    )

    with pytest.raises(ValueError, match="request_id context conflict"):
        commands.remember(
            chat_envelope(user_id="alice"),
            {"auth_session": "session-a"},
        )

    commands.remember(
        chat_envelope(),
        {"auth_session": "session-a", "mode": "ignored"},
    )

    monkeypatch.setattr(WorkerCronCommands, "_ACTIVE_CONTEXT_LIMIT", 1)
    with pytest.raises(WorkerCronContextCapacityError, match="active context cache is full"):
        commands.remember(chat_envelope(request_id="request-2"), None)


def test_remember_ignores_anonymous_or_invalid_users() -> None:
    commands = make_runtime()

    commands.remember(chat_envelope(user_id=""), {"auth_session": "session"})
    commands.remember(chat_envelope(user_id="default"), {"auth_session": "session"})
    commands.remember(chat_envelope(request_id="r" * 257), None)
    commands.remember(chat_envelope(user_id="u" * 257), None)
    commands.remember(chat_envelope(session_id="s" * 257), None)

    commands.remember(
        SimpleNamespace(
            method="chat.send",
            request_id="",
            user_id="owner",
            session_id="session",
            params={},
        ),
        {"auth_session": "session"},
    )
    commands.remember(chat_envelope(), {"auth_session": "session"})

    commands.complete("request-1")


@pytest.mark.asyncio
async def test_complete_keeps_bounded_terminal_context_for_sixty_seconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    now = 100.0
    monkeypatch.setattr(WorkerCronCommands, "_now", lambda self: now)
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    commands.remember(chat_envelope(), {"auth_session": "session-a"})
    commands.complete("request-1")

    await commands.accept(make_command(command_id="command-1"))
    await wait_until(lambda: len(handler.calls) == 1)
    assert handler.calls[0]["_trusted_cron_metadata"] == {
        "auth_session": "session-a",
        "mode": "work",
    }

    now = 159.9
    await commands.accept(make_command(command_id="command-2"))
    await wait_until(lambda: len(handler.calls) == 2)
    assert handler.calls[1]["_trusted_cron_metadata"] is not None

    now = 160.0
    await commands.accept(make_command(command_id="command-3"))
    await wait_until(lambda: len(handler.calls) == 3)
    assert handler.calls[2]["_trusted_cron_metadata"] == {}
    await commands.stop()


@pytest.mark.asyncio
async def test_terminal_context_is_bounded_after_capacity_is_reached(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    monkeypatch.setattr(WorkerCronCommands, "_FINAL_CONTEXT_LIMIT", 1)
    commands.remember(chat_envelope(request_id="request-1"), {"auth_session": "session-a"})
    commands.complete("request-1")
    commands.remember(chat_envelope(request_id="request-2"), {"auth_session": "session-b"})
    commands.complete("request-2")

    await commands.accept(make_command(request_id="request-1", command_id="command-1"))
    await commands.accept(make_command(request_id="request-2", command_id="command-2"))
    await wait_until(lambda: len(handler.calls) == 2)
    assert handler.calls[0]["_trusted_cron_metadata"] == {}
    assert handler.calls[1]["_trusted_cron_metadata"] == {
        "auth_session": "session-b",
        "mode": "work",
    }
    await commands.stop()


@pytest.mark.asyncio
async def test_handle_push_routes_remote_command_without_auth_metadata() -> None:
    remote_slot = 1 - owner_slot()
    client = RecordingControlClient({"status": "accepted"})
    commands = make_runtime(slot_id=remote_slot, client=client)

    accepted = await commands.handle_push(
        payload={
            "status": "ok",
            "event_type": "cron.response",
            "action": "list",
            "command_id": "command-1",
            "data": {"x": 1},
        },
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata={
            **owner_metadata(),
            "auth_session": "must-not-cross-control",
            "user_id": "ignored",
        },
        user_id="ignored-user",
    )

    assert accepted is True
    assert client.calls == [
        {
            "operation": "cron.route_command",
            "arguments": make_command(data={"x": 1}),
            "timeout": 2.0,
        }
    ]
    assert "auth_session" not in client.calls[0]["arguments"]


@pytest.mark.asyncio
async def test_handle_push_maps_control_statuses_and_leaves_unknown_unacked() -> None:
    remote_slot = 1 - owner_slot()
    handler = RecordingHandler()
    rejected = make_runtime(
        slot_id=remote_slot,
        handler=handler,
        client=RecordingControlClient({"status": "rejected"}),
    )
    assert await rejected.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is False
    assert handler.command_acks[0]["command_id"] == "command-1"
    assert handler.command_acks[0]["data"]["code"] == "CRON_ROUTE_REJECTED"

    from agent_gateway.application.worker_cron_commands import WorkerCronRouteUnknown

    unknown = make_runtime(slot_id=remote_slot, client=RecordingControlClient({"status": "unknown"}))
    with pytest.raises(WorkerCronRouteUnknown):
        await unknown.handle_push(
            payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
            request_id="request-1",
            channel_id="web",
            session_id="session-1",
            metadata=owner_metadata(),
        )


@pytest.mark.asyncio
async def test_accept_rejects_router_server_id_extra_field() -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    command = {**make_command(), "server_id": "as-fixed"}

    await commands.accept(command)
    await wait_until(lambda: len(handler.command_acks) == 1)
    await commands.stop()

    assert handler.calls == []
    assert handler.command_acks[0]["command_id"] == "command-1"
    assert handler.command_acks[0]["data"]["code"] == "CRON_INVALID_COMMAND"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "code"),
    [
        (ControlServiceBusy(), "CRON_ROUTE_BUSY"),
        (ControlServiceOperationError("OPERATION_NOT_ENABLED"), "CRON_ROUTE_REJECTED"),
    ],
)
async def test_remote_deterministic_rejection_sends_negative_ack(
    error: BaseException,
    code: str,
) -> None:
    remote_slot = 1 - owner_slot()
    handler = RecordingHandler()
    commands = make_runtime(
        slot_id=remote_slot,
        handler=handler,
        client=RecordingControlClient(error=error),
    )

    accepted = await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    )

    assert accepted is False
    assert handler.command_acks[0]["command_id"] == "command-1"
    assert handler.command_acks[0]["data"]["code"] == code


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "client",
    [
        RecordingControlClient(error=ControlServiceBusy()),
        RecordingControlClient({"status": "rejected"}),
    ],
    ids=["busy", "rejected"],
)
async def test_remote_deterministic_rejection_is_deduplicated(
    client: RecordingControlClient,
) -> None:
    remote_slot = 1 - owner_slot()
    handler = RecordingHandler()
    commands = make_runtime(slot_id=remote_slot, handler=handler, client=client)

    assert await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is False
    assert await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is True

    assert len(client.calls) == 1
    assert len(handler.command_acks) == 1
    assert handler.command_acks[0]["data"]["code"] in {
        "CRON_ROUTE_BUSY",
        "CRON_ROUTE_REJECTED",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        ConnectionError("control disconnected"),
        TimeoutError(),
        ControlServiceOperationError(
            "CONTROL_SERVICE_FAILED",
        ),
    ],
)
async def test_remote_unknown_errors_do_not_ack(error: BaseException) -> None:
    remote_slot = 1 - owner_slot()
    handler = RecordingHandler()
    commands = make_runtime(
        slot_id=remote_slot,
        handler=handler,
        client=RecordingControlClient(error=error),
    )

    with pytest.raises(WorkerCronRouteUnknown):
        await commands.handle_push(
            payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
            request_id="request-1",
            channel_id="web",
            session_id="session-1",
            metadata=owner_metadata(),
        )

    assert handler.command_acks == []
    assert commands._seen_commands == {}


@pytest.mark.asyncio
async def test_handle_push_invalid_schema_with_safe_command_id_acks_and_deduplicates() -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    invalid_payload = {
        "event_type": "cron.response",
        "action": "invoke",
        "command_id": "command-invalid",
        "data": {},
    }

    assert await commands.handle_push(
        payload=invalid_payload,
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is False
    assert await commands.handle_push(
        payload=invalid_payload,
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is True

    assert await commands.handle_push(
        payload={**invalid_payload, "action": "list"},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is True
    rejection_record = commands._seen_commands["command-invalid"]
    await asyncio.sleep(0)
    await commands.stop()

    assert handler.calls == []
    assert len(handler.command_acks) == 1
    assert handler.command_acks[0]["command_id"] == "command-invalid"
    assert handler.command_acks[0]["data"]["code"] == "CRON_INVALID_COMMAND"
    assert rejection_record.fingerprint


@pytest.mark.asyncio
async def test_valid_long_string_suffix_change_is_a_command_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import agent_gateway.application.worker_cron_commands as module

    warnings: list[str] = []
    monkeypatch.setattr(
        module.logger,
        "warning",
        lambda message, *args, **kwargs: warnings.append(message % args if args else message),
    )
    handler = RecordingHandler()
    handler.block = True
    commands = make_runtime(handler=handler)

    await commands.accept(
        make_command(
            command_id="command-long",
            data={"description": "a" * 256 + "firstAAAA"},
        )
    )
    await asyncio.wait_for(handler.started.wait(), 1)
    await commands.accept(
        make_command(
            command_id="command-long",
            data={"description": "a" * 256 + "secondBBB"},
        )
    )

    handler.release.set()
    await commands.stop()

    assert len(handler.calls) == 1
    assert handler.command_acks == []
    assert any("different payload" in message for message in warnings)


@pytest.mark.asyncio
async def test_invalid_command_with_non_json_payload_uses_safe_deduplication() -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    command = make_command(command_id="command-non-json")
    command["payload"]["data"] = {"value": object()}

    await commands.accept(command)
    await commands.accept(command)
    rejection_record = commands._seen_commands["command-non-json"]
    await commands.stop()

    assert handler.calls == []
    assert len(handler.command_acks) == 1
    assert handler.command_acks[0]["data"]["code"] == "CRON_INVALID_COMMAND"
    assert rejection_record.fingerprint
    assert not hasattr(rejection_record, "command")


@pytest.mark.asyncio
async def test_owner_received_command_takes_priority_over_malformed_same_id() -> None:
    handler = RecordingHandler()
    handler.block = True
    commands = make_runtime(handler=handler)

    await commands.accept(make_command(command_id="command-1"))
    await asyncio.wait_for(handler.started.wait(), 1)
    await commands.accept(
        make_command(command_id="command-1", action="invoke")
    )

    handler.release.set()
    await commands.stop()

    assert len(handler.calls) == 1
    assert handler.command_acks == []


@pytest.mark.asyncio
async def test_local_schema_invalid_with_safe_command_id_sends_negative_ack() -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    invalid_command = {**make_command(), "extra": "field"}

    await commands.accept(invalid_command)
    await wait_until(lambda: len(handler.command_acks) == 1)
    await commands.stop()

    assert handler.calls == []
    assert handler.command_acks[0]["command_id"] == "command-1"
    assert handler.command_acks[0]["data"]["code"] == "CRON_INVALID_COMMAND"


@pytest.mark.asyncio
async def test_local_accept_admits_before_return_and_deduplicates_command() -> None:
    handler = RecordingHandler()
    handler.block = True
    commands = make_runtime(handler=handler)
    command = make_command()

    await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
        user_id="ignored",
    )
    assert not handler.started.is_set()

    await commands.accept(command)
    handler.release.set()
    await asyncio.wait_for(handler.started.wait(), 1)
    await commands.stop()

    assert len(handler.calls) == 1


@pytest.mark.asyncio
async def test_cached_context_conflict_sends_negative_ack_without_business_execution() -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    commands.remember(chat_envelope(session_id="chat-session"), {"auth_session": "session-a"})

    command = make_command(
        session_id="as-session",
        action="run_now",
        command_id="command-run",
        data={"job_id": "job-1"},
    )
    await commands.accept(command)
    await wait_until(lambda: bool(handler.command_acks))
    await commands.stop()

    assert handler.calls == []
    assert handler.command_acks[0]["command_id"] == "command-run"
    assert handler.command_acks[0]["data"]["code"] == "CRON_CONTEXT_CONFLICT"
    assert handler.run_now_acks[0]["request_id"] == "request-1"
    assert handler.run_now_acks[0]["error"] is not None


@pytest.mark.asyncio
async def test_accept_rejects_credential_reference_and_internal_metadata() -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)

    forbidden_commands = [
        make_command(
            command_id="command-credential",
            data={"credential_ref": "login:ref", "job_id": "job-1"},
        ),
        make_command(
            command_id="command-internal",
            data={"metadata": {"auth_session": "internal"}, "job_id": "job-1"},
        ),
        make_command(
            command_id="command-auth-session",
            data={"_auth_session": "llm-session", "job_id": "job-1"},
        ),
    ]
    for forbidden_command in forbidden_commands:
        await commands.accept(forbidden_command)

    await wait_until(lambda: len(handler.command_acks) == 3)

    await commands.stop()

    assert handler.calls == []
    assert [ack["command_id"] for ack in handler.command_acks] == [
        "command-credential",
        "command-internal",
        "command-auth-session",
    ]
    assert all(
        ack["data"]["code"] == "CRON_INVALID_COMMAND" for ack in handler.command_acks
    )


@pytest.mark.asyncio
async def test_accept_rejects_login_models_before_b4(monkeypatch: pytest.MonkeyPatch) -> None:
    import agent_gateway.application.worker_cron_commands as module

    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    monkeypatch.setattr(module, "_cron_model_source", lambda data: "login")

    await commands.accept(
        make_command(command_id="command-login", data={"model_name": "login-model"})
    )
    await wait_until(lambda: bool(handler.command_acks))
    await commands.stop()

    assert handler.calls == []
    assert handler.command_acks[0]["data"]["code"] == "CRON_LOGIN_MODEL_DISABLED"


@pytest.mark.asyncio
async def test_accept_rejects_login_model_selection_before_b4(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import agent_gateway.application.worker_cron_commands as module

    handler = RecordingHandler()
    commands = make_runtime(handler=handler)

    def resolve(raw: Any, zen_entries: Any = None) -> tuple[str | None, str]:
        assert raw == "login-model-id"
        return "login-model", "login"

    monkeypatch.setattr(module, "resolve_cron_model", resolve)

    await commands.accept(
        make_command(
            command_id="command-login-selection",
            data={"model_selection": {"type": "model", "id": "login-model-id"}},
        )
    )
    await wait_until(lambda: bool(handler.command_acks))
    await commands.stop()

    assert handler.calls == []
    assert handler.command_acks[0]["data"]["code"] == "CRON_LOGIN_MODEL_DISABLED"


@pytest.mark.asyncio
async def test_handle_push_rejects_non_canonical_as_owner_metadata() -> None:
    remote_slot = 1 - owner_slot()
    client = RecordingControlClient()
    commands = make_runtime(slot_id=remote_slot, client=client)

    with pytest.raises(ValueError, match="canonical named owner"):
        await commands.handle_push(
            payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
            request_id="request-1",
            channel_id="web",
            session_id="session-1",
            metadata={**owner_metadata(), "_jiuwenswarm_cron_owner_user_id": " owner "},
        )

    assert client.calls == []


@pytest.mark.asyncio
async def test_local_queue_busy_rejection_is_deduplicated_and_not_reexecuted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    monkeypatch.setattr(WorkerCronCommands, "_QUEUE_LIMIT", 0)

    assert await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is False
    assert await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is True

    monkeypatch.setattr(WorkerCronCommands, "_QUEUE_LIMIT", 32)
    assert await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is True
    await asyncio.sleep(0)
    await commands.stop()

    assert handler.calls == []
    assert len(handler.command_acks) == 1
    assert handler.command_acks[0]["data"]["code"] == "CRON_COMMAND_BUSY"


@pytest.mark.asyncio
async def test_queue_error_fallback_rejection_is_deduplicated_and_not_reexecuted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    active = False
    handler = RecordingHandler()
    commands = make_runtime(handler=handler, enabled=lambda: active)
    monkeypatch.setattr(WorkerCronCommands, "_QUEUE_LIMIT", 0)

    assert await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is True
    assert await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is True

    active = True
    monkeypatch.setattr(WorkerCronCommands, "_QUEUE_LIMIT", 32)
    assert await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    ) is True
    await asyncio.sleep(0)
    await commands.stop()

    assert handler.calls == []
    assert len(handler.command_acks) == 1
    assert handler.command_acks[0]["data"]["code"] == "CRON_NOT_ACTIVE"


@pytest.mark.asyncio
async def test_accept_queue_busy_raises_for_control_event_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    monkeypatch.setattr(WorkerCronCommands, "_QUEUE_LIMIT", 0)

    with pytest.raises(ControlServiceBusy):
        await commands.accept(make_command(command_id="command-1"))

    assert handler.command_acks == []
    await commands.stop()


@pytest.mark.asyncio
async def test_negative_ack_timeout_is_not_reported_as_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import agent_gateway.application.worker_cron_commands as module
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    monkeypatch.setattr(module, "_ACK_TIMEOUT", 0.01)
    monkeypatch.setattr(WorkerCronCommands, "_QUEUE_LIMIT", 0)
    handler = SlowAckHandler()
    commands = make_runtime(handler=handler)

    with pytest.raises(TimeoutError):
        await commands.handle_push(
            payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
            request_id="request-1",
            channel_id="web",
            session_id="session-1",
            metadata=owner_metadata(),
        )

    assert handler.command_acks == []


@pytest.mark.asyncio
async def test_inflight_command_dedup_survives_ttl_and_keeps_fingerprint_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    monkeypatch.setattr(WorkerCronCommands, "_COMMAND_TTL", 0.0)
    handler = RecordingHandler()
    handler.block = True
    commands = make_runtime(handler=handler)

    await commands.accept(make_command(data={"description": "first"}))
    await wait_until(lambda: len(handler.calls) == 1)
    record = commands._seen_commands["command-1"]
    assert record.fingerprint
    assert not hasattr(record, "command")

    await commands.accept(make_command(data={"description": "different"}))
    assert len(handler.calls) == 1

    handler.release.set()
    await commands.stop()
    assert len(handler.calls) == 1
    assert handler.command_acks == []


@pytest.mark.asyncio
async def test_bounded_commands_execute_concurrently() -> None:
    handler = RecordingHandler()
    handler.block = True
    commands = make_runtime(handler=handler)

    await commands.accept(make_command(command_id="command-1"))
    await commands.accept(make_command(command_id="command-2"))
    await wait_until(lambda: len(handler.calls) == 2)

    handler.release.set()
    await commands.stop()
    assert len(handler.calls) == 2


@pytest.mark.asyncio
async def test_command_not_started_before_deadline_sends_negative_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    monkeypatch.setattr(WorkerCronCommands, "_COMMAND_DEADLINE", 0.0)
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)

    await commands.accept(make_command())
    await wait_until(lambda: len(handler.command_acks) == 1)
    await commands.stop()

    assert handler.calls == []
    assert handler.command_acks[0]["data"]["code"] == "CRON_COMMAND_DEADLINE"


@pytest.mark.asyncio
async def test_inflight_deadline_expires_without_deterministic_ack(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    monkeypatch.setattr(WorkerCronCommands, "_COMMAND_DEADLINE", 0.01)
    handler = RecordingHandler()
    handler.block = True
    commands = make_runtime(handler=handler)

    await commands.accept(make_command(command_id="command-1"))
    await wait_until(lambda: len(handler.calls) == 1)
    await asyncio.sleep(0.05)
    handler.block = False

    await commands.accept(make_command(command_id="command-2"))
    await wait_until(lambda: len(handler.calls) == 2)
    await commands.stop()

    assert len(handler.calls) == 2
    assert handler.command_acks == []


@pytest.mark.asyncio
async def test_local_accept_is_gated_by_enabled_callable() -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler, enabled=lambda: False)

    await commands.handle_push(
        payload={"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}},
        request_id="request-1",
        channel_id="web",
        session_id="session-1",
        metadata=owner_metadata(),
    )
    await wait_until(lambda: len(handler.command_acks) == 1)
    await commands.stop()

    assert handler.calls == []
    assert handler.command_acks[0]["data"]["code"] == "CRON_NOT_ACTIVE"


@pytest.mark.asyncio
async def test_business_failure_does_not_send_reverse_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import agent_gateway.application.worker_cron_commands as module

    handler = FailingHandler()
    commands = make_runtime(handler=handler)
    monkeypatch.setattr(module.logger, "exception", lambda *_args, **_kwargs: None)

    await commands.accept(make_command(command_id="command-failed"))
    await wait_until(lambda: len(handler.calls) == 1)
    await commands.stop()

    assert len(handler.calls) == 1
    assert handler.command_acks == []


@pytest.mark.asyncio
async def test_misrouted_command_uses_shared_deduplication() -> None:
    handler = RecordingHandler()
    commands = make_runtime(slot_id=1 - owner_slot(), handler=handler)
    command = make_command()

    await commands.accept(command)
    await commands.accept(command)
    await wait_until(lambda: len(handler.command_acks) == 1)
    await commands.stop()

    assert handler.calls == []
    assert len(handler.command_acks) == 1


@pytest.mark.asyncio
async def test_stop_rejects_new_commands_cancels_work_and_clears_context() -> None:
    handler = RecordingHandler()
    handler.block = True
    client = RecordingControlClient()
    commands = make_runtime(handler=handler, client=client)
    commands.remember(chat_envelope(), {"auth_session": "session-a"})

    await commands.accept(make_command())
    await commands.stop()
    handler.release.set()
    await asyncio.sleep(0)

    assert handler.calls == []
    assert client.disconnect_called is False
    with pytest.raises(RuntimeError, match="Worker Cron command runtime is stopped"):
        await commands.accept(make_command())
    with pytest.raises(RuntimeError, match="Worker Cron command runtime is stopped"):
        commands.remember(chat_envelope(user_id="alice"), {"auth_session": "other"})


@pytest.mark.asyncio
async def test_stop_is_bounded_and_raises_for_uncancellable_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import agent_gateway.application.worker_cron_commands as module
    from agent_gateway.application.worker_cron_commands import (
        WorkerCronCommandStopError,
        WorkerCronCommands,
    )

    monkeypatch.setattr(WorkerCronCommands, "_STOP_TIMEOUT", 0.01)
    errors: list[str] = []
    monkeypatch.setattr(
        module.logger,
        "error",
        lambda message, *args, **kwargs: errors.append(message % args if args else message),
    )
    handler = UncancellableHandler()
    commands = make_runtime(handler=handler)
    await commands.accept(make_command(command_id="command-1"))
    await asyncio.wait_for(handler.started.wait(), 1)
    task = next(iter(commands._tasks))

    started_at = asyncio.get_running_loop().time()
    with pytest.raises(WorkerCronCommandStopError, match="did not stop"):
        await commands.stop()
    elapsed = asyncio.get_running_loop().time() - started_at

    assert 0.01 <= elapsed < 0.5
    assert not task.done()
    assert task in commands._tasks
    assert any("did not stop" in message for message in errors)

    handler.release.set()
    await asyncio.wait_for(task, 1)


@pytest.mark.asyncio
async def test_seen_history_purges_expired_records_behind_inflight_head(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    monkeypatch.setattr(WorkerCronCommands, "_COMMAND_TTL", 0.0)
    monkeypatch.setattr(WorkerCronCommands, "_COMMAND_LIMIT", 2)
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)
    handler.block = True

    await commands.accept(make_command(command_id="command-1"))
    await wait_until(lambda: len(handler.calls) == 1)
    handler.block = False
    await commands.accept(make_command(command_id="command-2"))
    await wait_until(lambda: len(handler.calls) == 2)
    await asyncio.sleep(0)

    assert commands._seen_commands["command-1"].expires_at is None
    assert commands._seen_commands["command-2"].expires_at is not None

    await commands.accept(make_command(command_id="command-3"))
    await wait_until(lambda: len(handler.calls) == 3)
    await commands.stop()

    assert len(handler.calls) == 3


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "command",
    [
        {**make_command(), "request_id": ""},
        {**make_command(), "request_id": "r" * 257},
        {**make_command(), "session_id": "s" * 257},
        {**make_command(), "channel_id": "c" * 257},
        {**make_command(), "owner_user_id": "default"},
        {**make_command(), "owner_group_id": "tenant"},
        {**make_command(), "owner_bot_id": "tenant"},
        {**make_command(), "extra": "field"},
        {**make_command(), "payload": {**make_command()["payload"], "event_type": "chat.response"}},
        {**make_command(), "payload": {**make_command()["payload"], "action": "invoke"}},
        {**make_command(), "payload": {**make_command()["payload"], "command_id": ""}},
        {**make_command(), "payload": {**make_command()["payload"], "data": []}},
    ],
)
async def test_accept_strictly_validates_private_command_schema(command: dict[str, Any]) -> None:
    handler = RecordingHandler()
    commands = make_runtime(handler=handler)

    if command["payload"]["command_id"]:
        await commands.accept(command)
        await wait_until(lambda: len(handler.command_acks) == 1)
        assert handler.command_acks[0]["data"]["code"] == "CRON_INVALID_COMMAND"
    else:
        with pytest.raises(ValueError, match="invalid Cron command"):
            await commands.accept(command)

    await commands.stop()
    assert handler.calls == []


@pytest.mark.asyncio
async def test_inflight_deadline_uses_event_loop_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import agent_gateway.application.worker_cron_commands as module
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands

    monkeypatch.setattr(WorkerCronCommands, "_COMMAND_DEADLINE", 0.01)
    monkeypatch.setattr(module.logger, "warning", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "monotonic",
        lambda: asyncio.get_running_loop().time() + 100,
    )
    handler = RecordingHandler()
    handler.block = True
    commands = make_runtime(handler=handler)

    await commands.accept(make_command(command_id="command-loop-deadline"))
    await wait_until(lambda: len(handler.calls) == 1)
    await asyncio.sleep(0.05)

    assert commands._seen_commands["command-loop-deadline"].expires_at is not None
    assert handler.command_acks == []
    handler.release.set()
    await commands.stop()
