import asyncio
import os
import socket
from dataclasses import replace

import pytest
from pathlib import Path

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.control_commands import ControlCronCommands
from agent_gateway.cron.control_commands import validate_cron_command
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.processes.control_channel import ControlServiceChannel
from agent_gateway.processes.control_protocol import (
    OPERATIONS,
    EVENTS,
    validate_message,
)
from agent_gateway.processes.control_service import (
    ControlServiceAdmission,
    ControlServiceBusy,
    ControlServiceClient,
    ControlServiceIdentity,
    ControlServiceOperationError,
    ControlServiceServer,
)


GROUP_ID = "gateway-group"
SERVER_ID = "as-one"


def ownership() -> CronOwnership:
    return CronOwnership(WorkerManifest((0, 1)), LocalIdentityPolicy())


def identity(slot_id: int) -> ControlServiceIdentity:
    return ControlServiceIdentity(
        group_id=GROUP_ID,
        slot_id=slot_id,
        generation=1,
        worker_pid=1000 + slot_id,
        control_pid=os.getpid(),
    )


def valid_command(command_id: str = "command-1", **data: object) -> dict[str, object]:
    return {
        "request_id": "request-1",
        "session_id": "session-1",
        "channel_id": "web",
        "owner_user_id": "owner",
        "owner_group_id": "default",
        "owner_bot_id": "default",
        "payload": {
            "event_type": "cron.response",
            "action": "create",
            "command_id": command_id,
            "data": dict({"description": " nightly report "}, **data),
        },
    }


class FakeEndpoint:
    def __init__(self, endpoint_identity: ControlServiceIdentity) -> None:
        self.identity = endpoint_identity
        self.notifications: list[dict[str, object]] = []

    async def notify(
        self,
        event: str,
        arguments: dict[str, object],
        *,
        timeout: float,
    ) -> None:
        assert event == "cron.command_deliver"
        self.notifications.append(arguments)


class FailingEndpoint(FakeEndpoint):
    def __init__(
        self,
        endpoint_identity: ControlServiceIdentity,
        error: BaseException,
    ) -> None:
        super().__init__(endpoint_identity)
        self.error = error

    async def notify(
        self,
        event: str,
        arguments: dict[str, object],
        *,
        timeout: float,
    ) -> None:
        await super().notify(event, arguments, timeout=timeout)
        raise self.error


class SlowEndpoint(FakeEndpoint):
    async def notify(
        self,
        event: str,
        arguments: dict[str, object],
        *,
        timeout: float,
    ) -> None:
        await super().notify(event, arguments, timeout=timeout)
        async with asyncio.timeout(timeout):
            await asyncio.sleep(0.05)


class CancellingEndpoint(FakeEndpoint):
    async def notify(
        self,
        event: str,
        arguments: dict[str, object],
        *,
        timeout: float,
    ) -> None:
        await super().notify(event, arguments, timeout=timeout)
        raise asyncio.CancelledError


class BlockingEndpoint(FakeEndpoint):
    def __init__(self, endpoint_identity: ControlServiceIdentity) -> None:
        super().__init__(endpoint_identity)
        self.release = asyncio.Event()

    async def notify(
        self,
        event: str,
        arguments: dict[str, object],
        *,
        timeout: float,
    ) -> None:
        await super().notify(event, arguments, timeout=timeout)
        await self.release.wait()


def test_public_validator_returns_sanitized_command() -> None:
    arguments = valid_command(
        user_id="untrusted",
        group_id="untrusted",
        bot_id="untrusted",
    )

    sanitized = validate_cron_command(arguments)

    assert sanitized == {
        **arguments,
        "payload": {
            **arguments["payload"],
            "data": {"description": " nightly report "},
        },
    }


def test_public_validator_rejects_internal_and_non_canonical_fields() -> None:
    invalid_values = [
        {"_auth_session": "session"},
        {"_gateway_verified": True},
        {"_jiuwenswarm_user_id": "internal"},
        {"_agentos_credential": "internal"},
    ]

    for data in invalid_values:
        arguments = valid_command(**data)
        with pytest.raises(ValueError):
            validate_cron_command(arguments)

    for owner_user_id in ("default", " owner "):
        arguments = valid_command()
        arguments["owner_user_id"] = owner_user_id
        with pytest.raises(ValueError):
            validate_cron_command(arguments)


def test_delivery_timeout_is_shorter_than_caller_budget() -> None:
    assert ControlCronCommands(ownership(), SERVER_ID)._DELIVERY_TIMEOUT == 1.0


def test_server_id_rejects_the_agent_server_url_bound() -> None:
    with pytest.raises(ValueError, match="server_id must be a non-empty bounded string"):
        ControlCronCommands(ownership(), "s" * 2049)


def test_protocol_reserves_cron_command_operation_and_delivery_event() -> None:
    request = {
        "protocol_version": 1,
        "group_id": GROUP_ID,
        "slot_id": 1,
        "generation": 1,
        "type": "REQUEST",
        "request_id": "0" * 32,
        "operation": "cron.route_command",
        "arguments": valid_command(),
    }
    event = {
        "protocol_version": 1,
        "group_id": GROUP_ID,
        "slot_id": 0,
        "generation": 1,
        "type": "EVENT",
        "event_id": "1" * 32,
        "event": "cron.command_deliver",
        "arguments": valid_command(),
    }

    assert "cron.route_command" in OPERATIONS
    assert "cron.command_deliver" in EVENTS
    assert validate_message(request)["operation"] == "cron.route_command"
    assert validate_message(event)["event"] == "cron.command_deliver"


@pytest.mark.parametrize(
    "invalid",
    [
        {**valid_command(), "server_id": "worker-supplied"},
        {**valid_command(), "metadata": {}},
        {**valid_command(), "auth_session": "session-secret"},
        {**valid_command(), "payload": {**valid_command()["payload"], "metadata": {}}},
        {
            **valid_command(),
            "payload": {
                **valid_command()["payload"],
                "data": {"credential_ref": "credential-ref"},
            },
        },
        {
            **valid_command(),
            "payload": {
                **valid_command()["payload"],
                "data": {"nested": {"auth_session": "session-secret"}},
            },
        },
        {**valid_command(), "owner_user_id": "default"},
        {**valid_command(), "owner_user_id": " owner "},
        {**valid_command(), "owner_group_id": "other-group"},
        {**valid_command(), "owner_bot_id": "other-bot"},
        {**valid_command(), "session_id": "s" * 257},
        {**valid_command(), "request_id": ""},
        {
            **valid_command(),
            "payload": {**valid_command()["payload"], "event_type": "chat.response"},
        },
        {
            **valid_command(),
            "payload": {**valid_command()["payload"], "action": "invoke"},
        },
        {
            **valid_command(),
            "payload": {**valid_command()["payload"], "command_id": ""},
        },
    ],
)
async def test_route_rejects_non_cron_or_internal_identity_fields(
    invalid: dict[str, object],
) -> None:
    commands = ControlCronCommands(ownership(), SERVER_ID)

    result = await commands.handlers_for(identity(1))["cron.route_command"](invalid)

    assert result == {"status": "rejected", "code": "INVALID_COMMAND"}


async def test_route_removes_worker_owned_identity_from_task_data() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = FakeEndpoint(identity(owner_slot))
    source_endpoint = FakeEndpoint(identity(1 - owner_slot))
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    arguments = valid_command(user_id="untrusted", group_id="untrusted", bot_id="untrusted")

    result = await commands.handlers_for(identity(1 - owner_slot))[
        "cron.route_command"
    ](arguments)

    assert result == {"status": "accepted", "code": "DELIVERED"}
    assert endpoint.notifications == [
        {
            **arguments,
            "payload": {
                **arguments["payload"],
                "data": {"description": " nightly report "},
            },
        }
    ]


async def test_route_rejects_over_limit_delivery_before_notify() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    long_group_id = "g" * 256
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = FakeEndpoint(replace(identity(owner_slot), group_id=long_group_id))
    source_identity = replace(identity(1 - owner_slot), group_id=long_group_id)
    source_endpoint = FakeEndpoint(source_identity)
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)

    def command_for(last_length: int) -> dict[str, object]:
        data = {
            f"field_{index:02d}": "x" * 4096 for index in range(15)
        }
        data["field_15"] = "x" * last_length
        return valid_command(**data)

    def public_validator_accepts(arguments: dict[str, object]) -> bool:
        try:
            validate_cron_command(arguments)
            return True
        except ValueError:
            return False

    low, high = 0, 4096
    while low < high:
        middle = (low + high + 1) // 2
        if public_validator_accepts(command_for(middle)):
            low = middle
        else:
            high = middle - 1
    arguments = command_for(low)

    result = await commands.handlers_for(source_identity)["cron.route_command"](
        arguments
    )

    assert result == {"status": "rejected", "code": "COMMAND_TOO_LARGE"}
    assert endpoint.notifications == []


async def test_disconnected_source_can_read_existing_result_but_not_route_new_commands() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    source_identity = identity(1 - owner_slot)
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = FakeEndpoint(identity(owner_slot))
    source_endpoint = FakeEndpoint(source_identity)
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    old_handler = commands.handlers_for(source_identity)["cron.route_command"]

    assert await old_handler(valid_command("command-1")) == {
        "status": "accepted",
        "code": "DELIVERED",
    }
    commands.endpoint_disconnected(source_identity)
    replacement_identity = replace(source_identity, worker_pid=4242)
    commands.endpoint_connected(FakeEndpoint(replacement_identity))

    assert await old_handler(valid_command("command-1")) == {
        "status": "accepted",
        "code": "DELIVERED",
    }
    assert await old_handler(valid_command("command-2")) == {
        "status": "rejected",
        "code": "SOURCE_ENDPOINT_UNAVAILABLE",
    }
    assert await commands.handlers_for(replacement_identity)["cron.route_command"](
        valid_command("command-2")
    ) == {"status": "accepted", "code": "DELIVERED"}
    assert len(endpoint.notifications) == 2


@pytest.mark.parametrize(
    ("error", "code"),
    [
        (ControlServiceBusy(), "OWNER_BUSY"),
        (ControlServiceOperationError("CONTROL_SERVICE_FAILED"), "OWNER_REJECTED"),
    ],
)
async def test_endpoint_rejections_are_deterministic(
    error: BaseException, code: str
) -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = FailingEndpoint(identity(owner_slot), error)
    source_endpoint = FakeEndpoint(identity(1 - owner_slot))
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    handler = commands.handlers_for(identity(1 - owner_slot))["cron.route_command"]

    first = await handler(valid_command())
    second = await handler(valid_command())

    assert first == {"status": "rejected", "code": code}
    assert second == first
    assert len(endpoint.notifications) == 1


async def test_connection_failure_is_unknown_and_never_retried() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = FailingEndpoint(identity(owner_slot), ConnectionError("closed"))
    source_endpoint = FakeEndpoint(identity(1 - owner_slot))
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    handler = commands.handlers_for(identity(1 - owner_slot))["cron.route_command"]

    first = await handler(valid_command())
    second = await handler(valid_command())

    assert first == {"status": "unknown", "code": "DELIVERY_UNKNOWN"}
    assert second == first
    assert len(endpoint.notifications) == 1


async def test_delivery_timeout_is_unknown_and_never_retried() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    commands._DELIVERY_TIMEOUT = 0.01
    endpoint = SlowEndpoint(identity(owner_slot))
    source_endpoint = FakeEndpoint(identity(1 - owner_slot))
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    handler = commands.handlers_for(identity(1 - owner_slot))["cron.route_command"]

    first = await handler(valid_command())
    second = await handler(valid_command())

    assert first == {"status": "unknown", "code": "DELIVERY_UNKNOWN"}
    assert second == first
    assert len(endpoint.notifications) == 1


async def test_cancelled_delivery_is_unknown_and_never_retried() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = CancellingEndpoint(identity(owner_slot))
    source_endpoint = FakeEndpoint(identity(1 - owner_slot))
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    handler = commands.handlers_for(identity(1 - owner_slot))["cron.route_command"]

    with pytest.raises(asyncio.CancelledError):
        await handler(valid_command())

    assert await handler(valid_command()) == {
        "status": "unknown",
        "code": "DELIVERY_UNKNOWN",
    }
    assert len(endpoint.notifications) == 1


async def test_duplicate_commands_and_payload_conflicts_do_not_deliver_twice() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = FakeEndpoint(identity(owner_slot))
    source_endpoint = FakeEndpoint(identity(1 - owner_slot))
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    handler = commands.handlers_for(identity(1 - owner_slot))["cron.route_command"]
    first_arguments = valid_command()
    second_arguments = valid_command()
    first_arguments["payload"]["data"]["description"] = "first"
    second_arguments["payload"]["data"]["description"] = "second"

    assert await handler(first_arguments) == {
        "status": "accepted",
        "code": "DELIVERED",
    }
    assert await handler(second_arguments) == {
        "status": "rejected",
        "code": "COMMAND_CONFLICT",
    }
    assert await handler(first_arguments) == {
        "status": "accepted",
        "code": "DELIVERED",
    }
    assert len(endpoint.notifications) == 1


async def test_command_deduplication_has_a_bounded_ttl() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = FakeEndpoint(identity(owner_slot))
    source_endpoint = FakeEndpoint(identity(1 - owner_slot))
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    handler = commands.handlers_for(identity(1 - owner_slot))["cron.route_command"]
    now = 0.0
    commands._now = lambda: now
    arguments = valid_command()

    assert await handler(arguments) == {"status": "accepted", "code": "DELIVERED"}
    now = commands._COMMAND_TTL - 0.001
    assert await handler(arguments) == {"status": "accepted", "code": "DELIVERED"}
    assert len(endpoint.notifications) == 1

    now = commands._COMMAND_TTL + 0.001
    assert await handler(arguments) == {"status": "accepted", "code": "DELIVERED"}
    assert len(endpoint.notifications) == 2


async def test_command_history_is_bounded_without_evicting_live_commands() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    commands._COMMAND_LIMIT = 1
    endpoint = FakeEndpoint(identity(owner_slot))
    source_endpoint = FakeEndpoint(identity(1 - owner_slot))
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    handler = commands.handlers_for(identity(1 - owner_slot))["cron.route_command"]

    assert await handler(valid_command("command-1")) == {
        "status": "accepted",
        "code": "DELIVERED",
    }
    assert await handler(valid_command("command-2")) == {
        "status": "rejected",
        "code": "COMMAND_HISTORY_FULL",
    }
    assert await handler(valid_command("command-1")) == {
        "status": "accepted",
        "code": "DELIVERED",
    }
    assert len(endpoint.notifications) == 1


async def test_completed_commands_retain_fingerprint_without_payload() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = FakeEndpoint(identity(owner_slot))
    source_endpoint = FakeEndpoint(identity(1 - owner_slot))
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    handler = commands.handlers_for(identity(1 - owner_slot))["cron.route_command"]

    assert await handler(valid_command(data={"description": "private-management-body"})) == {
        "status": "accepted",
        "code": "DELIVERED",
    }

    record = next(iter(commands._commands.values()))
    assert record.fingerprint
    assert "private-management-body" not in repr(record)


async def test_disconnect_settles_pending_delivery_as_unknown_without_retry() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    source_identity = identity(1 - owner_slot)
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    endpoint = BlockingEndpoint(identity(owner_slot))
    source_endpoint = FakeEndpoint(source_identity)
    commands.endpoint_connected(endpoint)
    commands.endpoint_connected(source_endpoint)
    handler = commands.handlers_for(source_identity)["cron.route_command"]

    pending = asyncio.create_task(handler(valid_command()))
    try:
        async with asyncio.timeout(1.0):
            while not endpoint.notifications:
                await asyncio.sleep(0)
            commands.endpoint_disconnected(endpoint.identity)

            assert await pending == {"status": "unknown", "code": "DELIVERY_UNKNOWN"}

            replacement = FakeEndpoint(identity(owner_slot))
            commands.endpoint_connected(replacement)
            assert await handler(valid_command()) == {
                "status": "unknown",
                "code": "DELIVERY_UNKNOWN",
            }
            assert len(endpoint.notifications) == 1
            assert replacement.notifications == []
    finally:
        endpoint.release.set()
        if not pending.done():
            pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)


def test_endpoint_scope_remains_bound_after_all_disconnects() -> None:
    commands = ControlCronCommands(ownership(), SERVER_ID)
    first = FakeEndpoint(identity(0))
    second = FakeEndpoint(identity(1))
    commands.endpoint_connected(first)
    commands.endpoint_connected(second)
    commands.endpoint_disconnected(first.identity)
    commands.endpoint_disconnected(second.identity)

    with pytest.raises(ValueError, match="cron endpoint group is already bound"):
        commands.endpoint_connected(
            FakeEndpoint(replace(identity(0), group_id="other-group"))
        )


def test_endpoint_binding_rejects_group_generation_and_incarnation_conflicts() -> None:
    commands = ControlCronCommands(ownership(), SERVER_ID)
    commands.endpoint_connected(FakeEndpoint(identity(0)))

    with pytest.raises(ValueError, match="cron endpoint generation is already bound"):
        commands.endpoint_connected(FakeEndpoint(replace(identity(0), generation=2)))
    with pytest.raises(ValueError, match="cron endpoint group is already bound"):
        commands.endpoint_connected(FakeEndpoint(replace(identity(1), group_id="other")))
    with pytest.raises(ValueError, match="live bound incarnation"):
        commands.endpoint_connected(FakeEndpoint(replace(identity(0), worker_pid=999)))

    commands.endpoint_disconnected(identity(0))
    commands.endpoint_connected(FakeEndpoint(replace(identity(0), worker_pid=1002)))
    with pytest.raises(ValueError, match="live bound incarnation"):
        commands.endpoint_connected(FakeEndpoint(replace(identity(0), worker_pid=1003)))
    commands.endpoint_disconnected(replace(identity(0), worker_pid=1002))

    commands.endpoint_connected(FakeEndpoint(identity(0)))


async def test_route_uses_real_control_channel_and_owner_selection() -> None:
    cron_ownership = ownership()
    owner_slot = cron_ownership.owner_slot("owner")
    recipient_slot = 1 - owner_slot
    commands = ControlCronCommands(cron_ownership, SERVER_ID)
    description = "d" * 4096
    arguments = valid_command()
    arguments["payload"]["data"]["description"] = description
    delivered: list[dict[str, object]] = []

    async def owner_receives(event_arguments: dict[str, object]) -> None:
        validate_cron_command(event_arguments)
        delivered.append(event_arguments)

    bundles = []
    for slot_id in (0, 1):
        worker_socket, control_socket = socket.socketpair()
        endpoint_identity = identity(slot_id)
        client = ControlServiceClient(
            ControlServiceChannel(worker_socket),
            endpoint_identity,
            event_handlers={"cron.command_deliver": owner_receives},
        )
        server = ControlServiceServer(
            ControlServiceChannel(control_socket),
            endpoint_identity,
            handlers=commands.handlers_for(endpoint_identity),
            admission=ControlServiceAdmission(),
        )
        bundles.append((client, server))

    try:
        await asyncio.gather(
            bundles[0][1].start(),
            bundles[0][0].start(),
            bundles[1][1].start(),
            bundles[1][0].start(),
        )
        commands.endpoint_connected(bundles[0][1])
        commands.endpoint_connected(bundles[1][1])

        recipient, _owner = bundles[recipient_slot], bundles[owner_slot]
        result = await recipient[0].call(
            "cron.route_command", arguments, timeout=2.0
        )
        duplicate = await recipient[0].call(
            "cron.route_command", arguments, timeout=2.0
        )

        assert result == {"status": "accepted", "code": "DELIVERED"}
        assert duplicate == result
        assert delivered == [
            {
                **arguments,
                "payload": {
                    **arguments["payload"],
                    "data": {"description": description},
                },
            }
        ]
    finally:
        await asyncio.gather(
            bundles[0][0].stop(),
            bundles[0][1].stop(),
            bundles[1][0].stop(),
            bundles[1][1].stop(),
        )
