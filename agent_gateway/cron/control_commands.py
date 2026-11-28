# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Narrow routing of private Cron management commands between Workers."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from functools import partial
from types import MappingProxyType
from time import monotonic
from typing import Any

from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.processes.control_protocol import (
    CONTROL_PROTOCOL_VERSION,
    ControlProtocolError,
    encode_message,
)
from agent_gateway.processes.control_service import (
    ControlServiceBusy,
    ControlServiceIdentity,
    ControlServiceOperationError,
    Handler,
)


COMMAND_FIELDS = frozenset(
    {
        "request_id",
        "session_id",
        "channel_id",
        "owner_user_id",
        "owner_group_id",
        "owner_bot_id",
        "payload",
    }
)
PAYLOAD_FIELDS = frozenset({"event_type", "action", "command_id", "data"})
CRON_ACTIONS = frozenset(
    {
        "list",
        "get",
        "create",
        "update",
        "delete",
        "toggle",
        "preview",
        "run_now",
    }
)
_DEFAULT_ENDPOINT_IDS = frozenset({"", "default"})
_IDENTITY_DATA_FIELDS = frozenset({"user_id", "group_id", "bot_id"})
_BANNED_DATA_FIELDS = frozenset(
    {
        "metadata",
        "_auth_session",
        "auth_header",
        "auth_session",
        "auth_token",
        "authorization",
        "access_token",
        "id_token",
        "internal_user_id",
        "internal_group_id",
        "internal_bot_id",
        "refresh_token",
        "session_token",
        "token",
    }
)
_BANNED_DATA_FIELD_PARTS = ("credential", "secret", "password", "api_key")
_INTERNAL_DATA_FIELD_PREFIXES = (
    "_gateway_",
    "_jiuwenswarm_",
    "_agentos_",
)
_CANONICAL_OWNER_POLICY = LocalIdentityPolicy()
_MAX_SERVER_ID_LENGTH = 2048
_MAX_TEXT_LENGTH = 256


class CronCommandValidationError(ValueError):
    """A Cron command violated the shared Worker/Control schema."""


@dataclass
class _CommandRecord:
    fingerprint: str
    future: asyncio.Future[dict[str, str]]
    expires_at: float
    source_slot: int
    target_slot: int


class ControlCronCommands:
    """Route a bounded Cron command to its fixed owner Worker endpoint."""

    _COMMAND_TTL = 60.0
    _COMMAND_LIMIT = 1024
    _DELIVERY_TIMEOUT = 1.0

    def __init__(self, ownership: CronOwnership, server_id: str) -> None:
        if not isinstance(ownership, CronOwnership):
            raise ValueError("cron routing requires a CronOwnership instance")
        if (
            not isinstance(server_id, str)
            or not server_id
            or len(server_id) > _MAX_SERVER_ID_LENGTH
        ):
            raise ValueError("server_id must be a non-empty bounded string")

        self._ownership = ownership
        self._server_id = server_id
        self._group_id: str | None = None
        self._generation: int | None = None
        self._endpoints: dict[int, tuple[ControlServiceIdentity, Any]] = {}
        self._commands: OrderedDict[
            tuple[str, str, int, str], _CommandRecord
        ] = OrderedDict()

    def handlers_for(self, identity: ControlServiceIdentity) -> Mapping[str, Handler]:
        """Bind the reserved operation to one authenticated endpoint identity."""
        self._bind_identity_scope(identity)
        return MappingProxyType(
            {"cron.route_command": partial(self._route_command, identity)}
        )

    def endpoint_connected(self, server: Any) -> None:
        """Bind one live control endpoint to a fixed Worker incarnation."""
        identity = getattr(server, "identity", None)
        if not isinstance(identity, ControlServiceIdentity):
            raise ValueError("cron endpoint requires a control-service identity")
        if not callable(getattr(server, "notify", None)):
            raise ValueError("cron endpoint requires a bounded notify method")
        self._bind_identity_scope(identity)
        if identity.slot_id in self._endpoints:
            raise ValueError("cron endpoint slot already has a live bound incarnation")

        self._endpoints[identity.slot_id] = (identity, server)

    def endpoint_disconnected(self, identity: ControlServiceIdentity) -> None:
        """Remove only the endpoint whose full incarnation disconnected."""
        if not isinstance(identity, ControlServiceIdentity):
            raise ValueError("cron disconnect requires a control-service identity")
        bound = self._endpoints.get(identity.slot_id)
        was_bound = bound is not None and bound[0] == identity
        if was_bound:
            del self._endpoints[identity.slot_id]
        if was_bound:
            self._settle_records_for_disconnect(identity)

    def _bind_identity_scope(self, identity: ControlServiceIdentity) -> None:
        if not isinstance(identity, ControlServiceIdentity):
            raise ValueError("cron handler requires a control-service identity")
        if self._group_id is None:
            self._group_id = identity.group_id
        elif identity.group_id != self._group_id:
            raise ValueError("cron endpoint group is already bound")
        if self._generation is None:
            self._generation = identity.generation
        elif identity.generation != self._generation:
            raise ValueError("cron endpoint generation is already bound")

    def _settle_records_for_disconnect(
        self,
        identity: ControlServiceIdentity,
    ) -> None:
        records = tuple(self._commands.values())
        if self._endpoints:
            records = tuple(
                record
                for record in records
                if identity.slot_id in {record.source_slot, record.target_slot}
            )
        for record in records:
            self._finish_record(
                record.future,
                {"status": "unknown", "code": "DELIVERY_UNKNOWN"},
            )

    def _now(self) -> float:
        return monotonic()

    def _purge_expired_commands(self) -> None:
        now = self._now()
        while self._commands:
            record = next(iter(self._commands.values()))
            if record.expires_at > now:
                break
            if not record.future.done():
                break
            self._commands.popitem(last=False)

    async def _route_command(
        self,
        identity: ControlServiceIdentity,
        arguments: dict[str, Any],
    ) -> dict[str, str]:
        try:
            command = validate_cron_command(arguments)
            owner_slot = self._ownership.owner_slot(command["owner_user_id"])
        except (CronCommandValidationError, ValueError):
            return {"status": "rejected", "code": "INVALID_COMMAND"}

        fingerprint = _command_fingerprint(command)

        command_key = (
            self._server_id,
            identity.group_id,
            identity.generation,
            command["payload"]["command_id"],
        )
        self._purge_expired_commands()
        existing = self._commands.get(command_key)
        if existing is not None:
            if existing.fingerprint != fingerprint:
                return {"status": "rejected", "code": "COMMAND_CONFLICT"}
            return await asyncio.shield(existing.future)

        if len(self._commands) >= self._COMMAND_LIMIT:
            return {"status": "rejected", "code": "COMMAND_HISTORY_FULL"}

        source_bound = self._endpoints.get(identity.slot_id)
        if source_bound is None or source_bound[0] != identity:
            return {"status": "rejected", "code": "SOURCE_ENDPOINT_UNAVAILABLE"}

        target_bound = self._endpoints.get(owner_slot)
        if (
            target_bound is None
            or target_bound[0].group_id != identity.group_id
            or target_bound[0].generation != identity.generation
        ):
            return {"status": "rejected", "code": "OWNER_ENDPOINT_UNAVAILABLE"}

        target_identity, target_server = target_bound
        delivery_arguments = command
        try:
            encode_message(
                {
                    "protocol_version": CONTROL_PROTOCOL_VERSION,
                    "group_id": target_identity.group_id,
                    "slot_id": target_identity.slot_id,
                    "generation": target_identity.generation,
                    "type": "EVENT",
                    "event_id": "0" * 32,
                    "event": "cron.command_deliver",
                    "arguments": delivery_arguments,
                }
            )
        except ControlProtocolError:
            return {"status": "rejected", "code": "COMMAND_TOO_LARGE"}

        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, str]] = loop.create_future()
        record = _CommandRecord(
            fingerprint=fingerprint,
            future=future,
            expires_at=self._now() + self._COMMAND_TTL,
            source_slot=identity.slot_id,
            target_slot=owner_slot,
        )
        self._commands[command_key] = record

        notify_task = asyncio.create_task(
            target_server.notify(
                "cron.command_deliver",
                delivery_arguments,
                timeout=self._DELIVERY_TIMEOUT,
            )
        )

        def cancel_notify(_future: asyncio.Future[dict[str, str]]) -> None:
            if not notify_task.done():
                notify_task.cancel()

        future.add_done_callback(cancel_notify)
        try:
            await notify_task
            result = {"status": "accepted", "code": "DELIVERED"}
        except asyncio.CancelledError:
            if future.done():
                return future.result()
            self._finish_record(
                future,
                {"status": "unknown", "code": "DELIVERY_UNKNOWN"},
            )
            raise
        except ControlServiceBusy:
            result = {"status": "rejected", "code": "OWNER_BUSY"}
        except ControlServiceOperationError:
            result = {"status": "rejected", "code": "OWNER_REJECTED"}
        except (ConnectionError, TimeoutError):
            result = {"status": "unknown", "code": "DELIVERY_UNKNOWN"}
        except Exception:
            result = {"status": "unknown", "code": "DELIVERY_UNKNOWN"}
        finally:
            future.remove_done_callback(cancel_notify)
        return self._finish_record(future, result)

    def _finish_record(
        self,
        future: asyncio.Future[dict[str, str]],
        result: dict[str, str],
    ) -> dict[str, str]:
        if not future.done():
            future.set_result(result)
        return future.result()

def _is_bounded_text(value: Any, *, allow_empty: bool = False) -> bool:
    return (
        isinstance(value, str)
        and (allow_empty or value)
        and len(value) <= _MAX_TEXT_LENGTH
    )


def _sanitize_data(value: Any) -> Any:
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise CronCommandValidationError() from None
            if key in _IDENTITY_DATA_FIELDS:
                continue
            lowered_key = key.lower()
            if key in _BANNED_DATA_FIELDS or lowered_key.startswith(
                _INTERNAL_DATA_FIELD_PREFIXES
            ) or any(
                part in lowered_key for part in _BANNED_DATA_FIELD_PARTS
            ):
                raise CronCommandValidationError() from None
            result[key] = _sanitize_data(child)
        return result
    if isinstance(value, list):
        return [_sanitize_data(child) for child in value]
    return value


def validate_cron_command(arguments: Any) -> dict[str, Any]:
    """Validate and sanitize the shared Cron command schema.

    The generic JSON limits are checked through a bound-independent valid
    EVENT shell, so Workers and Control use the same wire budget.
    """
    if not isinstance(arguments, dict):
        raise CronCommandValidationError() from None
    try:
        encode_message(
            {
                "protocol_version": CONTROL_PROTOCOL_VERSION,
                "group_id": "cron-command-validation",
                "slot_id": 0,
                "generation": 1,
                "type": "EVENT",
                "event_id": "0" * 32,
                "event": "cron.command_deliver",
                "arguments": arguments,
            }
        )
    except ControlProtocolError as error:
        raise CronCommandValidationError() from error

    if set(arguments) != COMMAND_FIELDS:
        raise CronCommandValidationError() from None
    if not _is_bounded_text(arguments["request_id"]):
        raise CronCommandValidationError() from None
    session_id = arguments["session_id"]
    if session_id is not None and not _is_bounded_text(session_id):
        raise CronCommandValidationError() from None
    if not _is_bounded_text(arguments["channel_id"], allow_empty=True):
        raise CronCommandValidationError() from None
    if not isinstance(arguments["owner_group_id"], str) or (
        arguments["owner_group_id"] not in _DEFAULT_ENDPOINT_IDS
    ):
        raise CronCommandValidationError() from None
    if not isinstance(arguments["owner_bot_id"], str) or (
        arguments["owner_bot_id"] not in _DEFAULT_ENDPOINT_IDS
    ):
        raise CronCommandValidationError() from None
    if arguments["owner_user_id"] == "default":
        raise CronCommandValidationError() from None

    payload = arguments["payload"]
    if not isinstance(payload, dict) or set(payload) != PAYLOAD_FIELDS:
        raise CronCommandValidationError() from None
    if not isinstance(payload["event_type"], str) or payload["event_type"] != "cron.response":
        raise CronCommandValidationError() from None
    if not isinstance(payload["action"], str) or payload["action"] not in CRON_ACTIONS:
        raise CronCommandValidationError() from None
    if not _is_bounded_text(payload["command_id"]):
        raise CronCommandValidationError() from None
    if not isinstance(payload["data"], dict):
        raise CronCommandValidationError() from None

    try:
        IngressIdentity(
            contract_id=_CANONICAL_OWNER_POLICY.contract_id,
            mode="local_compat",
            domain=_CANONICAL_OWNER_POLICY.domain,
            kind="named",
            business_user_id=arguments["owner_user_id"],
        )
    except (TypeError, ValueError) as error:
        raise CronCommandValidationError() from error

    return {
        "request_id": arguments["request_id"],
        "session_id": session_id,
        "channel_id": arguments["channel_id"],
        "owner_user_id": arguments["owner_user_id"],
        "owner_group_id": arguments["owner_group_id"],
        "owner_bot_id": arguments["owner_bot_id"],
        "payload": {
            "event_type": payload["event_type"],
            "action": payload["action"],
            "command_id": payload["command_id"],
            "data": _sanitize_data(payload["data"]),
        },
    }


def _command_fingerprint(command: dict[str, Any]) -> str:
    serialized = json.dumps(
        command,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()
