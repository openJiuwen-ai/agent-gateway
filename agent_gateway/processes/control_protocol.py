# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Strict wire protocol for the private control service channel."""

import json
import math
import re
import struct
from collections.abc import Mapping
from typing import Any


class ControlProtocolError(ValueError):
    """Raised when a control service message violates the wire protocol."""


class ControlFrameTooLarge(ControlProtocolError):
    """Raised when a locally built frame exceeds the configured frame limit."""


CONTROL_PROTOCOL_VERSION = 1
DEFAULT_FRAME_LIMIT = 65536

_MAX_GROUP_ID_LENGTH = 256
_MAX_JSON_DEPTH = 8
_MAX_JSON_NODES = 256
_MAX_JSON_STRING_LENGTH = 4096
_IDENTIFIER_PATTERN = re.compile(r"[0-9a-f]{32}")
_COMMON_FIELDS = frozenset(
    {"protocol_version", "group_id", "slot_id", "generation", "type"}
)
_MESSAGE_TYPES = frozenset(
    {"HELLO", "SERVICE_READY", "REQUEST", "RESPONSE", "EVENT", "EVENT_ACK"}
)
_FIELDS_BY_TYPE = {
    "HELLO": _COMMON_FIELDS | {"pid"},
    "SERVICE_READY": _COMMON_FIELDS | {"pid", "worker_pid"},
    "REQUEST": _COMMON_FIELDS | {"request_id", "operation", "arguments"},
    "RESPONSE": _COMMON_FIELDS | {"request_id", "status", "result", "error_code"},
    "EVENT": _COMMON_FIELDS | {"event_id", "event", "arguments"},
    "EVENT_ACK": _COMMON_FIELDS | {"event_id", "status", "error_code"},
}

OPERATIONS = frozenset(
    {
        "credentials.request_model_auth",
        "credentials.refresh_for_ref",
        "credentials.update_result",
        "bindings.remember",
        "bindings.lookup",
        "bindings.resolve_model_auth",
        "catalog.list_login_models",
        "catalog.get_login_model_snapshot",
        "cron.route_command",
        "cron.project_begin",
        "cron.project_finish",
        "cron.project_status",
        "cron.bind_login_model",
        "cron.resolve_model_auth",
        "cron.validate_login_job",
        "cron.get_login_snapshot",
    }
)
EVENTS = frozenset({
    "credentials.invalidate", "cron.command_deliver",
    "cron.project_fence", "cron.project_drain", "cron.project_release",
})
ERROR_CODES = frozenset(
    {
        "OPERATION_NOT_ENABLED",
        "CONTROL_SERVICE_BUSY",
        "CONTROL_SERVICE_FAILED",
        "EVENT_NOT_ENABLED",
    }
)


def validate_message(
    message: Any,
    *,
    expected: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(message, dict):
        raise ControlProtocolError("message must be a JSON object")

    message_type = message.get("type")
    if (
        not isinstance(message_type, str)
        or message_type not in _MESSAGE_TYPES
    ):
        raise ControlProtocolError("unknown control message type")

    allowed_fields = _FIELDS_BY_TYPE[message_type]
    if allowed_fields != message.keys():
        raise ControlProtocolError("message fields do not match the type schema")

    protocol_version = message["protocol_version"]
    if (
        type(protocol_version) is not int
        or protocol_version != CONTROL_PROTOCOL_VERSION
    ):
        raise ControlProtocolError("unsupported control protocol version")

    group_id = message["group_id"]
    if (
        not isinstance(group_id, str)
        or not group_id
        or len(group_id) > _MAX_GROUP_ID_LENGTH
    ):
        raise ControlProtocolError("group_id must be a non-empty bounded string")

    slot_id = message["slot_id"]
    if type(slot_id) is not int or slot_id < 0:
        raise ControlProtocolError("slot_id must be a non-negative integer")

    generation = message["generation"]
    if type(generation) is not int or generation < 1:
        raise ControlProtocolError("generation must be a positive integer")

    if message_type in {"HELLO", "SERVICE_READY"}:
        _validate_pid(message["pid"])
    if message_type == "SERVICE_READY":
        _validate_pid(message["worker_pid"])

    if message_type == "REQUEST":
        _validate_identifier(message["request_id"], "request_id")
        operation = message["operation"]
        if not isinstance(operation, str) or operation not in OPERATIONS:
            raise ControlProtocolError("unknown control operation")
        _validate_json_object(message["arguments"], "arguments")
    elif message_type == "RESPONSE":
        _validate_identifier(message["request_id"], "request_id")
        _validate_status_payload(
            status=message["status"],
            result=message["result"],
            error_code=message["error_code"],
        )
    elif message_type == "EVENT":
        _validate_identifier(message["event_id"], "event_id")
        event = message["event"]
        if not isinstance(event, str) or event not in EVENTS:
            raise ControlProtocolError("unknown control event")
        _validate_json_object(message["arguments"], "arguments")
    elif message_type == "EVENT_ACK":
        _validate_identifier(message["event_id"], "event_id")
        _validate_ack_payload(
            status=message["status"], error_code=message["error_code"]
        )

    if expected is not None:
        if not isinstance(expected, Mapping):
            raise ControlProtocolError("expected identity must be an object")
        expected_fields = {"group_id", "slot_id", "generation"}
        if not expected_fields.issubset(expected):
            raise ControlProtocolError("expected identity is missing required fields")
        for field in expected_fields:
            if message[field] != expected[field]:
                raise ControlProtocolError(
                    "message identity does not match expected identity"
                )

    return message


def encode_message(
    message: Any,
    *,
    frame_limit: int = DEFAULT_FRAME_LIMIT,
) -> bytes:
    if type(frame_limit) is not int or frame_limit < 5:
        raise ControlProtocolError(
            "frame_limit must be an integer of at least 5 bytes"
        )

    validate_message(message)
    try:
        payload = json.dumps(
            message,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError):
        raise ControlProtocolError("failed to encode control frame") from None

    frame = struct.pack(">I", len(payload)) + payload
    if len(frame) > frame_limit:
        raise ControlFrameTooLarge(
            f"encoded control frame is {len(frame)} bytes, exceeding the "
            f"{frame_limit}-byte frame limit"
        )
    return frame


def decode_message(payload: bytes) -> dict[str, Any]:
    if not isinstance(payload, bytes):
        raise ControlProtocolError("payload must be bytes")

    try:
        decoded = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except ControlProtocolError:
        raise
    except (UnicodeDecodeError, ValueError, TypeError, RecursionError):
        raise ControlProtocolError("payload is not valid control frame JSON") from None

    return validate_message(decoded)


def _validate_pid(pid: Any) -> None:
    if type(pid) is not int or pid <= 0:
        raise ControlProtocolError("pid must be a positive integer")


def _validate_identifier(identifier: Any, field_name: str) -> None:
    if (
        not isinstance(identifier, str)
        or _IDENTIFIER_PATTERN.fullmatch(identifier) is None
    ):
        raise ControlProtocolError(f"{field_name} must be 32 lowercase hex characters")


def _validate_status_payload(
    *,
    status: Any,
    result: Any,
    error_code: Any,
) -> None:
    if status == "OK":
        if error_code is not None:
            raise ControlProtocolError("successful responses must not have an error")
        _validate_json_object(result, "result")
    elif status == "ERROR":
        if result is not None:
            raise ControlProtocolError("error responses must not have a result")
        if not isinstance(error_code, str) or error_code not in ERROR_CODES:
            raise ControlProtocolError("unknown control error code")
    else:
        raise ControlProtocolError("unknown control response status")


def _validate_ack_payload(*, status: Any, error_code: Any) -> None:
    if status == "OK":
        if error_code is not None:
            raise ControlProtocolError("successful acknowledgements must not have an error")
    elif status == "ERROR":
        if not isinstance(error_code, str) or error_code not in ERROR_CODES:
            raise ControlProtocolError("unknown control error code")
    else:
        raise ControlProtocolError("unknown control acknowledgement status")


def _validate_json_object(value: Any, field_name: str) -> None:
    if not isinstance(value, dict):
        raise ControlProtocolError(f"{field_name} must be a JSON object")

    node_count = 0

    def visit(item: Any, depth: int) -> None:
        nonlocal node_count
        node_count += 1
        if node_count > _MAX_JSON_NODES:
            raise ControlProtocolError(
                f"{field_name} exceeds the bounded JSON element limit"
            )
        if depth > _MAX_JSON_DEPTH:
            raise ControlProtocolError(f"{field_name} exceeds the bounded JSON depth")

        if isinstance(item, dict):
            for key, child in item.items():
                if not isinstance(key, str) or len(key) > _MAX_JSON_STRING_LENGTH:
                    raise ControlProtocolError(
                        f"{field_name} contains an invalid JSON object key"
                    )
                visit(child, depth + 1)
        elif isinstance(item, list):
            for child in item:
                visit(child, depth + 1)
        elif isinstance(item, str):
            if len(item) > _MAX_JSON_STRING_LENGTH:
                raise ControlProtocolError(
                    f"{field_name} contains an oversized JSON string"
                )
        elif type(item) is float and not math.isfinite(item):
            raise ControlProtocolError(f"{field_name} contains a non-finite number")
        elif type(item) not in {bool, int, float} and item is not None:
            raise ControlProtocolError(f"{field_name} contains an unsupported JSON value")

    visit(value, 1)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ControlProtocolError("duplicate JSON key")
        result[key] = value
    return result
