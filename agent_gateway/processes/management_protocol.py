# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Wire protocol for parent-worker process management messages.

Direction:
    Parent -> Worker: INIT, STOP, CRON_ACTIVATE.
    Worker -> Parent: READY, RUNTIME_READY, CRON_PREPARED, CRON_ACTIVE,
    STOPPED, FAILED.

Expected lifecycle:
    INIT -> READY -> RUNTIME_READY -> CRON_PREPARED* -> CRON_ACTIVATE
    -> CRON_ACTIVE -> STOP -> STOPPED.

Protocol v1 uses a closed schema. Adding a field requires a protocol version
bump and a coordinated parent/worker deployment.
"""

import json
import re
import struct
from collections.abc import Mapping
from typing import Any


class ManagementProtocolError(ValueError):
    """Raised when a message violates the management process protocol."""


def _is_utf8_encodable(value: str) -> bool:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


PROTOCOL_VERSION = 1
DEFAULT_FRAME_LIMIT = 16384
FRAME_HEADER_SIZE = struct.calcsize(">I")
MIN_FRAME_LIMIT = FRAME_HEADER_SIZE + 1

_MAX_GROUP_ID_LENGTH = 256
_MAX_RUNTIME_COUNT = 64
_MAX_RUNTIME_ID_LENGTH = 256
_MAX_ERROR_CODE_LENGTH = 128
_ERROR_CODE_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.-]*")
_COMMON_FIELDS = frozenset(
    {"protocol_version", "group_id", "slot_id", "generation", "type"}
)
_IDENTITY_FIELDS = frozenset({"group_id", "slot_id", "generation"})
_MESSAGE_TYPES = frozenset({
    "INIT", "READY", "RUNTIME_READY", "CRON_PREPARED", "CRON_ACTIVATE",
    "CRON_ACTIVE", "STOP", "STOPPED", "FAILED",
})
_PID_MESSAGE_TYPES = frozenset({
    "READY", "RUNTIME_READY", "CRON_PREPARED", "CRON_ACTIVE", "STOPPED", "FAILED",
})
_FIELDS_BY_TYPE = {
    "INIT": frozenset(_COMMON_FIELDS),
    "READY": _COMMON_FIELDS | {"pid", "stage"},
    "RUNTIME_READY": _COMMON_FIELDS | {"pid", "runtime_ids"},
    "CRON_PREPARED": _COMMON_FIELDS | {"pid"},
    "CRON_ACTIVATE": frozenset(_COMMON_FIELDS),
    "CRON_ACTIVE": _COMMON_FIELDS | {"pid"},
    "STOP": frozenset(_COMMON_FIELDS),
    "STOPPED": _COMMON_FIELDS | {"pid"},
    "FAILED": _COMMON_FIELDS | {"pid", "error_code"},
}


def _validate_identity(identity: Mapping[str, Any], *, prefix: str = "") -> None:
    group_id = identity["group_id"]
    if (
        not isinstance(group_id, str)
        or not group_id
        or len(group_id) > _MAX_GROUP_ID_LENGTH
        or not _is_utf8_encodable(group_id)
    ):
        raise ManagementProtocolError(
            f"{prefix}group_id must be a non-empty bounded string"
        )

    slot_id = identity["slot_id"]
    if type(slot_id) is not int or slot_id < 0:
        raise ManagementProtocolError(
            f"{prefix}slot_id must be a non-negative integer"
        )

    generation = identity["generation"]
    if type(generation) is not int or generation < 1:
        raise ManagementProtocolError(
            f"{prefix}generation must be a positive integer"
        )


def _validate_type_fields(message_type: str, message: Mapping[str, Any]) -> None:
    if message_type in _PID_MESSAGE_TYPES:
        pid = message["pid"]
        if type(pid) is not int or pid <= 0:
            raise ManagementProtocolError("pid must be a positive integer")

    if message_type == "READY" and message["stage"] != "management_ready":
        raise ManagementProtocolError("READY stage must be management_ready")

    if message_type == "RUNTIME_READY":
        runtime_ids = message["runtime_ids"]
        if type(runtime_ids) is not list or not 1 <= len(runtime_ids) <= _MAX_RUNTIME_COUNT:
            raise ManagementProtocolError(
                f"runtime_ids must be a non-empty list of at most {_MAX_RUNTIME_COUNT} entries"
            )
        for runtime_id in runtime_ids:
            if (
                not isinstance(runtime_id, str)
                or not runtime_id
                or len(runtime_id) > _MAX_RUNTIME_ID_LENGTH
                or not _is_utf8_encodable(runtime_id)
            ):
                raise ManagementProtocolError(
                    "each runtime_id must be a non-empty bounded string"
                )
        if len(set(runtime_ids)) != len(runtime_ids):
            raise ManagementProtocolError("runtime_ids must not contain duplicates")

    if message_type == "FAILED":
        error_code = message["error_code"]
        if (
            not isinstance(error_code, str)
            or not error_code
            or len(error_code) > _MAX_ERROR_CODE_LENGTH
            or not _ERROR_CODE_PATTERN.fullmatch(error_code)
        ):
            raise ManagementProtocolError(
                "error_code must be a bounded machine-readable string"
            )


def _validate_expected_identity(
    message: Mapping[str, Any],
    expected: Mapping[str, Any] | None,
) -> None:
    if expected is None:
        return
    if not isinstance(expected, Mapping):
        raise ManagementProtocolError("expected identity must be an object")
    if not _IDENTITY_FIELDS.issubset(expected):
        raise ManagementProtocolError("expected identity is missing required fields")

    _validate_identity(expected, prefix="expected ")
    for field in _IDENTITY_FIELDS:
        if message[field] != expected[field]:
            raise ManagementProtocolError(
                f"message {field} does not match expected identity"
            )


def validate_message(message: Any, *, expected: Mapping[str, Any] | None = None) -> dict[str, Any]:
    if not isinstance(message, dict):
        raise ManagementProtocolError("message must be a JSON object")

    message_type = message.get("type")
    if not isinstance(message_type, str) or message_type not in _MESSAGE_TYPES:
        raise ManagementProtocolError(f"unknown message type: {message_type!r}")

    allowed_fields = _FIELDS_BY_TYPE[message_type]
    missing_fields = allowed_fields - message.keys()
    unknown_fields = message.keys() - allowed_fields
    if missing_fields:
        raise ManagementProtocolError(f"missing fields: {sorted(missing_fields)}")
    if unknown_fields:
        raise ManagementProtocolError(f"unknown fields: {sorted(unknown_fields)}")

    protocol_version = message["protocol_version"]
    if type(protocol_version) is not int or protocol_version != PROTOCOL_VERSION:
        raise ManagementProtocolError(f"unsupported protocol version: {protocol_version!r}")

    _validate_identity(message)
    _validate_type_fields(message_type, message)
    _validate_expected_identity(message, expected)

    return message


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ManagementProtocolError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def encode_message(
    message: Any,
    *,
    frame_limit: int = DEFAULT_FRAME_LIMIT,
) -> bytes:
    if type(frame_limit) is not int or frame_limit < MIN_FRAME_LIMIT:
        raise ManagementProtocolError(
            f"frame_limit must be an integer of at least {MIN_FRAME_LIMIT} bytes"
        )

    validated_message = validate_message(message)
    try:
        payload = json.dumps(
            validated_message,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (UnicodeEncodeError, ValueError, TypeError, RecursionError) as error:
        raise ManagementProtocolError("message cannot be encoded as UTF-8 JSON") from error
    if len(payload) > 0xFFFFFFFF:
        raise ManagementProtocolError("message exceeds the length header capacity")

    frame = struct.pack(">I", len(payload)) + payload
    if len(frame) > frame_limit:
        raise ManagementProtocolError(
            f"encoded frame is {len(frame)} bytes, exceeding the "
            f"{frame_limit}-byte frame limit"
        )
    return frame


def decode_message(payload: bytes) -> dict[str, Any]:
    if not isinstance(payload, bytes):
        raise ManagementProtocolError("payload must be bytes")

    try:
        decoded = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except ManagementProtocolError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as error:
        raise ManagementProtocolError("payload is not valid UTF-8 JSON") from error

    return validate_message(decoded)
