# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Unit tests for the process management wire protocol."""

import json

import pytest

from agent_gateway.processes.protocol import (
    DEFAULT_FRAME_LIMIT,
    PROTOCOL_VERSION,
    ProtocolError,
    decode_message,
    encode_message,
    validate_message,
)


def base_message(message_type: str) -> dict[str, object]:
    message: dict[str, object] = {
        "protocol_version": PROTOCOL_VERSION,
        "group_id": "gateway-group",
        "slot_id": 0,
        "generation": 1,
        "type": message_type,
    }
    if message_type == "READY":
        message["pid"] = 123
        message["stage"] = "management_ready"
    elif message_type == "RUNTIME_READY":
        message["pid"] = 123
        message["runtime_ids"] = ["web-http", "web-ws", "protocol"]
    elif message_type in {"STOPPED", "FAILED"}:
        message["pid"] = 123
    if message_type == "FAILED":
        message["error_code"] = "worker_start_failed"
    return message


@pytest.mark.parametrize(
    "message_type", ["INIT", "READY", "RUNTIME_READY", "STOP", "STOPPED", "FAILED"]
)
def test_validate_accepts_all_message_types(message_type: str) -> None:
    message = base_message(message_type)

    assert validate_message(message) == message


def test_validate_only_checks_expected_identity_fields() -> None:
    message = base_message("INIT")
    expected = {
        "group_id": "gateway-group",
        "slot_id": 0,
        "generation": 1,
        "ignored": "value",
    }

    assert validate_message(message, expected=expected) == message


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("protocol_version", 0),
        ("protocol_version", "1"),
        ("group_id", ""),
        ("group_id", 1),
        ("group_id", "a" * 257),
        ("slot_id", -1),
        ("slot_id", True),
        ("slot_id", "0"),
        ("generation", 0),
        ("generation", False),
        ("generation", 1.0),
        ("type", "PING"),
    ],
)
def test_validate_rejects_invalid_common_fields(field: str, value: object) -> None:
    message = base_message("INIT")
    message[field] = value

    with pytest.raises(ProtocolError):
        validate_message(message)


@pytest.mark.parametrize(
    ("message_type", "field", "value"),
    [
        ("READY", "pid", 0),
        ("READY", "pid", True),
        ("READY", "stage", "business_ready"),
        ("RUNTIME_READY", "pid", 0),
        ("RUNTIME_READY", "pid", True),
        ("RUNTIME_READY", "runtime_ids", []),
        ("RUNTIME_READY", "runtime_ids", ("web-http",)),
        ("RUNTIME_READY", "runtime_ids", ["web-http", 1]),
        ("RUNTIME_READY", "runtime_ids", [""]),
        ("RUNTIME_READY", "runtime_ids", ["r" * 257]),
        ("RUNTIME_READY", "runtime_ids", ["same", "same"]),
        ("RUNTIME_READY", "runtime_ids", [f"id-{index}" for index in range(65)]),
        ("STOPPED", "pid", 0),
        ("STOPPED", "pid", True),
        ("FAILED", "pid", 0),
        ("FAILED", "pid", True),
        ("FAILED", "error_code", ""),
        ("FAILED", "error_code", "unexpected exception"),
        ("FAILED", "error_code", 1),
        ("FAILED", "error_code", "e" * 129),
    ],
)
def test_validate_rejects_invalid_type_fields(
    message_type: str, field: str, value: object
) -> None:
    message = base_message(message_type)
    message[field] = value

    with pytest.raises(ProtocolError):
        validate_message(message)


@pytest.mark.parametrize(
    ("message_type", "field"),
    [
        ("INIT", "group_id"),
        ("READY", "pid"),
        ("RUNTIME_READY", "pid"),
        ("RUNTIME_READY", "runtime_ids"),
        ("STOP", "group_id"),
        ("STOPPED", "pid"),
        ("FAILED", "pid"),
    ],
)
def test_validate_rejects_missing_required_fields(
    message_type: str, field: str
) -> None:
    message = base_message(message_type)
    del message[field]

    with pytest.raises(ProtocolError):
        validate_message(message)


@pytest.mark.parametrize(
    ("message_type", "extra"),
    [
        ("INIT", {"payload": {}}),
        ("INIT", {"error_code": "invalid"}),
        ("READY", {"status": "ready"}),
        ("RUNTIME_READY", {"stage": "management_ready"}),
        ("STOP", {"reason": "shutdown"}),
        ("STOPPED", {"exit_code": 0}),
        ("FAILED", {"error": "secret"}),
    ],
)
def test_validate_rejects_fields_outside_each_type_schema(
    message_type: str, extra: dict[str, object]
) -> None:
    message = base_message(message_type)
    message.update(extra)

    with pytest.raises(ProtocolError):
        validate_message(message)


def test_validate_rejects_expected_identity_mismatch() -> None:
    message = base_message("INIT")
    expected = {"group_id": "other-group", "slot_id": 0, "generation": 1}

    with pytest.raises(ProtocolError):
        validate_message(message, expected=expected)


def test_validate_rejects_non_dict_message() -> None:
    with pytest.raises(ProtocolError):
        validate_message(["INIT"])


def test_encode_message_frames_utf8_json() -> None:
    message = base_message("INIT")
    message["group_id"] = "网关-组"
    payload = json.dumps(
        message, ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    frame = len(payload).to_bytes(4, "big") + payload

    assert encode_message(message) == frame


def test_encode_message_respects_custom_frame_limit() -> None:
    message = base_message("INIT")

    with pytest.raises(ProtocolError):
        encode_message(message, frame_limit=16)


@pytest.mark.parametrize("frame_limit", [True, 0, 4])
def test_encode_message_rejects_invalid_frame_limit(frame_limit: int) -> None:
    message = base_message("INIT")

    with pytest.raises(ProtocolError):
        encode_message(message, frame_limit=frame_limit)


def test_decode_message_accepts_json_object_payload() -> None:
    message = base_message("READY")
    payload = json.dumps(message, ensure_ascii=False).encode("utf-8")

    assert decode_message(payload) == message


@pytest.mark.parametrize(
    "payload",
    [
        b"{",
        b"[]",
        b'{"type": "INIT"}',
        b"\xff\xfe",
        "not-bytes",
    ],
)
def test_decode_message_rejects_invalid_payload(payload: object) -> None:
    with pytest.raises(ProtocolError):
        decode_message(payload)  # type: ignore[arg-type]


def test_decode_message_rejects_duplicate_json_keys() -> None:
    payload = (
        b'{"protocol_version":1, "protocol_version":1, '
        b'"group_id":"gateway-group", "slot_id":0, "generation":1, "type":"INIT"}'
    )

    with pytest.raises(ProtocolError, match="duplicate"):
        decode_message(payload)


def test_decode_message_converts_json_recursion_error_to_protocol_error() -> None:
    payload = b"[" * 10000 + b"]" * 10000

    with pytest.raises(ProtocolError, match="JSON"):
        decode_message(payload)


def test_decode_rejects_integer_beyond_interpreter_limit_as_protocol_error() -> None:
    payload = b'{"slot_id":' + b'9' * 5000 + b'}'
    with pytest.raises(ProtocolError):
        decode_message(payload)
