"""Unit tests for the control service wire protocol."""

import json
import math
import struct

import pytest

from agent_gateway.processes.control_protocol import (
    CONTROL_PROTOCOL_VERSION,
    DEFAULT_FRAME_LIMIT,
    ERROR_CODES,
    EVENTS,
    OPERATIONS,
    ControlProtocolError,
    decode_message,
    encode_message,
    validate_message,
)


def base_message(message_type: str) -> dict[str, object]:
    message: dict[str, object] = {
        "protocol_version": CONTROL_PROTOCOL_VERSION,
        "group_id": "gateway-group",
        "slot_id": 0,
        "generation": 1,
        "type": message_type,
    }
    if message_type == "HELLO":
        message["pid"] = 123
    elif message_type == "SERVICE_READY":
        message["pid"] = 456
        message["worker_pid"] = 123
    elif message_type == "REQUEST":
        message["request_id"] = "0123456789abcdef0123456789abcdef"
        message["operation"] = "credentials.request_model_auth"
        message["arguments"] = {"model_name": "test-model"}
    elif message_type == "RESPONSE":
        message["request_id"] = "0123456789abcdef0123456789abcdef"
        message["status"] = "OK"
        message["result"] = {"available": True}
        message["error_code"] = None
    elif message_type == "EVENT":
        message["event_id"] = "fedcba9876543210fedcba9876543210"
        message["event"] = "credentials.invalidate"
        message["arguments"] = {"reason": "logout"}
    elif message_type == "EVENT_ACK":
        message["event_id"] = "fedcba9876543210fedcba9876543210"
        message["status"] = "ERROR"
        message["error_code"] = "EVENT_NOT_ENABLED"
    return message


@pytest.mark.parametrize(
    "message_type",
    ["HELLO", "SERVICE_READY", "REQUEST", "RESPONSE", "EVENT", "EVENT_ACK"],
)
def test_validate_accepts_every_control_message_type(message_type: str) -> None:
    message = base_message(message_type)

    assert validate_message(message) == message


def test_protocol_exports_the_locked_names_and_enums() -> None:
    assert CONTROL_PROTOCOL_VERSION == 1
    assert DEFAULT_FRAME_LIMIT == 65536
    assert OPERATIONS == frozenset(
        {
            "cron.route_command",
            "cron.project_begin",
            "cron.project_finish",
            "cron.project_status",
            "cron.bind_login_model",
            "cron.resolve_model_auth",
            "cron.validate_login_job",
            "cron.get_login_snapshot",
            "credentials.request_model_auth",
            "credentials.refresh_for_ref",
            "credentials.update_result",
            "bindings.remember",
            "bindings.lookup",
            "bindings.resolve_model_auth",
            "catalog.list_login_models",
            "catalog.get_login_model_snapshot",
        }
    )
    assert EVENTS == frozenset({"credentials.invalidate", "cron.command_deliver",
                                "cron.project_fence", "cron.project_drain", "cron.project_release"})
    assert ERROR_CODES == frozenset(
        {
            "OPERATION_NOT_ENABLED",
            "CONTROL_SERVICE_BUSY",
            "CONTROL_SERVICE_FAILED",
            "EVENT_NOT_ENABLED",
        }
    )


def test_validate_checks_expected_identity_without_rejecting_extra_context() -> None:
    message = base_message("HELLO")
    expected = {
        "group_id": "gateway-group",
        "slot_id": 0,
        "generation": 1,
        "caller_context": "ignored",
    }

    assert validate_message(message, expected=expected) == message


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("protocol_version", 0),
        ("protocol_version", "1"),
        ("protocol_version", True),
        ("group_id", ""),
        ("group_id", None),
        ("group_id", "g" * 257),
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
    message = base_message("HELLO")
    message[field] = value

    with pytest.raises(ControlProtocolError):
        validate_message(message)


@pytest.mark.parametrize("field", ["group_id", "slot_id", "generation", "type", "pid"])
def test_validate_rejects_missing_required_fields(field: str) -> None:
    message = base_message("HELLO")
    del message[field]

    with pytest.raises(ControlProtocolError):
        validate_message(message)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pid", 0),
        ("pid", -1),
        ("pid", True),
        ("pid", "123"),
    ],
)
def test_validate_rejects_invalid_pids(field: str, value: object) -> None:
    message = base_message("HELLO")
    message[field] = value

    with pytest.raises(ControlProtocolError):
        validate_message(message)


@pytest.mark.parametrize(
    ("message_type", "field"),
    [
        ("HELLO", "pid"),
        ("SERVICE_READY", "pid"),
        ("SERVICE_READY", "worker_pid"),
        ("REQUEST", "request_id"),
        ("REQUEST", "operation"),
        ("REQUEST", "arguments"),
        ("RESPONSE", "request_id"),
        ("RESPONSE", "status"),
        ("RESPONSE", "result"),
        ("RESPONSE", "error_code"),
        ("EVENT", "event_id"),
        ("EVENT", "event"),
        ("EVENT", "arguments"),
        ("EVENT_ACK", "event_id"),
        ("EVENT_ACK", "status"),
        ("EVENT_ACK", "error_code"),
    ],
)
def test_validate_rejects_missing_type_specific_fields(
    message_type: str, field: str
) -> None:
    message = base_message(message_type)
    del message[field]

    with pytest.raises(ControlProtocolError):
        validate_message(message)


@pytest.mark.parametrize(
    ("message_type", "extra"),
    [
        ("HELLO", {"worker_pid": 456}),
        ("SERVICE_READY", {"stage": "ready"}),
        ("REQUEST", {"event_id": "0" * 32}),
        ("RESPONSE", {"operation": "credentials.request_model_auth"}),
        ("EVENT", {"request_id": "0" * 32}),
        ("EVENT_ACK", {"result": {}}),
    ],
)
def test_validate_rejects_fields_outside_each_type_schema(
    message_type: str, extra: dict[str, object]
) -> None:
    message = base_message(message_type)
    message.update(extra)

    with pytest.raises(ControlProtocolError):
        validate_message(message)


@pytest.mark.parametrize(
    ("id_field", "value"),
    [
        ("request_id", "0" * 31),
        ("request_id", "0" * 33),
        ("request_id", "0123456789ABCDEF0123456789ABCDEF"),
        ("request_id", "z" * 32),
        ("request_id", 123),
        ("event_id", "0" * 31),
        ("event_id", "0" * 33),
        ("event_id", "0123456789ABCDEF0123456789ABCDEF"),
        ("event_id", True),
    ],
)
def test_validate_rejects_invalid_hex_identifiers(id_field: str, value: object) -> None:
    message = base_message("REQUEST" if id_field == "request_id" else "EVENT")
    message[id_field] = value

    with pytest.raises(ControlProtocolError):
        validate_message(message)


@pytest.mark.parametrize("operation", sorted(OPERATIONS))
def test_validate_accepts_only_the_reserved_operations(operation: str) -> None:
    message = base_message("REQUEST")
    message["operation"] = operation

    assert validate_message(message)["operation"] == operation


@pytest.mark.parametrize(
    "operation",
    ["credentials.request", "debug.echo", "bindings.remember "],
)
def test_validate_rejects_unknown_operations(operation: str) -> None:
    message = base_message("REQUEST")
    message["operation"] = operation

    with pytest.raises(ControlProtocolError):
        validate_message(message)


def test_validate_accepts_nested_small_json_values_in_arguments() -> None:
    message = base_message("REQUEST")
    message["arguments"] = {
        "text": "value",
        "number": 12,
        "float": 1.5,
        "boolean": True,
        "none": None,
        "list": [{"nested": [False, 0, ""]}],
    }

    assert validate_message(message) == message


@pytest.mark.parametrize(
    "value",
    [
        b"bytes",
        {"set": {1, 2}},
        ("tuple",),
        object(),
        math.inf,
        -math.inf,
        math.nan,
    ],
)
def test_validate_rejects_non_json_or_non_finite_values(value: object) -> None:
    message = base_message("REQUEST")
    message["arguments"] = {"value": value}

    with pytest.raises(ControlProtocolError):
        validate_message(message)


@pytest.mark.parametrize(
    "value",
    [
        [],
        "value",
        1,
        None,
        True,
        {"value": {"value": {"value": {"value": {"value": {"value": {"value": {"value": {"value": 1}}}}}}}}},
        {"items": ["x" * 4097]},
        {str(index): index for index in range(257)},
    ],
)
def test_validate_rejects_invalid_or_unbounded_json_values(value: object) -> None:
    message = base_message("REQUEST")
    message["arguments"] = value

    with pytest.raises(ControlProtocolError):
        validate_message(message)


@pytest.mark.parametrize(
    ("status", "result", "error_code"),
    [
        ("OK", None, None),
        ("OK", {"value": 1}, "CONTROL_SERVICE_FAILED"),
        ("OK", {"value": 1}, "unexpected"),
        ("ERROR", {"value": 1}, "CONTROL_SERVICE_FAILED"),
        ("ERROR", None, "unexpected"),
        ("ERROR", None, None),
        ("FAILED", None, "CONTROL_SERVICE_FAILED"),
    ],
)
def test_validate_rejects_invalid_response_shape(
    status: object, result: object, error_code: object
) -> None:
    message = base_message("RESPONSE")
    message["status"] = status
    message["result"] = result
    message["error_code"] = error_code

    with pytest.raises(ControlProtocolError):
        validate_message(message)


@pytest.mark.parametrize("error_code", sorted(ERROR_CODES))
def test_validate_accepts_every_error_response_code(error_code: str) -> None:
    message = base_message("RESPONSE")
    message["status"] = "ERROR"
    message["result"] = None
    message["error_code"] = error_code

    assert validate_message(message)["error_code"] == error_code


@pytest.mark.parametrize(
    ("status", "error_code"),
    [
        ("OK", "unexpected"),
        ("ERROR", None),
        ("ERROR", "unexpected"),
        ("FAILED", "EVENT_NOT_ENABLED"),
    ],
)
def test_validate_rejects_invalid_event_ack_shape(
    status: object, error_code: object
) -> None:
    message = base_message("EVENT_ACK")
    message["status"] = status
    message["error_code"] = error_code

    with pytest.raises(ControlProtocolError):
        validate_message(message)


def test_validate_rejects_unknown_events() -> None:
    message = base_message("EVENT")
    message["event"] = "control.debug"

    with pytest.raises(ControlProtocolError):
        validate_message(message)


def test_validate_rejects_expected_identity_mismatch() -> None:
    message = base_message("REQUEST")
    expected = {"group_id": "other-group", "slot_id": 0, "generation": 1}

    with pytest.raises(ControlProtocolError):
        validate_message(message, expected=expected)


def test_encode_message_adds_four_byte_length_and_round_trips() -> None:
    message = base_message("REQUEST")

    frame = encode_message(message)

    payload_length = struct.unpack(">I", frame[:4])[0]
    assert frame[4:] == json.dumps(
        message, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    assert payload_length == len(frame) - 4
    assert decode_message(frame[4:]) == message


def test_encode_message_rejects_frames_over_the_configured_budget() -> None:
    message = base_message("REQUEST")
    message["arguments"] = {"value": "x" * 256}
    baseline = encode_message(message)

    with pytest.raises(ControlProtocolError, match="frame limit") as error:
        encode_message(message, frame_limit=len(baseline) - 1)

    assert "x" * 256 not in str(error.value)


def test_decode_message_rejects_duplicate_json_keys_without_secret_context() -> None:
    payload = b'{"type": "REQUEST", "type": "RESPONSE"}'

    with pytest.raises(ControlProtocolError, match="duplicate JSON key") as error:
        decode_message(payload)

    assert error.value.__cause__ is None
    assert "RESPONSE" not in str(error.value)


@pytest.mark.parametrize(
    "payload",
    [
        b"{",
        b"[]",
        b"\xff\xfe",
        b'{"secret": "value"}',
    ],
)
def test_decode_message_rejects_invalid_payloads_without_secret_context(
    payload: bytes,
) -> None:
    with pytest.raises(ControlProtocolError) as error:
        decode_message(payload)

    assert error.value.__cause__ is None
    assert b"secret" not in str(error.value).encode("utf-8")
