from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from agent_gateway.auth.public_model_catalog import (
    CatalogError,
    CatalogResult,
    PublicLoginModel,
    validate_auth_session,
    validate_catalog_response,
)
from agent_gateway.processes import control_protocol
from agent_gateway.processes.control_protocol import (
    ControlProtocolError,
    DEFAULT_FRAME_LIMIT,
    encode_message,
)


@pytest.fixture(autouse=True)
def isolated_environment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    home = tmp_path / "home"
    data = tmp_path / "data"
    logs = tmp_path / "logs"
    temporary = tmp_path / "tmp"
    for directory in (home, data, logs, temporary):
        directory.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_HOME", str(home))
    monkeypatch.setenv("JIUWENSWARM_DATA_DIR", str(data))
    monkeypatch.setenv("JIUWENSWARM_LOG_DIR", str(logs))
    monkeypatch.setenv("AGENT_GATEWAY_LOG_DIR", str(logs))
    monkeypatch.setenv("AGENTOS_GATEWAY_LOG_DIR", str(logs))
    monkeypatch.setenv("TMPDIR", str(temporary))


def test_catalog_error_is_a_fixed_value_error() -> None:
    error = CatalogError("too_large")

    assert isinstance(error, ValueError)
    assert error.reason == "too_large"
    assert str(error) == "Public login model catalog failed"
    assert error.__cause__ is None


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("", None),
        ("   ", None),
        (" login-session ", "login-session"),
        ("s" * 256, "s" * 256),
    ],
)
def test_validate_auth_session_normalizes_bounded_strings(
    value: object, expected: str | None
) -> None:
    assert validate_auth_session(value) == expected


@pytest.mark.parametrize("value", [7, True, b"session", "x" * 257])
def test_validate_auth_session_rejects_invalid_values_without_echo(value: object) -> None:
    with pytest.raises(CatalogError) as caught:
        validate_auth_session(value)

    assert caught.value.reason == "protocol"
    assert str(value) not in str(caught.value)
    assert caught.value.__cause__ is None


def test_public_login_model_is_immutable_and_serializes_only_public_fields() -> None:
    model = PublicLoginModel(
        model_name="login-model",
        display_name="Login Model",
        description="Public description",
    )

    assert model.to_dict() == {
        "model_name": "login-model",
        "display_name": "Login Model",
        "description": "Public description",
    }
    with pytest.raises(Exception):
        model.model_name = "changed"


def test_validate_catalog_response_returns_immutable_snapshot() -> None:
    result = validate_catalog_response(
        {
            "status": "ok",
            "models": [
                {
                    "model_name": "login-model",
                    "display_name": "Login Model",
                    "description": "Public description",
                }
            ],
        }
    )

    assert isinstance(result, CatalogResult)
    assert result.status == "ok"
    assert result.models == (
        PublicLoginModel(
            model_name="login-model",
            display_name="Login Model",
            description="Public description",
        ),
    )
    assert result.to_dict() == {
        "status": "ok",
        "models": [
            {
                "model_name": "login-model",
                "display_name": "Login Model",
                "description": "Public description",
            }
        ],
    }
    with pytest.raises(Exception):
        result.status = "unavailable"
    with pytest.raises(Exception):
        result.models[0].model_name = "changed"


@pytest.mark.parametrize(
    "value",
    [
        [],
        {"status": "ok"},
        {"status": "ok", "models": [], "extra": 1},
        {"status": "unknown", "models": []},
        {"status": "ok", "models": {}},
        {"status": "unauthenticated", "models": [{"model_name": "m"}]},
        {"status": "too_large", "models": [{"model_name": "m"}]},
        {"status": "unavailable", "models": [{"model_name": "m"}]},
        {"status": "ok", "models": [{"model_name": "m"}]},
        {"status": "ok", "models": [{"model_name": "m", "display_name": "d"}]},
        {
            "status": "ok",
            "models": [
                {
                    "model_name": "m",
                    "display_name": "d",
                    "description": "",
                    "credential_ref": "secret",
                }
            ],
        },
        {"status": "ok", "models": [{"model_name": 7, "display_name": "d", "description": ""}]},
    ],
)
def test_validate_catalog_response_enforces_strict_shape(value: object) -> None:
    with pytest.raises(CatalogError) as caught:
        validate_catalog_response(value)

    assert caught.value.reason == "protocol"
    assert "secret" not in str(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    "value",
    [
        {
            "status": "ok",
            "models": [
                {"model_name": "m" * 257, "display_name": "d", "description": ""}
            ],
        },
        {
            "status": "ok",
            "models": [
                {"model_name": "m", "display_name": "d" * 257, "description": ""}
            ],
        },
        {
            "status": "ok",
            "models": [
                {"model_name": "m", "display_name": "d", "description": "x" * 1025}
            ],
        },
        {"status": "ok", "models": [{"model_name": f"m-{index}", "display_name": "d", "description": ""} for index in range(49)]},
    ],
)
def test_validate_catalog_response_rejects_capacity_without_truncation(value: object) -> None:
    with pytest.raises(CatalogError) as caught:
        validate_catalog_response(value)

    assert caught.value.reason == "too_large"
    assert caught.value.__cause__ is None


def test_validate_catalog_response_allows_control_characters_and_multibyte_text() -> None:
    result = validate_catalog_response(
        {
            "status": "ok",
            "models": [
                {
                    "model_name": "模型\n",
                    "display_name": "显示\t",
                    "description": "描述\x00😀",
                }
            ],
        }
    )

    encoded = json.dumps(
        result.to_dict(), separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    assert b"\xe6\xa8\xa1\xe5\x9e\x8b" in encoded
    assert b"\\u0000" in encoded


def test_validate_catalog_response_rejects_surrogates_as_protocol_failure() -> None:
    with pytest.raises(CatalogError) as caught:
        validate_catalog_response(
            {
                "status": "ok",
                "models": [
                    {"model_name": "\ud800", "display_name": "d", "description": ""}
                ],
            }
        )

    assert caught.value.reason == "protocol"
    assert caught.value.__cause__ is None


def test_validate_catalog_response_enforces_complete_utf8_result_limit() -> None:
    model = {
        "model_name": "😀" * 256,
        "display_name": "😀" * 256,
        "description": "😀" * 1024,
    }
    value = {"status": "ok", "models": [dict(model) for _ in range(8)]}

    with pytest.raises(CatalogError) as caught:
        validate_catalog_response(value)

    assert caught.value.reason == "too_large"
    assert caught.value.__cause__ is None


def test_maximum_multibyte_group_and_models_pass_original_control_codec() -> None:
    models = [
        {
            "model_name": "m" * 64,
            "display_name": "d" * 64,
            "description": "😀" * 192,
        }
        for _ in range(48)
    ]
    result = validate_catalog_response({"status": "ok", "models": models})
    encoded_result = json.dumps(
        result.to_dict(), separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    assert len(encoded_result) <= 48 * 1024

    response: dict[str, Any] = {
        "protocol_version": 1,
        "group_id": "😀" * 256,
        "slot_id": 0,
        "generation": 1,
        "type": "RESPONSE",
        "request_id": "a" * 32,
        "status": "OK",
        "result": result.to_dict(),
        "error_code": None,
    }
    frame = encode_message(response)

    assert len(frame) <= DEFAULT_FRAME_LIMIT


@pytest.mark.parametrize("status", ["ok", "unauthenticated", "too_large", "unavailable"])
def test_validator_encodes_complete_response_with_original_codec(
    monkeypatch: pytest.MonkeyPatch, status: str
) -> None:
    responses: list[dict[str, Any]] = []
    frames: list[bytes] = []
    models = (
        [{"model_name": "m", "display_name": "Model", "description": "Public"}]
        if status == "ok"
        else []
    )
    value = {"status": status, "models": models}

    def record_encoding(message: dict[str, Any]) -> bytes:
        frame = encode_message(message)
        responses.append(message)
        frames.append(frame)
        return frame

    monkeypatch.setattr(control_protocol, "encode_message", record_encoding)

    result = validate_catalog_response(value)

    assert result.to_dict() == value
    assert responses == [
        {
            "protocol_version": 1,
            "group_id": "\U0010ffff" * 256,
            "slot_id": 0,
            "generation": 1,
            "type": "RESPONSE",
            "request_id": "a" * 32,
            "status": "OK",
            "result": value,
            "error_code": None,
        }
    ]
    assert len(frames) == 1
    assert len(frames[0]) <= DEFAULT_FRAME_LIMIT
    assert control_protocol.decode_message(frames[0][4:]) == responses[0]


def test_validator_sanitizes_original_codec_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_encoding(message: dict[str, Any]) -> bytes:
        raise ControlProtocolError("codec-secret-value")

    monkeypatch.setattr(control_protocol, "encode_message", reject_encoding)

    with pytest.raises(CatalogError) as caught:
        validate_catalog_response({"status": "ok", "models": []})

    assert caught.value.reason == "too_large"
    assert str(caught.value) == "Public login model catalog failed"
    assert "codec-secret-value" not in str(caught.value)
    assert caught.value.__cause__ is None
    assert caught.value.__suppress_context__ is True
