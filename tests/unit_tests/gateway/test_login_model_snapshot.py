from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from agent_gateway.auth.public_model_catalog import (
    CatalogError,
    LoginModelSnapshot,
    PublicLoginModel,
    validate_login_model_snapshot,
)
from agent_gateway.processes import control_protocol
from agent_gateway.processes.control_protocol import DEFAULT_FRAME_LIMIT, encode_message


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


def test_snapshot_is_frozen_and_serializes_only_four_fields() -> None:
    model = PublicLoginModel("model-a", "Model A", "Public description")
    snapshot = LoginModelSnapshot(
        status="ok",
        api_base="https://models.example.invalid/v1",
        models=(model,),
    )

    assert snapshot.version == 1
    assert snapshot.to_dict() == {
        "version": 1,
        "status": "ok",
        "api_base": "https://models.example.invalid/v1",
        "models": [model.to_dict()],
    }
    with pytest.raises(Exception):
        snapshot.status = "unavailable"
    with pytest.raises(Exception):
        snapshot.models[0].model_name = "changed"


def test_validate_snapshot_returns_immutable_contract() -> None:
    value = {
        "version": 1,
        "status": "ok",
        "api_base": "http://127.0.0.1:8080/v1",
        "models": [
            {
                "model_name": "model-a",
                "display_name": "Model A",
                "description": "",
            }
        ],
    }

    result = validate_login_model_snapshot(value)

    assert result == LoginModelSnapshot(
        status="ok",
        api_base="http://127.0.0.1:8080/v1",
        models=(
            PublicLoginModel("model-a", "Model A", ""),
        ),
        version=1,
    )
    assert result.to_dict() == value


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"version": 1, "status": "ok", "api_base": ""},
        {"version": 1, "status": "ok", "api_base": "", "models": [], "extra": 1},
        {"version": True, "status": "ok", "api_base": "", "models": []},
        {"version": 2, "status": "ok", "api_base": "", "models": []},
        {"version": 1, "status": "unknown", "api_base": "", "models": []},
        {"version": 1, "status": "unavailable", "api_base": "https://a.invalid", "models": []},
        {"version": 1, "status": "unauthenticated", "api_base": "", "models": [
            {"model_name": "m", "display_name": "M", "description": ""},
        ]},
        {"version": 1, "status": "ok", "api_base": None, "models": []},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid", "models": {}},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid", "models": [
            {"model_name": "m", "display_name": "M"},
        ]},
        {"version": 1, "status": "ok", "api_base": "ftp://a.invalid", "models": []},
        {"version": 1, "status": "ok", "api_base": "http:///v1", "models": []},
        {"version": 1, "status": "ok", "api_base": "http://user@a.invalid", "models": []},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid/v1?x=1", "models": []},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid/v1#frag", "models": []},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid:99999/", "models": []},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid:abc/", "models": []},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid:443:80/", "models": []},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid:0/", "models": []},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid/v1?", "models": []},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid/v1#", "models": []},
        {"version": 1, "status": "ok", "api_base": "h" * 2049, "models": []},
        {"version": 1, "status": "ok", "api_base": "", "models": [
            {"model_name": "m", "display_name": "M", "description": ""},
        ]},
    ],
)
def test_validate_snapshot_rejects_invalid_values_without_echo(value: object) -> None:
    with pytest.raises(CatalogError) as caught:
        validate_login_model_snapshot(value)

    assert caught.value.reason in {"protocol", "too_large"}
    assert caught.value.__cause__ is None
    assert "secret-value" not in str(caught.value)


@pytest.mark.parametrize(
    "value",
    [
        {"version": 1, "status": "ok", "api_base": "", "models": [
            {"model_name": "m" * 257, "display_name": "M", "description": ""},
        ]},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid", "models": [
            {"model_name": "m", "display_name": "M" * 257, "description": ""},
        ]},
        {"version": 1, "status": "ok", "api_base": "https://a.invalid", "models": [
            {"model_name": "m", "display_name": "M", "description": "d" * 1025},
        ]},
        {
            "version": 1,
            "status": "ok",
            "api_base": "https://a.invalid",
            "models": [
                {"model_name": f"m-{index}", "display_name": "M", "description": ""}
                for index in range(49)
            ],
        },
    ],
)
def test_validate_snapshot_enforces_existing_model_limits(value: dict[str, Any]) -> None:
    with pytest.raises(CatalogError) as caught:
        validate_login_model_snapshot(value)

    assert caught.value.reason == "too_large"


def test_validate_snapshot_enforces_complete_result_and_original_codec_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    models = [
        {
            "model_name": f"model-{index}",
            "display_name": f"Model {index}",
            "description": "d" * 1000,
        }
        for index in range(48)
    ]
    value = {
        "version": 1,
        "status": "ok",
        "api_base": "https://models.example.invalid/v1",
        "models": models,
    }

    with pytest.raises(CatalogError) as caught:
        validate_login_model_snapshot(value)
    assert caught.value.reason == "too_large"

    value["api_base"] = "https://a.invalid"
    value["models"] = models[:1]
    value["models"][0]["description"] = ""
    result = validate_login_model_snapshot(value)
    response = {
        "protocol_version": 1,
        "group_id": "\U0010ffff" * 256,
        "slot_id": 0,
        "generation": 1,
        "type": "RESPONSE",
        "request_id": "a" * 32,
        "status": "OK",
        "result": result.to_dict(),
        "error_code": None,
    }
    frame = encode_message(response)

    encoded_result = json.dumps(
        result.to_dict(), separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    assert len(encoded_result) <= 48 * 1024
    assert len(frame) <= DEFAULT_FRAME_LIMIT
    assert control_protocol.decode_message(frame[4:]) == response


def test_validator_sanitizes_original_codec_rejection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_encoding(message: dict[str, Any]) -> bytes:
        raise control_protocol.ControlProtocolError("codec-secret-value")

    monkeypatch.setattr(control_protocol, "encode_message", reject_encoding)

    with pytest.raises(CatalogError) as caught:
        validate_login_model_snapshot(
            {"version": 1, "status": "ok", "api_base": "https://a.invalid", "models": []}
        )

    assert caught.value.reason == "too_large"
    assert "codec-secret-value" not in str(caught.value)
