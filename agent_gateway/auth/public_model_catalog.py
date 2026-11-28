"""Public login-model catalog contracts shared by Control and Worker."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal, Protocol
from urllib.parse import urlsplit

from agent_gateway.processes import control_protocol


CatalogStatus = Literal["ok", "unauthenticated", "too_large", "unavailable"]
CatalogErrorReason = Literal[
    "too_large", "unavailable", "protocol", "busy", "not_enabled"
]

_CATALOG_STATUSES = frozenset(
    {"ok", "unauthenticated", "too_large", "unavailable"}
)
_CATALOG_ERROR_REASONS = frozenset(
    {"too_large", "unavailable", "protocol", "busy", "not_enabled"}
)
_MAX_AUTH_SESSION_LENGTH = 256
_MAX_MODELS = 48
_MAX_MODEL_NAME_LENGTH = 256
_MAX_DISPLAY_NAME_LENGTH = 256
_MAX_DESCRIPTION_LENGTH = 1024
_MAX_RESULT_BYTES = 48 * 1024
_MAX_API_BASE_LENGTH = 2048


class CatalogError(ValueError):
    """A sanitized login-model catalog failure."""

    def __init__(self, reason: CatalogErrorReason = "unavailable") -> None:
        if reason not in _CATALOG_ERROR_REASONS:
            raise ValueError("Unsupported login model catalog failure reason")
        self.reason = reason
        super().__init__("Public login model catalog failed")


@dataclass(frozen=True, slots=True)
class PublicLoginModel:
    """The immutable, public projection of a login model."""

    model_name: str
    display_name: str
    description: str

    def to_dict(self) -> dict[str, str]:
        return {
            "model_name": self.model_name,
            "display_name": self.display_name,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class CatalogResult:
    """An immutable catalog snapshot or fixed business failure status."""

    status: CatalogStatus
    models: tuple[PublicLoginModel, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "models": [model.to_dict() for model in self.models],
        }


@dataclass(frozen=True, slots=True)
class LoginModelSnapshot:
    """An immutable public model catalog snapshot with its API base."""

    status: CatalogStatus
    api_base: str
    models: tuple[PublicLoginModel, ...]
    version: int = 1

    def to_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "status": self.status,
            "api_base": self.api_base,
            "models": [model.to_dict() for model in self.models],
        }


class PublicLoginModelCatalog(Protocol):
    """Port for fetching the public login-model catalog."""

    async def list_models(self, auth_session: str | None) -> CatalogResult:
        """Return a snapshot for the selected login session."""
        ...


class LoginModelSnapshotPort(Protocol):
    """Port for fetching a complete public login-model snapshot."""

    async def get_snapshot(self, auth_session: str | None) -> LoginModelSnapshot:
        """Return model metadata and its public inference API base."""
        ...


def validate_auth_session(value: object) -> str | None:
    """Validate and normalize a bounded auth-session identifier."""
    if value is None:
        return None
    if type(value) is not str or len(value) > _MAX_AUTH_SESSION_LENGTH:
        raise CatalogError("protocol")
    normalized = value.strip()
    return normalized if normalized else None


def _validate_models(status: CatalogStatus, models_value: object) -> tuple[PublicLoginModel, ...]:
    if type(models_value) is not list:
        raise CatalogError("protocol")
    if status != "ok" and models_value:
        raise CatalogError("protocol")
    if len(models_value) > _MAX_MODELS:
        raise CatalogError("too_large")

    models: list[PublicLoginModel] = []
    for model_value in models_value:
        if type(model_value) is not dict or set(model_value) != {
            "model_name",
            "display_name",
            "description",
        }:
            raise CatalogError("protocol")

        model_name = model_value["model_name"]
        display_name = model_value["display_name"]
        description = model_value["description"]
        if (
            type(model_name) is not str
            or type(display_name) is not str
            or type(description) is not str
        ):
            raise CatalogError("protocol")
        if (
            len(model_name) > _MAX_MODEL_NAME_LENGTH
            or len(display_name) > _MAX_DISPLAY_NAME_LENGTH
            or len(description) > _MAX_DESCRIPTION_LENGTH
        ):
            raise CatalogError("too_large")

        models.append(
            PublicLoginModel(
                model_name=model_name,
                display_name=display_name,
                description=description,
            )
        )
    return tuple(models)


def _validate_wire_result(result_dict: dict[str, object]) -> None:
    try:
        encoded_result = json.dumps(
            result_dict,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except UnicodeError:
        raise CatalogError("protocol") from None

    if len(encoded_result) > _MAX_RESULT_BYTES:
        raise CatalogError("too_large")

    try:
        control_protocol.encode_message(
            {
                "protocol_version": 1,
                # Budget for the longest UTF-8 group ID accepted by the codec.
                "group_id": "\U0010ffff" * 256,
                "slot_id": 0,
                "generation": 1,
                "type": "RESPONSE",
                "request_id": "a" * 32,
                "status": "OK",
                "result": result_dict,
                "error_code": None,
            }
        )
    except control_protocol.ControlProtocolError:
        raise CatalogError("too_large") from None


def _validate_api_base(value: object) -> str:
    if type(value) is not str:
        raise CatalogError("protocol")
    if len(value) > _MAX_API_BASE_LENGTH:
        raise CatalogError("too_large")
    if not value:
        return value

    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise CatalogError("protocol") from None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or "?" in value
        or "#" in value
        or (port is not None and not 1 <= port <= 65535)
    ):
        raise CatalogError("protocol")
    return value


def validate_catalog_response(value: object) -> CatalogResult:
    """Validate a complete Control catalog response without truncation."""
    if type(value) is not dict or set(value) != {"status", "models"}:
        raise CatalogError("protocol")

    status = value["status"]
    models_value = value["models"]
    if type(status) is not str or status not in _CATALOG_STATUSES:
        raise CatalogError("protocol")
    models = _validate_models(status, models_value)

    result = CatalogResult(status=status, models=models)
    result_dict = result.to_dict()
    _validate_wire_result(result_dict)
    return result


def validate_login_model_snapshot(value: object) -> LoginModelSnapshot:
    """Validate a complete Control snapshot without truncation or token leakage."""
    if type(value) is not dict or set(value) != {
        "version",
        "status",
        "api_base",
        "models",
    }:
        raise CatalogError("protocol")

    version = value["version"]
    if type(version) is not int or version != 1:
        raise CatalogError("protocol")
    status = value["status"]
    if type(status) is not str or status not in _CATALOG_STATUSES:
        raise CatalogError("protocol")
    api_base = _validate_api_base(value["api_base"])
    models = _validate_models(status, value["models"])
    if status != "ok" and (api_base or models):
        raise CatalogError("protocol")
    if models and not api_base:
        raise CatalogError("protocol")

    result = LoginModelSnapshot(
        status=status,
        api_base=api_base,
        models=models,
        version=version,
    )
    _validate_wire_result(result.to_dict())
    return result
