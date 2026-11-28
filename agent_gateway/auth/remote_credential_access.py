"""Worker-side ports with an explicit, endpoint-bound lifecycle mode."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from agent_gateway.auth.credential_access import (
    CredentialAccessError,
    CredentialRefresh,
    CredentialUpdate,
    ModelCredential,
    OwnsResolveDeadline,
)

if TYPE_CHECKING:
    from agent_gateway.auth.worker_credential_runtime import WorkerCredentialRuntime
from agent_gateway.auth.login_credentials import is_credential_ref
from agent_gateway.auth.session_login_bindings import SessionLoginBinding
from agent_gateway.processes.control_service import (
    ControlServiceClient,
    ControlServiceError,
    ControlServiceOperationError,
)


_REQUEST_TIMEOUT_S = 25.0
_BINDING_TIMEOUT_S = 5.0
_BINDING_AUTH_TIMEOUT_S = _REQUEST_TIMEOUT_S
_MAX_API_BASE_LENGTH = 2048
_MAX_API_KEY_LENGTH = 4096
_MAX_MODEL_NAME_LENGTH = 256
_MAX_SESSION_ID_LENGTH = 256


class _BindingNotFound(Exception):
    pass


@dataclass(frozen=True)
class _RemoteSessionLoginBinding(SessionLoginBinding):
    """A binding with the session identity needed for remote resolution."""

    session_id: str


def _validated_input(
    value: object, *, max_length: int, allow_empty: bool
) -> str | None:
    if type(value) is not str:
        return None
    if len(value) > max_length:
        return None
    normalized = value.strip()
    if not allow_empty and not normalized:
        return None
    return normalized


def _is_remote_credential_ref(value: object) -> bool:
    return type(value) is str and len(value) == 32 and is_credential_ref(value)


def _auth_session_value(auth_session: str | None) -> str | None:
    if auth_session is None:
        return None
    value = _validated_input(
        auth_session,
        max_length=_MAX_SESSION_ID_LENGTH,
        allow_empty=True,
    )
    if value is None:
        raise CredentialAccessError("invalid")
    if value == "":
        return None
    return value


async def _call_control(
    client: ControlServiceClient, operation: str, arguments: dict[str, Any], timeout: float
) -> dict[str, Any]:
    try:
        return await client.call(operation, arguments, timeout=timeout)
    except asyncio.CancelledError:
        raise
    except TimeoutError:
        raise CredentialAccessError("unavailable") from None
    except ControlServiceOperationError as error:
        reason = (
            "not_enabled"
            if error.error_code == "OPERATION_NOT_ENABLED"
            else "unavailable"
        )
        raise CredentialAccessError(reason) from None
    except ControlServiceError:
        raise CredentialAccessError("unavailable") from None
    except Exception:
        raise CredentialAccessError("unavailable") from None


def _credential(response: dict[str, Any]) -> ModelCredential:
    if not isinstance(response, dict):
        raise CredentialAccessError("protocol")
    if set(response) != {"status", "credential"} or response.get("status") != "ok":
        raise CredentialAccessError("protocol")
    credential = response.get("credential")
    if not isinstance(credential, dict) or set(credential) != {
        "api_base",
        "api_key",
        "credential_ref",
    }:
        raise CredentialAccessError("protocol")
    api_base = credential.get("api_base")
    api_key = credential.get("api_key")
    credential_ref = credential.get("credential_ref")
    if (
        type(api_base) is not str
        or type(api_key) is not str
        or type(credential_ref) is not str
        or len(api_base) > _MAX_API_BASE_LENGTH
        or len(api_key) > _MAX_API_KEY_LENGTH
        or not api_base.strip()
        or not api_key.strip()
        or not _is_remote_credential_ref(credential_ref)
    ):
        raise CredentialAccessError("protocol")
    return ModelCredential(
        api_base=api_base, api_key=api_key, credential_ref=credential_ref
    )


def _model_auth_response(
    response: dict[str, Any], *, allow_not_required: bool,
    client: ControlServiceClient | None = None,
    runtime: WorkerCredentialRuntime | None = None,
) -> ModelCredential | None:
    if not isinstance(response, dict):
        raise CredentialAccessError("protocol")
    status = response.get("status")
    if status == "invalid" and set(response) == {"status"}:
        raise CredentialAccessError("invalid")
    if (
        allow_not_required
        and status == "not_required"
        and set(response) == {"status"}
    ):
        return None
    if runtime is None:
        return _credential(response)
    if set(response) != {"status", "credential", "lease"} or client is None:
        raise CredentialAccessError("protocol")
    credential = _credential({"status": status, "credential": response["credential"]})
    lease = response["lease"]
    identity = client.identity
    if (
        not isinstance(lease, dict)
        or set(lease) != {"group_id", "slot_id", "generation", "server_id",
                          "credential_ref", "ref_revision"}
        or lease["group_id"] != identity.group_id
        or type(lease["slot_id"]) is not int
        or lease["slot_id"] != identity.slot_id
        or type(lease["generation"]) is not int
        or lease["generation"] != identity.generation
        or lease["server_id"] != runtime.server_id
        or lease["credential_ref"] != credential.credential_ref
        or type(lease["ref_revision"]) is not int
        or lease["ref_revision"] <= 0
    ):
        raise CredentialAccessError("protocol")
    return ModelCredential(
        api_base=credential.api_base, api_key=credential.api_key,
        credential_ref=credential.credential_ref,
        lease=runtime.issue(credential.credential_ref, lease["ref_revision"], lease["server_id"]),
    )


def _binding_response(response: dict[str, Any], session_id: str) -> _RemoteSessionLoginBinding:
    if not isinstance(response, dict):
        raise CredentialAccessError("protocol")
    status = response.get("status")
    if status == "not_found" and set(response) == {"status"}:
        raise _BindingNotFound()
    if set(response) != {"status", "binding"} or status != "ok":
        raise CredentialAccessError("protocol")
    binding = response.get("binding")
    if not isinstance(binding, dict) or set(binding) != {"credential_ref", "model_name"}:
        raise CredentialAccessError("protocol")
    credential_ref = binding.get("credential_ref")
    model_name = binding.get("model_name")
    if (
        type(credential_ref) is not str
        or type(model_name) is not str
        or len(model_name) > _MAX_MODEL_NAME_LENGTH
        or not model_name.strip()
        or not _is_remote_credential_ref(credential_ref)
    ):
        raise CredentialAccessError("protocol")
    return _RemoteSessionLoginBinding(
        credential_ref=credential_ref,
        model_name=model_name,
        session_id=session_id,
    )


class RemoteCredentialAccess:
    """Request-scoped credential access without Worker-side auth state."""

    def __init__(
        self, client: ControlServiceClient, *, runtime: WorkerCredentialRuntime | None = None,
    ) -> None:
        self._client = client
        self._runtime = runtime

    def release_request(self, envelope: Any) -> None:
        """Release an acquired lease if preprocessing aborts before MH owns it."""
        if self._runtime is not None:
            self._runtime.release(envelope)

    async def request_model_auth(
        self, auth_session: str | None, model_name: str
    ) -> ModelCredential | None:
        normalized_session = _auth_session_value(auth_session)
        normalized_model = _validated_input(
            model_name,
            max_length=_MAX_MODEL_NAME_LENGTH,
            allow_empty=True,
        )
        if normalized_model is None:
            raise CredentialAccessError("invalid")
        response = await _call_control(
            self._client,
            "credentials.request_model_auth",
            {"auth_session": normalized_session, "model_name": normalized_model},
            timeout=_REQUEST_TIMEOUT_S,
        )
        return _model_auth_response(response, allow_not_required=True,
                                    client=self._client, runtime=self._runtime)

    async def credential_update_for_ref(self, credential_ref: str) -> CredentialUpdate | None:
        raise CredentialAccessError("not_enabled")

    async def credential_refresh_for_ref(
        self, credential_ref: str, server_id: str,
    ) -> CredentialRefresh | None:
        if self._runtime is None:
            raise CredentialAccessError("not_enabled")
        if not _is_remote_credential_ref(credential_ref) or server_id != self._runtime.server_id:
            raise CredentialAccessError("invalid")
        response = await _call_control(
            self._client, "credentials.refresh_for_ref",
            {"credential_ref": credential_ref, "server_id": server_id},
            timeout=_REQUEST_TIMEOUT_S,
        )
        if response == {"status": "invalid"}:
            raise CredentialAccessError("invalid")
        if (
            set(response) != {"status", "operation_id", "server_id", "ref_revision", "update"}
            or response["status"] != "ok"
            or not _is_remote_credential_ref(response["operation_id"])
            or response["server_id"] != server_id
            or type(response["ref_revision"]) is not int
            or response["ref_revision"] <= 0
        ):
            raise CredentialAccessError("protocol")
        update = response["update"]
        if not isinstance(update, dict) or update.get("credential_ref") != credential_ref:
            raise CredentialAccessError("protocol")
        if set(update) == {"credential_ref", "revoked"} and update["revoked"] is True:
            value = CredentialUpdate(credential_ref, revoked=True)
        elif (
            set(update) == {"credential_ref", "api_key"}
            and type(update["api_key"]) is str
            and 0 < len(update["api_key"].strip()) <= _MAX_API_KEY_LENGTH
        ):
            value = CredentialUpdate(credential_ref, api_key=update["api_key"])
        else:
            raise CredentialAccessError("protocol")
        return CredentialRefresh(response["operation_id"], server_id,
                                 response["ref_revision"], value)

    async def credential_ref_for_login_session(self, auth_session: str | None) -> str | None:
        raise CredentialAccessError("not_enabled")

    async def model_auth_for_ref(self, credential_ref: str) -> ModelCredential | None:
        raise CredentialAccessError("not_enabled")


class RemoteSessionLoginBindings(OwnsResolveDeadline):
    """Request-scoped session binding access without Worker-side persistence."""

    resolve_deadline_s = _BINDING_AUTH_TIMEOUT_S

    def __init__(
        self, client: ControlServiceClient, *, runtime: WorkerCredentialRuntime | None = None,
    ) -> None:
        self._client = client
        self._runtime = runtime

    async def remember(
        self, session_id: str, credential_ref: str, model_name: str
    ) -> None:
        normalized_session = _validated_input(
            session_id,
            max_length=_MAX_SESSION_ID_LENGTH,
            allow_empty=False,
        )
        normalized_model = _validated_input(
            model_name,
            max_length=_MAX_MODEL_NAME_LENGTH,
            allow_empty=False,
        )
        if (
            normalized_session is None
            or normalized_model is None
            or type(credential_ref) is not str
            or not _is_remote_credential_ref(credential_ref)
        ):
            raise CredentialAccessError("invalid")
        response = await _call_control(
            self._client,
            "bindings.remember",
            {
                "session_id": normalized_session,
                "credential_ref": credential_ref,
                "model_name": normalized_model,
            },
            timeout=_BINDING_TIMEOUT_S,
        )
        if response != {"status": "ok"}:
            raise CredentialAccessError("protocol")

    async def lookup(self, session_id: str) -> SessionLoginBinding | None:
        normalized_session = _validated_input(
            session_id,
            max_length=_MAX_SESSION_ID_LENGTH,
            allow_empty=False,
        )
        if normalized_session is None:
            raise CredentialAccessError("invalid")
        response = await _call_control(
            self._client,
            "bindings.lookup",
            {"session_id": normalized_session},
            timeout=_BINDING_TIMEOUT_S,
        )
        try:
            return _binding_response(response, normalized_session)
        except _BindingNotFound:
            return None

    async def model_auth_for_binding(
        self, binding: SessionLoginBinding
    ) -> ModelCredential | None:
        if not isinstance(binding, _RemoteSessionLoginBinding):
            raise CredentialAccessError("invalid")
        session_id = _validated_input(
            binding.session_id,
            max_length=_MAX_SESSION_ID_LENGTH,
            allow_empty=False,
        )
        model_name = _validated_input(
            binding.model_name,
            max_length=_MAX_MODEL_NAME_LENGTH,
            allow_empty=False,
        )
        if (
            session_id is None
            or model_name is None
            or not _is_remote_credential_ref(binding.credential_ref)
        ):
            raise CredentialAccessError("invalid")
        response = await _call_control(
            self._client,
            "bindings.resolve_model_auth",
            {
                "session_id": session_id,
                "credential_ref": binding.credential_ref,
                "model_name": model_name,
            },
            timeout=_BINDING_AUTH_TIMEOUT_S,
        )
        return _model_auth_response(response, allow_not_required=False,
                                    client=self._client, runtime=self._runtime)
