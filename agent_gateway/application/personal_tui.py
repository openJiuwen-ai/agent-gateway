# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Compatibility requests for the local native TUI, without session binding."""

from __future__ import annotations

from collections.abc import Sequence
from ipaddress import ip_address
from typing import Any
from urllib.parse import urlsplit

from e2a.message import ReqMethod

from agent_gateway.common.agentos_runtime import is_agentos_runtime
from agent_gateway.common.ingress_identity import IngressIdentityError
from agent_gateway.common.protocol_ids import InvalidProtocolId, validate_session_id
from agent_gateway.enterprise import is_enterprise
from agent_gateway.routing import e2a_proxy
from agent_gateway.routing.agent_request_timeout import coerce_client_timeout_ms


_READ_METHODS = frozenset({
    ReqMethod.CONFIG_GET, ReqMethod.MODELS_LIST, ReqMethod.MEMORY_STATUS,
    ReqMethod.COMMAND_MODEL, ReqMethod.SESSION_PREVIEW, ReqMethod.HISTORY_LIST_TURNS,
    ReqMethod.COMMAND_STATUS, ReqMethod.COMMAND_CONTEXT, ReqMethod.COMMAND_SESSION,
    ReqMethod.COMMAND_DIFF, ReqMethod.COMMAND_BTW, ReqMethod.COMMAND_RECAP,
    ReqMethod.COMMAND_SIMPLIFY, ReqMethod.SKILLS_LIST, ReqMethod.COMMAND_MCP,
})
_READ_BY_NAME = {method.value: method for method in _READ_METHODS}
_CONTEXT = {"session_id", "cwd", "project_dir", "trusted_dirs", "user_id"}
_CONFIG_FIELDS = {
    "preferred_language", "skill_evolution", "auto_recap_enabled", "model",
    "model_provider", "app_version", "enable_free_models",
}
_MODEL_FIELDS = {
    "index", "name", "alias", "model_id", "model_name", "model_provider", "provider",
    "client_provider", "is_current", "is_agentos", "context_window_tokens", "reasoning_level",
}


def _loopback(host: Any) -> bool:
    try:
        return ip_address(host).is_loopback if isinstance(host, str) else False
    except ValueError:
        return False


def enable_personal_tui(agent_server_url: str) -> bool:
    """Offered only to the native TUI of a personal loopback deployment.

    The Worker calls this for its account-free profile. The public launcher
    already confines the listeners to loopback; the AgentServer must be there
    too, because these requests are answered from its user directory without a
    Gateway-side session binding.
    """
    if is_enterprise() or is_agentos_runtime():
        return False
    host = urlsplit(agent_server_url or "").hostname
    return host == "localhost" or _loopback(host)



def _handshake_headers(ws: Any) -> tuple[tuple[bytes, bytes], ...]:
    """Raw handshake pairs of an adopted protocol socket; unreadable means foreign."""
    headers = (
        getattr(getattr(ws, "request", None), "headers", None)
        or getattr(ws, "request_headers", None)
    )
    raw_items = getattr(headers, "raw_items", None)
    if callable(raw_items):
        pairs = raw_items()
    else:
        raw = getattr(headers, "raw", None)
        if not isinstance(raw, Sequence):
            raise IngressIdentityError()
        pairs = raw

    encoded: list[tuple[bytes, bytes]] = []
    try:
        for name, value in pairs:
            encoded_name = name if isinstance(name, bytes) else str(name).encode("ascii")
            encoded_value = value if isinstance(value, bytes) else str(value).encode("latin1")
            encoded.append((encoded_name, encoded_value))
    except (UnicodeError, TypeError, ValueError):
        raise IngressIdentityError() from None
    return tuple(encoded)


def _scalar_fields(payload: dict[str, Any], fields: set[str]) -> dict[str, Any]:
    return {key: value for key, value in payload.items()
            if key in fields and type(value) in (str, bool, int, float)}


def _project_read(method: ReqMethod, payload: dict[str, Any], key: str | None) -> dict[str, Any]:
    if method == ReqMethod.CONFIG_GET:
        return _scalar_fields(payload, _CONFIG_FIELDS if key is None else {key})
    if method not in (ReqMethod.MODELS_LIST, ReqMethod.COMMAND_MODEL):
        return payload
    result = _scalar_fields(payload, {"active_model", "current", "current_model_name"})
    models = payload.get("models")
    if isinstance(models, list):
        result["models"] = [_scalar_fields(item, _MODEL_FIELDS) for item in models if isinstance(item, dict)]
    available = payload.get("available_models")
    if isinstance(available, list):
        result["available_models"] = [item for item in available if isinstance(item, str)]
    return result


#: Frame names the personal Worker admits on /tui for the compatibility hook.
PERSONAL_TUI_METHODS = frozenset(method.value for method in _READ_METHODS) | {"tui.disconnect"}


class PersonalTuiRequests:
    def __init__(self, runtime: Any, client: Any) -> None:
        self.runtime = runtime
        self.client = client

    def _native_peer(self, ws: Any) -> bool:
        remote = getattr(ws, "remote_address", None)
        if not isinstance(remote, tuple) or not remote or not _loopback(remote[0]):
            return False
        try:
            return all(name.lower() != b"origin" for name, _ in _handshake_headers(ws))
        except IngressIdentityError:
            return False

    async def _error(self, ws: Any, req_id: str, code: str, message: str) -> bool:
        await self.runtime.send_response(ws, req_id, ok=False, code=code, error=message)
        return True

    async def handle(
        self, ws: Any, req_id: str, method: str, raw_params: Any,
        timeout_ms: Any, user_id: str | None,
    ) -> bool:
        req_method = _READ_BY_NAME.get(method)
        if req_method is None and method != "tui.disconnect":
            return False
        if not self._native_peer(ws):
            return await self._error(ws, req_id, "FORBIDDEN", "This method requires a native local TUI")
        if raw_params is not None and not isinstance(raw_params, dict):
            return await self._error(ws, req_id, "BAD_REQUEST", "params must be an object")
        params = dict(raw_params or {})
        params.pop("user_id", None)
        session_id = params.get("session_id")
        if session_id is not None or method in ("session.preview", "history.list_turns", "tui.disconnect"):
            try:
                session_id = validate_session_id(session_id)
            except InvalidProtocolId as exc:
                return await self._error(ws, req_id, "BAD_REQUEST", str(exc))
        if method == "tui.disconnect":
            disconnect = self.runtime.config.routes["/tui"].local_handlers[method]
            await disconnect(ws, req_id, params, session_id)
            return True

        key = None
        if req_method in (ReqMethod.CONFIG_GET, ReqMethod.COMMAND_MODEL, ReqMethod.MODELS_LIST):
            args = {name: value for name, value in params.items() if name not in _CONTEXT}
            if req_method == ReqMethod.CONFIG_GET:
                key = args.pop("key", None)
            if args or (key is not None and (not isinstance(key, str) or key not in _CONFIG_FIELDS)):
                return await self._error(ws, req_id, "UNSUPPORTED_REQUEST", "Only display queries are supported")
            params = {}
        if req_method == ReqMethod.COMMAND_MCP:
            action = params.get("action", "list")
            if not isinstance(action, str) or action.strip().lower() not in {"list", "show", "list_tools"}:
                return await self._error(ws, req_id, "UNSUPPORTED_REQUEST", "MCP changes are not supported")
            if not isinstance(params.get("name", ""), str):
                return await self._error(ws, req_id, "BAD_REQUEST", "name must be a string")
            params = {"action": action.strip().lower(), "name": params.get("name", "")}

        ok, payload = await self._fetch(req_method, params, session_id, user_id, timeout_ms)
        if not ok:
            if req_method in (ReqMethod.CONFIG_GET, ReqMethod.MODELS_LIST, ReqMethod.COMMAND_MODEL):
                return await self._error(ws, req_id, payload["code"], "Could not read display configuration")
            return await self._error(ws, req_id, payload["code"], payload["error"])
        await self.runtime.send_response(ws, req_id, ok=True, payload=_project_read(req_method, payload, key))
        return True

    async def _fetch(self, method, params, session_id, user_id, timeout_ms):
        metadata = {}
        timeout = coerce_client_timeout_ms(timeout_ms)
        if timeout is not None:
            metadata["client_timeout_ms"] = min(timeout, 60_000)
        payload_ok, payload = await e2a_proxy.fetch_agent_unary(
            agent_client=self.client, req_method=method, params=params,
            session_id=session_id, user_id=user_id, channel_id="tui", metadata=metadata,
        )
        if not isinstance(payload, dict):
            return False, {"code": "SERVICE_UNAVAILABLE", "error": "Invalid AgentServer response"}
        return payload_ok, payload
