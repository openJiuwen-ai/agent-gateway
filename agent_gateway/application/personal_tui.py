# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Compatibility requests for the local native TUI, without session binding."""

from __future__ import annotations

from collections.abc import Sequence
from collections.abc import Callable
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
_REWIND_METHODS = frozenset({
    ReqMethod.SESSION_REWIND, ReqMethod.SESSION_RESTORE_FILES,
    ReqMethod.SESSION_REWIND_AND_RESTORE, ReqMethod.SESSION_REWIND_COMPACT,
})
_IDLE_REQUIRED = _REWIND_METHODS | {ReqMethod.SESSION_REBIND_PROJECT}
_SESSION_WRITES = _IDLE_REQUIRED | {ReqMethod.SESSION_COLOR_SET, ReqMethod.SESSION_DELETE}
_METHOD_BY_NAME = {
    method.value: method
    for method in (_READ_METHODS | _SESSION_WRITES | {ReqMethod.COMMAND_ADD_DIR}) - {ReqMethod.SESSION_REWIND_COMPACT}
}
_METHOD_BY_NAME["command.rewind_compact"] = ReqMethod.SESSION_REWIND_COMPACT
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
PERSONAL_TUI_METHODS = frozenset(_METHOD_BY_NAME) | {"tui.disconnect"}


class PersonalTuiRequests:
    def __init__(
        self, runtime: Any, client: Any, web_session_busy: Callable[..., bool] | None = None,
    ) -> None:
        self.runtime = runtime
        self.client = client
        self.web_session_busy = web_session_busy

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
        req_method = _METHOD_BY_NAME.get(method)
        if req_method is None and method != "tui.disconnect":
            return False
        if not self._native_peer(ws):
            return await self._error(ws, req_id, "FORBIDDEN", "This method requires a native local TUI")
        if raw_params is not None and not isinstance(raw_params, dict):
            return await self._error(ws, req_id, "BAD_REQUEST", "params must be an object")
        params = dict(raw_params or {})
        params.pop("user_id", None)
        session_id = params.get("session_id")
        if (
            session_id is not None or req_method in _SESSION_WRITES
            or method in ("session.preview", "history.list_turns", "tui.disconnect")
        ):
            try:
                session_id = validate_session_id(session_id)
            except InvalidProtocolId as exc:
                return await self._error(ws, req_id, "BAD_REQUEST", str(exc))
        if method == "tui.disconnect":
            disconnect = self.runtime.config.routes["/tui"].local_handlers[method]
            await disconnect(ws, req_id, params, session_id)
            return True

        if req_method in _SESSION_WRITES or req_method == ReqMethod.COMMAND_ADD_DIR:
            return await self._write(ws, req_id, req_method, params, session_id, user_id, timeout_ms)

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

    async def _check_session(self, method, session_id, user_id, timeout_ms):
        ok, metadata = await self._fetch(
            ReqMethod.SESSION_GET_METADATA, {"session_id": session_id, "enable_writeback": False},
            session_id, user_id, timeout_ms,
        )
        if not ok:
            return metadata
        owner = metadata.get("user_id")
        if ("" if owner is None else owner) != (user_id or ""):
            return {"code": "SESSION_IDENTITY_REQUIRED", "error": "Session belongs to another user"}
        if method in _IDLE_REQUIRED:
            # The AS snapshot is scoped to the caller's app and agent. A Web
            # request in this same Worker can be busy under a different scope.
            if self.web_session_busy is not None and self.web_session_busy(session_id):
                return {"code": "SESSION_BUSY", "error": "Session is running"}
            snapshot = metadata.get("runtime")
            if isinstance(snapshot, dict) and not snapshot.get("recovery_pending"):
                processing = snapshot.get("is_processing")
                if processing is False:
                    return None
                if processing is True:
                    return {"code": "SESSION_BUSY", "error": "Session is running"}
            return {"code": "SERVICE_UNAVAILABLE", "error": "Session execution state is unavailable"}
        return None

    async def _write(self, ws, req_id, method, params, session_id, user_id, timeout_ms):
        if method == ReqMethod.COMMAND_ADD_DIR:
            path = params.get("path")
            if not isinstance(path, str) or not path.strip():
                return await self._error(ws, req_id, "BAD_REQUEST", "path is required")
            remember = params.get("remember", False)
            if not isinstance(remember, bool):
                return await self._error(ws, req_id, "BAD_REQUEST", "remember must be a boolean")
            params = {"path": path, "remember": remember}
        else:
            if method == ReqMethod.SESSION_DELETE:
                # The TUI deletes one checked session. The AS also accepts a
                # batch selector, which must not bypass that ownership check.
                if "session_ids" in params:
                    return await self._error(ws, req_id, "BAD_REQUEST", "Only one session may be deleted")
                params = {"session_id": session_id}
            elif method == ReqMethod.SESSION_COLOR_SET:
                params = {"session_id": session_id, "color": params.get("color")}
            elif method == ReqMethod.SESSION_REBIND_PROJECT:
                directory = params.get("project_dir")
                if not isinstance(directory, str) or not directory.strip():
                    return await self._error(ws, req_id, "BAD_REQUEST", "project_dir is required")
                params = {"session_id": session_id, "project_dir": directory}
            elif method in _REWIND_METHODS:
                turn_index = params.get("turn_index")
                if type(turn_index) is not int or turn_index < 1:
                    return await self._error(ws, req_id, "BAD_REQUEST", "turn_index must be a positive integer")
                direction = params.get("direction", "from")
                params = {"session_id": session_id, "turn_index": turn_index}
                if method == ReqMethod.SESSION_REWIND_COMPACT:
                    if direction not in ("from", "up_to"):
                        return await self._error(ws, req_id, "BAD_REQUEST", "direction must be 'from' or 'up_to'")
                    params["direction"] = direction
            problem = await self._check_session(method, session_id, user_id, timeout_ms)
            if problem is not None:
                return await self._error(ws, req_id, problem["code"], problem["error"])

        if method == ReqMethod.SESSION_REWIND_COMPACT:
            return await self._rewind_with_summary(ws, req_id, params, session_id, user_id, timeout_ms)
        ok, payload = await self._fetch(method, params, session_id, user_id, timeout_ms)
        if not ok:
            return await self._error(ws, req_id, payload["code"], payload["error"])
        await self.runtime.send_response(ws, req_id, ok=True, payload=payload)
        return True

    async def _rewind_with_summary(self, ws, req_id, params, session_id, user_id, timeout_ms):
        ok, summary_result = await self._fetch(
            ReqMethod.COMMAND_COMPACT_PARTIAL, params, session_id, user_id, timeout_ms,
        )
        if not ok:
            return await self._error(ws, req_id, summary_result["code"], summary_result["error"])
        summary = summary_result.get("summary")
        count = summary_result.get("summarized_count")
        if (
            summary_result.get("status") != "ok" or not isinstance(summary, str)
            or not summary.strip() or type(count) is not int or count < 1
        ):
            return await self._error(ws, req_id, "SUMMARY_FAILED", "Could not summarize the selected turns")
        problem = await self._check_session(ReqMethod.SESSION_REWIND_COMPACT, session_id, user_id, timeout_ms)
        if problem is not None:
            return await self._error(ws, req_id, problem["code"], problem["error"])
        ok, payload = await self._fetch(
            ReqMethod.SESSION_REWIND_COMPACT,
            {**params, "compact_summary": summary, "summarized_count": count},
            session_id, user_id, timeout_ms,
        )
        if not ok:
            return await self._error(ws, req_id, payload["code"], payload["error"])
        await self.runtime.send_response(
            ws, req_id, ok=True, payload={**payload, "summary": summary, "summarized_messages": count},
        )
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
