# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Admission of inbound frames for the named users a static Worker serves.

The Worker host already filters every frame where it is received. A routed
Worker adds the checks that need its route tables, so a frame is refused before
any handler registers session or request state for it:

* the connection declared a user this Worker owns a target for;
* the frame agrees with that user (``params.user_id``) and names the builtin
  agent only;
* a session it names is not confirmed for another target of this Worker;
* its request id is not active or protected on any target;
* Web attachments are absent: each user has a private AgentServer directory,
  so the Gateway cannot import files on the user's behalf.

A refused frame is handled by the host's existing policy for unsupported
frames. Nothing of the frame is retained or logged.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from agent_gateway.application.worker_data import is_data_frame

_BUILTIN_AGENT_TYPE = "jiuwenswarm"
_ATTACHMENT_KEYS = ("files", "media_items", "attachments", "documents")
_SESSION_KEYS = ("session_id", "group_id")


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


class RoutedFrameGate:
    """One gate per Worker; each connection is bound to the user it declared."""

    def __init__(self, client: Any, *, base: Callable[..., bool] = is_data_frame) -> None:
        if getattr(client, "user_routing", False) is not True:
            raise ValueError("routed ingress requires a routed Worker client")
        if not callable(base):
            raise ValueError("routed ingress requires the Worker frame policy")
        self._client = client
        self._base = base

    @property
    def client(self) -> Any:
        return self._client

    def for_user(self, user_id: str | None) -> Callable[[str | bytes], bool]:
        """The frame gate of one Web socket; its user is fixed at the handshake."""
        def allowed(raw: str | bytes) -> bool:
            return self(raw, user_id=user_id)

        return allowed

    def for_protocol(self, protocol: Any) -> Callable[..., bool]:
        """The frame gate of one TUI/ACP socket, read per frame like the runtime does."""
        def allowed(raw: str | bytes, *, acp: bool = False, tui: bool = False) -> bool:
            return self(
                raw, acp=acp, tui=tui, user_id=getattr(protocol, "_gateway_user_id", None),
                path=getattr(protocol, "path", "") or "",
            )

        return allowed

    def with_policy(self, base: Callable[..., bool]) -> RoutedFrameGate:
        """The same route checks behind the frame policy of another listener."""
        return RoutedFrameGate(self._client, base=base)

    def __call__(
        self, raw: str | bytes, *, acp: bool = False, tui: bool = False, user_id: str | None = None, path: str = "",
    ) -> bool:
        try:
            if not self._base(raw, acp=acp, tui=tui):
                return False
            frame = json.loads(raw)
            if not isinstance(frame, dict):
                return False
            params = frame.get("params")
            if params is None:
                params = {}
            if not isinstance(params, dict):
                return False
            target = self._client.validate_bound_user(user_id)
            if frame.get("jsonrpc") == "2.0":
                return self._admit_acp(target, params)
            return self._admit_request(target, frame, params)
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
            return False

    def _admit_acp(self, target: Any, params: dict[str, Any]) -> bool:
        """An ACP session id is client-chosen; one id belongs to one target."""
        external = _text(params.get("sessionId"))
        if external:
            self._client.session_routes.claim_acp_session(target, external)
        return True

    def _admit_request(self, target: Any, frame: dict[str, Any], params: dict[str, Any]) -> bool:
        if frame.get("type") != "req":
            return False
        raw_id = frame.get("id")
        request_id = _text(raw_id)
        if not request_id or raw_id != request_id:
            return False
        context = frame.get("channel_context")
        if context is not None and not isinstance(context, dict):
            return False
        for mapping in (params, context):
            if mapping is not None and mapping.get("agent_type") not in (None, _BUILTIN_AGENT_TYPE):
                return False
        declared = params.get("user_id")
        if declared is not None and (not isinstance(declared, str) or _text(declared) not in ("", target.user_id)):
            return False
        if frame.get("method") == "chat.send" and any(params.get(key) for key in _ATTACHMENT_KEYS):
            return False
        for key in _SESSION_KEYS:
            session_id = params.get(key)
            if session_id is not None and not isinstance(session_id, str):
                return False
            self._client.validate_bound_session(target.user_id, session_id)
        self._client.validate_bound_request(target.user_id, request_id)
        # The user is fixed by the connection: every reply to this id, local
        # or forwarded, is delivered to that user only.
        self._client.remember_request(target.user_id, request_id)
        return True


__all__ = ["RoutedFrameGate"]
