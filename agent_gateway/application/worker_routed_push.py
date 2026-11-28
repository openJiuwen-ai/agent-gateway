# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Bound a pushed frame to the request or session that established its route.

Every AgentServer target shares the Worker's downstream consumers, which route
by request and session id. A frame is handed over only when the sending target
proved the association: it answered the request, or confirmed the session.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable

from e2a.message import PROMPT_SUGGESTION_READY_EVENT, SESSION_REQUEST_COMPLETED_EVENT
from e2a.wire_codec import parse_agent_server_wire_chunk

from agent_gateway.common.session_message import (
    SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY,
    SESSION_MESSAGE_ORIGIN,
    SESSION_MESSAGE_OWNER_SCOPE_METADATA_KEY,
)
from agent_gateway.common.southbound_routes import SouthboundTarget

logger = logging.getLogger(__name__)

_WEB_SESSION_EVENTS = frozenset({"trace.updated", "trajectory.diagnosis.progress", "trajectory.diagnosis.status"})
_SESSION_EVENTS = _WEB_SESSION_EVENTS | frozenset({
    "context.compression_state", "context.usage", "plan.mode_exited", "session.message.updated",
})
_REQUEST_EVENTS = frozenset({
    SESSION_REQUEST_COMPLETED_EVENT, PROMPT_SUGGESTION_READY_EVENT,
    "acp.output_request", "plan.approval_required",
})
_CHANNELS = frozenset({"web", "tui", "acp"})


def _key(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


@dataclass(frozen=True)
class RoutedRequestKey:
    target_id: str
    generation: int | None
    request_id: str

    @classmethod
    def for_target(cls, target: Any, request_id: str | None) -> RoutedRequestKey:
        return cls(target.target_id, getattr(target, "generation", None), _key(request_id))


class RoutedPush(dict):
    """An admitted wire frame with its connection-derived correlation key."""

    request_key: RoutedRequestKey | None = None
    target: SouthboundTarget | None = None


class RoutedRequestBusy(ValueError):
    """Request admission could not reserve space for safe completion."""


@dataclass
class _RequestOwner:
    channel: str
    session_id: str
    sent: bool = False
    sending: bool = False
    chat_stream: bool = False
    stream_completed: bool = False
    host_completed: bool = False


class RoutedPushGuard:
    """Keep bounded live requests, protect completed IDs and admit scoped pushes.

    Request ids are unique across every target of one Worker: shared consumers
    index their state by request id alone, so an id that is active or recently
    retired on any target is refused instead of being re-associated.
    """

    _PROTECTION_SECONDS = 300.0

    def __init__(
        self, *, session_routes: Any, max_requests: int = 4096, max_recent: int = 16384,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if type(max_requests) is not int or max_requests < 1:
            raise ValueError("routed request capacity must be a positive integer")
        if type(max_recent) is not int or max_recent < max_requests:
            raise ValueError("routed recent capacity must cover the active requests")
        self._max_requests = max_requests
        self._max_recent = max_recent
        self._clock = clock
        self._active: dict[RoutedRequestKey, _RequestOwner] = {}
        self._recent: OrderedDict[RoutedRequestKey, tuple[_RequestOwner, float]] = OrderedDict()
        self._used_ids: dict[str, RoutedRequestKey] = {}
        self._expiry_handle: asyncio.TimerHandle | None = None
        self._capacity_rejections = 0
        self.session_routes = session_routes

    def counts(self) -> tuple[int, int, int]:
        self.expire()
        return len(self._active), len(self._recent), self._capacity_rejections

    def owner_of(self, target: Any, request_id: str | None) -> _RequestOwner | None:
        """Return the owner of a dispatched request, active or recently retired."""
        self.expire()
        key = RoutedRequestKey.for_target(target, request_id)
        owner = self._active.get(key)
        if owner is not None:
            return owner if owner.sent else None
        record = self._recent.get(key)
        return record[0] if record is not None else None

    def begin(self, target: Any, envelope: Any) -> RoutedRequestKey:
        """Register an outbound request once its session route was verified."""
        rid = _key(envelope.request_id)
        if not rid:
            raise ValueError("routed request requires a request ID")
        key = self.check_request(target, rid)
        if len(self._active) >= self._max_requests:
            self._capacity_rejections += 1
            raise RoutedRequestBusy("routed active request capacity exceeded")
        # Reserve room for each admitted request's eventual tombstone.
        if len(self._recent) + len(self._active) >= self._max_recent:
            self._capacity_rejections += 1
            raise RoutedRequestBusy("routed recent request capacity exceeded")
        sid = _key(envelope.params.get("session_id") or envelope.session_id)
        # Verification precedes registration; refuse a session another target owns.
        self.session_routes.check(target, sid)
        owner = _RequestOwner(_key(envelope.channel), sid)
        owner.sending = True
        owner.chat_stream = envelope.method == "chat.send" and bool(envelope.is_stream)
        self._active[key] = owner
        self._used_ids[rid] = key
        return key

    def check_request(self, target: Any, request_id: str | None) -> RoutedRequestKey:
        """Return the key an outbound request would take, or refuse a used id.

        Ingress calls this before a frame can register any shared downstream
        state, so one user's frame never replaces another user's route.
        """
        rid = _key(request_id)
        if not rid:
            raise ValueError("routed request requires a request ID")
        self.expire()
        if rid in self._used_ids:
            raise ValueError("routed request requires an unused request ID")
        return RoutedRequestKey.for_target(target, rid)

    def mark_dispatched(self, key: RoutedRequestKey) -> None:
        """The envelope was handed to the target transport."""
        self._active[key].sent = True

    def mark_stream_completed(self, key: RoutedRequestKey) -> None:
        """The host observed the stream's own terminal chunk."""
        owner = self._active.get(key)
        if owner is not None:
            owner.stream_completed = True
            owner.host_completed = True

    def mark_host_completed(self, key: RoutedRequestKey) -> None:
        owner = self._active.get(key) or (self._recent.get(key) or (None,))[0]
        if owner is not None:
            owner.host_completed = True

    def finish(self, key: RoutedRequestKey) -> None:
        """Release the request slot; keep a sent request protected for a while."""
        owner = self._active.get(key)
        if owner is not None:
            owner.sending = False
            self._settle(key, owner)

    def _settle(self, key: RoutedRequestKey, owner: _RequestOwner) -> None:
        if owner.sending:
            return
        self._active.pop(key, None)
        if owner.sent:
            self._recent[key] = (owner, self._clock() + self._PROTECTION_SECONDS)
            self._schedule_expiry()
        else:
            self._used_ids.pop(key.request_id, None)

    def _schedule_expiry(self) -> None:
        if self._expiry_handle is None and self._recent:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
            deadline = next(iter(self._recent.values()))[1]
            self._expiry_handle = loop.call_later(max(0.0, deadline - self._clock()), self._expire_timer)

    def _expire_timer(self) -> None:
        self._expiry_handle = None
        self.expire()
        self._schedule_expiry()

    def expire(self) -> None:
        """Drop tombstones whose protection elapsed."""
        now = self._clock()
        while self._recent:
            key = next(iter(self._recent))
            _owner, deadline = self._recent[key]
            if deadline > now:
                break
            self._recent.pop(key)
            self._used_ids.pop(key.request_id, None)
        if not self._recent and self._expiry_handle is not None:
            self._expiry_handle.cancel()
            self._expiry_handle = None

    def observe_response(self, target: Any, envelope: Any, response: Any) -> None:
        """Record the session an AgentServer confirmed for the target."""
        method = str(envelope.method)
        if not getattr(response, "ok", False) or method not in {
            "session.create", "session.fork", "session.get_metadata",
        }:
            return
        payload = getattr(response, "payload", None)
        if not isinstance(payload, dict) or not payload:
            return
        result = payload.get("result", payload)
        if not isinstance(result, dict) or not result:
            return
        if method == "session.get_metadata":
            sid = _key(envelope.params.get("session_id") or envelope.session_id)
            returned_id = _key(result.get("session_id") or result.get("sessionId"))
            if returned_id and returned_id != sid:
                return
        else:
            sid = _key(result.get("session_id") or result.get("sessionId"))
        if sid:
            self.session_routes.claim(target, sid)

    def admit(self, target: Any, wire: dict[str, Any]) -> RoutedPush | None:
        """Validate the trusted source and every routing selector a consumer reads."""
        return self._admit(target, wire, allow_cron=False)

    def admit_cron(self, target: Any, wire: dict[str, Any]) -> RoutedPush | None:
        """Admit only a Cron tool response, for its source target's owner."""
        return self._admit(target, wire, allow_cron=True)

    def _approval_prompt(self, target: Any, sid: str) -> RoutedRequestKey | None:
        """An approval push has its own wire id: require one unambiguous live prompt."""
        def belongs(key: RoutedRequestKey, owner: _RequestOwner) -> bool:
            return (
                owner.chat_stream and owner.channel == "acp" and owner.session_id == sid
                and RoutedRequestKey.for_target(target, key.request_id) == key
            )

        candidate = None
        for key, owner in self._active.items():
            if not owner.sent or not belongs(key, owner) or owner.host_completed:
                continue
            if not owner.sending or candidate is not None:
                return None
            candidate = key
        if candidate is None:
            return None
        for key, (owner, _deadline) in self._recent.items():
            if not (owner.host_completed or owner.stream_completed) and belongs(key, owner):
                return None
        return candidate

    def _names_foreign_request(self, target: Any, request_id: Any) -> bool:
        """Whether an id is active or protected on another target.

        Shared consumers index state by request id alone. A frame that names
        another user's request must not reach them, whatever its event type.
        """
        rid = _key(request_id)
        used = self._used_ids.get(rid) if rid else None
        return used is not None and used != RoutedRequestKey.for_target(target, rid)

    def _session_belongs_to(self, target: Any, sid: str) -> bool:
        from agent_gateway.application.worker_session_routes import SessionRouteError

        try:
            return self.session_routes.check(target, sid)
        except SessionRouteError:
            logger.debug("routed session source rejected for target=%s", target.target_id)
            return False

    def _admit(self, target: Any, wire: dict[str, Any], *, allow_cron: bool) -> RoutedPush | None:
        if not isinstance(wire, dict):
            return None
        if wire.get("metadata") is not None and not isinstance(wire["metadata"], dict):
            return None
        try:
            chunk = parse_agent_server_wire_chunk(wire)
        except Exception:
            return None
        payload = chunk.payload if isinstance(chunk.payload, dict) else {}
        event = _key(payload.get("event_type"))
        if allow_cron and event != "cron.response":
            return None
        # Downstream channels broadcast these shapes globally. They are outside
        # the named-user data-only scope of a static Worker.
        if payload.get("source") == "proactive_notification" or isinstance(payload.get("cron"), dict):
            return None
        rid = _key(chunk.request_id)
        channel = _key(chunk.channel_id)
        key = RoutedRequestKey.for_target(target, rid)
        owner = self.owner_of(target, rid)
        turn_id = _key(payload.get("turn_request_id"))
        if self._names_foreign_request(target, rid) or self._names_foreign_request(target, turn_id):
            return None
        if turn_id:
            # A turn-scoped frame belongs to the request that owns the turn.
            parent_key = RoutedRequestKey.for_target(target, turn_id)
            parent = self.owner_of(target, turn_id)
            if parent is not None:
                if owner is not None and owner is not parent:
                    return None
                owner, key = parent, parent_key
        if wire.get("user_id") is not None and wire["user_id"] != target.user_id:
            return None
        sessions = {_key(value) for value in (wire.get("session_id"), payload.get("session_id")) if _key(value)}
        if len(sessions) > 1:
            return None
        sid = next(iter(sessions), "")
        if event == "acp.output_request" and owner is None and channel == "acp" and sid:
            prompt_key = self._approval_prompt(target, sid)
            if prompt_key is None:
                return None
            key, owner = prompt_key, self._active[prompt_key]
        if event in _SESSION_EVENTS:
            # Session-scoped notifications carry no request: the sending target
            # must have confirmed the session itself.
            if owner is not None and owner.session_id != sid:
                return None
            if not sid or not self._session_belongs_to(target, sid):
                return None
            if channel not in _CHANNELS or (event in _WEB_SESSION_EVENTS and channel != "web"):
                return None
        else:
            supported = (
                (allow_cron and event == "cron.response") or event in _REQUEST_EVENTS
                or event.startswith(("chat.", "evolution.")) or (not event and chunk.is_complete)
            )
            if owner is None and turn_id and payload.get("message_origin") == SESSION_MESSAGE_ORIGIN:
                # An AgentServer-owned background turn is not a Gateway RPC. Its
                # explicit marker is admitted only for a session this target confirmed.
                if not sid or not self._session_belongs_to(target, sid) or channel not in _CHANNELS:
                    return None
                owner = _RequestOwner(channel, sid)
            elif owner is None and allow_cron and event == "cron.response" and sid and self._session_belongs_to(target, sid):
                # A Cron tool answer may follow its chat request after the host
                # retired it; the sending target must still hold the session.
                owner = _RequestOwner(channel, sid)
            # The host completion of a chat request may omit the channel id.
            host_completion = event == SESSION_REQUEST_COMPLETED_EVENT and not channel
            # A Cron answer reports the channel its AgentServer recorded; only
            # the request or the confirmed session names its route.
            cron_answer = allow_cron and event == "cron.response"
            if not supported or owner is None or (channel != owner.channel and not host_completion and not cron_answer):
                return None
            if sid and sid != owner.session_id:
                return None
            sid = sid or owner.session_id
            if not sid:
                return None
        if sid and not self._session_belongs_to(target, sid):
            return None
        # Typed E2A frames may carry a legacy chunk whose metadata the codec
        # merges into the Message as well as the outer wire metadata.
        for metadata in (wire.get("metadata"), chunk.metadata):
            if not isinstance(metadata, dict):
                continue
            if allow_cron:
                cron_owner = metadata.get("_jiuwenswarm_cron_owner_user_id")
                if cron_owner is not None and cron_owner != target.user_id:
                    return None
            if metadata.get("fan_out_targets"):
                return None
            scope = metadata.get(SESSION_MESSAGE_OWNER_SCOPE_METADATA_KEY)
            if scope is not None and scope != target.user_id:
                return None
            anonymous = metadata.get(SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY)
            if anonymous is not None and anonymous is not False:
                return None
            alias = _key(metadata.get("acp_original_session_id"))
            if alias and self.session_routes.acp_alias(target.target_id, alias) != sid:
                return None
        payload_rid = _key(payload.get("request_id"))
        if payload_rid and payload_rid not in {rid, turn_id}:
            return None
        if event == "acp.output_request":
            rpc = payload.get("jsonrpc")
            if channel != "acp" or not isinstance(rpc, dict):
                return None
            raw_id = rpc.get("id")
            rpc_id = str(raw_id).strip() if type(raw_id) in (str, int) else ""
            if not rpc_id or not isinstance(rpc.get("method"), str):
                return None
            params = rpc.get("params")
            if isinstance(params, dict) and "sessionId" in params and params["sessionId"] != sid:
                return None
        result = RoutedPush(wire)
        result.target = target
        # Only a dispatched request can lend its key to a session-scoped frame.
        result.request_key = key if owner is not None and owner.sent else None
        # The owner comes from the connection, never from a wire field a target
        # could leave empty. Consumers route by these two values.
        result["user_id"] = target.user_id
        result["session_id"] = sid
        if allow_cron:
            metadata = dict(chunk.metadata or {})
            metadata.update(wire.get("metadata") or {})
            metadata["_jiuwenswarm_cron_owner_user_id"] = target.user_id
            result["metadata"] = metadata
        return result

    def clear(self) -> None:
        if self._expiry_handle is not None:
            self._expiry_handle.cancel()
            self._expiry_handle = None
        self._recent.clear()
        self._active.clear()
        self._used_ids.clear()
