# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Bind a pushed frame to the request or session that proved its generation.

Every generation of one Worker shares the downstream consumers. They route by
request and session id alone, and several of them branch on fields the sending
target controls, a few by broadcasting to every client. A frame is handed over
only when the sending generation proved the association: the Worker dispatched
that request on it, a request on it owns the frame's turn, or a session this
generation holds is the frame's scope. Anything else is dropped.

These are the rules of the static routed guard, keyed by physical generation.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable
import time
from typing import Any, Callable

from e2a.message import PROMPT_SUGGESTION_READY_EVENT, SESSION_REQUEST_COMPLETED_EVENT
from e2a.models import E2AEnvelope
from e2a.wire_codec import parse_agent_server_wire_chunk

from agent_gateway.common.session_message import (
    SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY,
    SESSION_MESSAGE_ORIGIN,
    SESSION_MESSAGE_OWNER_SCOPE_METADATA_KEY,
)
from agent_gateway.southbound.orchestration.instance_provider import InstanceTarget, OwnedRoute

MAX_ACTIVE_REQUESTS = 4096
MAX_PROTECTED_REQUESTS = 16384
MAX_SESSIONS = 4096
MAX_ACP_INTERACTIONS = 4096
# A completed request still owns its id: its completion push may follow the response.
PROTECTION_SECONDS = 300.0

_WEB_SESSION_EVENTS = frozenset({"trace.updated", "trajectory.diagnosis.progress", "trajectory.diagnosis.status"})
# Session-scoped notifications name no Gateway request.
_SESSION_EVENTS = _WEB_SESSION_EVENTS | frozenset({
    "context.compression_state", "context.usage", "plan.mode_exited", "session.message.updated",
})
_REQUEST_EVENTS = frozenset({
    SESSION_REQUEST_COMPLETED_EVENT, PROMPT_SUGGESTION_READY_EVENT,
    "acp.output_request", "plan.approval_required",
})
_CHANNELS = frozenset({"web", "tui", "acp"})


def _key(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ''


class _Request:
    """What a dispatched request fixed: its generation, session and channel."""

    __slots__ = ('target_id', 'generation', 'session_id', 'channel', 'dispatched')

    def __init__(
        self, target: InstanceTarget, session_id: str, channel: str, *, dispatched: bool = False,
    ) -> None:
        self.target_id = target.target_id
        self.generation = target.generation
        self.session_id = session_id
        self.channel = channel
        self.dispatched = dispatched

    @property
    def generation_key(self) -> tuple[str, str]:
        return self.target_id, self.generation


class InstancePushGuard:
    """Keep bounded live requests and admit only pushes this generation earned."""

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._requests: dict[str, _Request] = {}
        # Finished requests and the ids a frame associated with them.
        self._retired: OrderedDict[str, tuple[_Request, float]] = OrderedDict()
        self._sessions: dict[str, tuple[str, str | None]] = {}
        self._acp_interactions: dict[str, tuple[str, str, str]] = {}

    @property
    def counts(self) -> tuple[int, int, int]:
        self._expire()
        return len(self._requests), len(self._retired), len(self._sessions)

    def _expire(self) -> None:
        now = self._clock()
        while self._retired:
            _request, deadline = next(iter(self._retired.values()))
            if deadline > now:
                break
            self._retired.popitem(last=False)

    def _reserve(self, request_id: Any) -> str:
        """Request ids are unique across every route: consumers index by id alone."""
        rid = _key(request_id)
        if not rid:
            raise ValueError('instance request requires a request ID')
        self._expire()
        if rid in self._requests or rid in self._retired:
            raise ValueError('instance request requires an unused request ID')
        return rid

    def reserve_request(self, route: OwnedRoute, request_id: Any) -> None:
        """Refuse a used id before shared downstream state registers the frame."""
        self._reserve(request_id)

    def claim_session(self, route: OwnedRoute, session_id: Any) -> None:
        """Reserve a session for one route before its generation is known."""
        sid = _key(session_id)
        if not sid:
            return
        owner = self._sessions.get(sid)
        if owner is None:
            self._new_session(sid, route.target_id, None)
        elif owner[0] != route.target_id:
            raise ValueError('session is owned by another instance route')

    def _new_session(self, sid: str, target_id: str, generation: str | None) -> None:
        if len(self._sessions) >= MAX_SESSIONS:
            raise RuntimeError('instance session association capacity exceeded')
        self._sessions[sid] = (target_id, generation)

    def session_target_id(self, session_id: str | None) -> str | None:
        owner = self._sessions.get(_key(session_id))
        return owner[0] if owner is not None else None

    def pin(self, target: InstanceTarget, session_id: Any) -> None:
        """A session belongs to the physical generation that first served it.

        The route is fixed by whoever reserved the session first, or by the
        generation that first named it; a later generation may not take it.
        """
        sid = _key(session_id)
        if not sid:
            return
        owner = self._sessions.get(sid)
        if owner is None:
            self._new_session(sid, target.target_id, target.generation)
        elif owner[1] is None:
            # Reserved by ingress admission: only that route may serve it.
            if owner[0] != target.target_id:
                raise ValueError('session is owned by another instance route')
            self._sessions[sid] = (target.target_id, target.generation)
        elif owner != (target.target_id, target.generation):
            if owner[0] != target.target_id:
                raise ValueError('session is owned by another instance route')
            raise ValueError('session is pinned to another instance generation')

    def begin(self, target: InstanceTarget, envelope: E2AEnvelope) -> str:
        """Register one outbound request of a leased generation."""
        rid = self._reserve(envelope.request_id)
        if (
            len(self._requests) >= MAX_ACTIVE_REQUESTS
            or len(self._requests) + len(self._retired) >= MAX_PROTECTED_REQUESTS
        ):
            raise RuntimeError('instance request capacity exceeded')
        sid = _key(envelope.params.get('session_id') or envelope.session_id)
        self.pin(target, sid)
        self._requests[rid] = _Request(target, sid, _key(envelope.channel))
        return rid

    def dispatched(self, rid: str) -> None:
        """The request was handed to its transport; its frames may now arrive."""
        request = self._requests.get(rid)
        if request is not None:
            request.dispatched = True

    def finish(self, rid: str) -> None:
        """Release the request slot; a dispatched request keeps its id protected."""
        request = self._requests.pop(rid, None)
        if request is not None and request.dispatched:
            # Associations are bounded: the oldest is dropped before this one.
            while len(self._retired) >= MAX_PROTECTED_REQUESTS:
                self._retired.popitem(last=False)
            self._retired[rid] = (request, self._clock() + PROTECTION_SECONDS)

    def _lookup(self, rid: str) -> _Request | None:
        """The dispatched request an id names on any route, or None."""
        if not rid:
            return None
        self._expire()
        request = self._requests.get(rid)
        if request is None:
            record = self._retired.get(rid)
            request = record[0] if record is not None else None
        if request is None or not request.dispatched:
            return None
        return request

    def _remember(self, ids: Iterable[str], request: _Request) -> bool:
        """Let later frames of an interaction find the request that owns them.

        An association that cannot be kept, or that an earlier frame of another
        generation already made, is not made at all.
        """
        ids = tuple(ids)
        fresh = sum(1 for rid in ids if rid and rid not in self._requests and rid not in self._retired)
        if len(self._retired) + fresh > MAX_PROTECTED_REQUESTS:
            return False
        for rid in ids:
            if not rid or rid in self._requests:
                continue
            record = self._retired.get(rid)
            if record is not None and record[0].generation_key != request.generation_key:
                return False
        for rid in ids:
            if not rid or rid in self._requests:
                continue
            self._retired[rid] = (request, self._clock() + PROTECTION_SECONDS)
            self._retired.move_to_end(rid)
        return True

    def admit(self, target: InstanceTarget, wire: Any) -> dict[str, Any] | None:
        """Return the frame with its route filled in from the connection, or drop it."""
        if type(wire) is not dict:
            return None
        if wire.get('metadata') is not None and not isinstance(wire['metadata'], dict):
            return None
        try:
            chunk = parse_agent_server_wire_chunk(wire)
        except Exception:
            return None
        payload = chunk.payload if isinstance(chunk.payload, dict) else {}
        event = _key(payload.get('event_type'))
        # Consumers broadcast these shapes without looking at the route.
        if payload.get('source') == 'proactive_notification' or isinstance(payload.get('cron'), dict):
            return None
        rid = _key(chunk.request_id)
        channel = _key(chunk.channel_id)
        request = self._lookup(rid)
        turn = _key(payload.get('turn_request_id'))
        if turn:
            # A turn-scoped frame belongs to the request that owns the turn.
            parent = self._lookup(turn)
            if parent is not None:
                if request is not None and request is not parent:
                    return None
                request = parent
        if request is not None and request.generation_key != (target.target_id, target.generation):
            return None
        if wire.get('user_id') is not None and wire['user_id'] != target.user_id:
            return None

        def held(session_id: str) -> bool:
            return self._sessions.get(session_id) == (target.target_id, target.generation)

        named = {_key(value) for value in (wire.get('session_id'), payload.get('session_id')) if _key(value)}
        if len(named) > 1:
            return None
        sid = next(iter(named), '')
        if event in _SESSION_EVENTS:
            if request is not None and request.session_id != sid:
                return None
            # Only a session this generation served itself lends its route.
            if not sid or not held(sid):
                return None
            if channel not in _CHANNELS or (event in _WEB_SESSION_EVENTS and channel != 'web'):
                return None
        else:
            supported = (
                event in _REQUEST_EVENTS
                or event.startswith(('chat.', 'evolution.'))
                or (not event and chunk.is_complete)
            )
            if request is None and turn and payload.get('message_origin') == SESSION_MESSAGE_ORIGIN:
                # An AgentServer-owned background turn is not a Gateway RPC. Its
                # marker lends a route only to a session this generation holds.
                if not sid or not held(sid) or channel not in _CHANNELS:
                    return None
                request = _Request(target, sid, channel, dispatched=True)
            # The host completion of a chat request is the one frame without a channel.
            host_completion = not channel and event == SESSION_REQUEST_COMPLETED_EVENT
            if not supported or request is None:
                return None
            if channel != request.channel and not host_completion:
                return None
            if sid and sid != request.session_id:
                return None
            sid = sid or request.session_id
            if not sid:
                return None
        if sid and not held(sid):
            return None
        # Typed E2A frames may carry a legacy chunk whose metadata the codec
        # merges into the Message as well as the outer wire metadata.
        for scope in (wire.get('metadata'), chunk.metadata):
            if not isinstance(scope, dict):
                continue
            if scope.get('fan_out_targets'):
                return None
            if scope.get(SESSION_MESSAGE_OWNER_SCOPE_METADATA_KEY) not in (None, target.user_id):
                return None
            if scope.get(SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY) not in (None, False):
                return None
            # The ACP channel re-addresses a frame through this alias.
            raw_alias = scope.get('acp_original_session_id')
            if raw_alias is not None and not isinstance(raw_alias, str):
                return None
            if _key(raw_alias) and not held(_key(raw_alias)):
                return None
        payload_request = _key(payload.get('request_id'))
        if payload_request and payload_request not in {rid, turn}:
            return None
        if event == 'acp.output_request':
            # One interaction id addresses one session of one generation.
            rpc = payload.get('jsonrpc')
            if channel != 'acp' or not isinstance(rpc, dict):
                return None
            raw_id = rpc.get('id')
            rpc_id = _key(raw_id) if type(raw_id) is str else (
                str(raw_id) if type(raw_id) is int else ''
            )
            if not rpc_id or not isinstance(rpc.get('method'), str):
                return None
            params = rpc.get('params')
            if isinstance(params, dict) and 'sessionId' in params and params['sessionId'] != sid:
                return None
            context = (target.target_id, target.generation, sid)
            previous = self._acp_interactions.get(rpc_id)
            if previous is not None and previous != context:
                return None
            if previous is None and len(self._acp_interactions) >= MAX_ACP_INTERACTIONS:
                return None
            self._acp_interactions[rpc_id] = context
        if request is not None and not self._remember((rid, payload_request, turn), request):
            return None
        result = dict(wire)
        # The route comes from the connection, never from a wire field a target
        # could leave empty. Consumers route by these two values.
        result['user_id'] = target.user_id
        result['session_id'] = sid
        return result

    def clear(self) -> None:
        self._requests.clear()
        self._retired.clear()
        self._sessions.clear()
        self._acp_interactions.clear()
