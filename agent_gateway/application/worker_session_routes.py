# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Worker-private index of the sessions each static target has confirmed."""

from __future__ import annotations

from collections import OrderedDict
from typing import Any


class SessionRouteError(ValueError):
    """The caller cannot use a session on the selected target."""


def _key(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


class SessionRouteIndex:
    """Bind one session id to at most one static target inside this Worker.

    Shared AgentServer consumers keep session-keyed state, so two users may not
    use one session id in the same Worker even though their AgentServers have
    independent namespaces. A binding is established only by an AgentServer
    answer on the owning target, or by a probe that found the id on no other
    owned target. Client input alone never creates one.

    The index is process private and bounded. An evicted binding is verified
    again by the next request that names the session.
    """

    def __init__(self, *, max_sessions: int = 16384, max_acp_sessions: int = 4096) -> None:
        if type(max_sessions) is not int or max_sessions < 1:
            raise ValueError("session route capacity must be a positive integer")
        if type(max_acp_sessions) is not int or max_acp_sessions < 1:
            raise ValueError("ACP session capacity must be a positive integer")
        self._max_sessions = max_sessions
        self._max_acp_sessions = max_acp_sessions
        # session id -> (target_id, generation); least recently used first.
        self._routes: OrderedDict[str, tuple[str, int]] = OrderedDict()
        # ACP external session id -> target id.
        self._acp_sessions: OrderedDict[str, str] = OrderedDict()
        # (target id, ACP external id) -> AgentServer session id; least recently used first.
        self._acp_aliases: OrderedDict[tuple[str, str], str] = OrderedDict()

    def __len__(self) -> int:
        return len(self._routes)

    def owner(self, session_id: str | None) -> tuple[str, int] | None:
        """Return the confirmed owner of a session id, if any."""
        return self._routes.get(_key(session_id))

    def check(self, target: Any, session_id: str | None) -> bool:
        """Whether the target owns the session; refuse every other owner.

        One id belongs to one target under either name it is known by: an
        AgentServer session id, or the client-visible id of an ACP session.
        A frame of any channel that names an ACP session of another target is
        refused like one that names a confirmed session of another target.
        """
        sid = _key(session_id)
        if not sid:
            return False
        acp_owner = self._acp_sessions.get(sid)
        if acp_owner is not None and acp_owner != target.target_id:
            raise SessionRouteError("ACP session is owned by another static target")
        current = self._routes.get(sid)
        if current is None:
            return False
        self._routes.move_to_end(sid)
        if current[0] != target.target_id:
            raise SessionRouteError("session is owned by another static target")
        if current[1] != target.generation:
            raise SessionRouteError("session is pinned to another instance generation")
        return True

    def claim(self, target: Any, session_id: str | None) -> None:
        """Bind a session to the target after the target confirmed it."""
        sid = _key(session_id)
        if not sid:
            return
        current = self._routes.get(sid)
        if current is not None and current[0] != target.target_id:
            raise SessionRouteError("session is owned by another static target")
        acp_owner = self._acp_sessions.get(sid)
        if acp_owner is not None and acp_owner != target.target_id:
            raise SessionRouteError("ACP session is owned by another static target")
        self._routes[sid] = (target.target_id, target.generation)
        self._routes.move_to_end(sid)
        while len(self._routes) > self._max_sessions:
            self._routes.popitem(last=False)

    def acp_owner(self, external_id: str | None) -> str | None:
        return self._acp_sessions.get(_key(external_id))

    def check_acp_session(self, target: Any, external_id: str | None) -> bool:
        external = _key(external_id)
        if not external:
            return False
        current = self._acp_sessions.get(external)
        if current is None:
            return False
        self._acp_sessions.move_to_end(external)
        if current != target.target_id:
            raise SessionRouteError("ACP session is owned by another static target")
        return True

    def claim_acp_session(self, target: Any, external_id: str | None) -> None:
        """Bind a client-visible ACP session id to one target."""
        external = _key(external_id)
        if not external:
            return
        current = self._acp_sessions.get(external)
        if current is not None and current != target.target_id:
            raise SessionRouteError("ACP session is owned by another static target")
        # Another target's confirmed AgentServer session id is not a free
        # client-visible id: taking it would lock its owner out of it.
        confirmed = self._routes.get(external)
        if confirmed is not None and confirmed[0] != target.target_id:
            raise SessionRouteError("session is owned by another static target")
        self._acp_sessions[external] = target.target_id
        self._acp_sessions.move_to_end(external)
        while len(self._acp_sessions) > self._max_acp_sessions:
            self._acp_sessions.popitem(last=False)

    def acp_alias(self, target_id: str, external_id: str | None) -> str | None:
        """Return the AgentServer session id behind a target's ACP alias."""
        key = (target_id, _key(external_id))
        if key not in self._acp_aliases:
            return None
        self._acp_aliases.move_to_end(key)
        return self._acp_aliases[key]

    def remember_acp_alias(self, target_id: str, external_id: str | None, session_id: str | None) -> None:
        external, internal = _key(external_id), _key(session_id)
        if not external or not internal:
            raise SessionRouteError("ACP alias requires both session ids")
        owner = self._routes.get(internal)
        if owner is None or owner[0] != target_id:
            raise SessionRouteError("ACP session creation has no confirmed route")
        self._acp_aliases[(target_id, external)] = internal
        self._acp_aliases.move_to_end((target_id, external))
        while len(self._acp_aliases) > self._max_acp_sessions:
            self._acp_aliases.popitem(last=False)

    def clear(self) -> None:
        self._routes.clear()
        self._acp_sessions.clear()
        self._acp_aliases.clear()
