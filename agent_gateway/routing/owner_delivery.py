# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Named-owner delivery for a Worker whose users sit on static AgentServer targets.

A gateway with one AgentServer may send a session event to every socket bound
to the session and a session-less notice to every client of a channel. A
static Worker serves several named users from one process, so each outbound
frame names the user it belongs to and may only reach that user's sockets.

The gate is installed per Worker. For a gated channel every frame passes
``_enqueue_send`` while an outbound owner is in scope. A frame without an
owner, or one addressed to a socket of another user, is dropped: a route
that cannot be proven is not guessed.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
import logging
from typing import Any

logger = logging.getLogger(__name__)

_OUTBOUND_OWNER: ContextVar[str | None] = ContextVar("gateway_outbound_owner", default=None)


def _text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


class OwnerDelivery:
    """Resolve the named owner of an outbound message and gate its recipients."""

    def __init__(
        self,
        *,
        request_owner: Callable[[str], str | None],
        session_owner: Callable[[str], str | None],
    ) -> None:
        if not callable(request_owner) or not callable(session_owner):
            raise ValueError("owner delivery requires request and session owner lookups")
        self._request_owner = request_owner
        self._session_owner = session_owner

    @staticmethod
    def owner_in_scope() -> str | None:
        """The owner of the outbound message being delivered, if any."""
        return _OUTBOUND_OWNER.get()

    @contextmanager
    def scope(self, owner: str | None) -> Iterator[None]:
        """Bind every frame sent inside the block to one named owner."""
        token = _OUTBOUND_OWNER.set(owner)
        try:
            yield
        finally:
            _OUTBOUND_OWNER.reset(token)

    def of_request(self, request_id: str | None) -> str | None:
        rid = _text(request_id)
        owner = self._request_owner(rid) if rid else None
        return _text(owner) or None

    def of_session(self, session_id: str | None) -> str | None:
        sid = _text(session_id)
        owner = self._session_owner(sid) if sid else None
        return _text(owner) or None

    def of_message(self, msg: Any) -> str | None:
        """The route of the request first, then the declared user, then the session.

        A request id was admitted for one connection user. A Message that
        names another user for that id keeps the recorded route: the owner is
        read from the route tables, never from what a response claims.
        """
        owner = self.of_request(getattr(msg, "id", None))
        if owner is not None:
            return owner
        declared = _text(getattr(msg, "user_id", None))
        if declared:
            return declared
        payload = getattr(msg, "payload", None)
        sid = _text(getattr(msg, "session_id", None)) or (
            _text(payload.get("session_id")) if isinstance(payload, dict) else ""
        )
        return self.of_session(sid)

    @staticmethod
    def allows(connection_user: str | None, owner: str | None) -> bool:
        """Only a named owner receives frames, and only on its own sockets."""
        user = _text(connection_user)
        named = _text(owner)
        return bool(user) and bool(named) and user == named


__all__ = ["OwnerDelivery"]
