# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Connection lifecycle state for the standalone WebSocket gateway."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable


logger = logging.getLogger("gateway")


class ConnectionRegistry:
    """Own request, session, and pending-session routing tables.

    The route-key callback may return either a 2-tuple or a 3-tuple.  Lookups
    deliberately fall back in both directions so old callers that omit
    ``agent_ref`` still resolve entries registered with it.
    """

    def __init__(self, client_route_key: Callable[..., tuple | None]) -> None:
        self._client_route_key = client_route_key
        self.clients: set[Any] = set()
        self.request_to_client: dict[tuple, Any] = {}
        self.session_to_client: dict[tuple, Any] = {}
        self.pending_session_clients: dict[tuple[str, str], Any] = {}

    def add_client(self, ws: Any) -> None:
        self.clients.add(ws)

    def discard_client(self, ws: Any) -> None:
        self.clients.discard(ws)

    def bind_request(self, request_key: tuple | None, ws: Any) -> None:
        if request_key is not None:
            self.request_to_client[request_key] = ws

    def remove_client(self, ws: Any) -> tuple[list[tuple], list[tuple], list[tuple]]:
        """Remove every routing entry for one ws and return its stale keys."""
        self.clients.discard(ws)
        stale_request_keys = [
            key for key, client in self.request_to_client.items() if client is ws
        ]
        for request_key in stale_request_keys:
            self.request_to_client.pop(request_key, None)

        stale_session_keys = [
            key for key, client in self.session_to_client.items() if client is ws
        ]
        for session_key in stale_session_keys:
            self.session_to_client.pop(session_key, None)

        stale_pending_session_keys = [
            key for key, client in self.pending_session_clients.items()
            if client is ws
        ]
        for session_key in stale_pending_session_keys:
            self.pending_session_clients.pop(session_key, None)

        return stale_request_keys, stale_session_keys, stale_pending_session_keys

    def clear(self) -> None:
        self.clients.clear()
        self.request_to_client.clear()
        self.session_to_client.clear()
        self.pending_session_clients.clear()

    def find_channel_clients(self, channel_id: str) -> list[Any]:
        return [
            client_ws for key, client_ws in self.session_to_client.items()
            if isinstance(key, tuple) and key[0] == channel_id
            and not getattr(client_ws, "closed", False)
        ]

    def lookup_client(
        self,
        table: dict[tuple, Any],
        channel_id: str | None,
        scope: str | None,
        agent_ref: Any = None,
    ) -> Any | None:
        """Look up a ws with exact and legacy 2/3-tuple fallbacks."""
        key = self._client_route_key(channel_id, scope, agent_ref)
        if key is None:
            return None
        ws = table.get(key)
        if ws is not None:
            return ws
        if len(key) >= 3:
            ws = table.get((key[0], key[1]))
            if ws is not None:
                return ws
        if len(key) == 2:
            for route_key, client_ws in table.items():
                if (
                    isinstance(route_key, tuple)
                    and len(route_key) == 3
                    and route_key[:2] == key
                ):
                    return client_ws
        return table.get(scope)

    def is_session_bound_to_client(
        self,
        channel_id: str,
        session_id: str,
        ws: Any,
    ) -> bool:
        session_key = self._client_route_key(channel_id, session_id)
        if session_key is None:
            return False
        return self.session_to_client.get(session_key) is ws

    def active_session_ids(
        self,
        channel_id: str,
        exclude_ws: Any = None,
    ) -> set[str]:
        active: set[str] = set()
        for key, client_ws in self.session_to_client.items():
            if not isinstance(key, tuple) or len(key) < 2:
                continue
            if key[0] != channel_id:
                continue
            if client_ws is exclude_ws:
                continue
            if bool(getattr(client_ws, "closed", False)):
                continue
            session_id = key[1]
            if isinstance(session_id, str) and session_id:
                active.add(session_id)
        return active

    def bind_acp_session_client(
        self,
        session_id: str,
        ws: Any,
        agent_ref: Any = None,
    ) -> None:
        session_key = self._client_route_key("acp", session_id, agent_ref)
        if session_key is not None:
            self.session_to_client[session_key] = ws

    async def bind_route_session_client(
        self,
        route: Any,
        session_id: str,
        ws: Any,
    ) -> bool:
        session_key = self._client_route_key(route.channel_id, session_id)
        if session_key is None:
            return False

        existing_ws = self.session_to_client.get(session_key)
        if (
            existing_ws is not None
            and existing_ws is not ws
            and not bool(getattr(existing_ws, "closed", False))
        ):
            if self._is_open(ws):
                self.pending_session_clients[session_key] = ws
            return False

        self.session_to_client[session_key] = ws
        if self.pending_session_clients.get(session_key) is ws:
            self.pending_session_clients.pop(session_key, None)

        if existing_ws is ws or route.session_bind_handler is None:
            return True
        try:
            result = route.session_bind_handler(route.channel_id, session_id)
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.warning(
                "ConnectionRegistry session bind handler failed: "
                "channel_id=%s session_id=%s",
                route.channel_id,
                session_id,
                exc_info=True,
            )
        return True

    async def promote_pending_session_client(
        self,
        route: Any,
        session_key: tuple[str, str],
    ) -> None:
        pending_ws = self.pending_session_clients.pop(session_key, None)
        if not self._is_open(pending_ws):
            return

        existing_ws = self.session_to_client.get(session_key)
        if (
            existing_ws is not None
            and existing_ws is not pending_ws
            and self._is_open(existing_ws)
        ):
            return

        self.session_to_client[session_key] = pending_ws
        if route.session_bind_handler is None:
            return
        try:
            result = route.session_bind_handler(
                route.channel_id, session_key[1]
            )
            if asyncio.iscoroutine(result):
                await result
        except Exception:
            logger.warning(
                "ConnectionRegistry pending session bind handler failed: "
                "channel_id=%s session_id=%s",
                route.channel_id,
                session_key[1],
                exc_info=True,
            )

    @staticmethod
    def _is_open(ws: Any) -> bool:
        return ws is not None and not bool(getattr(ws, "closed", False))
