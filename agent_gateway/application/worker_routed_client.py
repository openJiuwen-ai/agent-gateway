# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Fixed per-user southbound targets owned by one Worker; no default fallback.

Each bound user has exactly one local AgentServer target in the static
directory. The Worker opens one existing ``StaticAgentClient`` per target and
lends this facade to the handlers in place of a single AgentServer client.

Scope of the trial:

* A request is routed only by the named user of its envelope. There is no
  default AgentServer, discovery or cross-Worker forwarding.
* Each target's socket is owned by the Worker that serves the user. A server
  push is handed to shared consumers only when the sending target proved the
  request or session association (see ``RoutedPushGuard``). An unassociated
  push is dropped: this gateway does not claim it reaches any owner.
* Whether an AgentServer delivers unsolicited pushes to this gateway at all
  depends on the gateway owning that server's single push slot. That is an
  AgentServer property and is not asserted here.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import aclosing
from dataclasses import dataclass, field
import logging
import math
import time
from typing import Any
import uuid

from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.gateway_normalize import e2a_from_agent_fields
from e2a.message import ReqMethod
from e2a.models import E2AEnvelope

from agent_gateway.application.worker_routed_push import RoutedPushGuard
from agent_gateway.application.worker_route_provider import StaticRouteProvider
from agent_gateway.application.worker_session_routes import SessionRouteError, SessionRouteIndex
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.southbound_routes import SouthboundTarget, StaticRouteDirectory
from agent_gateway.southbound.orchestration.client import SouthboundAgentClient, StaticAgentClient, build_static_client

logger = logging.getLogger(__name__)

_PROBE_CHANNEL_ID = "gateway-route"


class RoutedTargetsUnreachable(RuntimeError):
    """At least one owned target could not be connected at activation."""


class RoutedConfigurationError(ValueError):
    """The Worker, its slot or a request does not fit the static directory."""


# Recent request ids and the user each was admitted for; a bounded LRU.
_REQUEST_USER_LIMIT = 4096
# These methods address no existing session, or create one whose id the
# AgentServer returns. Every other method must name a session this target owns.
_UNVERIFIED_METHODS = frozenset({
    "initialize", "models.list", "session.create", "session.list", "session.get_metadata",
})


def _key(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


@dataclass
class _TargetEntry:
    target: SouthboundTarget
    client: StaticAgentClient
    failed_at_activation: BaseException | None = field(default=None, repr=False)


class WorkerRoutedClient(SouthboundAgentClient):
    """Route bound named users to the static targets owned by this Worker."""

    agent_client_mode = "websocket"
    # The Worker connects explicitly after consumers registered their push handler.
    requires_initial_connection = False
    # Each user has a private AgentServer directory; no gateway-local fallback
    # may read or write another user's files.
    shared_directory_layout = False
    managed_routing = False
    # Consumed by is_user_routing_client: downstream handlers deliver to the
    # bound user only.
    user_routing = True

    def __init__(
        self, directory: StaticRouteDirectory, *, slot_id: int, connect_timeout: float = 12.0,
        probe_timeout: float = 5.0, stop_timeout: float = 8.0,
        session_routes: SessionRouteIndex | None = None,
    ) -> None:
        for name, value in (("connect", connect_timeout), ("probe", probe_timeout), ("stop", stop_timeout)):
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"routed {name} timeout must be finite and positive")
        self._provider = StaticRouteProvider(directory, slot_id=slot_id)
        self._connect_timeout = connect_timeout
        self._probe_timeout = probe_timeout
        self._stop_timeout = stop_timeout
        self.session_routes = session_routes if session_routes is not None else SessionRouteIndex()
        self._push_guard = RoutedPushGuard(session_routes=self.session_routes)
        self._probe_locks: dict[str, asyncio.Lock] = {}
        self._request_users: OrderedDict[str, str] = OrderedDict()
        self._entries: dict[str, _TargetEntry] = {}
        self._frame_gate: Any = None
        self._push_handler: Callable[[dict[str, Any]], Awaitable[None]] | None = None
        self._activated = asyncio.Event()
        self._closing = False
        self._stop_task: asyncio.Task[None] | None = None
        for target in self._provider.owned_targets:
            client = build_static_client(target.ws_url, shared_directory_layout=False)
            if type(client) is not StaticAgentClient:  # pragma: no cover - fixed constructor
                raise TypeError("routed targets require the static AgentServer client")
            entry = _TargetEntry(target=target, client=client)
            client.set_server_push_handler(self._make_target_push(entry))
            self._entries[target.target_id] = entry

    # ------------------------------------------------------------------ #
    # Identity and directory
    # ------------------------------------------------------------------ #
    @property
    def identity_policy(self) -> LocalIdentityPolicy:
        """The immutable policy shared by the directory and ingress adapters."""
        return self._provider.identity_policy

    @property
    def owned_targets(self) -> tuple[SouthboundTarget, ...]:
        return self._provider.owned_targets

    @property
    def slot_id(self) -> int:
        return self._provider._slot_id

    def validate_bound_user(self, user_id: str | None, *, agent_type: str | None = None) -> SouthboundTarget:
        """Resolve a named local user to its target without opening a connection."""
        return self._provider.resolve_bound_user(user_id, agent_type=agent_type)

    def validate_bound_session(self, user_id: str | None, session_id: str | None) -> None:
        """Refuse a session that another target of this Worker confirmed."""
        target = self.validate_bound_user(user_id)
        self.session_routes.check(target, session_id)

    def validate_bound_request(self, user_id: str | None, request_id: str | None) -> None:
        """Refuse a request id that is active or protected on any owned target."""
        target = self.validate_bound_user(user_id)
        self._push_guard.check_request(target, request_id)

    def target_client(self, target_id: str) -> StaticAgentClient:
        """The transport owned by one target; callers must not replace its handlers."""
        return self._entries[target_id].client

    def request_counts(self) -> tuple[int, int, int]:
        """Active requests, protected recent ids and capacity rejections."""
        return self._push_guard.counts()

    @property
    def frame_gate(self) -> Any:
        """The ingress gate every northbound socket of this Worker uses."""
        from agent_gateway.application.worker_routed_ingress import RoutedFrameGate

        if self._frame_gate is None:
            self._frame_gate = RoutedFrameGate(self)
        return self._frame_gate

    def remember_request(self, user_id: str | None, request_id: str | None) -> None:
        """Record the user a request id was admitted or dispatched for."""
        rid, user = _key(request_id), _key(user_id)
        if not rid or not user:
            return
        self._request_users.pop(rid, None)
        self._request_users[rid] = user
        while len(self._request_users) > _REQUEST_USER_LIMIT:
            self._request_users.popitem(last=False)

    def request_owner(self, request_id: str | None) -> str | None:
        """The user a recent request id belongs to, from the route tables."""
        return self._request_users.get(_key(request_id))

    def session_owner(self, session_id: str | None) -> str | None:
        """The named user of a session, from the target that confirmed it."""
        owner = self.session_routes.owner(session_id)
        if owner is None:
            return None
        entry = self._entries.get(owner[0])
        if entry is None or entry.target.generation != owner[1]:
            return None
        return entry.target.user_id

    def _remember_acp_alias(self, entry: _TargetEntry, envelope: E2AEnvelope, session_id: str) -> None:
        """Bind an ACP client session to the AgentServer session created for it."""
        if _key(envelope.channel) != "acp" or not isinstance(envelope.channel_context, dict):
            return
        alias = _key(envelope.channel_context.get("acp_original_session_id"))
        if alias and alias != session_id:
            self.session_routes.remember_acp_alias(entry.target.target_id, alias, session_id)

    # ------------------------------------------------------------------ #
    # AgentServerClient contract
    # ------------------------------------------------------------------ #
    @property
    def server_ready(self) -> bool:
        """Routing admission. It is not the health of any individual AgentServer."""
        return self._activated.is_set() and not self._closing

    @property
    def can_reconnect(self) -> bool:
        """A request to a dropped target restores that target's transport."""
        return self.server_ready

    @property
    def agent_ready(self) -> bool:
        """Coarse over every owned target: False if any runtime cannot execute."""
        return bool(self._entries) and all(entry.client.agent_ready for entry in self._entries.values())

    def set_server_push_handler(self, handler: Callable[[dict[str, Any]], Awaitable[None]] | None) -> None:
        """Register the shared consumer for admitted pushes, before activation."""
        if self._closing:
            raise RuntimeError("routed Worker is stopped")
        if handler is not None and not callable(handler):
            raise ValueError("routed push handler must be callable")
        self._push_handler = handler

    async def connect(self, uri: str) -> None:
        raise ValueError("routed connections require the static directory and activate()")

    def set_or_update_server_config(self, *, config: dict[str, Any], env: dict[str, str] | None = None) -> None:
        """Static targets do not consume gateway config snapshots."""

    async def activate(self) -> None:
        """Connect every owned target; fail the Worker if one is unreachable."""
        if self._closing:
            raise RuntimeError("routed Worker is stopped")
        if self._activated.is_set():
            return
        limit = asyncio.Semaphore(4)

        async def connect(entry: _TargetEntry) -> None:
            async with limit:
                async with asyncio.timeout(self._connect_timeout):
                    await entry.client.connect(entry.target.ws_url)

        results = await asyncio.gather(
            *(connect(entry) for entry in self._entries.values()), return_exceptions=True,
        )
        failures = [
            (entry, error) for entry, error in zip(self._entries.values(), results)
            if isinstance(error, BaseException)
        ]
        if failures or self._closing:
            for entry, error in failures:
                entry.failed_at_activation = error
            await self.disconnect()
            if not failures:
                raise RuntimeError("routed Worker is stopped during activation")
            # The ids are configuration labels; the exception text never
            # carries an address or a user.
            raise RoutedTargetsUnreachable(
                "static targets unreachable at activation: "
                + ",".join(entry.target.target_id for entry, _error in failures),
            )
        self._activated.set()

    async def disconnect(self, *, deadline: float | None = None) -> None:
        """Stop admission and close every target once, within one bounded wait."""
        if self._stop_task is None:
            self._closing = True
            self._activated.clear()
            own = asyncio.get_running_loop().time() + self._stop_timeout
            self._stop_task = asyncio.create_task(
                self._cleanup(deadline if deadline is not None else own), name="gateway-routed-stop",
            )
            self._stop_task.add_done_callback(_observe_task)
        await asyncio.shield(self._stop_task)

    async def _cleanup(self, deadline: float) -> None:
        try:
            async with asyncio.timeout_at(deadline):
                await asyncio.gather(
                    *(entry.client.disconnect() for entry in self._entries.values()),
                    return_exceptions=True,
                )
        except TimeoutError:
            logger.warning("routed Worker targets did not close before the deadline")
        finally:
            active, recent, rejected = self._push_guard.counts()
            logger.debug(
                "routed requests at shutdown active=%d recent=%d capacity_rejections=%d", active, recent, rejected,
            )
            self._push_guard.clear()
            self.session_routes.clear()
            self._request_users.clear()
            self._probe_locks.clear()
            self._activated.set()  # Release any push waiter; it observes closing.

    # ------------------------------------------------------------------ #
    # Requests
    # ------------------------------------------------------------------ #
    def _resolve(self, envelope: Any) -> _TargetEntry:
        if not self.server_ready:
            raise RuntimeError("routed Worker is not active")
        try:
            if not isinstance(envelope, E2AEnvelope):
                raise ValueError
            for mapping in (envelope.params, envelope.channel_context):
                if mapping is not None and mapping.get("agent_type") not in (None, "jiuwenswarm"):
                    raise ValueError
            target = self._provider.resolve_bound_user(envelope.user_id)
            return self._entries[target.target_id]
        except (ValueError, TypeError, AttributeError, KeyError):
            raise ValueError("request has no owned static southbound target") from None

    async def _probe_found(self, entry: _TargetEntry, session_id: str) -> bool:
        """Ask one target whether it holds the session; failures count as absent."""
        envelope = e2a_from_agent_fields(
            request_id=f"gateway-route-{uuid.uuid4().hex}", channel_id=_PROBE_CHANNEL_ID,
            session_id=session_id, req_method=ReqMethod.SESSION_GET_METADATA,
            params={"session_id": session_id, "enable_writeback": False}, is_stream=False,
            timestamp=time.time(), user_id=entry.target.user_id,
        )
        try:
            response = await asyncio.wait_for(entry.client.send_request(envelope), timeout=self._probe_timeout)
        except Exception as error:  # noqa: BLE001 - an unreachable target holds nothing we can use
            logger.debug("session probe failed target=%s error=%s", entry.target.target_id, type(error).__name__)
            return False
        payload = getattr(response, "payload", None)
        if getattr(response, "ok", False) is not True or not isinstance(payload, dict) or not payload:
            return False
        result = payload.get("result", payload)
        return isinstance(result, dict) and bool(result)

    async def _verify_session(self, entry: _TargetEntry, envelope: E2AEnvelope) -> None:
        """Ensure the selected target may serve the session the request names."""
        method = str(envelope.method)
        session_id = _key(envelope.params.get("session_id") or envelope.session_id)
        if not session_id:
            return
        target = entry.target
        owner = self.session_routes.owner(session_id)
        if owner is not None:
            self.session_routes.check(target, session_id)
            return
        # The id of an ACP session of another target is never probed and
        # claimed as a new session here, whatever channel names it.
        self.session_routes.check_acp_session(target, session_id)
        if method in _UNVERIFIED_METHODS:
            return
        lock = self._probe_locks.setdefault(session_id, asyncio.Lock())
        async with lock:
            owner = self.session_routes.owner(session_id)
            if owner is not None:
                self.session_routes.check(target, session_id)
                return
            if await self._probe_found(entry, session_id):
                self.session_routes.claim(target, session_id)
                return
            # A session id is unique inside this Worker. Before a new session
            # takes the id, no other owned target may already hold it.
            others = [item for item in self._entries.values() if item is not entry]
            if others and any(await asyncio.gather(*(self._probe_found(item, session_id) for item in others))):
                raise SessionRouteError("session is owned by another static target")
            self.session_routes.claim(target, session_id)

    async def send_request(
        self, envelope: E2AEnvelope, *, timeout: float | None = None,
        on_written: Callable[[], None] | None = None,
    ) -> AgentResponse:
        entry = self._resolve(envelope)
        await self._verify_session(entry, envelope)
        self._remember_acp_alias(
            entry, envelope, _key(envelope.params.get("session_id") or envelope.session_id),
        )
        key = self._push_guard.begin(entry.target, envelope)
        self.remember_request(entry.target.user_id, getattr(envelope, "request_id", None))
        try:
            self._push_guard.mark_dispatched(key)
            response = await (
                entry.client.send_request(envelope, timeout=timeout)
                if on_written is None else entry.client.send_request(envelope, on_written=on_written)
            )
            self._push_guard.observe_response(entry.target, envelope, response)
            return response
        finally:
            self._push_guard.finish(key)

    async def send_request_stream(self, envelope: E2AEnvelope) -> AsyncIterator[AgentResponseChunk]:
        entry = self._resolve(envelope)
        await self._verify_session(entry, envelope)
        self._remember_acp_alias(
            entry, envelope, _key(envelope.params.get("session_id") or envelope.session_id),
        )
        key = self._push_guard.begin(entry.target, envelope)
        self.remember_request(entry.target.user_id, getattr(envelope, "request_id", None))
        try:
            self._push_guard.mark_dispatched(key)
            complete = False
            async with aclosing(entry.client.send_request_stream(envelope)) as stream:
                async for chunk in stream:
                    complete = complete or chunk.is_complete
                    yield chunk
            if complete:
                self._push_guard.mark_stream_completed(key)
        finally:
            self._push_guard.finish(key)

    # ------------------------------------------------------------------ #
    # Pushes
    # ------------------------------------------------------------------ #
    def _make_target_push(self, entry: _TargetEntry) -> Callable[[dict[str, Any]], Awaitable[None]]:
        async def receive(wire: dict[str, Any]) -> None:
            # A frame that arrives while sibling targets are still connecting
            # waits for activation; a failed activation releases and drops it.
            await self._activated.wait()
            if self._closing or self._push_handler is None:
                return
            admitted = self._push_guard.admit(entry.target, wire)
            if admitted is None:
                logger.debug(
                    "routed push without an owned route dropped target=%s request_id=%s",
                    entry.target.target_id, str(wire.get("request_id") or ""),
                )
                return
            await self._push_handler(admitted)

        return receive

    def push_owner(self, target: SouthboundTarget, request_id: str | None) -> Any:
        """Inspect the request a pushed id would bind to; active or recently retired."""
        return self._push_guard.owner_of(target, request_id)


def _observe_task(task: asyncio.Task[None]) -> None:
    if not task.cancelled():
        task.exception()


__all__ = ["WorkerRoutedClient", "SessionRouteError", "RoutedTargetsUnreachable", "RoutedConfigurationError"]
