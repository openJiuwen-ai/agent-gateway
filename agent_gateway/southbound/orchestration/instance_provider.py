# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Owner admission and exact activity leases, separate from physical provisioning."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import hashlib
import math
from typing import Protocol
from urllib.parse import urlsplit
from weakref import WeakSet

from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest
from agent_gateway.southbound.orchestration.agent_manager import (
    AgentCreator, AgentManager, AgentRuntime, DEFAULT_AGENT_KEY_FIELDS,
)


class InstanceControl(Protocol):
    """The managed control surface an owner route needs for its data endpoint.

    The control derives the endpoint from its own deployment settings (URL
    scheme, instance addressing, credentials handshake); the provider never
    assembles a platform URL and never carries a credential.
    """

    @property
    def frontend_endpoint(self) -> str: ...

    def ws_url(self, instance_id: str, port: int) -> str: ...


@dataclass(frozen=True)
class OwnedRoute:
    """An admitted logical owner; never carries a caller-supplied AS address."""

    target_id: str
    user_id: str
    agent_type: str
    owner_slot: int


@dataclass(frozen=True)
class InstanceTarget:
    """A verified physical generation and a frontend-derived data endpoint."""

    target_id: str
    physical_id: str
    generation: str
    user_id: str
    agent_type: str
    owner_slot: int
    ws_url: str = field(repr=False)


@dataclass(frozen=True, eq=False)
class TargetLease:
    """An unforgeable-by-copy local reference to one exact manager incarnation."""

    route: OwnedRoute
    target: InstanceTarget
    agent_id: str
    runtime: AgentRuntime = field(repr=False)


def _label(value: object) -> bool:
    return (
        type(value) is str and 0 < len(value) <= 256
        and not any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in value)
    )


class AgentOSInstanceProvider:
    """Use the original manager for single-flight creation and atomic task holds.

    The trusted creator must attest an independently verified
    ``physical_generation`` after management readiness verification. This first
    provider only supports the default user/type key; session aliases and
    restart adoption are not inferred.
    """

    def __init__(
        self, manager: AgentManager, *, creator: AgentCreator,
        control: InstanceControl, namespace: str, identity_policy: LocalIdentityPolicy,
        manifest: WorkerManifest, slot_id: int, admitted_users: tuple[str, ...],
    ) -> None:
        if type(manager) is not AgentManager or manager.key_fields != DEFAULT_AGENT_KEY_FIELDS:
            raise ValueError('instance provider requires the default-key manager')
        if not callable(creator) or not _label(namespace):
            raise ValueError('invalid instance management configuration')
        if not callable(getattr(control, 'ws_url', None)):
            raise ValueError('invalid instance management configuration')
        try:
            frontend_endpoint = control.frontend_endpoint
            if (
                type(frontend_endpoint) is not str or len(frontend_endpoint) > 2048
                or any(character.isspace() or ord(character) < 32 for character in frontend_endpoint)
            ):
                raise ValueError
            parsed = urlsplit(frontend_endpoint)
            if (
                parsed.scheme not in ('http', 'https') or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.path not in ('', '/') or parsed.query or parsed.fragment
            ):
                raise ValueError
            if parsed.port is not None and not 1 <= parsed.port <= 65535:
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise ValueError('invalid instance management endpoint') from None
        if type(identity_policy) is not LocalIdentityPolicy or type(manifest) is not WorkerManifest:
            raise ValueError('instance provider requires explicit identity and manifest')
        if type(slot_id) is not int or slot_id not in manifest.slots:
            raise ValueError('instance provider slot is not in the manifest')
        if type(admitted_users) is not tuple or not 1 <= len(admitted_users) <= 32:
            raise ValueError('instance provider requires one to 32 admitted users')
        affinity = FixedUserHash(manifest, identity_policy)
        routes = {}
        seen = set()
        for user in admitted_users:
            identity = IngressIdentity(
                identity_policy.contract_id, 'local_compat', identity_policy.domain, 'named', user,
            )
            if user in seen:
                raise ValueError('duplicate admitted instance user')
            seen.add(user)
            owner = affinity.select(identity)
            if owner == slot_id:
                route_id = 'owner-' + hashlib.sha256(identity.user_key_bytes()).hexdigest()
                routes[user] = OwnedRoute(route_id, user, 'jiuwenswarm', owner)
        self.manager = manager
        self.identity_policy = identity_policy
        self.manifest = manifest
        self.slot_id = slot_id
        self._creator = creator
        self._control = control
        self._frontend = parsed
        self._ws_scheme = 'wss' if parsed.scheme == 'https' else 'ws'
        self._namespace = namespace
        self._routes = routes
        self._targets: dict[str, InstanceTarget] = {}
        self._physical_owners: dict[str, tuple[str, str]] = {}
        self._leases: dict[int, TargetLease] = {}
        self._uncertain: set[int] = set()
        self._completed: WeakSet[TargetLease] = WeakSet()
        self._settling: dict[int, asyncio.Task] = {}
        self._release_holds: set[asyncio.Task] = set()
        self._acquiring: set[asyncio.Task] = set()
        self._closed = False

    @property
    def active_lease_count(self) -> int:
        return len(self._leases)

    @property
    def uncertain_lease_count(self) -> int:
        return len(self._uncertain)

    @property
    def pending_acquisition_count(self) -> int:
        return len(self._acquiring)

    def validate_owner(
        self, user_id: str | None, *, agent_type: str | None = None,
    ) -> OwnedRoute:
        """Validate admission synchronously before any manager/HTTP side effect."""
        if type(user_id) is not str or agent_type not in (None, 'jiuwenswarm'):
            raise ValueError('request has no owned instance route')
        route = self._routes.get(user_id)
        if route is None:
            raise ValueError('request has no owned instance route')
        return route

    async def acquire(
        self, route: OwnedRoute, *, existing_only: bool = False, deadline: float | None = None,
    ) -> TargetLease | None:
        if self._closed:
            raise RuntimeError('instance provider is closed')
        if type(route) is not OwnedRoute or self._routes.get(route.user_id) is not route:
            raise ValueError('unowned instance route')
        if type(existing_only) is not bool:
            raise ValueError('invalid instance acquisition mode')
        if len(self._acquiring) >= 128 or len(self._leases) >= 4096:
            raise RuntimeError('instance provider capacity exceeded')
        if deadline is not None and (type(deadline) not in (int, float) or not math.isfinite(deadline)):
            raise ValueError('invalid instance acquisition deadline')
        task = asyncio.create_task(self._acquire(route, existing_only), name='gateway-instance-acquire')
        self._acquiring.add(task)
        try:
            async with asyncio.timeout_at(deadline):
                return await task
        finally:
            self._acquiring.discard(task)

    async def _acquire(self, route: OwnedRoute, existing_only: bool) -> TargetLease | None:
        if existing_only:
            runtime = await self.manager.get_agent(route.user_id, route.agent_type, acquire=True)
            if runtime is None or not runtime.is_ready():
                return None
        else:
            runtime = await self.manager.get_or_create_agent(
                route.user_id, route.agent_type, creator=self._creator, acquire=True,
            )
        try:
            if self._closed or not await self.manager.is_current(runtime):
                raise RuntimeError('instance acquisition became stale')
            target = self._target(route, runtime)
            if len(self._leases) >= 4096:
                raise RuntimeError('instance lease capacity exceeded')
            lease = TargetLease(route, target, runtime.info.agent_id, runtime)
            self._leases[id(lease)] = lease
            return lease
        except BaseException:
            release = asyncio.create_task(
                self._release_hold(runtime.key, runtime.info.agent_id),
                name='gateway-instance-acquire-release',
            )
            self._release_holds.add(release)
            release.add_done_callback(self._release_holds.discard)
            await asyncio.shield(release)
            raise

    async def _release_hold(self, key: object, expected_agent_id: str) -> None:
        await self.manager.release(key, expected_agent_id=expected_agent_id)

    def _endpoint(self, instance_id: str, port: int) -> str:
        """Take the data endpoint from the control and pin it to the admitted frontend."""
        url = self._control.ws_url(instance_id, port)
        try:
            if type(url) is not str or any(
                character.isspace() or ord(character) < 32 or ord(character) == 127 for character in url
            ):
                raise ValueError
            endpoint = urlsplit(url)
        except ValueError:
            raise ValueError('instance endpoint is not derived from the admitted frontend') from None
        if endpoint.scheme != self._ws_scheme or endpoint.netloc != self._frontend.netloc:
            raise ValueError('instance endpoint is not derived from the admitted frontend')
        return url

    def _target(self, route: OwnedRoute, runtime: AgentRuntime) -> InstanceTarget:
        info = runtime.info
        generation = info.metadata.get('physical_generation')
        port = info.metadata.get('agent_port')
        if (
            not _label(info.sandbox_id) or not _label(generation)
            or type(port) is not int or not 1 <= port <= 65535
        ):
            raise ValueError('instance physical generation is not verified')
        physical = f'{self._namespace}:{info.sandbox_id}'
        target = self._targets.get(generation)
        if (
            target is not None
            and (
                target.target_id != route.target_id
                or target.physical_id != physical
                or target.user_id != route.user_id
                or target.agent_type != route.agent_type
                or target.owner_slot != route.owner_slot
            )
        ):
            raise ValueError('instance physical generation is already owned by another route')
        previous = self._physical_owners.get(physical)
        if previous is not None and previous != (route.target_id, generation):
            raise ValueError('instance physical ownership conflicts with an earlier generation')
        if target is None and len(self._targets) >= 4096:
            raise RuntimeError('instance generation capacity exceeded')
        # Resolve before recording ownership so a refused endpoint writes no state.
        endpoint = self._endpoint(info.sandbox_id, port) if target is None else target.ws_url
        self._physical_owners[physical] = (route.target_id, generation)
        if target is None:
            target = InstanceTarget(
                route.target_id, physical, generation, route.user_id, route.agent_type, route.owner_slot,
                endpoint,
            )
            self._targets[generation] = target
        return target

    async def is_current(self, lease: TargetLease) -> bool:
        if (
            type(lease) is not TargetLease
            or self._leases.get(id(lease)) is not lease
            or self._closed
        ):
            return False
        current = await self.manager.is_current(lease.runtime)
        return not self._closed and current

    async def is_target_current(self, target: InstanceTarget) -> bool:
        if (
            self._closed or type(target) is not InstanceTarget
            or self._targets.get(target.generation) is not target
        ):
            return False
        runtime = await self.manager.get_agent(target.user_id, target.agent_type)
        return bool(
            runtime is not None and runtime.is_ready()
            and runtime.info.metadata.get('physical_generation') == target.generation
            and f'{self._namespace}:{runtime.info.sandbox_id}' == target.physical_id
        )

    async def send_current_target(self, target: InstanceTarget, operation) -> None:
        """Bind the actual transport write to the current physical generation."""
        if not await self.is_target_current(target):
            raise RuntimeError('stale instance generation cannot write')
        runtime = await self.manager.get_agent(target.user_id, target.agent_type)
        if (
            self._closed or runtime is None
            or runtime.info.metadata.get('physical_generation') != target.generation
            or f'{self._namespace}:{runtime.info.sandbox_id}' != target.physical_id
        ):
            raise RuntimeError('stale instance generation cannot write')
        await self.manager.send_if_current(runtime, operation)

    async def release(self, lease: TargetLease, *, outcome: str) -> None:
        """Only a known terminal/not-written outcome releases an activity hold."""
        if type(lease) is not TargetLease or outcome not in ('finished', 'not_sent', 'unknown'):
            raise ValueError('invalid instance lease release')
        key = id(lease)
        if self._leases.get(key) is not lease:
            # Completed originals are idempotent; copied dataclasses are not leases.
            if lease in self._completed:
                return
            raise ValueError('unknown instance lease')
        if outcome == 'unknown':
            self._uncertain.add(key)
            return
        task = self._settling.get(key)
        if task is None:
            task = asyncio.create_task(self._release(lease), name='gateway-instance-release')
            self._settling[key] = task
        await asyncio.shield(task)

    async def _release(self, lease: TargetLease) -> None:
        key = id(lease)
        try:
            await self.manager.release(lease.runtime.key, expected_agent_id=lease.agent_id)
            self._leases.pop(key, None)
            self._uncertain.discard(key)
            self._completed.add(lease)
        finally:
            self._settling.pop(key, None)

    async def close(self, *, deadline: float | None = None) -> None:
        """Stop local acquisitions; never infer remote termination or delete ASs."""
        self._closed = True
        tasks = tuple(self._acquiring)
        for task in tasks:
            task.cancel()
        async with asyncio.timeout_at(deadline):
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.gather(
                *(asyncio.shield(task) for task in tuple(self._release_holds))
            )
            await asyncio.gather(*(asyncio.shield(task) for task in tuple(self._settling.values())))
