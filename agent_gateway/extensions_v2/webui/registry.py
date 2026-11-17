# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Track active render instances, revisions, locking and capacity eviction.

Copied from ``jiuwenswarm/extensions_v2/webui/registry.py``; the Gateway keeps
its own instance table because it addresses application slots by web socket.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

from agent_gateway.extensions_v2.webui.constants import MAX_INSTANCES_PER_SESSION, SlotScope
from agent_gateway.extensions_v2.webui.models import (
    ClearFrame,
    RenderFrame,
    RenderMode,
    RenderNode,
    RenderTarget,
    UiRenderInput,
)

logger = logging.getLogger(__name__)


@dataclass
class RenderInstance:
    """The latest frame state for one render instance, without history."""

    target: RenderTarget
    revision: int = 0
    mode: RenderMode = RenderMode.REPLACE
    root: RenderNode | None = None
    updated_at: float = 0.0
    visible_when: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class RenderClaim:
    """The instance, eviction candidates and creation flag for one claim."""

    instance: RenderInstance
    evicted: tuple[RenderInstance, ...] = field(default_factory=tuple)
    created: bool = False


class ActiveRenderRegistry:
    """Manage in-process render instances, revisions, eviction and scoped cleanup."""

    def __init__(self, max_instances_per_session: int = MAX_INSTANCES_PER_SESSION) -> None:
        self._instances: dict[str, RenderInstance] = {}
        self._revisions: dict[str, tuple[RenderTarget, int]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._lock_users: dict[str, int] = {}
        self._capacity_lock = asyncio.Lock()
        self._reserved_evictions: set[str] = set()
        self._max_per_session = max_instances_per_session

    def contains(self, target: RenderTarget) -> bool:
        return target.key in self._instances

    def get(self, target: RenderTarget) -> RenderInstance | None:
        return self._instances.get(target.key)

    def snapshot(self) -> tuple[RenderInstance, ...]:
        return tuple(self._instances.values())

    def clear(self) -> None:
        """Release active instances and revision history when the runtime stops."""
        self._instances.clear()
        self._revisions.clear()
        self._reserved_evictions.clear()
        for key in list(self._locks):
            if self._lock_users.get(key, 0) == 0:
                self._locks.pop(key, None)

    @asynccontextmanager
    async def claim(self, target: RenderTarget) -> AsyncIterator[RenderClaim]:
        """Claim one instance exclusively; independent instances may run concurrently."""

        async with self._lock_for(target.key):
            async with self._capacity_lock:
                evicted = self._evict_candidates(target)
                self._reserved_evictions.update(instance.target.key for instance in evicted)
                instance = self._instances.get(target.key)
                created = instance is None
                if instance is None:
                    instance = RenderInstance(
                        target=target,
                        revision=self._revisions.get(target.key, (target, 0))[1],
                    )
                    self._instances[target.key] = instance
                instance.updated_at = time.monotonic()
            try:
                yield RenderClaim(instance=instance, evicted=evicted, created=created)
            finally:
                # Early returns skip the eviction loop; never keep victims reserved.
                self._reserved_evictions.difference_update(item.target.key for item in evicted)

    def commit(self, instance: RenderInstance, request: UiRenderInput) -> RenderFrame:
        """Commit a delivered update and return its next revision frame."""

        key = instance.target.key
        # A retired instance may have reserved the next revision for its clear frame;
        # keep the render strictly above it so the browser never sees a duplicate revision.
        reserved = self._revisions.get(key, (instance.target, 0))[1]
        instance.revision = max(instance.revision, reserved) + 1
        current = self._instances.get(key)
        # Never overwrite the revision record a newer instance already owns.
        if current is None or current is instance:
            self._revisions[key] = (instance.target, instance.revision)
        instance.mode = request.mode
        instance.root = _snapshot_content(request.root) if request.mode is RenderMode.REPLACE else None
        instance.visible_when = _snapshot_content(request.visible_when)
        instance.updated_at = time.monotonic()
        return RenderFrame(
            target=instance.target,
            revision=instance.revision,
            mode=instance.mode,
            root=instance.root,
            visible_when=instance.visible_when,
        )

    def restore(self, instance: RenderInstance, previous: RenderInstance) -> None:
        """Restore the previous state after a failed delivery without skipping revisions."""

        instance.revision = previous.revision
        self._revisions[instance.target.key] = (instance.target, instance.revision)
        instance.mode = previous.mode
        instance.root = previous.root
        instance.visible_when = previous.visible_when
        instance.updated_at = previous.updated_at

    @asynccontextmanager
    async def evict(self, expected: RenderInstance) -> AsyncIterator[bool]:
        """Hold the instance lock and confirm it is still the revision to evict."""

        async with self._lock_for(expected.target.key):
            current = self._instances.get(expected.target.key)
            try:
                yield current is expected and current.revision == expected.revision
            finally:
                self._reserved_evictions.discard(expected.target.key)

    def drop_instance(self, target: RenderTarget) -> bool:
        """Remove one instance from the active registry."""

        removed = self._instances.pop(target.key, None) is not None
        self._reserved_evictions.discard(target.key)
        if removed and self._lock_users.get(target.key, 0) == 0:
            self._locks.pop(target.key, None)
        return removed

    def drop_session(self, session_id: str) -> tuple[RenderInstance, ...]:
        """Drop session instances when their conversation ends or is archived."""

        return self._drop_matching(
            lambda target: (
                target.resolved_scope != SlotScope.APPLICATION.value and target.session_id == session_id
            ),
            forget_revisions=True,
        )

    def drop_package(self, package_id: str) -> tuple[RenderInstance, ...]:
        """Drop all instances owned by an unloaded package."""

        return self._drop_matching(lambda target: target.package_id == package_id)

    def retire_package(self, package_id: str) -> tuple[ClearFrame, ...]:
        """Drop a released package's instances and reserve the revision of their clear frames.

        A later render of the same target, for example after a reload, then starts
        above the clear frame the browser has already accepted.
        """

        frames: list[ClearFrame] = []
        for instance in self.drop_package(package_id):
            revision = instance.revision + 1
            self._revisions[instance.target.key] = (instance.target, revision)
            frames.append(ClearFrame(target=instance.target, revision=revision, reason="released"))
        return tuple(frames)

    def retire_instance(self, target: RenderTarget, reason: str) -> ClearFrame | None:
        """Drop one instance and reserve the revision of its clear frame, like ``retire_package``."""

        instance = self._instances.get(target.key)
        if instance is None:
            return None
        self.drop_instance(target)
        revision = instance.revision + 1
        self._revisions[target.key] = (instance.target, revision)
        return ClearFrame(target=instance.target, revision=revision, reason=reason)

    def drop_connection(self, connection_id: str) -> tuple[RenderInstance, ...]:
        """Release application instances without touching any chat session."""

        return self._drop_matching(
            lambda target: (
                target.resolved_scope == SlotScope.APPLICATION.value
                and target.connection_id == connection_id
            ),
            forget_revisions=True,
        )

    @asynccontextmanager
    async def _lock_for(self, key: str) -> AsyncIterator[None]:
        """Hold a reference-counted lock and reclaim it when an absent instance has no users."""

        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        self._lock_users[key] = self._lock_users.get(key, 0) + 1
        try:
            async with lock:
                yield
        finally:
            remaining = self._lock_users.get(key, 1) - 1
            if remaining > 0:
                self._lock_users[key] = remaining
            else:
                self._lock_users.pop(key, None)
                if key not in self._instances and self._locks.get(key) is lock:
                    self._locks.pop(key, None)

    def _drop_matching(self, predicate, *, forget_revisions: bool = False) -> tuple[RenderInstance, ...]:
        if forget_revisions:
            for key, (target, _) in list(self._revisions.items()):
                if predicate(target):
                    self._revisions.pop(key, None)
        dropped: list[RenderInstance] = []
        for key, instance in list(self._instances.items()):
            if not predicate(instance.target):
                continue
            dropped.append(instance)
            self._instances.pop(key, None)
            self._reserved_evictions.discard(key)
            if self._lock_users.get(key, 0) == 0:
                self._locks.pop(key, None)
        return tuple(dropped)

    def _evict_candidates(self, target: RenderTarget) -> tuple[RenderInstance, ...]:
        """Select the oldest instances in the capacity group for removal after delivery.

        Instances already reserved by a concurrent claim are excluded from the
        candidate list but still count against the group budget, so two claims in
        the same group never over-evict or exceed the cap.
        """

        if self._max_per_session <= 0 or target.key in self._instances:
            return ()
        group = [
            instance
            for instance in self._instances.values()
            if _same_capacity_group(instance.target, target)
        ]
        candidates = [instance for instance in group if instance.target.key not in self._reserved_evictions]
        reserved = len(group) - len(candidates)
        excess = len(candidates) + reserved - self._max_per_session + 1
        if excess <= 0:
            return ()
        candidates.sort(key=lambda instance: instance.updated_at)
        return tuple(candidates[:excess])


def _same_capacity_group(left: RenderTarget, right: RenderTarget) -> bool:
    """Session instances share a chat budget. Application instances share a connection budget."""

    if right.resolved_scope == SlotScope.APPLICATION.value:
        return (
            left.resolved_scope == SlotScope.APPLICATION.value
            and left.connection_id == right.connection_id
        )
    return (
        left.resolved_scope != SlotScope.APPLICATION.value
        and left.session_id == right.session_id
    )


def log_eviction(instance: RenderInstance) -> None:
    """Record a capacity eviction after the new frame has been delivered."""

    logger.warning("[GatewayWebUI] 淘汰最久未更新实例: %s", instance.target.key)


def _snapshot_content(value: Any) -> Any:
    """Retain the delivered declarations independently of mutable plugin inputs."""
    if isinstance(value, RenderNode):
        return RenderNode(
            component=value.component,
            props=_snapshot_content(value.props),
            children=tuple(_snapshot_content(child) for child in value.children),
            events=_snapshot_content(value.events),
        )
    if isinstance(value, Mapping):
        return MappingProxyType({key: _snapshot_content(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_snapshot_content(item) for item in value)
    return value
