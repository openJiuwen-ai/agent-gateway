# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Assemble one Worker's dynamic-instance southbound from its private spec.

This is the whole construction the Worker seam needs for a spec that carries an
instance provider: the ADX leaf that owns the Environments, and the routed
client the consumers talk to. Construction is all-or-nothing. Closing is
ordered: sockets first, then the local leaf. Nothing is created, dialled or
deleted before a request asks for it, and nothing is deleted on close.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

import httpx

from agent_gateway.common.instance_routes import InstanceProviderSpec

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from agent_gateway.application.worker_instance_routed_client import WorkerInstanceRoutedClient
    from agent_gateway.extensions.adx.config import AdxRouterConfig
    from agent_gateway.extensions.adx.instance_backend import WorkerInstanceBackend


class InstanceSouthbound:
    """The routed client of one Worker and the leaf that owns its Environments."""

    def __init__(self, backend: WorkerInstanceBackend, client: WorkerInstanceRoutedClient, *, idle_interval: float) -> None:
        self.backend = backend
        self.client = client
        self._idle_interval = idle_interval
        self._idle_task: asyncio.Task | None = None

    @property
    def required_tasks(self) -> tuple[Any, ...]:
        return () if self._idle_task is None else (self._idle_task,)

    def start_idle_reaper(self) -> None:
        if self._idle_interval > 0 and self._idle_task is None:
            self._idle_task = asyncio.create_task(self._reap_idle(), name='gateway-adx-idle-reaper')

    async def _reap_idle(self) -> None:
        while True:
            await asyncio.sleep(self._idle_interval)
            try:
                await self.backend.reap_idle_once()
                await self.client.retire_stale_connections()
            except Exception:
                logger.exception('ADX Worker idle cleanup failed')

    async def close(self, *, deadline: float | None = None) -> None:
        """Close the sockets, then the local leaf. No Environment is deleted.

        The leaf's own close is local work that sends nothing to ADX, so it
        keeps a short budget even when the sockets used all of theirs.
        """
        leaf_deadline = deadline
        if deadline is not None:
            leaf_deadline = max(deadline, asyncio.get_running_loop().time() + 1.0)
        try:
            if self._idle_task is not None:
                self._idle_task.cancel()
                await asyncio.gather(self._idle_task, return_exceptions=True)
            await self.client.disconnect(deadline=deadline)
        finally:
            await self.backend.close(deadline=leaf_deadline)


async def build_instance_southbound(
    spec: InstanceProviderSpec, config: AdxRouterConfig, *, slot_id: int,
    transport: httpx.AsyncBaseTransport | None = None,
) -> InstanceSouthbound:
    """Start the leaf, then bind a routed client to it.

    The client takes the leaf's dial credential and network admission as
    hooks: neither becomes part of a route, a lease or the client's state.
    A failure after the leaf started closes it, so nothing is left behind.
    """
    from agent_gateway.application.worker_instance_routed_client import WorkerInstanceRoutedClient
    from agent_gateway.extensions.adx.instance_backend import WorkerInstanceBackend

    backend = WorkerInstanceBackend(spec, config, slot_id=slot_id, transport=transport)
    provider = await backend.start()
    try:
        client = WorkerInstanceRoutedClient(
            provider, dial_headers=backend.dial_headers, admit_network=backend.admit_network,
        )
    except BaseException:
        await backend.close()
        raise
    return InstanceSouthbound(backend, client, idle_interval=config.router.sandbox_idle_check_interval_seconds)


__all__ = ['InstanceSouthbound', 'build_instance_southbound']
