# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""AgentServer prewarm synchronization for Gateway startup."""

from __future__ import annotations

import asyncio
import logging
import uuid as uuid_module
from typing import Any

from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
from openjiuwen_protocol.message import ReqMethod

from agent_gateway.runtime.agent_prewarm import prewarm_enabled_by_env

logger = logging.getLogger("gateway")

_AGENT_PREWARM_EXCLUDED_CHANNELS = frozenset({"acp", "a2a"})


class AgentPrewarmCoordinator:
    """Coalesce channel/config churn into agent.prewarm.sync requests."""

    def __init__(
        self,
        *,
        client: Any,
        channel_manager: Any,
        using_adx_router: bool,
    ) -> None:
        self.client = client
        self.channel_manager = channel_manager
        self.using_adx_router = using_adx_router
        self._debounce_task: asyncio.Task[None] | None = None

    @property
    def debounce_task(self) -> asyncio.Task[None] | None:
        return self._debounce_task

    @property
    def should_run_periodic(self) -> bool:
        return not self.using_adx_router and prewarm_enabled_by_env()

    async def sync_agent_prewarm_channels(self) -> None:
        if self.using_adx_router or not prewarm_enabled_by_env():
            return
        try:
            prewarm_channels = {
                channel
                for channel in self.channel_manager.enabled_channels
                if channel.lower() not in _AGENT_PREWARM_EXCLUDED_CHANNELS
            }
            env = e2a_from_agent_fields(
                request_id=f"agent-prewarm-sync-{uuid_module.uuid4().hex[:8]}",
                channel_id="",
                req_method=ReqMethod.AGENT_PREWARM_SYNC,
                params={
                    "enabled_channels": sorted(prewarm_channels),
                },
            )
            resp = await self.client.send_request(env)
            if not getattr(resp, "ok", False):
                raise RuntimeError(
                    f"agent.prewarm.sync rejected: {getattr(resp, 'payload', None)}"
                )
        except Exception as exc:  # noqa: BLE001 - prewarm never blocks Gateway config
            logger.warning("[App] agent.prewarm.sync failed (non-fatal): %s", exc)

    async def periodic_agent_prewarm_sync(self) -> None:
        while True:
            await asyncio.sleep(60)
            await self.sync_agent_prewarm_channels()

    def schedule_agent_prewarm_sync(
        self, name: str, *, delay_seconds: float = 1.0
    ) -> None:
        """Coalesce startup/config/channel churn into one settled sync."""
        if self.using_adx_router or not prewarm_enabled_by_env():
            return
        previous = self._debounce_task
        if previous is not None and not previous.done():
            previous.cancel()

        async def _delayed_sync() -> None:
            try:
                await asyncio.sleep(max(0.0, delay_seconds))
                await self.sync_agent_prewarm_channels()
            except asyncio.CancelledError:
                return

        self._debounce_task = asyncio.create_task(
            _delayed_sync(), name=name
        )

    async def stop_debounce(self) -> None:
        if self._debounce_task is not None:
            self._debounce_task.cancel()
            try:
                await self._debounce_task
            except asyncio.CancelledError:
                pass
