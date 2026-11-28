# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Keep static AS command deduplication local to its admitted owner."""

from __future__ import annotations

import asyncio
from typing import Any

from agent_gateway.application.worker_cron_commands import WorkerCronCommands
from agent_gateway.common.southbound_routes import SouthboundTarget


class RoutedWorkerCronCommands:
    """A source cannot choose another target by supplying a payload owner."""

    def __init__(self, targets: tuple[SouthboundTarget, ...], **runtime_options: Any) -> None:
        self._runtimes = {target.user_id: WorkerCronCommands(**runtime_options) for target in targets}

    def remember(self, envelope: Any, metadata: dict | None) -> None:
        runtime = self._runtimes.get(envelope.user_id)
        if runtime is not None:
            runtime.remember(envelope, metadata)

    def complete(self, request_id: str) -> None:
        for runtime in self._runtimes.values():
            runtime.complete(request_id)

    async def handle_push(self, *, user_id: str | None, metadata: dict | None, **fields: Any) -> bool:
        if user_id not in self._runtimes or not isinstance(metadata, dict):
            return False
        if metadata.get('_jiuwenswarm_cron_owner_user_id') != user_id:
            return False
        return await self._runtimes[user_id].handle_push(metadata=metadata, user_id=user_id, **fields)

    async def stop(self) -> None:
        await asyncio.gather(*(runtime.stop() for runtime in self._runtimes.values()))
