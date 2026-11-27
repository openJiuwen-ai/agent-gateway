# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Routing policy for one configured, fixed AgentServer."""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from agent_gateway.southbound.control.interfaces import StaticControl
from agent_gateway.southbound.orchestration.interfaces import RouteRequest, RouteSelection


class StaticRouter:
    def __init__(self, control: StaticControl) -> None:
        self._control = control

    @asynccontextmanager
    async def resolve(self, request: RouteRequest) -> AsyncIterator[RouteSelection]:
        target = self._control.get_target()
        if request.origin is not None and request.origin.target != target:
            raise ValueError("request origin does not match the configured AgentServer")
        yield RouteSelection((target,), session_id=request.envelope.session_id)
