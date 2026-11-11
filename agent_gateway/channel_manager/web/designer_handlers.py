# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Gateway proxies for the Designer execution-graph RPCs."""

from __future__ import annotations

from agent_gateway.common.gateway_methods import GatewayMethod
from agent_gateway.routing.e2a_proxy import proxy_unary_request

_DESIGNER_METHODS: tuple[tuple[GatewayMethod, float | None], ...] = (
    (GatewayMethod.DESIGNER_WORKSPACE_CREATE, None),
    (GatewayMethod.DESIGNER_WORKSPACE_GET, None),
    (GatewayMethod.DESIGNER_WORKSPACE_SESSION_CREATE, None),
    (GatewayMethod.DESIGNER_WORKSPACE_SESSION_COMPOSE, None),
    (GatewayMethod.DESIGNER_GRAPH_GET, None),
    (GatewayMethod.DESIGNER_GRAPH_LIST, None),
    (GatewayMethod.DESIGNER_GRAPH_SAVE, None),
    (GatewayMethod.DESIGNER_GRAPH_PATCH, None),
    (GatewayMethod.DESIGNER_MEDIA_CAPABILITIES, None),
    (GatewayMethod.DESIGNER_RUN_START, None),
    (GatewayMethod.DESIGNER_RUN_GET, None),
    (GatewayMethod.DESIGNER_RUN_PAUSE, None),
    (GatewayMethod.DESIGNER_RUN_CANCEL, None),
)


def register_designer_handlers(channel, resolve_client):
    def proxy(req_method: GatewayMethod, timeout_seconds: float | None):
        async def handler(ws, req_id, params, session_id, user_id=None):
            await proxy_unary_request(
                channel=channel,
                agent_client=resolve_client(),
                ws=ws,
                req_id=req_id,
                params=params if isinstance(params, dict) else {},
                session_id=session_id,
                user_id=user_id,
                req_method=req_method,
                label=getattr(req_method, "value", req_method),
                timeout_seconds=timeout_seconds,
            )

        return handler

    for req_method, timeout_seconds in _DESIGNER_METHODS:
        channel.register_method(getattr(req_method, "value", req_method), proxy(req_method, timeout_seconds))
