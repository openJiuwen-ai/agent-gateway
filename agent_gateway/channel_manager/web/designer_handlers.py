# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Gateway proxies for the Designer execution-graph RPCs."""

from __future__ import annotations

from agent_gateway.routing.e2a_proxy import proxy_unary_request

_DESIGNER_METHODS: tuple[tuple[str, float | None], ...] = (
    ("designer.workspace.create", None),
    ("designer.workspace.get", None),
    ("designer.workspace.session.create", None),
    ("designer.workspace.session.compose", None),
    ("designer.graph.get", None),
    ("designer.graph.list", None),
    ("designer.graph.save", None),
    ("designer.graph.patch", None),
    ("designer.media.capabilities", None),
    ("designer.run.start", None),
    ("designer.run.get", None),
    ("designer.run.pause", None),
    ("designer.run.cancel", None),
)


def register_designer_handlers(channel, resolve_client):
    def proxy(req_method: str, timeout_seconds: float | None):
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
                label=req_method,
                timeout_seconds=timeout_seconds,
            )

        return handler

    for req_method, timeout_seconds in _DESIGNER_METHODS:
        channel.register_method(req_method, proxy(req_method, timeout_seconds))
