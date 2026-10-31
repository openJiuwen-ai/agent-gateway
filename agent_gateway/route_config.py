# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Gateway route configuration and protocol-binding factories."""

from __future__ import annotations

import inspect
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from agent_gateway.routing.route_binding import GatewayRouteBinding


@dataclass
class RouteConfig:
    """单条路由的配置（/acp, /cli 等）。"""

    path: str
    channel_id: str
    forward_methods: frozenset[str] = frozenset()
    forward_no_local_handler_methods: frozenset[str] = frozenset()
    local_handlers: dict[str, Callable[..., Awaitable[None]]] = field(default_factory=dict)
    inbound_interceptor: Callable[..., Awaitable[bool]] | None = None
    outbound_interceptor: Callable[..., Awaitable[bool]] | None = None
    cleanup_handler: Callable[..., Any] | None = None
    disconnect_handler: Callable[..., Any] | None = None
    session_bind_handler: Callable[..., Any] | None = None
    # V2: 委托 ws 注册的外部 Channel（如 tui 的 TuiChannel）。
    # GatewayServer 仍作 ws 宿主 + 入站帧解析，但把 ws + RoutingKey 委托注册进该
    # Channel 的五维索引（_clients_by_key / _ws_by_id），出站由 ChannelManager 派发
    # 到该 Channel.send（按 delivery.ws_id 物理寻址）。None 表示该 route 无需委托
    # （如 acp 自带 _session_to_client 反查，web 有独立 WebChannel 不经此路径）。
    ws_channel: Any = None


@dataclass
class GatewayServerConfig:
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 19001
    routes: dict[str, RouteConfig] = field(default_factory=dict)
    path: str | None = None
    channel_id: str | None = None

    def __post_init__(self) -> None:
        if self.routes:
            return
        path = str(self.path or "").strip()
        channel_id = str(self.channel_id or "").strip()
        if path and channel_id:
            self.routes[path] = RouteConfig(path=path, channel_id=channel_id)


@dataclass(frozen=True)
class LocalHandlerContext:
    ws: Any
    req_id: str
    params: dict[str, Any]
    session_id: str
    user_id: str | None


def build_acp_route_binding(
        *,
        path: str,
        channel_id: str,
        forward_methods: frozenset[str],
        forward_no_local_handler_methods: frozenset[str],
        on_message_cb,
) -> GatewayRouteBinding:
    return GatewayRouteBinding(
        path=path,
        channel_id=channel_id,
        forward_methods=forward_methods,
        forward_no_local_handler_methods=forward_no_local_handler_methods,
    )


def build_route_config_map(bindings: list[GatewayRouteBinding]) -> dict[str, RouteConfig]:
    return {
        binding.path: RouteConfig(
            path=binding.path,
            channel_id=binding.channel_id,
            forward_methods=binding.forward_methods,
            forward_no_local_handler_methods=binding.forward_no_local_handler_methods,
            inbound_interceptor=binding.inbound_interceptor,
            outbound_interceptor=binding.outbound_interceptor,
            cleanup_handler=binding.cleanup_handler,
            disconnect_handler=binding.disconnect_handler,
            session_bind_handler=binding.session_bind_handler,
            ws_channel=binding.ws_channel,
        )
        for binding in bindings
    }
