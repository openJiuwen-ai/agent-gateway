# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Wire TUI and ACP without binding their public listener."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import os
from typing import Any

from agent_gateway.application.inbound import InboundQueue, MessagePreprocessor
from agent_gateway.routing.protocol_runtime import ProtocolRuntime, ProtocolRuntimeConfig, RouteConfig
from agent_gateway.routing.route_binding import GatewayRouteBinding

def _build_acp_route_binding(
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


def _build_route_config_map(bindings: list[GatewayRouteBinding]) -> dict[str, RouteConfig]:
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



@dataclass
class TuiAcpAssembly:
    tui_channel: Any
    inbound_queue: InboundQueue
    protocol_runtime: ProtocolRuntime

    async def stop(self) -> None:
        await self.protocol_runtime.stop()
        await self.inbound_queue.stop()
        await self.tui_channel.stop()


async def assemble_tui_acp(
    *,
    agent_client: Any,
    message_handler: Any,
    channel_manager: Any,
    third_agent: Any,
    on_config_saved: Callable[..., Any],
    cron_controller: Any,
    heartbeat_controller: Any,
    preprocessor: MessagePreprocessor,
    log_stage: Callable[[str], None],
) -> TuiAcpAssembly:
    from agent_gateway.channel_manager.web.app_web_handlers import (
        _FORWARD_REQ_METHODS, _FORWARD_NO_LOCAL_HANDLER_METHODS, _DummyBus,
    )
    client = agent_client
    from agent_gateway.channel_manager.tui.tui_channel import (
        TuiChannel,
        TuiChannelConfig,
    )
    from agent_gateway.channel_manager.tui.tui_connect import (
        CLI_FORWARD_NO_LOCAL_HANDLER_METHODS,
        CLI_FORWARD_REQ_METHODS,
        CliRouteBindParams,
        build_cli_route_binding,
    )

    tui_channel = TuiChannel(TuiChannelConfig(enabled=True), _DummyBus())
    tui_norm_and_forward = preprocessor.for_channel(
        channel_manager,
        CLI_FORWARD_REQ_METHODS,
        CLI_FORWARD_NO_LOCAL_HANDLER_METHODS,
        "TUI",
    )
    channel_manager.register_channel_with_inbound(tui_channel, tui_norm_and_forward)

    # Web/TUI 注册后再订阅连接钩子，否则 get_channel 拿不到 channel。
    subscribe_fn = getattr(client, "set_channel_manager", None)
    if callable(subscribe_fn):
        subscribe_fn(channel_manager)

    acp_inbound_server = InboundQueue(
        lambda msg: preprocessor.forward(msg, channel_manager)
    )
    await acp_inbound_server.start()

    route_bindings = [
        _build_acp_route_binding(
            path="/acp",
            channel_id="acp",
            forward_methods=_FORWARD_REQ_METHODS,
            forward_no_local_handler_methods=_FORWARD_NO_LOCAL_HANDLER_METHODS,
            on_message_cb=acp_inbound_server.handle_message,
        ),
        build_cli_route_binding(
            CliRouteBindParams(
                agent_client=client,
                message_handler=message_handler,
                third_agent=third_agent,
                on_config_saved=on_config_saved,
                path="/tui",
                channel_id="tui",
                cron_controller=cron_controller,
                heartbeat_controller=heartbeat_controller,
                ws_channel=tui_channel,
            )
        ),
    ]

    protocol_runtime_config = ProtocolRuntimeConfig(
        enabled=True,
        host=os.getenv("GATEWAY_HOST", "127.0.0.1"),
        port=int(os.getenv("GATEWAY_PORT", "19001")),
        routes=_build_route_config_map(route_bindings),
    )
    protocol_runtime = ProtocolRuntime(protocol_runtime_config, _DummyBus())
    protocol_runtime.message_handler_ref = message_handler
    for binding in route_bindings:
        route_config = protocol_runtime_config.routes[binding.path]
        # tui 出站已由 TuiChannel 接管（register_channel_with_inbound），
        # 不再把 protocol_runtime 注册为 tui channel；acp 等仍走 protocol_runtime。
        # 但 install（register_cli_handlers 注册 local handler 如 _chat_send）
        # 仍须执行——local handler 依赖 protocol_runtime 上的 session/request 索引。
        if route_config.channel_id != "tui":
            channel_manager.register_external_channel(route_config.channel_id, protocol_runtime)
        if binding.install is not None:
            binding.install(protocol_runtime)
    protocol_runtime.on_message(acp_inbound_server.handle_message)
    log_stage("gateway_routes_registered")
    return TuiAcpAssembly(tui_channel, acp_inbound_server, protocol_runtime)
