# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.route_config import (
    GatewayServerConfig,
    RouteConfig,
    build_acp_route_binding,
    build_route_config_map,
)


def test_gateway_server_config_creates_route_from_path_and_channel():
    config = GatewayServerConfig(path="/cli", channel_id="cli")

    route = config.routes["/cli"]

    assert route.path == "/cli"
    assert route.channel_id == "cli"


def test_gateway_server_config_keeps_existing_routes():
    route = RouteConfig(path="/acp", channel_id="acp")
    config = GatewayServerConfig(routes={"/acp": route}, path="/web", channel_id="web")

    assert list(config.routes) == ["/acp"]
    assert config.routes["/acp"] is route


def test_build_acp_route_binding_discards_on_message_callback():
    binding = build_acp_route_binding(
        path="/acp",
        channel_id="acp",
        forward_methods=frozenset({"req"}),
        forward_no_local_handler_methods=frozenset({"req"}),
        on_message_cb=lambda msg: None,
    )

    assert binding.path == "/acp"
    assert binding.channel_id == "acp"
    assert binding.forward_methods == frozenset({"req"})
    assert binding.forward_no_local_handler_methods == frozenset({"req"})
    assert binding.install is None


def test_build_route_config_map_copies_binding_fields():
    interceptor = lambda msg: True
    binding = SimpleNamespace(
        path="/cli",
        channel_id="cli",
        forward_methods=frozenset({"res"}),
        forward_no_local_handler_methods=frozenset({"req"}),
        inbound_interceptor=interceptor,
        outbound_interceptor=None,
        cleanup_handler=lambda: None,
        disconnect_handler=lambda: None,
        session_bind_handler=lambda *args: None,
        ws_channel=None,
    )

    routes = build_route_config_map([binding])

    assert list(routes) == ["/cli"]
    route = routes["/cli"]
    assert route.path == "/cli"
    assert route.channel_id == "cli"
    assert route.forward_methods == frozenset({"res"})
    assert route.forward_no_local_handler_methods == frozenset({"req"})
    assert route.inbound_interceptor is interceptor
    assert route.outbound_interceptor is None
    assert route.cleanup_handler is not None
    assert route.disconnect_handler is not None
    assert route.session_bind_handler is not None
    assert route.ws_channel is None
