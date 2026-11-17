# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Compatibility exports for the former GatewayServer module.

New consumers should import ProtocolRuntime from routing.protocol_runtime.
"""

from agent_gateway.routing.protocol_runtime import (
    ProtocolRuntime,
    ProtocolRuntimeConfig,
    RouteConfig,
    _build_event_frame,
)

GatewayServer = ProtocolRuntime
GatewayServerConfig = ProtocolRuntimeConfig
