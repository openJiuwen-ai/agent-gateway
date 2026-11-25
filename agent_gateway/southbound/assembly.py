# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Southbound module composition for the fixed AgentServer deployment."""

from agent_gateway.southbound.contracts import ConnectionTarget, TransportKind
from agent_gateway.southbound.control.local import FixedTargetControl
from agent_gateway.southbound.data.transport import StaticWebSocketTransport
from agent_gateway.southbound.orchestration.client import SouthboundAgentClient
from agent_gateway.southbound.orchestration.static_router import StaticRouter


def build_static_client(uri: str) -> SouthboundAgentClient:
    target = ConnectionTarget(TransportKind.WEBSOCKET, uri)
    control = FixedTargetControl(target)
    return SouthboundAgentClient(control, StaticRouter(control), StaticWebSocketTransport(target))
