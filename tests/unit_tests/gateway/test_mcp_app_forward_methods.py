# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""MCP Apps host-bridge requests (mcp_app.*) are forwarded to AgentServer by the standalone gateway."""

from agent_gateway.channel_manager.web.app_web_handlers import (
    _FORWARD_NO_LOCAL_HANDLER_METHODS,
    _FORWARD_REQ_METHODS,
)
from jiuwenswarm.common.schema.message import ReqMethod

MCP_APP_METHODS = {
    "mcp_app.list_tools",
    "mcp_app.read_resource",
    "mcp_app.call_tool",
}


def test_mcp_app_methods_are_plain_forwards():
    assert MCP_APP_METHODS <= _FORWARD_REQ_METHODS
    assert MCP_APP_METHODS <= _FORWARD_NO_LOCAL_HANDLER_METHODS


def test_mcp_app_methods_resolve_to_a_request_method():
    # Cross-repo contract: the AgentServer decodes the forwarded name with its
    # own ReqMethod. The Gateway itself only forwards wire strings.
    known = {item.value for item in ReqMethod}
    assert MCP_APP_METHODS <= known
