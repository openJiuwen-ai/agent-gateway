# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A client attachment's requests (client.*) are forwarded to AgentServer by the standalone gateway."""

from agent_gateway.channel_manager.web.app_web_handlers import (
    _FORWARD_NO_LOCAL_HANDLER_METHODS,
    _FORWARD_REQ_METHODS,
)
from jiuwenswarm.common.schema.message import ReqMethod

CLIENT_METHODS = {
    "client.attach",
    "client.renew",
    "client.list",
    "client.conversation.messages",
    "client.conversations",
}


def test_client_methods_are_plain_forwards():
    assert CLIENT_METHODS <= _FORWARD_REQ_METHODS
    assert CLIENT_METHODS <= _FORWARD_NO_LOCAL_HANDLER_METHODS


def test_client_methods_resolve_to_a_request_method():
    # Cross-repo contract: these methods must exist in the protocol enum the
    # AgentServer decodes with, otherwise the forward fails server-side.
    # Gateway itself no longer needs the enum—it forwards wire strings.
    known = {item.value for item in ReqMethod}
    assert CLIENT_METHODS <= known
