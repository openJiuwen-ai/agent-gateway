# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A client attachment's requests (client.*) are forwarded to AgentServer by the standalone gateway."""

from openjiuwen_protocol.message import ReqMethod

import agent_gateway  # noqa: F401  registers the methods the pinned protocol has not published
from agent_gateway.channel_manager.web.app_web_handlers import (
    _FORWARD_NO_LOCAL_HANDLER_METHODS,
    _FORWARD_REQ_METHODS,
)

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
    # The forward path looks a method up in ReqMethod by value and answers "unknown method" when it is missing.
    known = {item.value for item in ReqMethod}
    assert CLIENT_METHODS <= known
