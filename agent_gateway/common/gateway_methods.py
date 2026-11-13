# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway-owned transport behavior; business methods belong to AgentServer.

Local methods are registered with their handlers, not duplicated in a catalog.
These acknowledgements are the only local handlers that also forward requests.
"""

ACK_METHODS: frozenset[str] = frozenset({
    "chat.send",
    "chat.resume",
    "chat.interrupt",
    "chat.user_answer",
    "chat.swarmflow_reply",
    "history.get",
})

# ACP has its own external JSON-RPC entry points.
ACP_METHODS: frozenset[str] = frozenset({
    "initialize",
    "session/new",
    "session/prompt",
    "session/cancel",
    "session/list",
    "session/load",
})
