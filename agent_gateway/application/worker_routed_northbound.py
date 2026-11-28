# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Bind the shared northbound handlers of a Worker to its static route tables.

A static Worker reuses the Web, TUI and ACP handlers of the single AgentServer
gateway. They keep shared tables (request id, session id, channel) that one
user's traffic used to fill alone. Nothing of those tables is rewritten. The
Worker instead binds every outbound frame to one named user:

* ``MessageHandler`` names the user of each response it publishes, from the
  route of the request the response answers. A response whose request is
  unknown keeps what it declares.
* ``OwnerDelivery`` is installed on the Web channel, the TUI channel and the
  protocol runtime. A frame reaches a socket only when that socket's handshake
  user is the owner of the frame. A frame without an owner is not delivered
  by session fan-out or broadcast.
* Stream work is cancelled by a request of its own user only, and an ACP
  prompt is routed by the user its connection declared.

In scope of the named-user routes: replies to a request, the stream events of
a request, and session events for a session the target confirmed. Out of scope
and therefore dropped on a static Worker: session-less broadcasts (health
check relay, proactive notices, ``models.updated``, ``push_terminated``), and
every push whose request or session route no owned target confirmed.
"""

from __future__ import annotations

from typing import Any

from agent_gateway.routing.owner_delivery import OwnerDelivery

__all__ = ["bind_routed_delivery"]


def bind_routed_delivery(routed: Any, *, handler: Any, web: Any, tui: Any) -> OwnerDelivery:
    """Install the named-owner gate of one routed Worker on its shared handlers.

    Call before a northbound runtime starts, after the handlers exist. The same
    gate object goes to every outbound layer so that they resolve owners alike.
    """
    for name, value in (("handler", handler), ("web", web), ("tui", tui)):
        if value is None:
            raise ValueError(f"routed delivery requires the {name}")
    if getattr(routed, "user_routing", False) is not True:
        raise ValueError("routed delivery requires a routed Worker client")
    protocol = getattr(tui, "protocol_runtime", None)
    if protocol is None:
        raise ValueError("routed delivery requires the protocol runtime")
    channel = getattr(tui, "tui_channel", None)
    if channel is None:
        raise ValueError("routed delivery requires the TUI channel")
    gate = OwnerDelivery(request_owner=routed.request_owner, session_owner=routed.session_owner)
    handler.set_owner_delivery(True, request_owner=routed.request_owner)
    web.owner_delivery = gate
    channel.owner_delivery = gate
    protocol.owner_delivery = gate
    bridge = getattr(protocol, "_acp_bridge", None)
    if bridge is not None:
        # An ACP prompt Message names the user of its connection.
        bridge.bind_request_user = True
    return gate


__all__ = ["bind_routed_delivery"]
