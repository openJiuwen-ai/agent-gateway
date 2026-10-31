# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.outbound_delivery import OutboundMessageDelivery


class FakeWs:
    def __init__(self, *, closed=False):
        self.closed = closed
        self.sent = []

    async def send(self, data):
        self.sent.append(data)


def build_delivery(
    *,
    request_to_client=None,
    session_to_client=None,
    routes=None,
    acp_bridge=None,
):
    request_to_client = request_to_client or {}
    session_to_client = session_to_client or {}
    routes = routes or {}

    def client_route_key(channel_id, scope, agent_ref=None):
        if not channel_id or not scope:
            return None
        if agent_ref is not None:
            return (channel_id, scope, agent_ref)
        return (channel_id, scope)

    def extract_routing_session_id(msg, *, include_top_level=True):
        if include_top_level:
            sid = getattr(msg, "session_id", None)
            if sid:
                return sid
        payload = getattr(msg, "payload", None)
        if isinstance(payload, dict):
            return payload.get("session_id")
        params = getattr(msg, "params", None)
        if isinstance(params, dict):
            return params.get("session_id")
        return None

    def lookup_client(table, channel_id, scope, agent_ref=None):
        return table.get((channel_id, scope, agent_ref))

    def find_channel_clients(channel_id):
        return [ws for key, ws in session_to_client.items() if key[0] == channel_id]

    return OutboundMessageDelivery(
        routes=routes,
        request_to_client=request_to_client,
        session_to_client=session_to_client,
        client_route_key=client_route_key,
        extract_routing_session_id=extract_routing_session_id,
        lookup_client=lookup_client,
        find_channel_clients=find_channel_clients,
        acp_bridge=acp_bridge,
    )


async def test_send_prefers_fan_out_target_over_request_key():
    request_ws = FakeWs()
    fanout_ws = FakeWs()
    msg = SimpleNamespace(
        id="req-1",
        type="event",
        event_type=None,
        payload={},
        channel_id="web",
        session_id="sess-1",
        params=None,
        agent_ref="agent:1",
    )
    delivery = build_delivery(
        request_to_client={("web", "req-1", "agent:1"): request_ws},
        session_to_client={("tui", "sess-2", "agent:1"): fanout_ws},
    )
    routing_target = SimpleNamespace(
        routing_keys=[
            SimpleNamespace(
                session_id="sess-2",
                channel_id="tui",
                agent_ref="agent:1",
            )
        ]
    )

    await delivery.send(msg, routing_target=routing_target)

    assert request_ws.sent == []
    assert len(fanout_ws.sent) == 1


async def test_send_falls_back_to_open_session_when_request_ws_is_closed():
    closed_ws = FakeWs(closed=True)
    session_ws = FakeWs()
    msg = SimpleNamespace(
        id="req-2",
        type="res",
        payload={"error": "boom", "code": "INTERNAL_ERROR"},
        ok=False,
        channel_id="web",
        session_id="sess-2",
        params=None,
        agent_ref=None,
    )
    delivery = build_delivery(
        request_to_client={("web", "req-2", None): closed_ws},
        session_to_client={("web", "sess-2", None): session_ws},
    )

    await delivery.send(msg)

    assert closed_ws.sent == []
    assert session_ws.sent == [
        '{"type": "res", "id": "req-2", "ok": false, "payload": '
        '{"error": "boom", "code": "INTERNAL_ERROR"}, '
        '"error": "boom", "code": "INTERNAL_ERROR"}'
    ]


async def test_send_without_session_broadcasts_to_channel_clients():
    first = FakeWs()
    second = FakeWs()
    msg = SimpleNamespace(
        id="req-3",
        type="event",
        event_type=None,
        payload=None,
        channel_id="web",
        session_id=None,
        params=None,
        agent_ref=None,
    )
    delivery = build_delivery(
        session_to_client={
            ("web", "", None): first,
            ("web", "other", None): second,
        }
    )

    await delivery.send(msg)

    assert first.sent == ['{"type": "event", "event": "chat.final", "payload": {"session_id": null, "content": ""}}']
    assert second.sent == first.sent


async def test_outbound_interceptor_can_stop_delivery():
    ws = FakeWs()
    handled = []
    interceptor_calls = []

    async def interceptor(msg, client_ws):
        interceptor_calls.append((msg, client_ws))
        handled.append(True)
        return True

    msg = SimpleNamespace(
        id="req-4",
        type="res",
        payload={},
        ok=True,
        channel_id="web",
        session_id="sess-4",
        params=None,
        agent_ref=None,
    )
    delivery = build_delivery(
        request_to_client={("web", "req-4", None): ws},
        routes={"/cli": SimpleNamespace(channel_id="web", outbound_interceptor=interceptor)},
    )

    await delivery.send(msg)

    assert interceptor_calls == [(msg, ws)]
    assert ws.sent == []
