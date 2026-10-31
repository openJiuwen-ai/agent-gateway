# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.app_gateway import GatewayServer


class _AgentRef:
    mode = "plan"
    id = "agent-1"


def test_extract_ws_user_id_prefers_header():
    ws = SimpleNamespace(request=SimpleNamespace(headers={"X-User-Id": " user-1 "}))

    assert GatewayServer._extract_ws_user_id(ws) == "user-1"


def test_extract_ws_user_id_falls_back_to_query():
    ws = SimpleNamespace(path="/cli?user_id=user-2&x=y")

    assert GatewayServer._extract_ws_user_id(ws) == "user-2"


def test_extract_ws_user_id_rejects_invalid_and_missing_ids():
    ws = SimpleNamespace(request=SimpleNamespace(headers={"X-User-Id": "bad/id"}))

    assert GatewayServer._extract_ws_user_id(ws) is None


def test_connection_identity_defaults():
    assert GatewayServer._connection_user_id(None) is None
    assert GatewayServer._connection_user_id(SimpleNamespace(_gateway_user_id=" user-3 ")) == "user-3"
    assert GatewayServer._connection_agent_type(None) == "jiuwenswarm"
    assert GatewayServer._connection_agent_type(SimpleNamespace(_gateway_agent_type="Agent-A")) == "agent-a"


def test_client_route_key_shapes():
    assert GatewayServer._client_route_key("web", "sess-1", _AgentRef()) == (
        "web", "sess-1", "plan:agent-1"
    )
    assert GatewayServer._client_route_key("web", "sess-1", {"mode": "code", "id": "agent-2"}) == (
        "web", "sess-1", "code:agent-2"
    )
    assert GatewayServer._client_route_key("web", "sess-1", "agent:3") == (
        "web", "sess-1", "agent:3"
    )
    assert GatewayServer._client_route_key("web", "sess-1") == ("web", "sess-1")
    assert GatewayServer._client_route_key("", "sess-1") is None


def test_extract_routing_session_id_order():
    top_level = SimpleNamespace(session_id="top")
    payload_level = SimpleNamespace(session_id=None, payload={"session_id": "payload"})
    params_level = SimpleNamespace(session_id=None, params={"session_id": "params"})

    assert GatewayServer._extract_routing_session_id(top_level) == "top"
    assert GatewayServer._extract_routing_session_id(payload_level) == "payload"
    assert GatewayServer._extract_routing_session_id(params_level) == "params"


def test_ws_open_check():
    assert GatewayServer._ws_is_open(None) is False
    assert GatewayServer._ws_is_open(SimpleNamespace(closed=True)) is False
    assert GatewayServer._ws_is_open(SimpleNamespace(closed=False)) is True
