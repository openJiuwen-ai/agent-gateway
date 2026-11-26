# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""HTTP identity headers ferry channel_context.cron like the WebSocket path."""

from __future__ import annotations

from e2a.models import E2AEnvelope

from agent_gateway.extensions.runtime_management_extension.pod_rest_map import (
    assemble_rest_request,
    identity_headers,
)


def test_identity_headers_forward_cron_targets_and_meta() -> None:
    envelope = E2AEnvelope(
        request_id="cron-job-1:1",
        channel="web",
        method="chat.send",
        session_id="web_sess",
        user_id="user-1",
        channel_context={
            "routing": {"group_id": "g1", "bot_id": "b1"},
            "targets": "web",
            "cron": {"job_id": "job-1", "run_id": "job-1:1", "timezone": "Asia/Shanghai"},
        },
    )
    headers = identity_headers(envelope, accept="text/event-stream")
    assert headers["X-Cron-Targets"] == "web"
    assert headers["X-Cron-Meta"] == (
        '{"job_id":"job-1","run_id":"job-1:1","timezone":"Asia/Shanghai"}'
    )
    assembled = assemble_rest_request(envelope, base_url="http://agentserver.test:8766")
    assert assembled.headers["X-Cron-Meta"] == headers["X-Cron-Meta"]
    assert "cron" not in (assembled.json_body or {})


def test_identity_headers_forward_non_default_a4p_web_route() -> None:
    """A4P authorize REST must carry resolved app_id/agent_ref, not only tenant ids."""
    envelope = E2AEnvelope(
        request_id="rpc-1",
        channel="web",
        method="a4p.authorization.pending",
        session_id="session-1",
        user_id="alice",
        channel_context={
            "ws_id": "ws-1",
            "app_id": "app-1",
            "agent_ref": {"mode": "team", "id": "research"},
            "routing": {"group_id": "group-1", "bot_id": "bot-1"},
        },
    )
    headers = identity_headers(envelope, accept="application/json")
    assert headers["X-App-Id"] == "app-1"
    assert headers["X-Agent-Ref-Mode"] == "team"
    assert headers["X-Agent-Ref-Id"] == "research"
    assert headers["X-Ws-Id"] == "ws-1"
    assert headers["X-Group-Id"] == "group-1"
    assert headers["X-Bot-Id"] == "bot-1"
    assembled = assemble_rest_request(envelope, base_url="http://agentserver.test:8766")
    assert assembled.headers["X-App-Id"] == "app-1"
    assert assembled.headers["X-Agent-Ref-Id"] == "research"
    assert "app_id" not in (assembled.json_body or {})
    assert "agent_ref" not in (assembled.json_body or {})


def test_identity_headers_preserve_empty_a4p_route_for_http_outbound() -> None:
    """Empty app/agent dims must be sent so AgentServer skips them (not default)."""
    envelope = E2AEnvelope(
        request_id="rpc-1",
        channel="web",
        method="a4p.authorization.pending",
        session_id="session-1",
        user_id="alice",
        channel_context={
            "ws_id": "http-out-1",
            "app_id": "",
            "agent_ref": {"mode": "", "id": ""},
            "routing": {"group_id": "group-1", "bot_id": "bot-1"},
        },
    )
    headers = identity_headers(envelope, accept="application/json")
    assert headers["X-App-Id"] == ""
    assert headers["X-Agent-Ref-Mode"] == ""
    assert headers["X-Agent-Ref-Id"] == ""
    assert "X-Ws-Id" in headers
