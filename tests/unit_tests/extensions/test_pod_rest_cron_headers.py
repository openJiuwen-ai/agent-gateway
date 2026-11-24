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
    assembled = assemble_rest_request(envelope, base_url="http://10.0.0.8:8766")
    assert assembled.headers["X-Cron-Meta"] == headers["X-Cron-Meta"]
    assert "cron" not in (assembled.json_body or {})
