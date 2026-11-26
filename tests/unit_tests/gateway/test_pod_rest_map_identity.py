# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""REST 北向请求透传平台展示身份（仅企业版）。"""

from __future__ import annotations

import pytest
from e2a.models import E2AEnvelope

from agent_gateway.southbound.data import rest_map as pod_rest_map
from agent_gateway.extensions.runtime_management_extension.pod_rest_map import (
    assemble_rest_request,
)

_AGENT_ID = "EyMFedy4mZ0b8btF1gxJFjsq4FOl7Pj6m2EYP3Ecmu7"


def _envelope(method: str, **kwargs) -> E2AEnvelope:
    return E2AEnvelope(
        request_id="req-1",
        channel="web",
        method=method,
        session_id="sess-1",
        params={"session_id": "sess-1"},
        **kwargs,
    )


def test_identity_headers_carry_agent_display_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pod_rest_map, "is_enterprise", lambda: True)
    envelope = _envelope(
        "chat.send",
        user_id="u",
        channel_context={
            "routing": {"group_id": "g", "bot_id": "b"},
            "agent_identity": {
                "agent_name": "agent_" + _AGENT_ID,
                "agent_id": _AGENT_ID,
                "runtime_template_id": "rt-1",
                "agent_template_id": "tpl-1",
            },
        },
    )
    headers = assemble_rest_request(envelope, base_url="http://gateway.test:8766").headers
    assert headers["X-Agent-Identity-Id"] == _AGENT_ID
    assert headers["X-Runtime-Template-Id"] == "rt-1"
    assert headers["X-Agent-Template-Id"] == "tpl-1"
    assert "X-Agent-Name" not in headers
    assert headers.get("X-Agent-Id") != _AGENT_ID


def test_identity_headers_absent_without_display_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pod_rest_map, "is_enterprise", lambda: True)
    envelope = _envelope(
        "chat.send",
        user_id="u",
        channel_context={"routing": {"group_id": "g", "bot_id": "b"}},
    )
    headers = assemble_rest_request(envelope, base_url="http://gateway.test:8766").headers
    for name in (
        "X-Agent-Name",
        "X-Agent-Identity-Id",
        "X-Runtime-Template-Id",
        "X-Agent-Template-Id",
    ):
        assert name not in headers


def test_identity_headers_personal_skips_display_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pod_rest_map, "is_enterprise", lambda: False)
    envelope = _envelope(
        "chat.send",
        user_id="u",
        channel_context={
            "routing": {"group_id": "g", "bot_id": "b"},
            "agent_identity": {
                "agent_name": "agent_" + _AGENT_ID,
                "agent_id": _AGENT_ID,
                "runtime_template_id": "rt-1",
                "agent_template_id": "tpl-1",
            },
        },
    )
    headers = assemble_rest_request(envelope, base_url="http://gateway.test:8766").headers
    for name in (
        "X-Agent-Name",
        "X-Agent-Identity-Id",
        "X-Runtime-Template-Id",
        "X-Agent-Template-Id",
    ):
        assert name not in headers
