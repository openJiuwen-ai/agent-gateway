# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web import invoke as web_invoke
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
from agent_gateway.enterprise.capabilities.skill_upload.policy import POLICY_UNAVAILABLE


@pytest.mark.asyncio
async def test_ws_rejects_enterprise_write_before_extension_interception(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.send_response = AsyncMock()
    channel.ensure_extensions = AsyncMock()
    callback = AsyncMock(return_value=True)
    channel.on_message(callback)

    ws = SimpleNamespace()
    await channel._handle_request_message(
        ws,
        {"is_stream": False, "request_context": {"headers": {}}},
        {},
        "req-forbidden",
        "plugins.install",
        {},
    )

    channel.send_response.assert_awaited_once_with(
        ws,
        "req-forbidden",
        ok=False,
        error="企业版配置由管理面统一下发",
        code="FORBIDDEN",
    )
    channel.ensure_extensions.assert_not_awaited()
    callback.assert_not_awaited()


def test_skill_import_upload_bypasses_shared_write_guard(monkeypatch):
    """The upload gate owns skills.import_upload; the shared guard must not reject it."""
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")

    assert web_invoke.is_enterprise_write_forbidden("skills.import_upload") is False


@pytest.mark.asyncio
async def test_ws_import_upload_rejects_without_trusted_template(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.send_response = AsyncMock()
    channel.ensure_extensions = AsyncMock()
    ws = SimpleNamespace()

    await channel._handle_request_message(
        ws,
        {"is_stream": False},
        {},
        "req-upload",
        "skills.import_upload",
        {},
    )

    channel.send_response.assert_awaited_once_with(
        ws,
        "req-upload",
        ok=False,
        error="skill upload policy cannot be verified: missing credentials or context",
        code=POLICY_UNAVAILABLE,
    )
    channel.ensure_extensions.assert_not_awaited()
