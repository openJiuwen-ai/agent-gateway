import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.channel_manager.web import invoke as web_invoke


@pytest.fixture
def capture_web_invoke_logs(caplog):
    """Capture web_invoke logger records without relying on root propagation."""
    caplog.set_level(logging.WARNING)
    handler = caplog.handler
    old_level = web_invoke.logger.level
    web_invoke.logger.setLevel(logging.WARNING)
    web_invoke.logger.addHandler(handler)
    try:
        yield caplog
    finally:
        web_invoke.logger.removeHandler(handler)
        web_invoke.logger.setLevel(old_level)


@pytest.fixture
def dispatch_channel():
    return SimpleNamespace(
        _method_handlers={},
        _local_only_methods=set(),
        _on_message_cb=AsyncMock(return_value=True),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("enterprise,should_warn", [(False, False), (True, True)])
async def test_missing_bot_id_warning_is_enterprise_only(
    monkeypatch, capture_web_invoke_logs, dispatch_channel, enterprise, should_warn
):
    monkeypatch.setattr(web_invoke, "is_enterprise", lambda: enterprise)
    monkeypatch.setattr(
        web_invoke,
        "enterprise_skill_upload_denial",
        AsyncMock(return_value=None),
    )

    message = SimpleNamespace(metadata={"routing": {}})
    await web_invoke.dispatch_web_request(
        dispatch_channel,
        method="chat.send",
        params={},
        request_id="request-id",
        outbound=None,
        session_id="session-id",
        user_message=message,
    )

    warnings = [
        record
        for record in capture_web_invoke_logs.records
        if "missing bot_id" in record.getMessage()
    ]
    assert bool(warnings) is should_warn
    dispatch_channel._on_message_cb.assert_awaited_once()


@pytest.mark.asyncio
async def test_local_only_unknown_method_is_not_forwarded(monkeypatch):
    monkeypatch.setattr(
        web_invoke,
        "enterprise_skill_upload_denial",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(web_invoke, "is_enterprise_write_forbidden", lambda method: False)

    send_response = AsyncMock()
    on_message_cb = AsyncMock(return_value=True)
    channel = SimpleNamespace(
        _method_handlers={},
        _local_only_methods={"demo.method"},
        _on_message_cb=on_message_cb,
        send_response=send_response,
    )
    message = SimpleNamespace(metadata={"routing": {}})

    await web_invoke.dispatch_web_request(
        channel,
        method="demo.method",
        params={},
        request_id="request-id",
        outbound=None,
        session_id="session-id",
        user_message=message,
    )

    send_response.assert_awaited_once()
    assert send_response.await_args.kwargs["code"] == "METHOD_NOT_FOUND"
    on_message_cb.assert_not_awaited()
