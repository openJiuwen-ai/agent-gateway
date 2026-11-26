import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.channel_manager.web import invoke as web_invoke


@pytest.mark.asyncio
@pytest.mark.parametrize("enterprise,should_warn", [(False, False), (True, True)])
async def test_missing_bot_id_warning_is_enterprise_only(
    monkeypatch, caplog, enterprise, should_warn
):
    monkeypatch.setattr(web_invoke, "is_enterprise", lambda: enterprise)
    monkeypatch.setattr(
        web_invoke,
        "enterprise_skill_upload_denial",
        AsyncMock(return_value=None),
    )

    channel = SimpleNamespace(
        _method_handlers={},
        _local_only_methods=set(),
        _on_message_cb=AsyncMock(return_value=True),
    )
    message = SimpleNamespace(metadata={"routing": {}})

    with caplog.at_level(logging.WARNING, logger=web_invoke.logger.name):
        await web_invoke.dispatch_web_request(
            channel,
            method="chat.send",
            params={},
            request_id="request-id",
            outbound=None,
            session_id="session-id",
            user_message=message,
        )

    warnings = [
        record
        for record in caplog.records
        if "missing bot_id" in record.getMessage()
    ]
    assert bool(warnings) is should_warn
