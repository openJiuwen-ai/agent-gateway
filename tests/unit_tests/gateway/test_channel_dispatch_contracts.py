"""Regression checks for channel dispatch and authenticated TUI requests."""
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.channel_manager.base import BaseChannel
from agent_gateway.channel_manager.tui.tui_connect import CliHandlersBindParams, register_cli_handlers


@pytest.mark.asyncio
async def test_missing_send_is_reported_as_unsupported():
    with pytest.raises(NotImplementedError):
        await BaseChannel.send(SimpleNamespace(channel_id="test"), SimpleNamespace())


@pytest.mark.asyncio
async def test_tui_cron_lookup_uses_authenticated_socket_owner():
    methods = {}
    def register(_path, name, handler):
        methods[name] = handler

    channel = SimpleNamespace(register_local_handler=register, send_response=AsyncMock())
    cron = SimpleNamespace(list_jobs=AsyncMock(return_value=[]))
    register_cli_handlers(CliHandlersBindParams(channel=channel, cron_controller=cron))
    await methods["cron.job.list"](
        SimpleNamespace(_gateway_user_id="owner"), "req", {"user_id": "someone-else"}, "session"
    )
    assert cron.list_jobs.await_args.kwargs["user_id"] == "owner"
