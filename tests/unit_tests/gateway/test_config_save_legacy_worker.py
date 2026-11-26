"""An older worker can apply runtime settings while leaving persistent models pending."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from e2a.wire_codec import (
    encode_agent_response_for_wire,
    parse_agent_server_wire_unary,
)
from e2a.agent_models import AgentResponse
from e2a.message import ReqMethod


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime_failures", [0, 4])
async def test_partial_persistent_model_reload_returns_without_retry_or_restart(runtime_failures, monkeypatch):
    from agent_gateway import app_gateway
    from agent_gateway.application.single import GatewayRestartRequest

    response = AgentResponse(
        request_id="model-reload", channel_id="web", ok=False,
        payload={"code": "PERSISTENT_MODELS_NOT_APPLIED", "error": "unpublished edits"},
    )
    wire = encode_agent_response_for_wire(response, response_id=response.request_id)
    recovering = SimpleNamespace(ok=False, payload={"error": "RUNTIME_FAILED"})
    send = AsyncMock(side_effect=[recovering] * runtime_failures + [parse_agent_server_wire_unary(wire)])
    restart = Mock()
    sleep = AsyncMock()
    from agent_gateway.application import config_saved_effects

    monkeypatch.setattr(config_saved_effects, "asyncio", SimpleNamespace(sleep=sleep))
    monkeypatch.setattr(app_gateway, "_schedule_gateway_restart", restart)
    callback = app_gateway._build_config_saved_callback(
        client=SimpleNamespace(set_or_update_server_config=Mock(), send_request=send),
        message_handler=object(),
        cron_controller=object(), schedule_agent_prewarm_sync=Mock(),
        restart_request=GatewayRestartRequest(),
    )

    assert await callback({"models.defaults"}) is False
    assert send.await_count == runtime_failures + 1
    assert sleep.await_count == runtime_failures
    restart.assert_not_called()
