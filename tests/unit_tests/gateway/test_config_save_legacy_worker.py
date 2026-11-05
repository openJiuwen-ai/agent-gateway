"""An older worker can apply runtime settings while leaving persistent models pending."""

import ast
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
import uuid

import pytest

from openjiuwen_protocol.e2a.wire_codec import (
    encode_agent_response_for_wire,
    parse_agent_server_wire_unary,
)
from openjiuwen_protocol.e2a.agent_models import AgentResponse
from openjiuwen_protocol.message import ReqMethod


@pytest.mark.asyncio
@pytest.mark.parametrize("runtime_failures", [0, 4])
async def test_partial_persistent_model_reload_returns_without_retry_or_restart(runtime_failures):
    source = Path(__file__).resolve().parents[3] / "agent_gateway/app_gateway.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    callback = next(
        node for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_on_config_saved"
    )
    response = AgentResponse(
        request_id="model-reload", channel_id="web", ok=False,
        payload={"code": "PERSISTENT_MODELS_NOT_APPLIED", "error": "unpublished edits"},
    )
    wire = encode_agent_response_for_wire(response, response_id=response.request_id)
    recovering = SimpleNamespace(ok=False, payload={"error": "RUNTIME_FAILED"})
    send = AsyncMock(side_effect=[recovering] * runtime_failures + [parse_agent_server_wire_unary(wire)])
    restart = Mock()
    sleep = AsyncMock()
    scope = {
        "Any": object, "ReqMethod": ReqMethod, "uuid_module": uuid,
        "client": SimpleNamespace(set_or_update_server_config=Mock(), send_request=send),
        "e2a_from_agent_fields": lambda **fields: fields,
        "logger": logging.getLogger(__name__), "asyncio": SimpleNamespace(sleep=sleep),
        "_schedule_gateway_restart": restart, "restart_request": object(),
    }
    exec(compile(ast.Module(body=[callback], type_ignores=[]), str(source), "exec"), scope)

    assert await scope["_on_config_saved"]({"models.defaults"}) is False
    assert send.await_count == runtime_failures + 1
    assert sleep.await_count == runtime_failures
    restart.assert_not_called()
