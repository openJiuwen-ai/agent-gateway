# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from inspect import signature
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.extensions.adx.control_adapter import AdxControl
from agent_gateway.extensions.agentos.agentos_router.control_adapter import YuanRongControl
from agent_gateway.southbound.contracts import ConnectionTarget, TransportKind
from agent_gateway.southbound.control.interfaces import ManagedControl
from agent_gateway.southbound.control.local import FixedTargetControl
from agent_gateway.southbound.control.managed import ManagedSandboxControl, SandboxCreateRequest, SandboxInfo


@pytest.mark.parametrize("endpoint,ws,http", [
    ("https://frontend/", "wss://frontend", "https://frontend"),
    ("http://frontend", "ws://frontend", "http://frontend"),
    ("", "ws://", "http://"),
])
def test_yuanrong_endpoints(endpoint, ws, http):
    control = YuanRongControl(SimpleNamespace(frontend_endpoint=endpoint, agent_namespace="tenant"))
    suffix = "?instance=instance&tenant_id=tenant&port=8080"
    assert control.ws_url("instance", 8080) == f"{ws}/serverless/v1/ws{suffix}"
    assert control.http_url("instance", 8080) == f"{http}/serverless/v1/http{suffix}"
    assert control.web_ws_url("instance", 8080) == f"{ws}/serverless/v1/http{suffix}"


def test_adx_endpoints_follow_backend_configuration():
    backend = SimpleNamespace(frontend_endpoint="https://ingress/")
    control = AdxControl(backend)
    suffix = "?target=urn%3Aadx%3Ainstance%3Ainstance&port=8080"
    assert control.ws_url("instance", 8080) == f"wss://ingress/agent/ws{suffix}"
    assert control.http_url("instance", 8080) == f"https://ingress/agent/http{suffix}"
    assert control.web_ws_url("instance", 8080) == f"wss://ingress/agent/ws{suffix}"
    backend.frontend_endpoint = "https://updated"
    assert control.http_url("instance", 8080) == f"https://updated/agent/http{suffix}"


@pytest.mark.asyncio
@pytest.mark.parametrize("adapter", [YuanRongControl, AdxControl])
async def test_managed_operations_preserve_arguments_results_and_errors(adapter):
    resource = SandboxInfo("physical-instance")
    info = {"status": "running"}
    backend = SimpleNamespace(create_sandbox=AsyncMock(return_value=resource),
                              get_agent_info=AsyncMock(return_value=info), delete_sandbox=AsyncMock())
    control = adapter(backend)
    request = SandboxCreateRequest("ns", "name", "/workspace", {"runtime": "python"}, {"A": "B"})
    assert await control.create(request) is resource
    backend.create_sandbox.assert_awaited_once_with(namespace="ns", name="name", workspace="/workspace",
                                                  runtime_spec=request.runtime_spec, env_vars=request.env_vars)
    assert await control.query(resource.sandbox_id) is info
    backend.get_agent_info.assert_awaited_once_with("physical-instance")
    await control.delete(resource.sandbox_id)
    backend.delete_sandbox.assert_awaited_once_with("physical-instance")
    for method, operation, arg in ((backend.create_sandbox, control.create, request),
                                   (backend.get_agent_info, control.query, resource.sandbox_id),
                                   (backend.delete_sandbox, control.delete, resource.sandbox_id)):
        error = RuntimeError("platform error")
        method.side_effect = error
        with pytest.raises(RuntimeError) as caught:
            await operation(arg)
        assert caught.value is error


def test_managed_adapter_preserves_contract():
    for method in ("create", "query", "delete"):
        expected = signature(getattr(ManagedControl, method))
        actual = signature(getattr(ManagedSandboxControl, method))
        assert [p.replace(annotation=p.empty) for p in actual.parameters.values()] == [
            p.replace(annotation=p.empty) for p in expected.parameters.values()
        ]


def test_fixed_target_control_returns_original_snapshot():
    target = ConnectionTarget(TransportKind.WEBSOCKET, "ws://agent")
    assert FixedTargetControl(target).get_target() is target
