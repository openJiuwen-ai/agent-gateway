# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Contract tests independent of the existing client assembly."""

from dataclasses import FrozenInstanceError

import pytest
from e2a.agent_models import AgentResponse
from e2a.models import E2AEnvelope

from agent_gateway.common.security.link_mtls import MTLSDeploymentIdentity
from agent_gateway.southbound.contracts import (
    ConnectionTarget, DeliveryState, Source, Sourced, TransportError, TransportKind,
)
from agent_gateway.southbound.control.interfaces import DelegatedControl, ManagedControl, StaticControl
from agent_gateway.southbound.data.interfaces import DataTransport, PushTransport, TunnelTransport
from agent_gateway.southbound.orchestration.interfaces import RouteRequest, RouteSelection


def test_target_snapshot_distinguishes_generation_and_hides_credentials():
    first = ConnectionTarget(
        TransportKind.WEBSOCKET, "ws://agent?token=secret", "sandbox", "gen-1",
        headers=(("Authorization", "secret"),),
    )
    second = ConnectionTarget(TransportKind.WEBSOCKET, first.endpoint, "sandbox", "gen-2")
    assert first != second
    assert "secret" not in repr(first)
    with pytest.raises(FrozenInstanceError):
        first.generation = "gen-2"


def test_result_keeps_existing_e2a_payload_and_actual_origin():
    target = ConnectionTarget(TransportKind.HTTP_SSE, "http://pod1", "pod1")
    source = Source(target, request_id="r", session_id="s")
    response = AgentResponse(request_id="r", channel_id="web", ok=False, payload={})
    result = Sourced(response, source)
    assert result.payload is response
    assert not result.payload.ok  # business rejection is still a protocol result
    assert result.source.target is target
    error = TransportError("response lost", delivery=DeliveryState.MAY_HAVE_EXECUTED, source=source)
    assert error.source is source
    assert error.delivery is not DeliveryState.NOT_SENT


def test_binding_epoch_distinguishes_same_pod_endpoint():
    first = ConnectionTarget(
        TransportKind.HTTP_SSE, "https://pod", "pod",
        binding=MTLSDeploymentIdentity("deployment", "binding", 1),
    )
    second = ConnectionTarget(
        TransportKind.HTTP_SSE, "https://pod", "pod",
        binding=MTLSDeploymentIdentity("deployment", "binding", 2),
    )
    assert first != second


def test_capabilities_are_not_forced_into_one_platform_interface():
    assert not hasattr(StaticControl, "create")
    assert not hasattr(ManagedControl, "route")
    assert not hasattr(DelegatedControl, "delete")
    assert not hasattr(DataTransport, "route")
    assert not hasattr(PushTransport, "create")
    assert not hasattr(TunnelTransport, "send_request")


def test_route_selection_can_preserve_origin_and_multiple_interrupt_targets():
    first = ConnectionTarget(TransportKind.HTTP_SSE, "http://pod1", "pod1")
    second = ConnectionTarget(TransportKind.HTTP_SSE, "http://pod2", "pod2")
    origin = Source(first, request_id="rpc", session_id="s")
    envelope = E2AEnvelope(method="acp.tool_response", request_id="r", session_id="s")
    request = RouteRequest(envelope, allow_create=False, origin=origin)
    assert request.origin.target is first
    assert not request.allow_create
    assert RouteSelection((first, second), session_id="s").targets == (first, second)
    assert RouteSelection(()).targets == ()
