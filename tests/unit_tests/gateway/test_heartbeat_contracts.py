# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway consumer contracts for cross-process Heartbeat requests."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from openjiuwen_protocol.message import EventType, ReqMethod

from agent_gateway.health_check import (
    GatewayHealthCheckService,
    HealthCheckConfig,
)
from agent_gateway.heartbeat import (
    HeartbeatControllerProxy,
    HeartbeatServiceUnavailableError,
)
from agent_gateway.message_handler import MessageHandler


def test_health_check_accepts_legacy_probe_wire_names() -> None:
    assert ReqMethod.HEARTBEAT_GET_CONF.value == "heartbeat.get_conf"
    assert ReqMethod.HEARTBEAT_SET_CONF.value == "heartbeat.set_conf"
    assert EventType.HEARTBEAT_RELAY is EventType.HEALTH_CHECK_RELAY
    assert EventType("heartbeat.relay") is EventType.HEALTH_CHECK_RELAY
    assert EventType("health_check.relay") is EventType.HEALTH_CHECK_RELAY


def test_legacy_health_check_relay_normalizes_at_gateway_ingress() -> None:
    message = MessageHandler._response_to_message(
        SimpleNamespace(
            request_id="legacy-probe",
            channel_id="web",
            payload={"event_type": "heartbeat.relay", "heartbeat": "HEALTH_CHECK_OK"},
            metadata={},
            agent_ref=None,
            ok=True,
        ),
        "health-check-session",
    )

    assert message.type == "event"
    assert message.event_type is EventType.HEALTH_CHECK_RELAY
    assert message.payload == {
        "event_type": "health_check.relay",
        "heartbeat": "HEALTH_CHECK_OK",
        "health_check": "HEALTH_CHECK_OK",
    }


async def test_health_check_relay_keeps_legacy_payload_alias() -> None:
    published = []

    class Client:
        async def send_request(self, envelope):  # noqa: ANN001
            return SimpleNamespace(payload={"health_check": "HEALTH_CHECK_OK"})

    class Handler:
        async def publish_robot_messages(self, message):  # noqa: ANN001
            published.append(message)

    service = GatewayHealthCheckService(
        Client(),
        HealthCheckConfig(interval_seconds=30, relay_channel_id="web"),
        message_handler=Handler(),
    )
    await service._tick()

    assert len(published) == 1
    assert published[0].event_type is EventType.HEALTH_CHECK_RELAY
    assert published[0].payload == {
        "health_check": "HEALTH_CHECK_OK",
        "heartbeat": "HEALTH_CHECK_OK",
    }


async def test_gateway_proxy_uses_one_unary_heartbeat_rpc() -> None:
    captured = []

    class Client:
        async def send_request(self, envelope):  # noqa: ANN001
            captured.append(envelope)
            return SimpleNamespace(
                ok=True,
                payload={"result": {"jobs": [{"id": "job-1"}]}},
            )

    proxy = HeartbeatControllerProxy(Client())
    result = await proxy.list_jobs(
        {}, access_session_id="session-1", user_id="user-1"
    )
    assert result == {"jobs": [{"id": "job-1"}]}
    envelope = captured[0]
    assert envelope.method == ReqMethod.HEARTBEAT_JOB.value
    assert envelope.session_id == "session-1"
    assert envelope.user_id == "user-1"
    assert envelope.params == {"action": "list", "data": {}}


@pytest.mark.parametrize(
    ("code", "error_type"),
    [
        ("BAD_REQUEST", ValueError),
        ("FORBIDDEN", PermissionError),
        ("NOT_FOUND", KeyError),
        ("CONFLICT", RuntimeError),
        ("SERVICE_UNAVAILABLE", HeartbeatServiceUnavailableError),
    ],
)
async def test_gateway_proxy_preserves_error_classes(code, error_type) -> None:
    class Client:
        async def send_request(self, envelope):  # noqa: ANN001
            return SimpleNamespace(
                ok=False,
                payload={"code": code, "error": "failed"},
            )

    with pytest.raises(error_type):
        await HeartbeatControllerProxy(Client()).get_job(
            "job-1",
            access_session_id="session-1",
        )
