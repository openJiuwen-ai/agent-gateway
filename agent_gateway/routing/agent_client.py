# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Legacy AgentServer client entry point; transport lives in southbound.data."""

from e2a.models import E2AEnvelope
from agent_gateway.common.client.agent_client import AgentServerClient

from agent_gateway.southbound.data.websocket import (
    AGENT_REQUEST_TIMEOUT_SECONDS,
    AGENT_WS_MAX_MESSAGE_BYTES,
    AgentServerUnaryTimeout,
    DuplicateRequestIdError,
    WebSocketConnection,
    _build_ws_origin,
    _e2a_to_wire,
    main,
    mock_agent_server_handler,
    run_mock_agent_server,
)


async def _stamp_expert_packages(envelope: E2AEnvelope) -> None:
    """Keep existing enterprise request preparation at the legacy boundary."""
    from agent_gateway.enterprise.edition import is_enterprise

    if not is_enterprise():
        return
    from agent_gateway.enterprise.capabilities.experts.outbound_packages import apply_packages_to_envelope
    from agent_gateway.enterprise.capabilities.experts.bound_packages import packages_for_bot
    from agent_gateway.common.request_identity import web_routing_identity

    bot_id = web_routing_identity(envelope.channel_context).get("bot_id", "")
    apply_packages_to_envelope(envelope, await packages_for_bot(bot_id))


class WebSocketAgentServerClient(WebSocketConnection, AgentServerClient):
    """Compatibility client for existing northbound and router callers."""

    shared_directory_layout = True

    async def _prepare_envelope(self, envelope: E2AEnvelope) -> None:
        await _stamp_expert_packages(envelope)


if __name__ == "__main__":
    main()
