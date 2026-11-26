# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Runtime-compatible HTTP entry point with business request preparation."""

import logging

from e2a.models import E2AEnvelope
from agent_gateway.common.client.agent_client import AgentServerClient

from agent_gateway.southbound.data.http_sse import (
    HttpSseConnection,
    http_unary_to_agent_response,
    iter_sse_data_frames,
    _PUSH_TERMINATED_EVENT_TYPE,
)
from agent_gateway.southbound.data.rest_map import assemble_rest_request

logger = logging.getLogger(__name__)


async def _assemble_with_allowlist(envelope: E2AEnvelope, api_root: str):
    """Preserve enterprise allowlist preparation before REST assembly."""
    from agent_gateway.enterprise.edition import is_enterprise

    if not is_enterprise():
        return assemble_rest_request(envelope, base_url=api_root)
    from agent_gateway.enterprise.capabilities.experts.allowlist import bind_outbound_allowlist
    from agent_gateway.enterprise.capabilities.experts.bound_packages import packages_for_bot
    from agent_gateway.extensions.runtime_management_extension.invoke_ids import routing_triple_from_envelope

    _, bot_id, _ = routing_triple_from_envelope(envelope)
    if not bot_id:
        logger.warning(
            "[RuntimeManagement] outbound allowlist has no trusted bot_id; "
            "expert packages resolve to empty: method=%s request_id=%s",
            getattr(envelope, "method", ""), getattr(envelope, "request_id", ""),
        )
    packages = await packages_for_bot(bot_id)
    with bind_outbound_allowlist(packages):
        return assemble_rest_request(envelope, base_url=api_root)


class HttpSseAgentServerClient(HttpSseConnection, AgentServerClient):
    """Retain existing Runtime client signatures and HTTP status handling."""

    async def _assemble_request(self, envelope: E2AEnvelope, api_root: str):
        return await _assemble_with_allowlist(envelope, api_root)
