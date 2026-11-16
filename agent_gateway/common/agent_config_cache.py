# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared agent configuration cache invalidation helpers."""

from __future__ import annotations

import uuid
from typing import Any

from agent_gateway.common.config import get_config


async def clear_agent_config_cache(agent_client: Any | None = None) -> None:
    """Reload agent-side config after writing config.yaml."""
    try:
        if agent_client is not None:
            from e2a.gateway_normalize import e2a_from_agent_fields
            from e2a.message import ReqMethod

            env = e2a_from_agent_fields(
                request_id=f"cfg-reload-{uuid.uuid4().hex[:8]}",
                channel_id="",
                req_method=ReqMethod.AGENT_RELOAD_CONFIG,
            )
            await agent_client.send_request(env)
        else:
            get_config()
    except Exception:  # noqa: BLE001
        pass
