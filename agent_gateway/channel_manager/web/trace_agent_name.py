# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Resolve the Manager-authorized display name for main Agent traces."""

from __future__ import annotations

import asyncio
import hashlib
import logging
from collections import OrderedDict
from time import monotonic
from typing import Any

import httpx

from agent_gateway.common.mode_matrix import deprecate_mode, is_single_agent_mode
from agent_gateway.common.request_ext import (
    METADATA_KEY,
    TRACE_AGENT_NAME_KEY,
    RequestExtCodecError,
    attach_to_metadata,
    encode_internal_header,
)
from agent_gateway.enterprise import is_enterprise

logger = logging.getLogger(__name__)

_MANAGER_TIMEOUT_S = 1.0
_CACHE_TTL_S = 30.0
_CACHE_MAX_ENTRIES = 256
_CONTEXT_FIELDS = ("jiuwenclaw_id", "user_id", "group_id", "bot_id")
USER_ACCESS_TOKEN_COOKIE = "openjiuwen_user_access_token"

AgentContextScope = tuple[str, str, str, str]


def _context_id(value: Any) -> str:
    return str(value or "").strip()


async def _query_agent_name(scope: AgentContextScope, authorization: str) -> str | None:
    try:
        from agent_gateway.enterprise.config.manager_config.core.instance.instance_service import (
            resolve_manager_http_base,
        )

        base_url = resolve_manager_http_base()
        if not base_url:
            return None
        async with asyncio.timeout(_MANAGER_TIMEOUT_S):
            async with httpx.AsyncClient(trust_env=False) as client:
                response = await client.get(
                    f"{base_url}/api/v1/user-console/agent-contexts",
                    headers={"authorization": authorization, "accept": "application/json"},
                )
                response.raise_for_status()
                body = response.json()
    except Exception as exc:
        # Optional telemetry must not interrupt chat. Do not log request credentials.
        logger.debug("[WebChannel] Agent display name lookup failed: %s", type(exc).__name__)
        return None

    data = body.get("data") if isinstance(body, dict) else None
    contexts = data.get("contexts") if isinstance(data, dict) else None
    if not isinstance(contexts, list):
        return None
    matches = [
        item.get("agent_name")
        for item in contexts
        if isinstance(item, dict)
        and tuple(_context_id(item.get(field)) for field in _CONTEXT_FIELDS) == scope
    ]
    if len(matches) != 1 or not isinstance(matches[0], str):
        return None
    name = matches[0].strip()
    return name if 0 < len(name) <= 128 else None


class TraceAgentNameLookup:
    """Share short-lived lookups across Web requests and connections."""

    def __init__(self) -> None:
        self._lookups: OrderedDict[
            tuple[AgentContextScope, bytes], tuple[float, asyncio.Task[str | None]]
        ] = OrderedDict()

    async def attach(
        self,
        *,
        params: dict[str, Any],
        metadata: dict[str, Any] | None,
        routing: dict[str, str],
        cluster_id: str,
        authorization: str,
        access_token: str,
    ) -> dict[str, Any] | None:
        if not is_enterprise() or params.get("team") or not is_single_agent_mode(
            deprecate_mode(params.get("mode", "agent"))
        ):
            return metadata

        cluster_id = _context_id(cluster_id)
        user_id = _context_id(routing.get("user_id"))
        group_id = _context_id(routing.get("group_id"))
        bot_id = _context_id(routing.get("bot_id"))
        if not (cluster_id and user_id and bot_id):
            return metadata

        authorization = authorization or f"Bearer {access_token}"
        scheme, _, credential = authorization.partition(" ")
        if scheme.lower() != "bearer" or not credential.strip():
            return metadata

        scope = (cluster_id, user_id, group_id, bot_id)
        name = await self._lookup(scope, authorization)
        if not name:
            return metadata

        ext = dict((metadata or {}).get(METADATA_KEY) or {})
        ext[TRACE_AGENT_NAME_KEY] = {"bot_id": bot_id, "name": name}
        try:
            encode_internal_header(ext)
        except RequestExtCodecError:
            # Keep existing request extensions if the optional name exceeds the carrier budget.
            return metadata
        return attach_to_metadata(metadata, ext=ext)

    async def _lookup(self, scope: AgentContextScope, authorization: str) -> str | None:
        # An authorization result can only be reused by the same credential and routing scope.
        key = (scope, hashlib.sha256(authorization.encode()).digest())
        now = monotonic()
        expires_at, task = self._lookups.get(key, (0.0, None))
        if task is None or (task.done() and expires_at <= now):
            task = asyncio.create_task(_query_agent_name(scope, authorization))
            self._lookups[key] = (now + _CACHE_TTL_S, task)
        self._lookups.move_to_end(key)
        if len(self._lookups) > _CACHE_MAX_ENTRIES:
            self._lookups.popitem(last=False)
        return await asyncio.shield(task)
