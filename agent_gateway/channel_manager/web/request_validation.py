"""Shared validation for WebSocket and HTTP Web RPC ingress."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from agent_gateway.common.protocol_ids import (
    InvalidProtocolId,
    validate_workflow_run_id,
)

SWARMFLOW_CONTROL_METHODS = frozenset({
    "swarmflow.pause",
    "swarmflow.resume",
    "swarmflow.stop",
})


def normalize_web_method(method: Any) -> str:
    """Return the canonical method name or reject an empty wire value."""
    if not isinstance(method, str):
        raise InvalidProtocolId("method must be a string")
    normalized = method.strip()
    if not normalized:
        raise InvalidProtocolId("method must not be blank")
    return normalized


def validate_swarmflow_request(method: str, params: Mapping[str, Any]) -> None:
    """Apply the WebSocket swarmflow ID contract to every Web ingress."""
    if method not in SWARMFLOW_CONTROL_METHODS:
        return
    run_id = params.get("run_id")
    workflow_run_id = params.get("workflow_run_id")
    if run_id in (None, "") and workflow_run_id in (None, ""):
        return
    if (
        run_id not in (None, "")
        and workflow_run_id not in (None, "")
        and run_id != workflow_run_id
    ):
        raise InvalidProtocolId("run_id and workflow_run_id must match")
    validate_workflow_run_id(run_id or workflow_run_id)
