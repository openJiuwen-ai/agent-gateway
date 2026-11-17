# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""HTTP contract tests for session-complete trajectory usage."""

from __future__ import annotations

import asyncio
import json

import pytest

from agent_gateway.channel_manager.web.trajectory_http import TrajectoryHttpService


async def _usage_metadata_loader(
    _session_id: str,
    _user_id: str | None = None,
) -> dict[str, str]:
    return {
        "mode": "agent.work.normal",
        "team_name": "",
    }


async def _usage_fetch(method, params, user_id):
    """Stand in for the AgentServer trajectory store (southbound)."""
    assert method == "trajectory.usage.get"
    assert params == {"session_id": "session-1"}
    return True, {
        "result": {
            "scope": "session",
            "items": [
                {
                    "trace_id": "1" * 32,
                    "inference_id": "inference-1",
                    "subject_id": "main",
                    "start_time_unix_nano": "10",
                    "usage": {"input": 2, "total": 3},
                    "cumulative_usage": {"input": 5, "total": 8},
                }
            ],
        }
    }


@pytest.mark.asyncio
async def test_http_exposes_session_scoped_request_usage() -> None:
    service = TrajectoryHttpService(
        metadata_loader=_usage_metadata_loader,
        fetch=_usage_fetch,
    )

    response = await asyncio.wait_for(service.get_session_usage("session-1"), timeout=1)

    payload = json.loads(response.body)
    assert response.status_code == 200
    assert payload["scope"] == "session"
    assert payload["items"][0]["cumulative_usage"] == {"input": 5, "total": 8}
