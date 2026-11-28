# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""issue #200：配额策略 priority 必须落在 INT32 范围内，超大值应在 Schema 层 422 拦截，
而不是落到 MySQL INT 列触发 1264 Out of range 后以 500 Internal Server Error 暴露。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from agent_gateway.enterprise.config.manager_config.schemas.quota_schemas import (
    INT32_MAX,
    PolicyUpsertRequest,
    TeamQuotaPolicyUpsertRequest,
)

_OVERFLOW_PRIORITY = 10_000_000_000_000_000_000  # 1e19，远超 INT32 上限


def _workspace_body(**overrides) -> dict:
    base = {
        "policy_id": "p-1",
        "policy_name": "quota",
        "match_expr": [],
        "priority": 0,
        "limit_bytes": -1,
        "soft_percent": 80,
        "hard_percent": 100,
        "enabled": True,
    }
    base.update(overrides)
    return base


def _team_body(**overrides) -> dict:
    base = {
        "policy_id": "t-1",
        "policy_name": "team-quota",
        "match_expr": [],
        "priority": 0,
        "max_teams_per_user": 5,
        "max_teammates_per_team": 5,
        "max_teams_per_org": 0,
        "enabled": True,
    }
    base.update(overrides)
    return base


def test_workspace_priority_rejects_overflow() -> None:
    with pytest.raises(ValidationError) as exc:
        PolicyUpsertRequest.model_validate(_workspace_body(priority=_OVERFLOW_PRIORITY))
    assert "priority" in str(exc.value)


def test_workspace_priority_accepts_int32_max() -> None:
    req = PolicyUpsertRequest.model_validate(_workspace_body(priority=INT32_MAX))
    assert req.priority == INT32_MAX


def test_workspace_priority_rejects_negative() -> None:
    with pytest.raises(ValidationError) as exc:
        PolicyUpsertRequest.model_validate(_workspace_body(priority=-1))
    assert "priority" in str(exc.value)


def test_team_priority_rejects_overflow() -> None:
    with pytest.raises(ValidationError) as exc:
        TeamQuotaPolicyUpsertRequest.model_validate(_team_body(priority=_OVERFLOW_PRIORITY))
    assert "priority" in str(exc.value)


def test_team_priority_accepts_int32_max() -> None:
    req = TeamQuotaPolicyUpsertRequest.model_validate(_team_body(priority=INT32_MAX))
    assert req.priority == INT32_MAX
