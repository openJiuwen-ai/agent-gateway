# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""issue #200：配额策略 priority 必须落在 INT32 范围内，超大值应在 Schema 层 422 拦截，
而不是落到 MySQL INT 列触发 1264 Out of range 后以 500 Internal Server Error 暴露。

负数域保留给审批策略（Manager 审批通过下发 priority=-1，可多条并存），故下界取
INT32_MIN 而非 0；本测试同时覆盖 Schema 边界、审批 -1 合法、以及 service 层
DataError 兜底不外泄原始 SQL/绑定参数。
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from pydantic import ValidationError
from sqlalchemy.exc import DataError

from agent_gateway.enterprise.config.manager_config.core.quota.workspace_quota_policy import (
    WorkspaceQuotaPolicyService,
)
from agent_gateway.enterprise.config.manager_config.schemas.quota_schemas import (
    INT32_MAX,
    INT32_MIN,
    PolicyUpsertRequest,
    TeamQuotaPolicyUpsertRequest,
)

_OVERFLOW_PRIORITY = 10_000_000_000_000_000_000  # 1e19，远超 INT32 上限
_UNDERFLOW_PRIORITY = -10_000_000_000_000_000_000  # -1e19，远低于 INT32 下限


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


def test_workspace_priority_rejects_underflow() -> None:
    with pytest.raises(ValidationError) as exc:
        PolicyUpsertRequest.model_validate(_workspace_body(priority=_UNDERFLOW_PRIORITY))
    assert "priority" in str(exc.value)


def test_workspace_priority_accepts_int32_max() -> None:
    req = PolicyUpsertRequest.model_validate(_workspace_body(priority=INT32_MAX))
    assert req.priority == INT32_MAX


def test_workspace_priority_accepts_int32_min() -> None:
    req = PolicyUpsertRequest.model_validate(_workspace_body(priority=INT32_MIN))
    assert req.priority == INT32_MIN


def test_workspace_priority_accepts_approval_minus_one() -> None:
    # 审批策略由 Manager 下发 priority=-1，Gateway 须接受（issue #200 检视意见）。
    req = PolicyUpsertRequest.model_validate(
        _workspace_body(priority=-1, source="approval", source_order_num="ORD-1")
    )
    assert req.priority == -1


def test_team_priority_rejects_overflow() -> None:
    with pytest.raises(ValidationError) as exc:
        TeamQuotaPolicyUpsertRequest.model_validate(_team_body(priority=_OVERFLOW_PRIORITY))
    assert "priority" in str(exc.value)


def test_team_priority_rejects_underflow() -> None:
    with pytest.raises(ValidationError) as exc:
        TeamQuotaPolicyUpsertRequest.model_validate(_team_body(priority=_UNDERFLOW_PRIORITY))
    assert "priority" in str(exc.value)


def test_team_priority_accepts_int32_max() -> None:
    req = TeamQuotaPolicyUpsertRequest.model_validate(_team_body(priority=INT32_MAX))
    assert req.priority == INT32_MAX


def test_team_priority_accepts_int32_min() -> None:
    req = TeamQuotaPolicyUpsertRequest.model_validate(_team_body(priority=INT32_MIN))
    assert req.priority == INT32_MIN


@pytest.mark.asyncio
async def test_workspace_upsert_data_error_converts_to_readable_value_error() -> None:
    # Schema 校验被绕过（如直调 service 或 DB 列收窄）时，落库的 MySQL 1264 须转成
    # 可读 ValueError，且 detail 不外泄原始 SQL/绑定参数（issue #200 检视意见 P3）。
    repo = AsyncMock()
    repo.get = AsyncMock(return_value=None)
    repo.create = AsyncMock(side_effect=DataError("statement", "params", "orig"))
    with patch(
        "agent_gateway.enterprise.config.manager_config.core.quota.workspace_quota_policy."
        "require_enterprise_repository",
        return_value=repo,
    ):
        with pytest.raises(ValueError) as exc:
            await WorkspaceQuotaPolicyService().upsert(_workspace_body(priority=0))
    msg = str(exc.value)
    assert "MySQL INT range" in msg
    assert f"{INT32_MIN}~{INT32_MAX}" in msg
    # 不外泄原始 SQL 语句 / 绑定参数
    assert "statement" not in msg
    assert "params" not in msg


@pytest.mark.asyncio
async def test_workspace_upsert_update_data_error_converts_to_readable_value_error() -> None:
    # 更新路径同样兜底（existing 非 None）。
    existing = {
        "policy_id": "p-1",
        "created_at": "2026-01-01T00:00:00",
    }
    repo = AsyncMock()
    repo.get = AsyncMock(return_value=existing)
    repo.update = AsyncMock(side_effect=DataError("statement", "params", "orig"))
    with patch(
        "agent_gateway.enterprise.config.manager_config.core.quota.workspace_quota_policy."
        "require_enterprise_repository",
        return_value=repo,
    ):
        with pytest.raises(ValueError) as exc:
            await WorkspaceQuotaPolicyService().upsert(_workspace_body(priority=0))
    msg = str(exc.value)
    assert "MySQL INT range" in msg
    assert "statement" not in msg
    assert "params" not in msg
