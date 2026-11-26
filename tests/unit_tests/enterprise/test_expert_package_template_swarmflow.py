# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Expert package template Swarmflow 企业策略列（JSON）的归一化与持久化。"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from agent_gateway.enterprise.config.manager_config.core.template import (
    expert_package_template as ep_module,
)
from agent_gateway.enterprise.config.manager_config.core.template.expert_package_template import (
    ExpertPackageTemplateService,
    _normalize_row_fields,
    _normalize_swarmflow,
    update_expert_package_template,
)
from agent_gateway.enterprise.config.manager_config.schemas.template_schemas import (
    ExpertPackageTemplateUpdateRequest,
)

_SWARMFLOW = {
    "allowed": False,
    "default_enabled": True,
    "max_workflows": 2,
    "agents_per_run": 3,
    "max_agents_total": 4,
    "swarmflow_budget": 1000,
}
_PACKAGES = [{"kind": "agent_group", "asset_id": "presentation-generation", "version": ""}]


def test_normalize_swarmflow_keeps_object() -> None:
    assert _normalize_swarmflow(_SWARMFLOW) == _SWARMFLOW


def test_normalize_swarmflow_null() -> None:
    assert _normalize_swarmflow(None) is None


def test_normalize_swarmflow_accepts_json_string() -> None:
    assert _normalize_swarmflow(json.dumps(_SWARMFLOW)) == _SWARMFLOW


@pytest.mark.parametrize("bad", [[1, 2], 3, "not-json", "[]"])
def test_normalize_swarmflow_rejects_non_object(bad: object) -> None:
    with pytest.raises(ValueError):
        _normalize_swarmflow(bad)


def test_normalize_row_fields_defaults_swarmflow_to_null() -> None:
    row = _normalize_row_fields({"template_name": "ep", "packages": _PACKAGES})
    assert row["swarmflow"] is None


def test_normalize_row_fields_keeps_swarmflow_object() -> None:
    row = _normalize_row_fields(
        {"template_name": "ep", "packages": _PACKAGES, "swarmflow": _SWARMFLOW}
    )
    assert row["swarmflow"] == _SWARMFLOW


def test_update_request_schema_accepts_swarmflow() -> None:
    req = ExpertPackageTemplateUpdateRequest.model_validate({"swarmflow": _SWARMFLOW})
    assert req.model_dump(exclude_unset=True)["swarmflow"] == _SWARMFLOW


def test_update_request_schema_omits_unset_swarmflow() -> None:
    req = ExpertPackageTemplateUpdateRequest.model_validate({"template_name": "ep"})
    assert "swarmflow" not in req.model_dump(exclude_unset=True)


@pytest.mark.asyncio
async def test_update_persists_swarmflow_to_repo() -> None:
    existing = {
        "template_id": "ep-1",
        "template_name": "ep",
        "description": None,
        "packages": [{"kind": "agent_group", "asset_id": "a", "version": ""}],
        "swarmflow": None,
        "enabled": True,
    }
    repo = AsyncMock()
    repo.update = AsyncMock(return_value={"template_id": "ep-1"})
    request = ExpertPackageTemplateUpdateRequest.model_validate({"swarmflow": _SWARMFLOW})

    result = await update_expert_package_template(repo, "ep-1", request, existing=existing)

    assert result == {"template_id": "ep-1"}
    _, updates = repo.update.await_args.args
    assert updates["swarmflow"] == _SWARMFLOW


@pytest.mark.asyncio
async def test_create_persists_swarmflow_to_repo(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = AsyncMock()
    repo.get = AsyncMock(return_value=None)
    repo.create = AsyncMock(return_value=None)
    monkeypatch.setattr(ep_module, "require_enterprise_repository", lambda table: repo)

    service = ExpertPackageTemplateService()
    await service.create(
        {
            "template_id": "ep-1",
            "template_name": "ep",
            "packages": _PACKAGES,
            "swarmflow": _SWARMFLOW,
        }
    )

    row = repo.create.await_args.args[0]
    assert row["swarmflow"] == _SWARMFLOW
