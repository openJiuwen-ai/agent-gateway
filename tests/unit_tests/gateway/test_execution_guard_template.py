# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""执行护栏模板：body 校验、落库，以及写接口对非法 body 返回 400。"""

from __future__ import annotations

import copy
import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_BODY_PATH = (
    Path(__file__).resolve().parents[3]
    / "agent_gateway/enterprise/config/manager_config/schemas/execution_guard_body.py"
)


def _body_module():
    spec = importlib.util.spec_from_file_location("execution_guard_body_under_test", _BODY_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_BODY = _body_module()
DEFAULT_EXECUTION_GUARD_BODY = _BODY.DEFAULT_EXECUTION_GUARD_BODY
validate_execution_guard_body = _BODY.validate_execution_guard_body


class _MemoryRepo:
    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}

    async def get(self, *, template_id: str):
        row = self.rows.get(template_id)
        return None if row is None else dict(row)

    async def create(self, row):
        self.rows[row["template_id"]] = dict(row)
        return dict(row)

    async def update(self, match, updates):
        current = self.rows.get(match["template_id"])
        if current is None:
            return None
        current.update(updates)
        return dict(current)

    async def delete(self, *, template_id: str):
        return self.rows.pop(template_id, None) is not None


def _gateway_modules():
    pytest.importorskip("e2a")
    from agent_gateway.enterprise.config.manager_config.core.template import (
        execution_guard_template,
    )
    from agent_gateway.enterprise.config.manager_config.routers import template_routers

    return execution_guard_template, template_routers


def test_default_body_is_accepted() -> None:
    assert validate_execution_guard_body(DEFAULT_EXECUTION_GUARD_BODY) == (
        DEFAULT_EXECUTION_GUARD_BODY
    )


def test_missing_field_is_rejected() -> None:
    body = copy.deepcopy(DEFAULT_EXECUTION_GUARD_BODY)
    del body["model_anomaly_detection_rail"]["transient_max_retries"]
    with pytest.raises(ValueError, match="missing fields: transient_max_retries"):
        validate_execution_guard_body(body)


def test_repeat_window_too_small_is_rejected() -> None:
    body = copy.deepcopy(DEFAULT_EXECUTION_GUARD_BODY)
    body["model_anomaly_detection_rail"]["repeat_window_chars"] = 10
    with pytest.raises(ValueError, match="repeat_window_chars must be >= 384"):
        validate_execution_guard_body(body)


def test_critical_below_warning_is_rejected() -> None:
    body = copy.deepcopy(DEFAULT_EXECUTION_GUARD_BODY)
    body["circuit_breaker"]["critical_threshold"] = 5
    with pytest.raises(ValueError, match="critical_threshold must be >= warning_threshold"):
        validate_execution_guard_body(body)


def test_unknown_field_is_rejected() -> None:
    body = copy.deepcopy(DEFAULT_EXECUTION_GUARD_BODY)
    body["model_anomaly_detection_rail"]["transient_base_delay_seconds"] = 2
    with pytest.raises(ValueError, match="unknown fields: transient_base_delay_seconds"):
        validate_execution_guard_body(body)


@pytest.mark.asyncio
async def test_create_stores_validated_body(monkeypatch: pytest.MonkeyPatch) -> None:
    execution_guard_template, _routers = _gateway_modules()
    repo = _MemoryRepo()
    monkeypatch.setattr(
        execution_guard_template,
        "require_enterprise_repository",
        lambda name: repo,
    )
    result = await execution_guard_template.ExecutionGuardTemplateService().create(
        {
            "template_id": " eg-1 ",
            "template_name": " 默认护栏 ",
            "body": DEFAULT_EXECUTION_GUARD_BODY,
        }
    )
    assert result == {"template_id": "eg-1"}
    assert repo.rows["eg-1"]["template_name"] == "默认护栏"
    assert repo.rows["eg-1"]["body"] == DEFAULT_EXECUTION_GUARD_BODY


def test_post_stores_body(monkeypatch: pytest.MonkeyPatch) -> None:
    execution_guard_template, template_routers = _gateway_modules()
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    repo = _MemoryRepo()
    monkeypatch.setattr(
        execution_guard_template,
        "require_enterprise_repository",
        lambda name: repo,
    )
    monkeypatch.setattr(template_routers, "trigger_runtime_config_update", lambda: None)
    app = FastAPI()
    app.include_router(template_routers.templates_router, prefix="/api/v1")
    response = TestClient(app).post(
        "/api/v1/execution-guard-templates",
        json={
            "template_id": "eg-1",
            "template_name": "默认护栏",
            "body": DEFAULT_EXECUTION_GUARD_BODY,
        },
    )
    assert response.status_code == 200
    assert repo.rows["eg-1"]["body"] == DEFAULT_EXECUTION_GUARD_BODY
    assert repo.rows["eg-1"]["enabled"] is True


@pytest.mark.parametrize(
    ("mutate", "fragment"),
    [
        (
            lambda body: body["model_anomaly_detection_rail"].pop("max_retries"),
            "missing fields: max_retries",
        ),
        (
            lambda body: body["model_anomaly_detection_rail"].__setitem__(
                "repeat_window_chars", 10
            ),
            "repeat_window_chars must be >= 384",
        ),
        (
            lambda body: body["circuit_breaker"].__setitem__("critical_threshold", 5),
            "critical_threshold must be >= warning_threshold",
        ),
    ],
)
def test_post_rejects_invalid_body(
    monkeypatch: pytest.MonkeyPatch,
    mutate,
    fragment: str,
) -> None:
    execution_guard_template, template_routers = _gateway_modules()
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    repo = _MemoryRepo()
    monkeypatch.setattr(
        execution_guard_template,
        "require_enterprise_repository",
        lambda name: repo,
    )
    monkeypatch.setattr(template_routers, "trigger_runtime_config_update", lambda: None)
    app = FastAPI()
    app.include_router(template_routers.templates_router, prefix="/api/v1")
    body = copy.deepcopy(DEFAULT_EXECUTION_GUARD_BODY)
    mutate(body)
    response = TestClient(app).post(
        "/api/v1/execution-guard-templates",
        json={"template_id": "eg-1", "template_name": "默认护栏", "body": body},
    )
    assert response.status_code == 400
    assert fragment in response.json()["detail"]
    assert repo.rows == {}
