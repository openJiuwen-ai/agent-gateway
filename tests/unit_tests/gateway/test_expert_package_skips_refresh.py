"""Expert package template writes must not trigger AgentServer config_refresh."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from agent_gateway.enterprise.config.manager_config.core.template.expert_package_template import (
    ExpertPackageTemplateService,
)
from agent_gateway.enterprise.config.manager_config.core.template.model_template import (
    ModelTemplateService,
)
from agent_gateway.enterprise.config.manager_config.routers import template_routers as tr

_EP_PACKAGE = {"kind": "agent_group", "asset_id": "presentation-generation", "version": ""}


@pytest.fixture
def refresh_calls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    calls: list[str] = []
    monkeypatch.setattr(tr, "trigger_runtime_config_update", lambda: calls.append("refresh"))
    return calls


def _client() -> TestClient:
    app = FastAPI()
    app.include_router(tr.templates_router, prefix="/api/v1")
    return TestClient(app)


def test_expert_package_create_skips_refresh(
    monkeypatch: pytest.MonkeyPatch, refresh_calls: list[str]
) -> None:
    monkeypatch.setattr(
        ExpertPackageTemplateService,
        "create",
        AsyncMock(return_value={"template_id": "ep-1"}),
    )
    response = _client().post(
        "/api/v1/expert-package-templates",
        json={
            "template_id": "ep-1",
            "template_name": "ep",
            "packages": [_EP_PACKAGE],
        },
    )
    assert response.status_code == 200, response.text
    assert refresh_calls == []


def test_expert_package_update_skips_refresh(
    monkeypatch: pytest.MonkeyPatch, refresh_calls: list[str]
) -> None:
    monkeypatch.setattr(
        ExpertPackageTemplateService,
        "update",
        AsyncMock(return_value={"template_id": "ep-1"}),
    )
    response = _client().patch(
        "/api/v1/expert-package-templates/ep-1",
        json={"packages": [_EP_PACKAGE]},
    )
    assert response.status_code == 200, response.text
    assert refresh_calls == []


def test_expert_package_delete_skips_refresh(
    monkeypatch: pytest.MonkeyPatch, refresh_calls: list[str]
) -> None:
    monkeypatch.setattr(ExpertPackageTemplateService, "delete", AsyncMock(return_value=None))
    response = _client().request(
        "DELETE",
        "/api/v1/expert-package-templates/ep-1",
        json={},
    )
    assert response.status_code == 200, response.text
    assert refresh_calls == []


def test_model_template_update_still_refreshes(
    monkeypatch: pytest.MonkeyPatch, refresh_calls: list[str]
) -> None:
    monkeypatch.setattr(ModelTemplateService, "update", AsyncMock(return_value=None))
    response = _client().patch(
        "/api/v1/model-templates/m-1",
        json={"template_name": "model-a"},
    )
    assert response.status_code == 200, response.text
    assert refresh_calls == ["refresh"]
