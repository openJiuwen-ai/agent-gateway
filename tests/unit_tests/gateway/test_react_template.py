# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved
"""react_template 网关 Config Receiver 单测：body 传输 → data 落库映射及 upsert/update 行为。"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

pytest.importorskip("a2a", reason="a2a-sdk is an optional extra (pyproject a2a group); skip when not installed")

from agent_gateway.enterprise.config.manager_config.core.template import react_template
from agent_gateway.enterprise.config.manager_config.core.template.react_template import (
    _build_row_from_template,
    _normalize_data,
    _normalize_template_id,
    _upsert_react_template_from_sync,
    delete_react_template,
    update_react_template,
)
from agent_gateway.enterprise.config.manager_config.schemas.template_schemas import (
    ReactTemplateUpdateRequest,
)

pytestmark = pytest.mark.unit


def test_normalize_data_accepts_dict_and_none():
    assert _normalize_data(None) == {}
    assert _normalize_data({"enable_task_loop": True}) == {"enable_task_loop": True}


def test_normalize_data_rejects_non_dict():
    with pytest.raises(ValueError):
        _normalize_data("not-a-dict")
    with pytest.raises(ValueError):
        _normalize_data(["a", "b"])


def test_normalize_template_id():
    assert _normalize_template_id("tpl-1") == "tpl-1"
    assert _normalize_template_id("  tpl-1  ") == "tpl-1"
    with pytest.raises(ValueError):
        _normalize_template_id("")
    with pytest.raises(ValueError):
        _normalize_template_id("x" * 101)


def test_build_row_maps_body_to_data():
    sentinel = object()
    body = {
        "agent_name": "main_agent",
        "enable_task_loop": True,
        "subagents": {"general_agent": {"enabled": True}},
        "context_engine_config": {"enabled": True},
    }
    row = _build_row_from_template(
        {
            "template_id": "tpl-1",
            "template_name": "react-tpl",
            "description": "desc",
            "body": body,
        },
        now=sentinel,
    )
    assert row["template_id"] == "tpl-1"
    assert row["template_name"] == "react-tpl"
    assert row["description"] == "desc"
    assert row["enabled"] is True
    assert row["data"] == body
    assert row["created_at"] is sentinel
    assert row["updated_at"] is sentinel


def test_build_row_defaults_when_body_missing():
    sentinel = object()
    row = _build_row_from_template(
        {"template_id": "tpl-2", "template_name": "react-tpl", "enabled": False},
        now=sentinel,
    )
    assert row["data"] == {}
    assert row["enabled"] is False


@pytest.mark.asyncio
async def test_upsert_creates_row_when_missing():
    repo = AsyncMock()
    repo.get.return_value = None
    body = {"agent_name": "main_agent", "enable_task_loop": True}

    await _upsert_react_template_from_sync(
        repo,
        {"template_id": "tpl-1", "template_name": "react-tpl", "body": body},
    )

    repo.create.assert_awaited_once()
    repo.update.assert_not_awaited()
    row = repo.create.await_args.args[0]
    assert row["template_id"] == "tpl-1"
    assert row["template_name"] == "react-tpl"
    assert row["data"] == body
    assert row["created_at"].tzinfo is not None
    assert row["updated_at"].tzinfo is not None


@pytest.mark.asyncio
async def test_upsert_updates_row_and_preserves_created_at():
    repo = AsyncMock()
    repo.get.return_value = {
        "template_id": "tpl-1",
        "template_name": "old",
        "data": {},
        "created_at": "2026-11-01T00:00:00Z",
    }

    await _upsert_react_template_from_sync(
        repo,
        {"template_id": "tpl-1", "template_name": "react-tpl", "body": {"enable_task_loop": True}},
    )

    repo.create.assert_not_awaited()
    match, updates = repo.update.await_args.args
    assert match == {"template_id": "tpl-1"}
    assert "template_id" not in updates
    assert updates["template_name"] == "react-tpl"
    assert updates["data"] == {"enable_task_loop": True}
    assert updates["created_at"].year == 2026
    assert updates["created_at"].day == 1
    assert updates["updated_at"].tzinfo is not None


@pytest.mark.asyncio
async def test_update_react_template_maps_body_to_data():
    repo = AsyncMock()
    repo.get.return_value = {"template_id": "tpl-1", "template_name": "old", "data": {}}
    repo.update.return_value = {"template_id": "tpl-1"}
    request = ReactTemplateUpdateRequest.model_validate({
        "template_name": "  new-name  ",
        "body": {"enable_task_loop": True},
    })

    result = await update_react_template(repo, "tpl-1", request)

    assert result == {"template_id": "tpl-1"}
    match, updates = repo.update.await_args.args
    assert match == {"template_id": "tpl-1"}
    assert updates["template_name"] == "new-name"
    assert updates["data"] == {"enable_task_loop": True}
    assert "body" not in updates
    assert updates["updated_at"].tzinfo is not None


@pytest.mark.asyncio
async def test_update_react_template_returns_none_when_not_found():
    repo = AsyncMock()
    repo.get.return_value = None
    request = ReactTemplateUpdateRequest.model_validate({"template_name": "new-name"})

    assert await update_react_template(repo, "tpl-1", request) is None
    repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_react_template_returns_false_when_not_found():
    repo = AsyncMock()
    repo.get.return_value = None

    assert await delete_react_template(repo, "tpl-1") is False
    repo.delete.assert_not_awaited()


@pytest.mark.asyncio
async def test_service_update_raises_when_not_found(monkeypatch):
    repo = AsyncMock()
    repo.get.return_value = None
    monkeypatch.setattr(react_template, "require_enterprise_repository", lambda name: repo)

    with pytest.raises(ValueError, match="not found"):
        await react_template.ReactTemplateService().update(
            "tpl-1", {"template_name": "new-name"}
        )
