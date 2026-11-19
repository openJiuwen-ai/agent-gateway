# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Cron model validation through AgentServer's models.list."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.common.remote_model_catalog import (
    RemoteModelCatalog,
    fetch_agent_model_rows,
    resolve_remote_cron_model,
)
from agent_gateway.cron.controller import CronController
from agent_gateway.cron.store import CronJobStore


def _row(
    model_id: str,
    model_name: str,
    *,
    alias: str = "",
    source: str = "",
    is_agentos: bool = False,
) -> dict:
    return {
        "model_id": model_id,
        "model_name": model_name,
        "alias": alias,
        "source": source,
        "is_agentos": is_agentos,
        "model_provider": "OpenAI",
        "api_base": "https://example.com/v1",
        "api_key": "sk-test",
        "is_default": True,
    }


def test_resolve_remote_cron_model_uses_name_alias_and_login_source() -> None:
    rows = [
        _row("mdl_1", "my-model", alias="我的模型"),
        _row("mdl_2", "GLM-5.2", source="huawei-maas-login"),
    ]

    assert resolve_remote_cron_model("my-model", rows) == ("my-model", "config")
    assert resolve_remote_cron_model("我的模型", rows) == ("my-model", "config")
    assert resolve_remote_cron_model("GLM-5.2", rows) == (
        "GLM-5.2",
        "login",
    )


@pytest.mark.asyncio
async def test_fetch_agent_model_rows_forwards_login_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from e2a.constants import E2A_AUTH_SESSION_PARAM_KEY

    captured: dict = {}

    async def fake_fetch(**kwargs):
        captured.update(kwargs)
        return True, {"models": [_row("mdl_1", "my-model")]}

    monkeypatch.setattr(
        "agent_gateway.common.remote_model_catalog.fetch_agent_unary",
        fake_fetch,
    )

    rows = await fetch_agent_model_rows(
        agent_client=object(), auth_session="auth-session-1"
    )

    assert rows is not None
    assert captured["params"][E2A_AUTH_SESSION_PARAM_KEY] == "auth-session-1"


def test_resolve_remote_cron_model_prefers_configured_model_on_name_collision() -> None:
    rows = [
        _row("mdl_login", "GLM-5.2", source="huawei-maas-login"),
        _row("mdl_config", "GLM-5.2"),
    ]

    assert resolve_remote_cron_model("GLM-5.2", rows) == ("GLM-5.2", "config")


def test_remote_model_catalog_supports_stable_model_ids() -> None:
    from agent_gateway.common.model_selection import ModelSelection

    catalog = RemoteModelCatalog([_row("mdl_1", "my-model")])
    selection = ModelSelection(type="model", id="mdl_1")

    assert catalog.has_selection(selection)
    assert catalog.get_model("mdl_1")["entry"]["model_client_config"]["model_name"] == "my-model"


def _make_controller(tmp_path) -> tuple[CronController, CronJobStore]:
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    scheduler = SimpleNamespace(
        reload=AsyncMock(),
        project_execution_allowed=AsyncMock(return_value=True),
    )
    controller = CronController(store=store, scheduler=scheduler)
    controller.bind_agent_client(object())
    return controller, store


def _job_params(**overrides) -> dict:
    params = {
        "name": "job",
        "cron_expr": "0 0 9 * * ? *",
        "timezone": "Asia/Shanghai",
        "description": "reminder",
        "targets": "web",
    }
    params.update(overrides)
    return params


@pytest.mark.asyncio
async def test_create_job_without_model_does_not_require_models_list(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, store = _make_controller(tmp_path)
    fetch = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "agent_gateway.common.remote_model_catalog.fetch_agent_model_rows",
        fetch,
    )

    job = await controller.create_job(_job_params())

    assert job.get("model_name") is None
    assert (await store.get_job(job["id"])) is not None
    fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_remote_model_rows_forwards_routing_identity(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, _ = _make_controller(tmp_path)
    captured: dict = {}

    async def fake_fetch(**kwargs):
        captured.update(kwargs)
        return [_row("mdl_1", "remote-model")]

    monkeypatch.setattr(
        "agent_gateway.common.remote_model_catalog.fetch_agent_model_rows",
        fake_fetch,
    )

    await controller._remote_model_rows(
        user_id="user-1",
        group_id="group-1",
        bot_id="bot-1",
    )

    assert captured["user_id"] == "user-1"
    assert captured["metadata"]


@pytest.mark.asyncio
async def test_create_job_accepts_model_from_agent_server(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, store = _make_controller(tmp_path)
    rows = [_row("mdl_1", "remote-model", alias="远端模型")]
    monkeypatch.setattr(
        "agent_gateway.common.remote_model_catalog.fetch_agent_model_rows",
        AsyncMock(return_value=rows),
    )

    job = await controller.create_job(_job_params(model_name="远端模型"))

    assert job["model_name"] == "remote-model"
    assert (await store.get_job(job["id"])) is not None


@pytest.mark.asyncio
async def test_create_job_rejects_unknown_model_from_agent_server(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, _ = _make_controller(tmp_path)
    monkeypatch.setattr(
        "agent_gateway.common.remote_model_catalog.fetch_agent_model_rows",
        AsyncMock(return_value=[_row("mdl_1", "remote-model")]),
    )

    with pytest.raises(ValueError, match="Unknown model 'missing-model'"):
        await controller.create_job(_job_params(model_name="missing-model"))


@pytest.mark.asyncio
async def test_update_job_validates_model_selection_with_agent_server(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    controller, store = _make_controller(tmp_path)
    created = await store.create_job(**_job_params(model_name="old-model"))
    rows = [_row("mdl_1", "remote-model")]
    monkeypatch.setattr(
        "agent_gateway.common.remote_model_catalog.fetch_agent_model_rows",
        AsyncMock(return_value=rows),
    )

    updated = await controller.update_job(
        created.id,
        {"model_selection": {"type": "model", "id": "mdl_1"}},
    )

    assert updated["model_selection"]["type"] == "model"
    assert updated["model_selection"]["id"] == "mdl_1"
    stored = await CronJobStore(path=store.path).get_job(created.id)
    assert stored.model_selection is not None
    assert stored.model_selection["id"] == "mdl_1"
    assert "_model_selection_remote_validated" not in stored.to_dict()


@pytest.mark.asyncio
async def test_update_job_ignores_client_supplied_remote_validation_marker(
    tmp_path,
) -> None:
    controller, store = _make_controller(tmp_path)
    controller._agent_client = None
    created = await store.create_job(**_job_params())

    with pytest.raises(Exception):
        await controller.update_job(
            created.id,
            {
                "model_selection": {"type": "model", "id": "missing-model"},
                "_model_selection_remote_validated": True,
            },
        )

    stored = await CronJobStore(path=store.path).get_job(created.id)
    assert stored.model_selection is None
