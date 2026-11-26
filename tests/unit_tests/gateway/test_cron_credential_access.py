# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from e2a.agent_models import AgentResponse, AgentResponseChunk

from agent_gateway.auth import model_catalog as _model_catalog
from agent_gateway.auth.credential_access import ModelCredential
from agent_gateway.auth.model_catalog import LoginModel
from agent_gateway.cron import controller as cron_controller_module
from agent_gateway.cron import scheduler as cron_scheduler_module
from agent_gateway.cron.controller import CronController
from agent_gateway.cron.scheduler import CronSchedulerService
from agent_gateway.cron.store import CronJobStore


_LOGIN_REF = "0123456789abcdef0123456789abcdef"
_AUTH_SESSION = "auth-session-1"
_MODEL_AUTH = {
    "api_base": "https://apig.example.com/v1",
    "api_key": "token-from-access",
    "credential_ref": _LOGIN_REF,
}


def _config_entry(model_name: str) -> dict:
    return {
        "model_client_config": {
            "api_base": "https://api.example.com/v1",
            "api_key": "sk-self",
            "model_name": model_name,
            "client_provider": "OpenAI",
        },
        "is_default": True,
        "alias": "",
    }


def _patch_model_sources(
    monkeypatch: pytest.MonkeyPatch,
    *,
    login_models: list[str],
    config_models: list[str] | None = None,
) -> None:
    monkeypatch.setattr(
        _model_catalog,
        "get_models",
        lambda *args, **kwargs: [
            LoginModel(model_name=name, display_name=name) for name in login_models
        ],
    )
    if config_models:
        entries = {name: _config_entry(name) for name in config_models}

        def fake_get_model_config(name, index=None):
            return entries.get(name)

        monkeypatch.setattr(
            "agent_gateway.common.config.get_model_config", fake_get_model_config
        )
        monkeypatch.setattr(
            "agent_gateway.common.config.get_model_names", lambda: list(config_models)
        )
    else:
        monkeypatch.setattr(
            "agent_gateway.common.config.get_model_config",
            lambda name, index=None: None,
        )
        monkeypatch.setattr(
            "agent_gateway.common.config.get_model_names", lambda: []
        )


def _credential_access(*, ref: str | None = _LOGIN_REF) -> SimpleNamespace:
    return SimpleNamespace(
        credential_ref_for_login_session=AsyncMock(return_value=ref),
        model_auth_for_ref=AsyncMock(
            return_value=ModelCredential(
                api_base=_MODEL_AUTH["api_base"],
                api_key=_MODEL_AUTH["api_key"],
                credential_ref=_LOGIN_REF,
            )
        ),
    )


def _make_controller(tmp_path) -> tuple[CronController, CronJobStore]:
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    scheduler = SimpleNamespace(
        reload=AsyncMock(),
        project_execution_allowed=AsyncMock(return_value=True),
    )
    return CronController(store=store, scheduler=scheduler), store


def _job_params(**overrides) -> dict:
    params: dict = {
        "name": "job",
        "cron_expr": "0 0 9 * * ? *",
        "timezone": "Asia/Shanghai",
        "description": "reminder",
        "targets": "web",
    }
    params.update(overrides)
    return params


@pytest.mark.asyncio
async def test_create_job_uses_credential_access_and_ignores_client_ref(
    tmp_path, monkeypatch
):
    controller, store = _make_controller(tmp_path)
    _patch_model_sources(monkeypatch, login_models=["GLM-5.2"])
    access = _credential_access()
    monkeypatch.setattr(
        cron_controller_module,
        "_login_credential_ref_from_session",
        lambda auth_session: pytest.fail("legacy session helper used"),
    )

    controller.set_credential_access(access)
    job = await controller.create_job(
        _job_params(
            model_name="GLM-5.2",
            _auth_session=_AUTH_SESSION,
            credential_ref="client-forged-ref",
        )
    )

    access.credential_ref_for_login_session.assert_awaited_once_with(_AUTH_SESSION)
    assert job["credential_ref"] == _LOGIN_REF
    assert "_auth_session" not in job
    assert (await store.get_job(job["id"])).credential_ref == _LOGIN_REF


@pytest.mark.asyncio
async def test_create_job_login_model_without_session_rejected(tmp_path, monkeypatch):
    controller, store = _make_controller(tmp_path)
    _patch_model_sources(monkeypatch, login_models=["GLM-5.2"])
    access = _credential_access(ref=None)
    monkeypatch.setattr(
        cron_controller_module,
        "_login_credential_ref_from_session",
        lambda auth_session: pytest.fail("legacy session helper used"),
    )
    controller.set_credential_access(access)

    with pytest.raises(ValueError, match="该模型需要登录华为账号后使用"):
        await controller.create_job(_job_params(model_name="GLM-5.2"))

    access.credential_ref_for_login_session.assert_awaited_once_with(None)
    assert await store.list_jobs() == []


@pytest.mark.asyncio
async def test_create_job_config_model_does_not_query_credentials(tmp_path, monkeypatch):
    controller, store = _make_controller(tmp_path)
    _patch_model_sources(
        monkeypatch,
        login_models=["GLM-5.2"],
        config_models=["GLM-5.2"],
    )
    access = _credential_access()
    monkeypatch.setattr(
        cron_controller_module,
        "_login_credential_ref_from_session",
        lambda auth_session: pytest.fail("legacy session helper used"),
    )
    controller.set_credential_access(access)

    job = await controller.create_job(
        _job_params(model_name="GLM-5.2", _auth_session=_AUTH_SESSION)
    )

    access.credential_ref_for_login_session.assert_not_awaited()
    assert "credential_ref" not in job
    assert (await store.get_job(job["id"])).credential_ref == ""


@pytest.mark.asyncio
async def test_update_job_uses_credential_access_and_ignores_client_ref(
    tmp_path, monkeypatch
):
    controller, store = _make_controller(tmp_path)
    _patch_model_sources(
        monkeypatch,
        login_models=["GLM-5.2"],
        config_models=["my-model"],
    )
    access = _credential_access()
    created = await store.create_job(**_job_params(model_name="my-model"))
    monkeypatch.setattr(
        cron_controller_module,
        "_login_credential_ref_from_session",
        lambda auth_session: pytest.fail("legacy session helper used"),
    )
    controller.set_credential_access(access)

    updated = await controller.update_job(
        created.id,
        {
            "model_name": "GLM-5.2",
            "_auth_session": _AUTH_SESSION,
            "credential_ref": "client-forged-ref",
        },
    )

    access.credential_ref_for_login_session.assert_awaited_once_with(_AUTH_SESSION)
    assert updated["credential_ref"] == _LOGIN_REF
    assert (await store.get_job(created.id)).credential_ref == _LOGIN_REF



@pytest.mark.asyncio
async def test_create_job_without_access_keeps_legacy_helper(tmp_path, monkeypatch):
    controller, store = _make_controller(tmp_path)
    _patch_model_sources(monkeypatch, login_models=["GLM-5.2"])
    monkeypatch.setattr(
        cron_controller_module,
        "_login_credential_ref_from_session",
        lambda auth_session: _LOGIN_REF,
    )

    job = await controller.create_job(
        _job_params(model_name="GLM-5.2", _auth_session=_AUTH_SESSION)
    )

    assert job["credential_ref"] == _LOGIN_REF
    assert (await store.get_job(job["id"])).credential_ref == _LOGIN_REF


class _FakeAgentClient:
    def __init__(self) -> None:
        self.unary_requests = []
        self.stream_requests = []
        self.created_session_id = "cron_agentserver_allocated"

    async def send_request(self, envelope, *args, **kwargs):
        self.unary_requests.append(envelope)
        if envelope.method == "session.create":
            return AgentResponse(
                request_id=envelope.request_id or "",
                channel_id=envelope.channel or "",
                ok=True,
                payload={"session_id": self.created_session_id},
            )
        if envelope.method == "project.lifecycle":
            return AgentResponse(
                request_id=envelope.request_id or "",
                channel_id=envelope.channel or "",
                ok=True,
                payload={"exists": True, "execution_blocked": False},
            )
        return AgentResponse(
            request_id=envelope.request_id or "",
            channel_id=envelope.channel or "",
            ok=True,
            payload={"content": {"output": "done", "result_type": "answer"}},
        )

    async def send_request_stream(self, envelope):
        self.stream_requests.append(envelope)
        payloads = [
            {
                "event_type": "workflow.updated",
                "workflow": {"id": "wf-1", "status": "completed"},
            },
            {"event_type": "chat.final", "content": "done"},
            {"is_complete": True},
        ]
        for payload in payloads:
            yield AgentResponseChunk(
                request_id=envelope.request_id or "",
                channel_id=envelope.channel or "",
                payload=payload,
                is_complete=bool(payload.get("is_complete")),
            )


class _FakeMessageHandler:
    def __init__(self) -> None:
        self.published = []

    async def publish_robot_messages(self, msg):
        self.published.append(msg)

    async def register_background_stream(self, envelope, session_id):
        return None

    async def finish_background_stream(self, envelope, session_id):
        return None

    def background_stream_has_other_work(self, request_id, session_id):
        return False

    async def publish_stream_chunk(self, chunk, *, session_id, request_metadata=None):
        return False

    async def _cancel_agent_work_for_session(self, msg, old_sid, **kwargs):
        return None


class _ExposedCronScheduler(CronSchedulerService):
    async def on_wake(self, job, run_id):
        return await self._on_wake(job, run_id)

    @property
    def run_tasks(self):
        return self._run_tasks


async def _create_scheduler_job(store: CronJobStore, **overrides):
    return await store.create_job(
        name="login-model-job",
        cron_expr="0 0 9 * * ? *",
        timezone="Asia/Shanghai",
        description="reminder",
        targets="web",
        user_id="route-user-1",
        model_name="GLM-5.2",
        **overrides,
    )


async def _run_wake_job(store: CronJobStore, job, scheduler):
    run_id = f"{job.id}:{int(time.time())}"
    await scheduler.on_wake(job, run_id)
    await scheduler.run_tasks[run_id]


@pytest.mark.asyncio
async def test_wake_request_uses_credential_access(tmp_path, monkeypatch):
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_scheduler_job(store, credential_ref=_LOGIN_REF)
    agent = _FakeAgentClient()
    scheduler = _ExposedCronScheduler(
        store=store,
        agent_client=agent,
        message_handler=_FakeMessageHandler(),
    )
    access = _credential_access()
    monkeypatch.setattr(
        cron_scheduler_module,
        "_login_model_auth_for_job",
        lambda job: pytest.fail("legacy credential helper used"),
    )
    scheduler.set_credential_access(access)

    await _run_wake_job(store, job, scheduler)

    chat_envelope = next(
        envelope
        for envelope in agent.stream_requests
        if "model_name" in (envelope.params or {})
    )
    access.model_auth_for_ref.assert_awaited_once_with(_LOGIN_REF)
    assert chat_envelope.params["model_name"] == "GLM-5.2"
    assert chat_envelope.params["_model_auth"] == _MODEL_AUTH


@pytest.mark.asyncio
async def test_wake_request_without_binding_does_not_query_access(tmp_path, monkeypatch):
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_scheduler_job(store)
    agent = _FakeAgentClient()
    scheduler = _ExposedCronScheduler(
        store=store,
        agent_client=agent,
        message_handler=_FakeMessageHandler(),
    )
    access = _credential_access()
    monkeypatch.setattr(
        cron_scheduler_module,
        "_login_model_auth_for_job",
        lambda job: pytest.fail("legacy credential helper used"),
    )
    scheduler.set_credential_access(access)

    await _run_wake_job(store, job, scheduler)

    chat_envelope = next(
        envelope
        for envelope in agent.stream_requests
        if "model_name" in (envelope.params or {})
    )
    access.model_auth_for_ref.assert_not_awaited()
    assert chat_envelope.params["model_name"] == "GLM-5.2"
    assert "_model_auth" not in chat_envelope.params


@pytest.mark.asyncio
async def test_wake_request_with_invalid_ref_does_not_query_access(
    tmp_path, monkeypatch
):
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_scheduler_job(store, credential_ref="not-a-credential-ref")
    agent = _FakeAgentClient()
    scheduler = _ExposedCronScheduler(
        store=store,
        agent_client=agent,
        message_handler=_FakeMessageHandler(),
    )
    access = _credential_access()
    monkeypatch.setattr(
        cron_scheduler_module,
        "_login_model_auth_for_job",
        lambda job: pytest.fail("legacy credential helper used"),
    )
    scheduler.set_credential_access(access)

    await _run_wake_job(store, job, scheduler)

    chat_envelope = next(
        envelope
        for envelope in agent.stream_requests
        if "model_name" in (envelope.params or {})
    )
    access.model_auth_for_ref.assert_not_awaited()
    assert "_model_auth" not in chat_envelope.params


@pytest.mark.asyncio
async def test_wake_request_continues_without_credential_on_access_error(tmp_path, monkeypatch):
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_scheduler_job(store, credential_ref=_LOGIN_REF)
    agent = _FakeAgentClient()
    scheduler = _ExposedCronScheduler(
        store=store,
        agent_client=agent,
        message_handler=_FakeMessageHandler(),
    )
    access = _credential_access()
    access.model_auth_for_ref = AsyncMock(side_effect=RuntimeError("credential access failed"))
    monkeypatch.setattr(
        cron_scheduler_module,
        "_login_model_auth_for_job",
        lambda job: pytest.fail("legacy credential helper used"),
    )
    scheduler.set_credential_access(access)

    await _run_wake_job(store, job, scheduler)

    chat_envelope = next(
        envelope
        for envelope in agent.stream_requests
        if "model_name" in (envelope.params or {})
    )
    access.model_auth_for_ref.assert_awaited_once_with(_LOGIN_REF)
    assert chat_envelope.params["model_name"] == "GLM-5.2"
    assert "_model_auth" not in chat_envelope.params


@pytest.mark.asyncio
async def test_wake_request_without_access_keeps_legacy_helper(tmp_path, monkeypatch):
    store = CronJobStore(path=tmp_path / "cron_jobs.json")
    job = await _create_scheduler_job(store, credential_ref=_LOGIN_REF)
    agent = _FakeAgentClient()
    scheduler = _ExposedCronScheduler(
        store=store,
        agent_client=agent,
        message_handler=_FakeMessageHandler(),
    )
    monkeypatch.setattr(
        cron_scheduler_module,
        "_login_model_auth_for_job",
        lambda job: _MODEL_AUTH,
    )

    await _run_wake_job(store, job, scheduler)

    chat_envelope = next(
        envelope
        for envelope in agent.stream_requests
        if "model_name" in (envelope.params or {})
    )
    assert chat_envelope.params["model_name"] == "GLM-5.2"
    assert chat_envelope.params["_model_auth"] == _MODEL_AUTH
