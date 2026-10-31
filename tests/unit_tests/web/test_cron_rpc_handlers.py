# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.routing import e2a_proxy
from agent_gateway.channel_manager.web.cron_rpc_handlers import (
    register_cron_handlers,
)


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


class _ProactiveJob:
    mode = "Proactive.Tick"
    user_id = "u1"


class _Controller:
    def __init__(self):
        self.calls = []

    async def list_jobs(self, user_id=None):
        self.calls.append(("list_jobs", user_id))
        return []

    def job_metadata(self):
        return {"modes": ["solo"]}

    async def get_job(self, job_id):
        self.calls.append(("get_job", job_id))
        return {"id": job_id, "user_id": "u1"}

    async def create_job(self, params):
        self.calls.append(("create_job", params))
        return dict(params)

    async def update_job(self, job_id, patch):
        self.calls.append(("update_job", job_id, patch))
        return {"id": job_id, **patch}

    async def delete_job(self, job_id):
        self.calls.append(("delete_job", job_id))
        return True

    async def toggle_job(self, job_id, enabled):
        self.calls.append(("toggle_job", job_id, enabled))
        return {"id": job_id, "enabled": enabled}

    async def preview_job(self, job_id, count):
        self.calls.append(("preview_job", job_id, count))
        return list(range(count))

    async def run_now_info(self, job_id):
        self.calls.append(("run_now_info", job_id))
        return {"run_id": "run-1", "session_id": "session-1"}


def _register(controller):
    channel = _FakeChannel()
    register_cron_handlers(
        channel,
        controller,
        agent_client=object(),
        _resolve=lambda ref: ref,
    )
    return channel


@pytest.mark.asyncio
async def test_cron_routes_are_registered_in_original_order():
    channel = _register(_Controller())

    assert list(channel.methods) == [
        "cron.job.list",
        "cron.job.meta",
        "cron.job.get",
        "cron.job.create",
        "cron.job.update",
        "cron.job.delete",
        "cron.job.toggle",
        "cron.job.preview",
        "cron.job.run_now",
    ]


@pytest.mark.asyncio
async def test_cron_job_list_filters_default_project_by_work_mode():
    controller = _Controller()

    async def list_jobs(user_id=None):
        return [
            {"project_id": "default", "work_mode": "work"},
            {"project_id": "", "work_mode": "work"},
            {"project_id": "", "work_mode": "code"},
            {"project_id": "", "work_mode": "bad"},
            {"project_id": "default_code", "work_mode": "code"},
            {"project_id": "p1", "work_mode": "work"},
        ]

    controller.list_jobs = list_jobs
    channel = _register(controller)

    await channel.methods["cron.job.list"](
        object(), "rpc-1", {"project_id": "default"}, "session-1", user_id="u1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-1",
            "ok": True,
            "payload": {
                "jobs": [
                    {"project_id": "default", "work_mode": "work"},
                    {"project_id": "", "work_mode": "work"},
                    {"project_id": "", "work_mode": "bad"},
                ]
            },
        }
    ]


@pytest.mark.asyncio
async def test_cron_job_get_enforces_owner():
    channel = _register(_Controller())

    await channel.methods["cron.job.get"](
        object(), "rpc-2", {"id": "job-1"}, "session-1", user_id="u2"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-2",
            "ok": False,
            "error": "job not found",
            "code": "NOT_FOUND",
        }
    ]


@pytest.mark.asyncio
async def test_cron_job_create_binds_agent_project(monkeypatch):
    async def resolve_binding(**kwargs):
        return True, {"project_id": "p1", "work_mode": "work"}

    monkeypatch.setattr(e2a_proxy, "resolve_agent_cron_project_binding", resolve_binding)
    controller = _Controller()
    channel = _register(controller)

    await channel.methods["cron.job.create"](
        object(), "rpc-3", {"name": "nightly"}, "", user_id="u1"
    )

    job = channel.responses[0]["payload"]["job"]
    assert job["name"] == "nightly"
    assert job["user_id"] == "u1"
    assert job["project_id"] == "p1"
    assert job["work_mode"] == "work"
    assert job["_agentos_project_binding_verified"] is True
    assert "project_dir" not in job


@pytest.mark.asyncio
async def test_cron_job_update_resolves_project_binding(monkeypatch):
    async def resolve_binding(**kwargs):
        return True, {"project_id": "p1", "work_mode": "code"}

    monkeypatch.setattr(e2a_proxy, "resolve_agent_cron_project_binding", resolve_binding)
    controller = _Controller()
    channel = _register(controller)

    await channel.methods["cron.job.update"](
        object(),
        "rpc-4",
        {"id": "job-1", "patch": {"project_dir": "/tmp/project"}},
        "session-1",
        user_id="u1",
    )

    job = channel.responses[0]["payload"]["job"]
    assert job["project_id"] == "p1"
    assert job["work_mode"] == "code"
    assert job["_agentos_project_binding_verified"] is True
    assert "project_dir" not in job


@pytest.mark.asyncio
async def test_cron_job_delete_blocks_proactive_tick():
    controller = _Controller()

    async def get_job(job_id):
        return _ProactiveJob()

    controller.get_job = get_job
    channel = _register(controller)

    await channel.methods["cron.job.delete"](
        object(), "rpc-5", {"id": "job-1"}, "session-1", user_id="u1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-5",
            "ok": False,
            "error": "主动推荐定时任务由设置→主动推荐开关控制，不能在面板删除；请到设置关闭开关。",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_cron_job_toggle_blocks_proactive_tick():
    controller = _Controller()

    async def get_job(job_id):
        return _ProactiveJob()

    controller.get_job = get_job
    channel = _register(controller)

    await channel.methods["cron.job.toggle"](
        object(), "rpc-6", {"id": "job-1", "enabled": True}, "session-1", user_id="u1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-6",
            "ok": False,
            "error": "主动推荐定时任务由设置→主动推荐开关控制，不能在面板启停。",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_cron_job_preview_requires_ownership():
    controller = _Controller()

    async def get_job(job_id):
        return None

    controller.get_job = get_job
    channel = _register(controller)

    await channel.methods["cron.job.preview"](
        object(), "rpc-7", {"id": "job-1", "count": 3}, "session-1", user_id="u1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-7",
            "ok": False,
            "error": "job not found",
            "code": "NOT_FOUND",
        }
    ]


@pytest.mark.asyncio
async def test_cron_job_run_now_returns_run_info():
    controller = _Controller()
    channel = _register(controller)

    await channel.methods["cron.job.run_now"](
        object(), "rpc-8", {"id": "job-1"}, "session-1", user_id="u1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-8",
            "ok": True,
            "payload": {
                "accepted": True,
                "run_id": "run-1",
                "session_id": "session-1",
            },
        }
    ]
