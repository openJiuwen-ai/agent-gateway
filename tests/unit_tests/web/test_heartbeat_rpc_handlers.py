# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import pytest

from agent_gateway.heartbeat import HeartbeatServiceUnavailableError
from agent_gateway.channel_manager.web.heartbeat_rpc_handlers import (
    register_heartbeat_handlers,
)


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})


class _FakeController:
    async def list_jobs(self, params, access_session_id, user_id):
        return {"jobs": [{"id": "job-1"}]}

    async def get_meta(self, user_id):
        return {"available": True}

    async def get_job(self, job_id, access_session_id, user_id):
        return {"id": job_id}

    async def create_job(self, params, user_id):
        return dict(params)

    async def update_job(self, job_id, patch, access_session_id, user_id):
        return {"id": job_id, **patch}

    async def delete_job(self, job_id, access_session_id, user_id):
        return {"deleted": True}

    async def toggle_job(self, job_id, enabled, access_session_id, user_id):
        return {"id": job_id, "enabled": enabled}

    async def preview_job(self, job_id, count, access_session_id, user_id):
        return {"count": count}

    async def run_now(self, job_id, reschedule, access_session_id, user_id):
        return {"id": job_id, "reschedule": reschedule}

    async def cancel_run(self, job_id, pause_schedule, access_session_id, user_id):
        return {"id": job_id, "pause_schedule": pause_schedule}


def _register(controller):
    channel = _FakeChannel()
    register_heartbeat_handlers(
        channel,
        controller,
        _resolve=lambda ref: ref,
    )
    return channel


def test_heartbeat_routes_are_registered_in_original_order():
    channel = _register(_FakeController())

    assert list(channel.methods) == [
        "heartbeat.job.list",
        "heartbeat.job.meta",
        "heartbeat.job.get",
        "heartbeat.job.create",
        "heartbeat.job.update",
        "heartbeat.job.delete",
        "heartbeat.job.toggle",
        "heartbeat.job.preview",
        "heartbeat.job.run_now",
        "heartbeat.job.cancel",
    ]


@pytest.mark.asyncio
async def test_heartbeat_controller_is_unavailable_for_none():
    channel = _register(None)

    await channel.methods["heartbeat.job.list"](
        object(), "rpc-1", {}, "session-1", user_id="u1"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-1",
            "ok": False,
            "error": "heartbeat not available",
            "code": "SERVICE_UNAVAILABLE",
        }
    ]


@pytest.mark.asyncio
async def test_heartbeat_get_forbidden():
    controller = _FakeController()

    async def get_job(job_id, access_session_id, user_id):
        raise PermissionError("denied")

    controller.get_job = get_job
    channel = _register(controller)

    await channel.methods["heartbeat.job.get"](
        object(), "rpc-2", {"id": "job-1"}, "session-2", user_id="u2"
    )

    assert channel.responses == [
        {"req_id": "rpc-2", "ok": False, "error": "denied", "code": "FORBIDDEN"}
    ]


@pytest.mark.asyncio
async def test_heartbeat_get_missing_job():
    controller = _FakeController()

    async def get_job(job_id, access_session_id, user_id):
        raise KeyError(job_id)

    controller.get_job = get_job
    channel = _register(controller)

    await channel.methods["heartbeat.job.get"](
        object(), "rpc-3", {"id": "job-1"}, "session-3", user_id="u3"
    )

    assert channel.responses == [
        {"req_id": "rpc-3", "ok": False, "error": "job not found", "code": "NOT_FOUND"}
    ]


@pytest.mark.asyncio
async def test_heartbeat_preview_value_error_is_bad_request():
    controller = _FakeController()

    async def preview_job(job_id, count, access_session_id, user_id):
        raise ValueError("bad value")

    controller.preview_job = preview_job
    channel = _register(controller)

    await channel.methods["heartbeat.job.preview"](
        object(), "rpc-4", {"id": "job-1"}, "session-4", user_id="u4"
    )

    assert channel.responses == [
        {"req_id": "rpc-4", "ok": False, "error": "bad value", "code": "BAD_REQUEST"}
    ]


@pytest.mark.asyncio
async def test_heartbeat_create_binds_web_session_and_source():
    controller = _FakeController()
    channel = _register(controller)

    await channel.methods["heartbeat.job.create"](
        object(), "rpc-5", {"name": "daily"}, "session-5", user_id="u5"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-5",
            "ok": True,
            "payload": {
                "job": {
                    "name": "daily",
                    "channel_id": "web",
                    "session_id": "session-5",
                    "source": "web_rpc",
                }
            },
        }
    ]


@pytest.mark.asyncio
async def test_heartbeat_preview_invalid_count_falls_back_to_five():
    controller = _FakeController()
    channel = _register(controller)

    await channel.methods["heartbeat.job.preview"](
        object(), "rpc-6", {"id": "job-1", "count": 0}, "session-6", user_id="u6"
    )

    assert channel.responses == [
        {"req_id": "rpc-6", "ok": True, "payload": {"count": 5}}
    ]


@pytest.mark.asyncio
async def test_heartbeat_run_now_requires_boolean_reschedule():
    controller = _FakeController()
    channel = _register(controller)

    await channel.methods["heartbeat.job.run_now"](
        object(), "rpc-7", {"id": "job-1", "reschedule": 1}, "session-7", user_id="u7"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-7",
            "ok": False,
            "error": "reschedule must be boolean",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_heartbeat_cancel_requires_boolean_pause_schedule():
    controller = _FakeController()
    channel = _register(controller)

    await channel.methods["heartbeat.job.cancel"](
        object(), "rpc-8", {"id": "job-1", "pause_schedule": 1}, "session-8", user_id="u8"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-8",
            "ok": False,
            "error": "pause_schedule must be boolean",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_heartbeat_service_error_maps_to_unavailable():
    controller = _FakeController()

    async def get_meta(user_id):
        raise HeartbeatServiceUnavailableError("service is starting")

    controller.get_meta = get_meta
    channel = _register(controller)

    await channel.methods["heartbeat.job.meta"](
        object(), "rpc-9", {}, "session-9", user_id="u9"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-9",
            "ok": False,
            "error": "service is starting",
            "code": "SERVICE_UNAVAILABLE",
        }
    ]
