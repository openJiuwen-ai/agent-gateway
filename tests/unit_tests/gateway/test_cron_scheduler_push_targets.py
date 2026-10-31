# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from types import SimpleNamespace

import pytest

from agent_gateway.cron.scheduler import CronSchedulerService


class _CapturingHandler:
    def __init__(self) -> None:
        self.messages = []

    async def publish_robot_messages(self, msg) -> None:
        self.messages.append(msg)


def _make_job(target: str, **overrides) -> SimpleNamespace:
    base = {
        "id": "job-1",
        "name": "Daily report",
        "enabled": True,
        "expired": False,
        "targets": target,
        "session_id": None,
        "chat_type": None,
        "mode": "agent",
        "user_id": "user-1",
        "app_id": "",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _make_state() -> SimpleNamespace:
    return SimpleNamespace(
        run_id="run-1",
        job_id="job-1",
        wake_at_iso="2026-01-15T09:30:00+08:00",
        push_at_iso="2026-01-15T10:00:00+08:00",
        status="succeeded",
        placeholder_sent=False,
        pushed_final=False,
        result_text=None,
        error=None,
        job_name="Daily report",
        targets="web",
        session_id=None,
        chat_type=None,
        timezone=None,
        exec_mode=None,
        exec_channel_id=None,
        exec_session_id=None,
        exec_user_id=None,
        exec_work_mode=None,
        exec_project_id=None,
        exec_project_dir=None,
        execution_session_allocated=False,
        manually_triggered=False,
    )


def _make_service(handler: _CapturingHandler) -> CronSchedulerService:
    service = object.__new__(CronSchedulerService)
    service._now_fn = lambda: 1234.5
    service._message_handler = handler
    return service


@pytest.mark.asyncio
async def test_push_to_targets_web_message_is_addressed_to_target_channel():
    handler = _CapturingHandler()
    service = _make_service(handler)
    job = _make_job("web")
    state = _make_state()

    await service._push_to_targets(
        job,
        state,
        text=" result ",
        is_placeholder=True,
    )

    assert len(handler.messages) == 1
    msg = handler.messages[0]
    assert msg.id == "cron-push-run-1-web"
    assert msg.type == "event"
    assert msg.channel_id == "web"
    assert msg.session_id is None
    assert msg.timestamp == 1234.5
    assert msg.payload["content"] == "result"
    assert msg.payload["user_id"] == "user-1"
    assert msg.payload["cron"]["job_id"] == "job-1"
    assert msg.payload["cron"]["run_id"] == "run-1"
    assert msg.payload["cron"]["is_placeholder"] is True
    assert msg.metadata == {}


@pytest.mark.asyncio
async def test_push_to_targets_web_is_placeholder_not_triggered_by_job_mode():
    handler = _CapturingHandler()
    service = _make_service(handler)
    job = _make_job("web", mode="proactive.tick")
    state = _make_state()

    await service._push_to_targets(job, state, text="x", is_placeholder=False)

    msg = handler.messages[0]
    assert msg.payload["cron"]["is_placeholder"] is False
    assert "source" not in msg.payload


@pytest.mark.asyncio
async def test_push_to_targets_proactive_manual_notification_uses_toast_source():
    handler = _CapturingHandler()
    service = _make_service(handler)
    job = _make_job("web", mode="proactive.tick")
    state = _make_state()
    state.manually_triggered = True

    await service._push_to_targets(job, state, text="x", is_placeholder=False)

    msg = handler.messages[0]
    assert msg.payload["source"] == "proactive_notification"
