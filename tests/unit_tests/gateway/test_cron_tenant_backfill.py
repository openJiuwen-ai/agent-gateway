# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""企业版定时任务：从 HTTP/WS 路由身份回填 group/bot，且路由优先于 body 残留值。"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from agent_gateway.channel_manager.web.app_web_handlers import (
    WebHandlersBindParams,
    _register_web_handlers,
)


class _FakeWebChannel:
    def __init__(self) -> None:
        self.methods: dict[str, object] = {}
        self.responses: list[dict] = []

    def register_method(self, name, handler, *, local_only: bool = False):
        # production channel.register_method 现支持 local_only；假对象需吞掉以免注册阶段炸。
        _ = local_only
        self.methods[name] = handler

    def on_connect(self, handler):
        return None

    def on_disconnect(self, handler):
        return None

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
        self.responses.append(
            {
                "id": req_id,
                "ok": ok,
                "payload": payload,
                "error": error,
                "code": code,
            }
        )


class _CapturingCronController:
    def __init__(self) -> None:
        self.params = None

    async def create_job(self, params):
        self.params = dict(params)
        return {"id": "cron-1"}


class _TenantCaptureCronController:
    """记录 list/get/delete/toggle/preview 收到的租户 kwargs。"""

    def __init__(self, *, user_id: str = "zxy") -> None:
        self.calls: list[tuple[str, dict]] = []
        self.job = {
            "id": "cron-tenant-1",
            "user_id": user_id,
            "group_id": "group-current",
            "bot_id": "bot-current",
            "mode": "agent",
            "enabled": True,
        }

    def _record(self, op: str, **kwargs) -> None:
        self.calls.append((op, dict(kwargs)))

    def last(self, op: str) -> dict:
        for name, kwargs in reversed(self.calls):
            if name == op:
                return kwargs
        raise AssertionError(f"no {op} call recorded: {self.calls!r}")

    async def list_jobs(self, project_id=None, user_id="", group_id="", bot_id=""):
        self._record(
            "list",
            project_id=project_id,
            user_id=user_id,
            group_id=group_id,
            bot_id=bot_id,
        )
        return [dict(self.job)]

    async def get_job(self, job_id, project_id=None, user_id="", group_id="", bot_id=""):
        self._record(
            "get",
            job_id=job_id,
            project_id=project_id,
            user_id=user_id,
            group_id=group_id,
            bot_id=bot_id,
        )
        return dict(self.job) if job_id == self.job["id"] else None

    async def delete_job(self, job_id, **kwargs):
        self._record("delete", job_id=job_id, **kwargs)
        return True

    async def toggle_job(self, job_id, enabled, **kwargs):
        self._record("toggle", job_id=job_id, enabled=enabled, **kwargs)
        out = dict(self.job)
        out["enabled"] = bool(enabled)
        return out

    async def preview_job(self, job_id, count, **kwargs):
        self._record("preview", job_id=job_id, count=count, **kwargs)
        return [{"wake_at": "t0", "push_at": "t0"}]


def _cron_http_request_with_routing(
    *,
    user_id: str = "zxy",
    group_id: str = "group-current",
    bot_id: str = "bot-current",
):
    """模拟 HTTP outbound：身份在 _web_routing（来自请求头），不在 body。"""
    return SimpleNamespace(
        _web_routing={
            "user_id": user_id,
            "group_id": group_id,
            "bot_id": bot_id,
        },
        _jiuwen_auth_session="",
        _jiuwen_app_id="default",
    )


def _bind(channel, cron):
    _register_web_handlers(
        WebHandlersBindParams(
            channel=channel,
            agent_client=object(),
            cron_controller=cron,
        )
    )


@pytest.mark.asyncio
async def test_cron_create_backfills_tenant_ids_from_http_routing(monkeypatch) -> None:
    """创建体不带 group/bot 时从路由回填；user_id 以连接侧为准。"""
    channel = _FakeWebChannel()
    cron = _CapturingCronController()
    _bind(channel, cron)
    monkeypatch.setattr(
        "agent_gateway.routing.e2a_proxy.is_legacy_shared_directory_client",
        lambda _client: False,
    )
    http_request = _cron_http_request_with_routing(
        user_id="routing-user",
        group_id="zuzhi1",
        bot_id="bot-abc",
    )

    await channel.methods["cron.job.create"](
        http_request,
        "req-cron-tenant",
        {"name": "job", "cron_expr": "0 9 * * *", "description": "hello"},
        "user-session",
        "zxy",
    )

    assert channel.responses[-1]["ok"] is True, channel.responses[-1]
    assert cron.params["user_id"] == "zxy"
    assert cron.params["group_id"] == "zuzhi1"
    assert cron.params["bot_id"] == "bot-abc"


@pytest.mark.asyncio
async def test_cron_create_routing_overrides_stale_body_tenant_ids(monkeypatch) -> None:
    """创建体残留旧组织时，以当前请求路由身份覆盖。"""
    channel = _FakeWebChannel()
    cron = _CapturingCronController()
    _bind(channel, cron)
    monkeypatch.setattr(
        "agent_gateway.routing.e2a_proxy.is_legacy_shared_directory_client",
        lambda _client: False,
    )
    http_request = _cron_http_request_with_routing()

    await channel.methods["cron.job.create"](
        http_request,
        "req-cron-stale-tenant",
        {
            "name": "job",
            "cron_expr": "0 9 * * *",
            "description": "hello",
            "group_id": "group-stale",
            "bot_id": "bot-stale",
        },
        "user-session",
        "zxy",
    )

    assert channel.responses[-1]["ok"] is True, channel.responses[-1]
    assert cron.params["group_id"] == "group-current"
    assert cron.params["bot_id"] == "bot-current"


@pytest.mark.asyncio
async def test_cron_query_paths_backfill_tenant_ids_from_routing() -> None:
    """list/get/delete/toggle/preview 必须把路由身份传给控制器。"""
    channel = _FakeWebChannel()
    cron = _TenantCaptureCronController()
    _bind(channel, cron)
    http_request = _cron_http_request_with_routing(
        group_id="group-route",
        bot_id="bot-route",
    )
    job_id = cron.job["id"]

    await channel.methods["cron.job.list"](http_request, "req-list", {}, "sess", "zxy")
    await channel.methods["cron.job.get"](
        http_request, "req-get", {"id": job_id}, "sess", "zxy"
    )
    await channel.methods["cron.job.toggle"](
        http_request, "req-toggle", {"id": job_id, "enabled": False}, "sess", "zxy"
    )
    await channel.methods["cron.job.preview"](
        http_request, "req-preview", {"id": job_id, "count": 2}, "sess", "zxy"
    )
    await channel.methods["cron.job.delete"](
        http_request, "req-delete", {"id": job_id}, "sess", "zxy"
    )

    assert all(r["ok"] is True for r in channel.responses[-5:]), channel.responses[-5:]
    for op in ("list", "get", "toggle", "preview", "delete"):
        kwargs = cron.last(op)
        assert kwargs["group_id"] == "group-route", (op, kwargs)
        assert kwargs["bot_id"] == "bot-route", (op, kwargs)
        assert kwargs["user_id"] == "zxy", (op, kwargs)


@pytest.mark.asyncio
async def test_cron_query_paths_routing_overrides_stale_body_tenant_ids() -> None:
    """查改删开关预览：body 残留旧 group/bot 时仍以路由为准。"""
    channel = _FakeWebChannel()
    cron = _TenantCaptureCronController()
    _bind(channel, cron)
    http_request = _cron_http_request_with_routing(
        group_id="group-route",
        bot_id="bot-route",
    )
    job_id = cron.job["id"]
    stale = {
        "id": job_id,
        "group_id": "group-stale",
        "bot_id": "bot-stale",
    }

    await channel.methods["cron.job.get"](http_request, "req-get", dict(stale), "sess", "zxy")
    await channel.methods["cron.job.toggle"](
        http_request, "req-toggle", {**stale, "enabled": True}, "sess", "zxy"
    )
    await channel.methods["cron.job.preview"](
        http_request, "req-preview", {**stale, "count": 1}, "sess", "zxy"
    )
    await channel.methods["cron.job.delete"](
        http_request, "req-delete", dict(stale), "sess", "zxy"
    )
    await channel.methods["cron.job.list"](
        http_request,
        "req-list",
        {"group_id": "group-stale", "bot_id": "bot-stale"},
        "sess",
        "zxy",
    )

    assert all(r["ok"] is True for r in channel.responses[-5:]), channel.responses[-5:]
    for op in ("get", "toggle", "preview", "delete", "list"):
        kwargs = cron.last(op)
        assert kwargs["group_id"] == "group-route", (op, kwargs)
        assert kwargs["bot_id"] == "bot-route", (op, kwargs)
