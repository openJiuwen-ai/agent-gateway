from __future__ import annotations

from typing import Any

import pytest

from agent_gateway.channel_manager.tui.tui_cron_handlers import (
    register_tui_cron_handlers,
)
import agent_gateway.routing.e2a_proxy as e2a_proxy


@pytest.mark.asyncio
async def test_cron_job_create_resolves_agent_client_ref_and_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Channel:
        def __init__(self) -> None:
            self.handlers: dict[str, Any] = {}
            self.responses: list[dict[str, Any]] = []

        def register_local_handler(self, path: str, method: str, handler: Any) -> None:
            self.handlers[method] = handler

        async def send_response(self, ws: Any, req_id: int, **kwargs: Any) -> None:
            self.responses.append({"ws": ws, "req_id": req_id, **kwargs})

    class _Cron:
        def __init__(self) -> None:
            self.created: dict[str, Any] | None = None

        async def create_job(self, params: dict[str, Any]) -> dict[str, Any]:
            self.created = params
            return {"id": "job-1"}

    channel = _Channel()
    cron = _Cron()
    agent_client_ref: dict[str, Any] = {"value": object()}
    captured: dict[str, Any] = {}

    async def _resolve_binding(**kwargs: Any) -> tuple[bool, dict[str, Any]]:
        captured.update(kwargs)
        params = kwargs["params"]
        params["project_id"] = "proj-1"
        params["work_mode"] = "code"
        return True, {"project_id": "proj-1", "work_mode": "code"}

    monkeypatch.setattr(
        e2a_proxy, "resolve_agent_cron_project_binding", _resolve_binding
    )
    register_tui_cron_handlers(channel, "tui", cron, agent_client_ref)

    await channel.handlers["cron.job.create"](
        object(), 1, {"name": "nightly"}, None, user_id="user-1"
    )

    assert captured["agent_client"] is agent_client_ref["value"]
    assert captured["user_id"] == "user-1"
    assert cron.created is not None
    assert cron.created["project_id"] == "proj-1"
    assert cron.created["work_mode"] == "code"
    assert cron.created["_agentos_project_binding_verified"] is True
    assert channel.responses == [
        {"ws": channel.responses[0]["ws"], "req_id": 1, "ok": True, "payload": {"job": {"id": "job-1"}}}
    ]
