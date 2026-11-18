"""TUI 项目路径纯逻辑与 session.delete/list 的 AgentServer 转发。"""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_gateway.channel_manager.tui.tui_connect import (
    CliHandlersBindParams,
    build_tui_session_create_channel_metadata,
    register_cli_handlers,
    resolve_tui_session_project_path,
    tui_session_matches_project_dir,
)


class _TuiChannel:
    channel_id = "tui"

    def __init__(self) -> None:
        self.local_handlers: dict[str, dict[str, object]] = {}
        self.responses: list[dict] = []

    def register_local_handler(self, path, method, handler) -> None:
        self.local_handlers.setdefault(path, {})[method] = handler

    def get_active_session_ids(self, _channel_id, *, exclude_ws=None):
        del exclude_ws
        return set()

    async def send_response(self, _ws, req_id, *, ok, payload=None, error=None, code=None):
        self.responses.append(
            {"id": req_id, "ok": ok, "payload": payload or {}, "error": error, "code": code}
        )


class TestResolveTuiSessionProjectPath:
    def test_prefers_channel_metadata_project_dir(self):
        session = {
            "project_dir": "E:/fallback",
            "channel_metadata": {"project_dir": "E:/from-meta", "cwd": "E:/cwd"},
        }
        assert resolve_tui_session_project_path(session) == "E:/from-meta"

    def test_falls_back_to_channel_metadata_cwd(self):
        session = {
            "project_dir": "E:/fallback",
            "channel_metadata": {"cwd": "E:/cwd-only"},
        }
        assert resolve_tui_session_project_path(session) == "E:/cwd-only"

    def test_falls_back_to_top_level_project_dir(self):
        session = {"project_dir": r"E:\jiuwenswarm", "channel_metadata": {}}
        assert resolve_tui_session_project_path(session) == r"E:\jiuwenswarm"

    def test_empty_when_no_path(self):
        assert resolve_tui_session_project_path({"channel_id": "tui"}) == ""
        assert resolve_tui_session_project_path(None) == ""


class TestTuiSessionMatchesProjectDir:
    def test_all_projects_always_matches(self):
        assert tui_session_matches_project_dir(
            {"project_dir": ""}, r"E:\jiuwenswarm", show_all_projects=True
        )

    def test_matches_top_level_without_channel_metadata(self, tmp_path: Path):
        project = tmp_path / "proj"
        project.mkdir()
        session = {"project_dir": str(project), "channel_id": "tui"}
        assert tui_session_matches_project_dir(session, str(project)) is True

    def test_excludes_when_path_missing(self, tmp_path: Path):
        project = tmp_path / "proj"
        project.mkdir()
        assert tui_session_matches_project_dir({"channel_id": "tui"}, str(project)) is False

    def test_matches_nested_session_under_project(self, tmp_path: Path):
        project = tmp_path / "proj"
        nested = project / "pkg"
        nested.mkdir(parents=True)
        session = {"channel_metadata": {"cwd": str(nested)}}
        assert tui_session_matches_project_dir(session, str(project)) is True


class TestBuildTuiSessionCreateChannelMetadata:
    def test_uses_resolved_project_dir(self, monkeypatch):
        monkeypatch.setattr("agent_gateway.common.paths.resolve_git_branch", lambda _path: "main")
        metadata = build_tui_session_create_channel_metadata(
            {"cwd": "E:/ignored"}, resolved_project_dir="E:/resolved"
        )
        assert metadata == {
            "project_dir": "E:/resolved",
            "cwd": "E:/resolved",
            "git_branch": "main",
        }

    def test_falls_back_to_params_cwd(self, monkeypatch):
        monkeypatch.setattr("agent_gateway.common.paths.resolve_git_branch", lambda _path: "HEAD")
        metadata = build_tui_session_create_channel_metadata(
            {"cwd": r"E:\jiuwenswarm"}, resolved_project_dir=""
        )
        assert metadata["project_dir"] == r"E:\jiuwenswarm"
        assert metadata["cwd"] == r"E:\jiuwenswarm"

    def test_returns_none_without_path(self):
        assert build_tui_session_create_channel_metadata({}, resolved_project_dir="") is None


@pytest.mark.asyncio
async def test_session_delete_delegates_heartbeat_lifecycle_to_agentserver() -> None:
    class Agent:
        def __init__(self) -> None:
            self.requests = []

        async def send_request(self, env):
            self.requests.append(env)
            return SimpleNamespace(ok=True, payload={"session_id": "tui-session"})

    channel = _TuiChannel()
    agent = Agent()
    register_cli_handlers(
        CliHandlersBindParams(
            channel=channel, agent_client=agent, message_handler=object(), path="/tui"
        )
    )

    await channel.local_handlers["/tui"]["session.delete"](
        object(), "req-delete", {"session_id": "tui-session"}, "current"
    )

    assert channel.responses[-1]["ok"] is True
    assert len(agent.requests) == 1
    assert agent.requests[0].method == "session.delete"
    assert agent.requests[0].params == {"session_id": "tui-session"}


@pytest.mark.asyncio
@pytest.mark.parametrize("agent_client", [None, {"value": None}])
async def test_session_list_offline_has_complete_pagination_payload(monkeypatch, agent_client):
    async def unexpected_fetch(**kwargs):
        pytest.fail("offline session.list must not call AgentServer")

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", unexpected_fetch)
    channel = _TuiChannel()
    register_cli_handlers(
        CliHandlersBindParams(channel=channel, agent_client=agent_client, path="/tui")
    )

    await channel.local_handlers["/tui"]["session.list"](object(), "req-list", {}, "current")

    response = channel.responses[-1]
    assert response["ok"] is True
    assert response["payload"] == {
        "sessions": [], "total": 0, "page_count": 0, "current_branch": "HEAD",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("include_current", [False, True])
@pytest.mark.parametrize("current_channel", ["tui", "web"])
@pytest.mark.parametrize("current_project", ["other", ""])
async def test_session_list_include_current_bypasses_project_filter(
    monkeypatch, tmp_path, include_current, current_channel, current_project
):
    project_dir = str(tmp_path / "project")
    sessions = [
        {
            "session_id": "current", "channel_id": current_channel,
            "project_dir": str(tmp_path / current_project) if current_project else "",
            "last_message_at": 200,
        },
        {
            "session_id": "matching", "channel_id": "tui",
            "project_dir": project_dir, "last_message_at": 100,
        },
        {
            "session_id": "other-project", "channel_id": "tui",
            "project_dir": str(tmp_path / "other"), "last_message_at": 90,
        },
        {
            "session_id": "other-channel", "channel_id": "web",
            "project_dir": project_dir, "last_message_at": 80,
        },
    ]

    async def fake_fetch(**kwargs):
        return True, {"sessions": sessions, "total": len(sessions)}

    monkeypatch.setattr("agent_gateway.routing.e2a_proxy.fetch_agent_unary", fake_fetch)
    monkeypatch.setattr("agent_gateway.common.paths.resolve_git_branch", lambda _path: "HEAD")
    channel = _TuiChannel()
    register_cli_handlers(
        CliHandlersBindParams(channel=channel, agent_client=object(), path="/tui")
    )

    await channel.local_handlers["/tui"]["session.list"](
        object(), "req-list",
        {"project_dir": project_dir, "include_current": include_current}, "current",
    )

    response = channel.responses[-1]
    assert response["ok"] is True
    payload = response["payload"]
    assert [session["session_id"] for session in payload["sessions"]] == (
        ["current", "matching"] if include_current else ["matching"]
    )
    assert payload["total"] == 4
    assert payload["page_count"] == 4


@pytest.mark.asyncio
async def test_session_list_current_dir_includes_top_level_only_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    project = tmp_path / "proj"
    project.mkdir()
    project_dir = str(project)
    monkeypatch.setattr("agent_gateway.common.paths.resolve_git_branch", lambda _path: "HEAD")

    class ListAgentClient:
        def __init__(self):
            self.requests = []

        async def send_request(self, env):
            self.requests.append(env)
            return SimpleNamespace(
                ok=True,
                payload={
                    "total": 17,
                    "sessions": [
                        {
                            "session_id": "tui_old_no_channel_meta",
                            "channel_id": "tui",
                            "project_dir": project_dir,
                            "last_message_at": 100.0,
                        },
                        {
                            "session_id": "tui_other_project",
                            "channel_id": "tui",
                            "project_dir": str(tmp_path / "other"),
                            "last_message_at": 90.0,
                        },
                        None,
                    ]
                },
            )

    channel = _TuiChannel()
    agent = ListAgentClient()
    register_cli_handlers(
        CliHandlersBindParams(channel=channel, agent_client=agent, path="/tui")
    )

    await channel.local_handlers["/tui"]["session.list"](
        object(),
        "req-list",
        {"project_dir": project_dir, "limit": 20},
        "current-sid",
    )

    assert len(agent.requests) == 1
    assert agent.requests[0].method == "session.list"
    assert agent.requests[0].params == {"project_dir": project_dir, "limit": 20}
    response = channel.responses[-1]
    assert response["ok"] is True
    assert response["payload"]["total"] == 17
    # Raw upstream row count, including filtered/invalid rows, never a page count.
    assert response["payload"]["page_count"] == 3
    assert response["payload"]["current_branch"] == "HEAD"
    sessions = response["payload"]["sessions"]
    assert [session["session_id"] for session in sessions] == ["tui_old_no_channel_meta"]
    assert sessions[0]["project_dir"] == os.path.realpath(project_dir)
