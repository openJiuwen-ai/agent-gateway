"""Behavior at the independent Gateway's resource and storage boundaries."""
from __future__ import annotations

from contextlib import closing
from pathlib import Path
import os
import sqlite3
import subprocess
import sys
from types import SimpleNamespace

import yaml
import pytest

from agent_gateway.auth.session_store import auth_dir
from agent_gateway.common import paths
from agent_gateway.enterprise.team.quota_policy import OwnerIdentity, TeamQuotaPolicyStore
from agent_gateway.enterprise.team.route import TeamRouteResolver
from agent_gateway.enterprise.team.route_binding_store import InMemoryTeamRouteBindingStore, SqliteTeamRouteBindingStore
from e2a.models import E2AEnvelope


def test_fresh_workspace_installs_own_template_without_overwriting_user_config():
    root = paths.get_user_workspace_dir()
    paths.prepare_runtime_workspace(cleanup_stale_descs=False)
    config = root / "config" / "config.yaml"
    assert config.is_file()
    assert paths.get_config_file() == config
    assert paths.get_workspace_dir() == root / "profile" / "agent" / "workspace"
    assert paths.get_agent_home_dir().is_dir()
    assert auth_dir() == root / "auth"
    assert paths.get_logs_dir() == root / "logs"
    template = yaml.safe_load(config.read_text())
    assert template["extensions"]["extension_dirs"] == "${EXTENSION_DIRS:-agent_gateway/extensions}"
    config.write_text("custom: retained\n")
    paths.prepare_runtime_workspace(cleanup_stale_descs=False)
    assert config.read_text() == "custom: retained\n"


def test_legacy_logs_and_checkpoints_migrate_once_without_replacing_new_data(monkeypatch):
    root = paths.get_user_workspace_dir()
    legacy = root / ".checkpoint"
    legacy.mkdir(parents=True)
    (legacy / "state").write_text("old state")
    logs = root / ".logs"
    logs.mkdir(parents=True)
    (logs / "old.log").write_text("old logs")
    monkeypatch.setattr(paths, "_legacy_migration_done", False)
    paths._migrate_legacy_checkpoint_and_logs()
    assert (root / "profile/agent/.checkpoint/state").read_text() == "old state"
    assert (root / "logs/agent/old.log").read_text() == "old logs"
    paths._migrate_legacy_checkpoint_and_logs()
    assert legacy.samefile(root / "profile/agent/.checkpoint")
    assert legacy.resolve() == root / "profile/agent/.checkpoint"


def _write_file(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_flat_layout_migration_merges_business_data_and_preserves_system_paths(monkeypatch):
    root = paths.get_user_workspace_dir()
    for relative in ("agent/sessions/session.json", "gateway/persistent/state.json",
                     "application_plugins/plugin.json", "workspace_example/state.json"):
        _write_file(root / relative, relative)
    for relative in ("config/config.yaml", "auth/sessions.json", "runtime/custom-agents/model.json"):
        _write_file(root / relative, relative)
    _write_file(root / "profile/agent/sessions/existing.json", "new")
    _write_file(root / ".logs/gateway.log", "legacy gateway")
    _write_file(root / "agent/.logs/agent.log", "legacy agent")
    monkeypatch.setattr(paths, "_legacy_migration_done", False)
    paths._migrate_legacy_checkpoint_and_logs()
    for relative in ("agent/sessions/session.json", "gateway/persistent/state.json",
                     "application_plugins/plugin.json", "workspace_example/state.json"):
        assert (root / "profile" / relative).read_text() == relative
        assert (root / relative).read_text() == relative
    for relative in ("config/config.yaml", "auth/sessions.json", "runtime/custom-agents/model.json"):
        assert (root / relative).read_text() == relative
        assert not (root / "profile" / relative).exists()
    assert (root / "profile/agent/sessions/existing.json").read_text() == "new"
    assert (root / "logs/agent/gateway.log").read_text() == "legacy gateway"
    assert (root / "logs/agent/agent.log").read_text() == "legacy agent"
    assert (root / "agent/.logs").resolve() == root / "logs/agent"
    # Simulate a new process, not just the same-process guard.
    monkeypatch.setattr(paths, "_legacy_migration_done", False)
    paths._migrate_legacy_checkpoint_and_logs()
    assert (root / "agent").samefile(root / "profile/agent")
    assert (root / "agent/.logs/agent.log").read_text() == "legacy agent"


def test_migration_preserves_conflicts_and_keeps_legacy_logs_local(monkeypatch):
    root = paths.get_user_workspace_dir()
    conflicting = _write_file(root / "agent/sessions/conflict.json", "old")
    target = _write_file(root / "profile/agent/sessions/conflict.json", "new")
    _write_file(root / "agent/sessions/other.json", "other")
    legacy_log = _write_file(root / "agent/.logs/agent.log", "old log")
    current_log = _write_file(root / "logs/agent/agent.log", "new log")
    _write_file(root / "agent/.checkpoint/state", "checkpoint")
    monkeypatch.setattr(paths, "_legacy_migration_done", False)
    paths._migrate_legacy_checkpoint_and_logs()
    assert conflicting.read_text() == "old"
    assert target.read_text() == "new"
    assert (root / "profile/agent/sessions/other.json").read_text() == "other"
    assert not (root / "agent").samefile(root / "profile/agent")
    assert legacy_log.read_text() == "old log"
    assert current_log.read_text() == "new log"
    assert not (root / "profile/agent/.logs").exists()
    assert (root / "agent/.checkpoint/state").samefile(root / "profile/agent/.checkpoint/state")


def test_migration_preserves_directory_when_destination_is_file(monkeypatch):
    root = paths.get_user_workspace_dir()
    old = _write_file(root / "gateway/persistent/state.json", "old")
    target = _write_file(root / "profile/gateway", "new")
    monkeypatch.setattr(paths, "_legacy_migration_done", False)
    paths._migrate_legacy_checkpoint_and_logs()
    assert old.read_text() == "old"
    assert target.read_text() == "new"


def test_migration_succeeds_when_symlinks_are_unavailable(monkeypatch, caplog):
    root = paths.get_user_workspace_dir()
    _write_file(root / "agent/sessions/session.json", "retained")

    def unavailable(*args, **kwargs):
        raise OSError("symlink unavailable")

    monkeypatch.setattr(Path, "symlink_to", unavailable)
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(paths, "_legacy_migration_done", False)
    paths._migrate_legacy_checkpoint_and_logs()
    assert (root / "profile/agent/sessions/session.json").read_text() == "retained"
    assert not (root / "agent").exists()
    assert not paths._legacy_migration_done
    assert "could not create legacy path link" in caplog.text


@pytest.mark.parametrize("layout", ["legacy", "migrated", "retained_logs", "failed_junction"])
def test_windows_migration_keeps_legacy_checkpoint_accessible(monkeypatch, layout):
    root = paths.get_user_workspace_dir()
    old = root / "agent/.checkpoint/state.db"
    new = root / "profile/agent/.checkpoint/state.db"
    database = old if layout in {"legacy", "failed_junction"} else new
    database.parent.mkdir(parents=True)
    with closing(sqlite3.connect(database)) as conn, conn:
        conn.execute("CREATE TABLE checkpoint (value TEXT)")
        conn.execute("INSERT INTO checkpoint VALUES ('retained')")
    if layout == "retained_logs":
        _write_file(root / "agent/.logs/agent.log", "legacy")
        _write_file(root / "logs/agent/agent.log", "current")

    # Exercise native junction creation on Windows; simulate it on POSIX.
    if sys.platform == "win32":
        import _winapi
        create_junction = _winapi.CreateJunction
    else:
        create_junction = lambda src, dst: os.symlink(src, dst, target_is_directory=True)
    monkeypatch.setitem(sys.modules, "_winapi", SimpleNamespace(CreateJunction=create_junction))
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setenv("JIUWENSWARM_EDITION", "personal")
    monkeypatch.setattr(Path, "symlink_to", lambda *a, **kw: pytest.fail("Windows directory must use junction"))
    if layout == "failed_junction":
        def unavailable(*args):
            raise OSError("junction unavailable")

        monkeypatch.setitem(sys.modules, "_winapi", SimpleNamespace(CreateJunction=unavailable))
        monkeypatch.setattr(paths, "_legacy_migration_done", False)
        paths._migrate_legacy_checkpoint_and_logs()
        assert new.is_file()
        assert not paths._legacy_migration_done
        monkeypatch.setitem(sys.modules, "_winapi", SimpleNamespace(CreateJunction=create_junction))
    for _ in range(2):
        monkeypatch.setattr(paths, "_legacy_migration_done", False)
        paths._migrate_legacy_checkpoint_and_logs()
        assert old.samefile(new)
        with closing(sqlite3.connect(old)) as conn:
            assert conn.execute("SELECT value FROM checkpoint").fetchone() == ("retained",)


def test_quota_policy_round_trip_uses_profile_database():
    store = TeamQuotaPolicyStore()
    row = store.upsert_policy({
        "policy_id": "default", "max_teams_per_user": 3,
        "max_teammates_per_team": 7, "max_teams_per_org": 12,
    })
    assert row["policy_id"] == "default"
    limits = store.resolve_limits(OwnerIdentity("user", "org", "bot"))
    assert limits.max_teams_per_user == 3
    assert limits.max_teammates_per_team == 7
    assert limits.max_teams_per_org == 12
    assert (paths.get_profile_data_dir() / "agent/teams/team_control.db").is_file()
    store.delete_policy("default")
    assert store.list_policies() == []
    store._conn.close()


def test_default_route_binding_survives_reopen_in_profile():
    store = SqliteTeamRouteBindingStore()
    store.upsert(team_session_id="leader-session", endpoint="http://pod:8766", owner=("org", "bot", "owner"))
    store.close()
    assert (paths.get_profile_data_dir() / "gateway/team_route_bindings.db").is_file()
    reopened = SqliteTeamRouteBindingStore()
    assert reopened.get_by_session("leader-session").owner_user_id == "owner"
    reopened.close()


def test_team_route_keeps_owner_when_another_member_refreshes_route():
    store = InMemoryTeamRouteBindingStore()
    resolver = TeamRouteResolver(store)
    envelope = E2AEnvelope(request_id="r1", channel="web", method="chat.send", session_id="team-session", params={"mode": "team"})
    resolver.record_route(envelope, base_url="http://pod1", pod_id="p1", route_id="r1", caller_triple=("org", "bot", "owner"))
    resolver.record_route(envelope, base_url="http://pod2", pod_id="p2", route_id="r2", caller_triple=("org", "bot", "member"))
    decision = resolver.resolve(envelope)
    assert decision.owner_triple == ("org", "bot", "owner")
    assert decision.base_url == "http://pod2"
    assert decision.binding.route_version == 2


def test_enterprise_modules_import_with_host_package_blocked():
    code = '''
import importlib.abc
import sys
class BlockHost(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "jiuwenswarm" or fullname.startswith("jiuwenswarm."):
            raise AssertionError("Gateway imported host: " + fullname)
sys.meta_path.insert(0, BlockHost())
from agent_gateway.common.paths import prepare_runtime_workspace
from agent_gateway.common.config import get_config
from secrets import token_hex
from fastapi import FastAPI
from agent_gateway.enterprise.team.quota_policy import TeamQuotaPolicyStore
from agent_gateway.enterprise.team.route import TeamRouteResolver
from agent_gateway.enterprise.capabilities.experts.bound_packages import packages_from_template_rows
from agent_gateway.common.protocol_methods import ReqMethod
prepare_runtime_workspace(cleanup_stale_descs=False)
assert token_hex(8)
assert isinstance(get_config(), dict)
assert ReqMethod.VIDEO_CONVERSATION_APPEND.value == "video.conversation.append"
'''
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("edition", ["personal", "enterprise"])
def test_shared_profile_keeps_each_pods_context_usage_and_logs_private(tmp_path, edition):
    import os

    shared_profile = tmp_path / "shared-profile"
    shared_profile.mkdir()
    roots = [tmp_path / "pod-a", tmp_path / "pod-b"]
    script = '''
import sys
from agent_gateway.common import paths
from agent_gateway.channel_manager.web.web_connect import WebChannel
from agent_gateway.common.logging_setup import setup_gateway_logger
workspace = paths.get_agent_workspace_dir()
workspace.mkdir(parents=True, exist_ok=True)
(workspace / sys.argv[1]).write_text("shared business data")
WebChannel._write_frontend_context_usage(sys.argv[1])
setup_gateway_logger().warning(sys.argv[1])
'''
    for index, root in enumerate(roots):
        root.mkdir()
        if sys.platform == "win32":
            import _winapi
            _winapi.CreateJunction(str(shared_profile), str(root / "profile"))
        else:
            (root / "profile").symlink_to(shared_profile, target_is_directory=True)
        env = dict(os.environ, JIUWENSWARM_DATA_DIR=str(root), JIUWENSWARM_EDITION=edition)
        env.pop("AGENTOS_GATEWAY_LOG_DIR", None)
        result = subprocess.run([sys.executable, "-c", script, f"pod-{index}"], env=env, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    for index, root in enumerate(roots):
        assert {entry.name for entry in root.iterdir()} == {"profile", "logs"}
        assert (root / "logs/agent/context_usage.jsonl").read_text() == f"pod-{index}\n"
        log = (root / "logs/agent/gateway.log").read_text()
        assert f"pod-{index}" in log
        assert f"pod-{1 - index}" not in log
        assert (root / "profile/agent/workspace/pod-0").is_file()
        assert (root / "profile/agent/workspace/pod-1").is_file()
    assert not (shared_profile / "logs").exists()
    assert not (shared_profile / "agent/.logs").is_symlink()
