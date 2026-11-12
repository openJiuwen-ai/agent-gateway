"""Behavior at the independent Gateway's resource and storage boundaries."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

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
    config = root / "infra" / "config" / "config.yaml"
    assert config.is_file()
    assert paths.get_config_file() == config
    assert paths.get_workspace_dir() == root / "domain" / "agent" / "workspace"
    assert paths.get_agent_home_dir().is_dir()
    assert auth_dir() == root / "infra" / "auth"
    assert paths.get_logs_dir() == root / "infra" / "logs"
    template = yaml.safe_load(config.read_text())
    assert template["extensions"]["extension_dirs"] == "${EXTENSION_DIRS:-agent_gateway/extensions}"
    config.write_text("custom: retained\n")
    paths.prepare_runtime_workspace(cleanup_stale_descs=False)
    assert config.read_text() == "custom: retained\n"


def test_legacy_logs_and_checkpoints_migrate_once():
    root = paths.get_user_workspace_dir()
    legacy = root / ".checkpoint"
    legacy.mkdir(parents=True)
    (legacy / "state").write_text("old state")
    logs = root / ".logs"
    logs.mkdir(parents=True)
    (logs / "old.log").write_text("old logs")
    paths.migrate_partitioned_data_root_layout()
    assert (root / "domain/agent/.checkpoint/state").read_text() == "old state"
    assert (root / "infra/logs/agent/old.log").read_text() == "old logs"
    paths.migrate_partitioned_data_root_layout()
    assert not legacy.exists()


def test_quota_policy_round_trip_uses_infra_database():
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
    assert (paths.get_infra_data_dir() / "agent/teams/team_control.db").is_file()
    store.delete_policy("default")
    assert store.list_policies() == []
    store._conn.close()


def test_default_route_binding_survives_reopen_in_infra():
    store = SqliteTeamRouteBindingStore()
    store.upsert(team_session_id="leader-session", endpoint="http://pod:8766", owner=("org", "bot", "owner"))
    store.close()
    assert (paths.get_infra_data_dir() / "gateway/team_route_bindings.db").is_file()
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
def test_shared_domain_keeps_each_pods_context_usage_and_logs_private(tmp_path, edition):
    shared_domain = tmp_path / "shared-domain"
    shared_domain.mkdir()
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
        (root / "domain").symlink_to(shared_domain, target_is_directory=True)
        env = dict(os.environ, JIUWENSWARM_DATA_DIR=str(root), JIUWENSWARM_EDITION=edition)
        env.pop("AGENTOS_GATEWAY_LOG_DIR", None)
        result = subprocess.run([sys.executable, "-c", script, f"pod-{index}"], env=env, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
    for index, root in enumerate(roots):
        assert {entry.name for entry in root.iterdir()} == {"domain", "infra"}
        assert (root / "infra/logs/agent/context_usage.jsonl").read_text() == f"pod-{index}\n"
        log = (root / "infra/logs/agent/gateway.log").read_text()
        assert f"pod-{index}" in log
        assert f"pod-{1 - index}" not in log
        assert (root / "domain/agent/workspace/pod-0").is_file()
        assert (root / "domain/agent/workspace/pod-1").is_file()
    assert not (shared_domain / "logs").exists()


def test_workspace_upgrade_preserves_config_and_local_jobs():
    root = paths.get_user_workspace_dir()
    legacy = {
        "config/config.yaml": ("infra/config/config.yaml", "gateway:\n  cron:\n    store_backend: etcd\n"),
        "config/.env": ("infra/config/.env", "UPGRADE_TEST=legacy\n"),
        "agent/home/cron_jobs.json": ("domain/agent/home/cron_jobs.json", '{"jobs": []}'),
        "agent/.checkpoint/session_map.json": ("domain/agent/.checkpoint/session_map.json", "{}"),
        "agent/.logs/gateway.log": ("infra/logs/agent/gateway.log", "legacy log"),
        "gateway/persistent/state.json": ("domain/gateway/persistent/state.json", "{}"),
    }
    for name, (_, contents) in legacy.items():
        source = root / name
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(contents)

    paths.prepare_runtime_workspace(cleanup_stale_descs=False)
    for name, (destination, contents) in legacy.items():
        assert not (root / name).exists()
        assert (root / destination).read_text() == contents


def test_workspace_migration_keeps_existing_config_and_legacy_copy():
    root = paths.get_user_workspace_dir()
    old = root / "config/config.yaml"
    new = root / "infra/config/config.yaml"
    old.parent.mkdir(parents=True)
    new.parent.mkdir(parents=True)
    old.write_text("legacy: true")
    new.write_text("current: true")

    paths.migrate_partitioned_data_root_layout()
    assert new.read_text() == "current: true"
    assert new.with_suffix(".yaml.migrated").read_text() == "legacy: true"
    assert not old.exists()

    snapshot = {p: p.stat().st_mtime_ns for p in root.rglob("*")}
    paths.migrate_partitioned_data_root_layout()
    assert {p: p.stat().st_mtime_ns for p in root.rglob("*")} == snapshot


@pytest.mark.parametrize("workspace_ready", ["0", "1"])
@pytest.mark.parametrize("explicit_backend", ["", "file", "etcd"])
def test_gateway_startup_restores_only_unconfigured_cron_etcd(
    tmp_path, workspace_ready, explicit_backend,
):
    from agent_gateway.common.cron_settings import load_cron_store_settings, restore_migrated_cron_etcd_settings

    root = tmp_path / "upgrade"
    old = root / "config/config.yaml"
    current = root / "infra/config/config.yaml"
    old.parent.mkdir(parents=True)
    current.parent.mkdir(parents=True)
    old.write_text(
        "gateway:\n  cron:\n    store_backend: etcd\n"
        "    etcd_endpoints: [http://old-etcd:2379]\n    etcd_prefix: /old/cron/\n",
        encoding="utf-8",
    )
    template = paths.get_bundled_resources_dir() / "config.yaml"
    current.write_bytes(template.read_bytes())
    if explicit_backend:
        (current.parent / ".env").write_text(f"CRON_STORE_BACKEND={explicit_backend}\n", encoding="utf-8")
    env = dict(
        os.environ, JIUWENSWARM_DATA_DIR=str(root), JIUWENSWARM_HOME=str(tmp_path),
        JIUWENSWARM_RUNTIME_WORKSPACE_READY=workspace_ready,
    )
    env.pop("CRON_STORE_BACKEND", None)
    result = subprocess.run(
        [sys.executable, "-c", "import agent_gateway.app_gateway"],
        env=env, capture_output=True, text=True, timeout=45,
    )
    assert result.returncode == 0, result.stderr

    raw = yaml.safe_load(current.read_text(encoding="utf-8"))
    if explicit_backend:
        assert current.read_bytes() == template.read_bytes()
        assert current.with_suffix(".yaml.migrated.skipped").is_file()
    else:
        settings = load_cron_store_settings(raw)
        assert settings.backend == "etcd"
        assert settings.endpoints == ("http://old-etcd:2379",)
        assert settings.prefix == "/old/cron/"
        assert current.with_suffix(".yaml.migrated.restored").is_file()

    raw["gateway"]["cron"] = {"store_backend": "file"}
    current.write_text(yaml.safe_dump(raw), encoding="utf-8")
    restore_migrated_cron_etcd_settings(current)
    assert load_cron_store_settings(yaml.safe_load(current.read_text(encoding="utf-8"))).backend == "file"


def test_migration_does_not_consume_unreadable_cron_backup(tmp_path):
    from agent_gateway.common.cron_settings import restore_migrated_cron_etcd_settings

    current = tmp_path / "config.yaml"
    current.write_text("gateway: {}\n", encoding="utf-8")
    backup = current.with_suffix(".yaml.migrated")
    backup.write_bytes(bytes([0xff]))
    restore_migrated_cron_etcd_settings(current)
    assert backup.read_bytes() == bytes([0xff])
    assert current.read_text(encoding="utf-8") == "gateway: {}\n"


def test_failed_final_rename_cannot_replay_cron_etcd_backup(tmp_path, monkeypatch):
    from agent_gateway.common.cron_settings import restore_migrated_cron_etcd_settings

    config = tmp_path / "config.yaml"
    config.write_text("gateway: {}\n", encoding="utf-8")
    backup = config.with_suffix(".yaml.migrated")
    legacy = "gateway: {cron: {store_backend: etcd, etcd_endpoints: [http://old-etcd:2379]}}\n"
    backup.write_text(legacy, encoding="utf-8")
    rename = Path.rename

    def fail_final_rename(source, target):
        if target == config.with_suffix(".yaml.migrated.restored"):
            raise PermissionError(target)
        return rename(source, target)

    with monkeypatch.context() as patch:
        patch.setattr(Path, "rename", fail_final_rename)
        restore_migrated_cron_etcd_settings(config)

    assert yaml.safe_load(config.read_text())["gateway"]["cron"]["store_backend"] == "etcd"
    assert not backup.exists()
    assert config.with_suffix(".yaml.migrated.restoring").read_text() == legacy
    file_config = "gateway: {cron: {store_backend: file}}\n"
    config.write_text(file_config, encoding="utf-8")
    restore_migrated_cron_etcd_settings(config)
    assert config.read_text() == file_config


def test_failed_config_write_leaves_cron_etcd_backup_retryable(tmp_path, monkeypatch):
    from agent_gateway.common import config as config_io
    from agent_gateway.common.cron_settings import restore_migrated_cron_etcd_settings

    config = tmp_path / "config.yaml"
    config.write_text("gateway: {}\n", encoding="utf-8")
    backup = config.with_suffix(".yaml.migrated")
    legacy = "gateway: {cron: {store_backend: etcd, etcd_endpoints: [http://old-etcd:2379]}}\n"
    backup.write_text(legacy, encoding="utf-8")

    def fail_write(path, _data):
        raise PermissionError(path)

    with monkeypatch.context() as patch:
        patch.setattr(config_io, "dump_yaml_round_trip", fail_write)
        restore_migrated_cron_etcd_settings(config)

    assert backup.read_text() == legacy
    assert config.read_text() == "gateway: {}\n"
    restore_migrated_cron_etcd_settings(config)
    assert yaml.safe_load(config.read_text())["gateway"]["cron"]["store_backend"] == "etcd"
    assert config.with_suffix(".yaml.migrated.restored").read_text() == legacy


@pytest.mark.parametrize("moved", [True, False])
def test_migration_only_accepts_confirmed_concurrent_move(tmp_path, monkeypatch, moved):
    source = tmp_path / "legacy"
    target = tmp_path / "new"
    source.write_text("old data", encoding="utf-8")

    def concurrent_move(_source, _target):
        if moved:
            source.rename(target)
        else:
            source.unlink()
        raise FileNotFoundError(source)

    monkeypatch.setattr(paths.shutil, "move", concurrent_move)
    if moved:
        paths._move_legacy_entry(source, target)
        assert target.read_text(encoding="utf-8") == "old data"
    else:
        with pytest.raises(FileNotFoundError):
            paths._move_legacy_entry(source, target)


def test_cleanup_finds_orphan_file_ops_in_migrated_history(monkeypatch):
    from agent_gateway import cleanup

    root = paths.get_user_workspace_dir()
    history = root / ".agent_history"
    history.mkdir(parents=True)
    for sid in ("missing", "live"):
        (history / f"file_ops_jiuwenswarm_{sid}.json").write_text("{}", encoding="utf-8")
    (root / "agent/sessions/live").mkdir(parents=True)
    paths.migrate_partitioned_data_root_layout()

    monkeypatch.setattr(cleanup, "_get_cleanup_period_days", lambda: 30)
    assert cleanup.cleanup_orphan_file_ops() == {"removed": 1, "errors": 0}
    assert (root / "domain/.agent_history/file_ops_jiuwenswarm_live.json").is_file()


def test_frozen_windows_cli_finds_migrated_site_packages(monkeypatch):
    from agent_gateway.common import external_cli_runtime

    root = paths.get_user_workspace_dir()
    legacy_site = root / "runtime/external-cli/codex/site-packages"
    legacy_site.mkdir(parents=True)
    paths.migrate_partitioned_data_root_layout()

    monkeypatch.setattr(external_cli_runtime, "_is_frozen_windows", lambda: True)
    site = external_cli_runtime._runtime_site_packages("codex")
    assert site == root / "infra" / legacy_site.relative_to(root)
    assert site.is_dir()


def test_workspace_migration_leaves_new_layout_alone():
    root = paths.get_user_workspace_dir()
    config = root / "infra/config/config.yaml"
    config.parent.mkdir(parents=True)
    config.write_text("current: true")

    paths.migrate_partitioned_data_root_layout()
    assert sorted(p.relative_to(root).as_posix() for p in root.rglob("*")) == [
        "infra", "infra/config", "infra/config/config.yaml",
    ]


def test_workspace_migration_keeps_legacy_sqlite_sidecars_with_legacy_database():
    root = paths.get_user_workspace_dir()
    old = root / "gateway/team_route_bindings.db"
    current = root / "infra/gateway/team_route_bindings.db"
    old.parent.mkdir(parents=True)
    current.parent.mkdir(parents=True)
    old.write_bytes(b"legacy database")
    old.with_name(old.name + "-wal").write_bytes(b"legacy WAL")
    current.write_bytes(b"current database")
    current.with_name(current.name + "-wal").write_bytes(b"current WAL")

    paths.migrate_partitioned_data_root_layout()

    assert current.read_bytes() == b"current database"
    assert current.with_name(current.name + "-wal").read_bytes() == b"current WAL"
    assert current.with_name(current.name + ".migrated").read_bytes() == b"legacy database"
    assert current.with_name(current.name + ".migrated-wal").read_bytes() == b"legacy WAL"
    assert not old.parent.exists()
