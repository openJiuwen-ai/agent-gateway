"""Behavior at the independent Gateway's resource and storage boundaries."""
from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import yaml

from agent_gateway.auth.session_store import auth_dir
from agent_gateway.common import paths
from agent_gateway.enterprise.team.quota_policy import OwnerIdentity, TeamQuotaPolicyStore
from agent_gateway.enterprise.team.route import TeamRouteResolver
from agent_gateway.enterprise.team.route_binding_store import InMemoryTeamRouteBindingStore, SqliteTeamRouteBindingStore
from openjiuwen_protocol.e2a.models import E2AEnvelope


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
    assert (root / "domain/agent/.checkpoint/state").read_text() == "old state"
    assert (root / "infra/logs/agent/old.log").read_text() == "old logs"
    paths._migrate_legacy_checkpoint_and_logs()
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
