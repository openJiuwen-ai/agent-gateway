# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from pathlib import Path
import re

import yaml

ROOT = Path(__file__).resolve().parents[3]
RESOURCES = ROOT / "agent_gateway" / "resources"
BASE = yaml.safe_load((RESOURCES / "config.yaml").read_text(encoding="utf-8"))
AGENTOS = yaml.safe_load(
    (RESOURCES / "config.agentos.yaml").read_text(encoding="utf-8")
)

def test_agentos_template_is_gateway_only_plus_deploy_handoff_keys() -> None:
    b2_owned = {
        "heartbeat", "mcp", "permissions", "experimental", "recommendation",
        "endpoint_profile_overrides", "rsi", "trajectory_ui",
    }
    # AgentOS 模板保持 Gateway-only，AS/B3 段不进入渲染模板。
    assert set(AGENTOS) == (set(BASE) - b2_owned - {"react", "memory"}) | {"code_graph", "sandbox"}
    for as_owned in (
        "react",
        "tools",
        "memory",
        "mcp",
        "permissions",
        "team",
        "observability",
        "embed",
        "task_memory",
    ):
        assert as_owned not in AGENTOS


def test_agentos_template_has_agentos_overrides() -> None:
    gateway = AGENTOS["gateway"]

    assert gateway["agent_client"]["type"] == "agentos_router"
    assert gateway["cron"]["store_backend"] == "<<CRON_STORE_BACKEND>>"
    assert gateway["cron"]["etcd_endpoints"] == "<<ETCD_ENDPOINTS>>"
    assert gateway["agentos"]["auth_enabled"] is True
    assert gateway["agentos"]["registry"]["endpoint"].startswith("http://")
    assert AGENTOS["channels"]["ssh"]["enabled"] is True
    assert AGENTOS["channels"]["web_proxy"]["enabled"] is True
    assert "code_graph" in AGENTOS
    assert "sandbox" in AGENTOS


def test_agentos_template_hooks_start_from_base_template() -> None:
    assert set(AGENTOS["hooks"]) == {"disable_all_hooks"}


def test_agentos_template_can_be_rendered() -> None:
    rendered = re.sub(r"<<[A-Z0-9_]+>>", "x", (RESOURCES / "config.agentos.yaml").read_text(encoding="utf-8"))

    parsed = yaml.safe_load(rendered)
    assert parsed
    assert "<<" not in rendered


def test_gateway_base_drops_pr2_b2_safe_keys() -> None:
    b2_safe = {
        "progressive_tool_enabled", "progressive_tool_optimize",
        "setup_guide", "auto_recap", "a4p", "kv_cache_affinity_config",
    }
    assert b2_safe.isdisjoint(BASE)


def test_gateway_templates_drop_auto_memory_enabled() -> None:
    assert "auto_memory_enabled" not in BASE
    assert "auto_memory_enabled" not in AGENTOS


def test_gateway_templates_drop_memory_section() -> None:
    assert "memory" not in BASE
    assert "memory" not in AGENTOS


def test_gateway_templates_drop_agents_section() -> None:
    assert "agents" not in BASE
    assert "agents" not in AGENTOS


def test_gateway_template_drops_as_only_react_keys() -> None:
    assert BASE["react"] == {"model_name": "${MODEL_NAME}"}
    assert "react" not in AGENTOS


def test_gateway_templates_drop_models_section() -> None:
    assert "models" not in BASE
    assert "models" not in AGENTOS


def test_gateway_templates_drop_preferred_language() -> None:
    assert "preferred_language" not in BASE
    assert "preferred_language" not in AGENTOS


def test_gateway_templates_drop_mcp_and_heartbeat() -> None:
    for key in ("mcp", "heartbeat"):
        assert key not in BASE
        assert key not in AGENTOS


def test_gateway_templates_drop_as_only_channel_keys() -> None:
    for template in (BASE, AGENTOS):
        for app in template["channels"]["xiaoyi"]["apps"]:
            assert "phone_tools_enabled" not in app


def test_gateway_templates_drop_enterprise_confirmed_flags() -> None:
    for key in (
        "a2ui",
        "symphony",
        "rsi",
        "trajectory_ui",
        "experimental",
        "recommendation",
    ):
        assert key not in BASE
        assert key not in AGENTOS


def test_gateway_template_drops_permissions() -> None:
    assert "permissions" not in BASE


def test_gateway_templates_drop_as_only_modes() -> None:
    for template in (BASE, AGENTOS):
        assert set(template["modes"]) == {"team"}
        assert "agent" not in template["modes"]
        assert "code" not in template["modes"]


def test_gateway_templates_drop_as_only_modes_runtime_keys() -> None:
    as_only = {
        "swarmflow_progressive_tool",
        "reliability",
        "swarmflow_concurrency",
        "worktree",
        "workspace",
        "transport",
        "storage",
        "memory",
        "team_name",
        "lifecycle",
        "teammate_mode",
        "spawn_mode",
        "dispatch_mode",
        "enable_task_verification",
        "enable_member_messaging",
        "max_subtask_depth",
        "leader",
        "agents",
    }
    for template in (BASE, AGENTOS):
        team = template["modes"]["team"]["jiuwen_team"]
        assert as_only.isdisjoint(team)


def test_gateway_modes_keep_only_gateway_owned_keys() -> None:
    for template in (BASE, AGENTOS):
        team = template["modes"]["team"]["jiuwen_team"]
        assert set(team) == {
            "enable_swarmflow",
            "external_cli_agents",
            "external_transport",
        }
