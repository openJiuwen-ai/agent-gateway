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
    assert set(AGENTOS) == set(BASE) | {"code_graph", "sandbox"}
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
