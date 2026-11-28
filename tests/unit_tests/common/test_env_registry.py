# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Keep JIUWENSWARM_* reads registered and .env.example in sync."""

from __future__ import annotations

import re
from pathlib import Path

from agent_gateway.common import env_names

_PACKAGE_ROOT = Path(__file__).resolve().parents[3] / "agent_gateway"
_ENV_EXAMPLE = Path(__file__).resolve().parents[3] / ".env.example"

_READ_PATTERNS = (
    re.compile(
        r"(?:getenv|environ\.get|environ\s*\[|environ\.setdefault|putenv)\(\s*"
        r"['\"]((?:JIUWENSWARM|JIUWENCLAW)_[A-Za-z0-9_]+)['\"]"
    ),
    re.compile(r"['\"]((?:JIUWENSWARM|JIUWENCLAW)_[A-Za-z0-9_]+)['\"]"),
)

# JWS product variables and names owned by a library. They predate the registry
# and stay as they are; see gateway-env-redesign-plan.md section 5.3.
_ALLOWED_UNREGISTERED = frozenset(
    {
        "JIUWENSWARM_EDITION",
        "JIUWENSWARM_CONFIG_URL",
        "JIUWENSWARM_RUNTIME_PROFILE",
        "JIUWENSWARM_RUNTIME_PLATFORM",
        "JIUWENSWARM_AGENT_PREWARM",
        "JIUWENSWARM_AGENTOS_SANDBOX_CONFIG",
        "JIUWENSWARM_AGENTOS_CODE_GRAPH_CONFIG",
        "JIUWENSWARM_GATEWAY_INTERNAL_TOKEN",
        "JIUWENSWARM_HOST_VERSION",
        "JIUWENSWARM_LINK_MTLS_MODE",
        "JIUWENSWARM_LINK_MTLS_PROFILE",
        "JIUWENSWARM_LINK_MTLS_CA_FILE",
        "JIUWENSWARM_LINK_MTLS_CERT_FILE",
        "JIUWENSWARM_LINK_MTLS_KEY_FILE",
        "JIUWENSWARM_DESKTOP",
        "JIUWENSWARM_ELECTRON",
        "JIUWENSWARM_CLI_PORTS",
        "JIUWENSWARM_RECOVERY_SESSION_ID",
        "JIUWENSWARM_RECOVERY_RESTART_SIGNAL_PATH",
        "JIUWENSWARM_START_CMD",
        "JIUWENSWARM_USER_DIRECTORY",
        "JIUWENSWARM_AGENT_ROOT",
        # Names the Gateway writes into ADX sandboxes for the AgentServer there.
        "JIUWENSWARM_RUNTIME_SETTINGS",
        "JIUWENSWARM_ADX_API_TOKEN",
        "JIUWENSWARM_ADX_NODE_INGRESS_URL",
        "JIUWENSWARM_ADX_NODE_CONTEXT_FILE",
        # Shared with the JWS AgentServer process; kept until the enterprise
        # deployment is confirmed not to set them (plan section 10.3).
        "JIUWENSWARM_CONFIG_DIR",
        "JIUWENCLAW_DATA_DIR",
        "JIUWENSWARM_AGENT_HTTP_BASE",
        "JIUWENSWARM_AGENT_UPLOAD_HTTP_BASE",
        "JIUWENSWARM_AGENT_HTTP_PORT",
        # Still read by the Gateway copies of the JWS file-download modules.
        # Removed together with those copies (plan section 10.4).
        "JIUWENSWARM_FILE_DOWNLOAD_SECRET",
        "JIUWENSWARM_DOWNLOAD_ASSET_ROOT",
        "JIUWENSWARM_WORKSPACE",
        "JIUWENCLAW_LOG_MASKING_PROBE",
        "JIUWENCLAW_REQUEST_EXT_FORWARD_HEADERS",
    }
)

_ALLOWED = _ALLOWED_UNREGISTERED | frozenset(
    name for var in env_names.ALL for name in var.legacy
)


def test_only_registered_legacy_names_are_read() -> None:
    offenders: dict[str, set[str]] = {}
    for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
        if path.name == "env_names.py":
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for pattern in _READ_PATTERNS:
            for match in pattern.finditer(text):
                name = match.group(1)
                if name.endswith("_"):
                    # A prefix such as JIUWENSWARM_LINK_, not a read of a variable.
                    continue
                if name in _ALLOWED:
                    continue
                offenders.setdefault(name, set()).add(str(path.relative_to(_PACKAGE_ROOT.parent)))
    assert not offenders, "unregistered JIUWENSWARM_*/JIUWENCLAW_* reads: %s" % {
        name: sorted(paths) for name, paths in offenders.items()
    }


def test_env_example_covers_every_public_runner_variable() -> None:
    assert _ENV_EXAMPLE.is_file(), "add .env.example at the repository root"
    text = _ENV_EXAMPLE.read_text(encoding="utf-8")

    missing = [var.name for var in env_names.ALL if not var.internal and var.name not in text]
    assert not missing, ".env.example is missing: %s" % missing


def test_internal_variables_are_not_documented_as_runner_settings() -> None:
    text = _ENV_EXAMPLE.read_text(encoding="utf-8")

    documented = [var.name for var in env_names.ALL if var.internal and var.name in text]
    assert not documented, ".env.example must not document internal handshakes: %s" % documented
