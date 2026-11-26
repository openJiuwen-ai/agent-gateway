# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""The Gateway-side recovery copy must see bundle members like the AgentServer."""

from __future__ import annotations

import json
from pathlib import Path

from agent_gateway.common.system_plugin_recovery import SystemPluginRecovery


def _write_bundle(root: Path) -> tuple[Path, Path]:
    bundle = root / "demo.bundle" / "1.0.0"
    (bundle / "plugins" / "demo.gw").mkdir(parents=True)
    (bundle / "manifest.json").write_text(
        json.dumps(
            {
                "package_type": "extension",
                "schemaVersion": 1,
                "is_bundle": True,
                "package": {"id": "demo.bundle", "version": "1.0.0"},
                "members": [{"path": "plugins/demo.gw"}],
            }
        ),
        encoding="utf-8",
    )
    member = bundle / "plugins" / "demo.gw"
    (member / "manifest.json").write_text(
        json.dumps(
            {
                "package_type": "extension",
                "schemaVersion": 1,
                "package": {"id": "demo.gw", "version": "2.0.0", "domain": "gateway"},
                "enable": True,
            }
        ),
        encoding="utf-8",
    )
    return bundle, member


def _write_state(data: Path, member: Path) -> None:
    data.mkdir(parents=True, exist_ok=True)
    (data / "system_plugin_install_state.json").write_text(
        json.dumps(
            {
                "packages": [],
                "bundles": [
                    {
                        "bundle_id": "demo.bundle",
                        "version": "1.0.0",
                        "origin": "user",
                        "members": [{"package_id": "demo.gw", "version": "2.0.0", "domain": "gateway"}],
                        "member_records": [
                            {
                                "package_id": "demo.gw",
                                "version": "2.0.0",
                                "domain": "gateway",
                                "root": str(member),
                                "enabled": True,
                                "active": True,
                                "status": "active",
                            }
                        ],
                        "status": "active",
                        "active": True,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_gateway_snapshot_records_bundle_members(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    data_dir = tmp_path / "extension"
    _, member = _write_bundle(install_dir)
    _write_state(data_dir, member)
    recovery = SystemPluginRecovery(install_dir, data_dir)

    recovery.begin_session("gateway-session")

    snapshot = recovery._scan_packages()
    assert [(item["package_id"], item["version"], item["bundle_id"]) for item in snapshot] == [
        ("demo.gw", "2.0.0", "demo.bundle"),
    ]


def test_gateway_restore_disables_member_and_updates_ledger(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    data_dir = tmp_path / "extension"
    _, member = _write_bundle(install_dir)
    _write_state(data_dir, member)
    recovery = SystemPluginRecovery(install_dir, data_dir)

    startup = recovery.begin_session("gateway-session")
    document = json.loads((data_dir / "system_plugin_recovery.json").read_text(encoding="utf-8"))
    document["records"][-1]["snapshot"] = [
        {"package_id": "demo.gw", "version": "2.0.0", "enabled": False},
    ]
    (data_dir / "system_plugin_recovery.json").write_text(json.dumps(document), encoding="utf-8")

    recovery.restore(startup["id"])

    manifest = json.loads((member / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["enable"] is False
    state = json.loads((data_dir / "system_plugin_install_state.json").read_text(encoding="utf-8"))
    record = state["bundles"][0]["member_records"][0]
    assert (record["enabled"], record["active"], record["status"]) == (False, False, "disabled")
    assert (state["bundles"][0]["active"], state["bundles"][0]["status"]) == (False, "disabled")
