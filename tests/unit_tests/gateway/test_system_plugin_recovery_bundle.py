# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""The Gateway-side recovery copy must see bundle members like the AgentServer."""

from __future__ import annotations

import json
from pathlib import Path

import portalocker
import pytest

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


def _write_standalone(root: Path, package_id: str, version: str, *, enabled: bool) -> Path:
    package = root / package_id / version
    package.mkdir(parents=True, exist_ok=True)
    (package / "manifest.json").write_text(
        json.dumps(
            {
                "package_type": "extension",
                "schemaVersion": 1,
                "package": {"id": package_id, "version": version, "domain": "gateway"},
                "enable": enabled,
            }
        ),
        encoding="utf-8",
    )
    return package


def _set_enabled(root: Path, enabled: bool) -> None:
    document = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    document["enable"] = enabled
    (root / "manifest.json").write_text(json.dumps(document), encoding="utf-8")


def _enabled(root: Path) -> bool:
    return json.loads((root / "manifest.json").read_text(encoding="utf-8"))["enable"]


def test_gateway_restore_keeps_duplicate_id_instances_separate(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    data_dir = tmp_path / "extension"
    standalone = _write_standalone(install_dir, "demo.gw", "2.0.0", enabled=True)
    _, member = _write_bundle(install_dir)
    _set_enabled(member, False)
    state = {
        "packages": [
            {
                "package_id": "demo.gw", "version": "2.0.0", "origin": "user",
                "install_path": str(standalone), "domain": "gateway",
                "enabled": True, "active": True, "status": "active",
            },
        ],
        "bundles": [
            {
                "bundle_id": "demo.bundle", "version": "1.0.0", "origin": "user",
                "members": [{"package_id": "demo.gw", "version": "2.0.0", "domain": "gateway"}],
                "member_records": [
                    {
                        "package_id": "demo.gw", "version": "2.0.0", "domain": "gateway",
                        "root": str(member), "enabled": False, "active": False, "status": "disabled",
                    }
                ],
                "status": "active", "active": True,
            }
        ],
    }
    (data_dir).mkdir(parents=True, exist_ok=True)
    (data_dir / "system_plugin_install_state.json").write_text(json.dumps(state), encoding="utf-8")
    recovery = SystemPluginRecovery(install_dir, data_dir)
    startup = recovery.begin_session("gateway-session")

    _set_enabled(standalone, False)
    _set_enabled(member, True)
    state["packages"][0].update(enabled=False, active=False, status="disabled")
    state["bundles"][0]["member_records"][0].update(enabled=True, active=True, status="active")
    (data_dir / "system_plugin_install_state.json").write_text(json.dumps(state), encoding="utf-8")

    recovery.restore(startup["id"])

    assert _enabled(standalone) is True
    assert _enabled(member) is False
    state = json.loads((data_dir / "system_plugin_install_state.json").read_text(encoding="utf-8"))
    assert state["packages"][0]["enabled"] is True
    assert state["bundles"][0]["member_records"][0]["enabled"] is False


def test_gateway_restore_preserves_an_uninstalling_tombstone(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    data_dir = tmp_path / "extension"
    _, member = _write_bundle(install_dir)
    state = {
        "packages": [],
        "bundles": [
            {
                "bundle_id": "demo.bundle", "version": "1.0.0", "origin": "user",
                "members": [{"package_id": "demo.gw", "version": "2.0.0", "domain": "gateway"}],
                "member_records": [
                    {
                        "package_id": "demo.gw", "version": "2.0.0", "domain": "gateway",
                        "root": str(member), "enabled": False, "active": False,
                        "status": "uninstalling", "error": "busy",
                    }
                ],
                "status": "uninstalling", "active": False,
            }
        ],
    }
    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "system_plugin_install_state.json").write_text(json.dumps(state), encoding="utf-8")
    recovery = SystemPluginRecovery(install_dir, data_dir)
    startup = recovery.begin_session("gateway-session")

    recovery.restore(startup["id"])

    assert _enabled(member) is True
    state = json.loads((data_dir / "system_plugin_install_state.json").read_text(encoding="utf-8"))
    record = state["bundles"][0]["member_records"][0]
    assert (record["status"], record["error"]) == ("uninstalling", "busy")
    assert state["bundles"][0]["status"] == "uninstalling"


def test_gateway_restore_rolls_back_when_the_ledger_lock_times_out(
    tmp_path: Path, monkeypatch,
) -> None:
    install_dir = tmp_path / "extension_pkgs"
    data_dir = tmp_path / "extension"
    package = _write_standalone(install_dir, "demo.one", "1.0.0", enabled=True)
    (data_dir).mkdir(parents=True, exist_ok=True)
    (data_dir / "system_plugin_install_state.json").write_text(
        json.dumps({"packages": [], "bundles": []}), encoding="utf-8",
    )
    recovery = SystemPluginRecovery(install_dir, data_dir)
    startup = recovery.begin_session("gateway-session")

    _set_enabled(package, False)

    def fail_lock(*_args):
        raise portalocker.LockException("ledger is busy")

    monkeypatch.setattr(recovery, "_sync_install_state", fail_lock)

    with pytest.raises(portalocker.LockException):
        recovery.restore(startup["id"])

    assert _enabled(package) is False
    assert [record["action"] for record in recovery.records()] == ["startup"]
