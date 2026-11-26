# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway-side instance sync, independent of AgentServer events."""

from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from agent_gateway.extensions_v2.config import GatewayExtensionConfig
from agent_gateway.extensions_v2.management import containers
from openjiuwen.core.events import HostPackageInstallRequest
from agent_gateway.extensions_v2.management.install_state import (
    ActiveInstance,
    BundleMemberRecord,
    ExtensionBundleMember,
    ExtensionBundleRecord,
    ExtensionInstallRecord,
    ExtensionInstallState,
)
from agent_gateway.extensions_v2.runtime import GatewayRuntime


class FakeManager:
    install_dir = Path(".")

    def __init__(self, *, fail: bool = False) -> None:
        self.active: list[SimpleNamespace] = []
        self.fail = fail
        self.calls: list[tuple[Any, ...]] = []
        self.managed: dict[tuple[str, str], Path] = {}

    def active_records(self) -> list[SimpleNamespace]:
        return list(self.active)

    def register_managed_root(self, package_id: str, version: str, root: Path) -> Path:
        self.managed[(package_id, version)] = Path(root)
        self.calls.append(("register_root", package_id, version))
        return Path(root)

    def unregister_managed_root(self, package_id: str, version: str, *, root: Path | None = None) -> None:
        self.managed.pop((package_id, version), None)
        self.calls.append(("unregister_root", package_id, version))

    async def enable_version(
        self, package_id: str, version: str, *, source=None, skip_dependencies: bool = False,
    ) -> SimpleNamespace:
        self.calls.append(("enable", package_id, version, source, skip_dependencies))
        if self.fail:
            return SimpleNamespace(status="FAILED", error=SimpleNamespace(message="load failed"))
        self.active.append(SimpleNamespace(package_id=package_id, version=version))
        return SimpleNamespace(status="SUCCESS", error=None)

    async def reload(self, package_id: str) -> SimpleNamespace:
        self.calls.append(("reload", package_id))
        return SimpleNamespace(status="SUCCESS", error=None)

    async def set_enabled(self, package_id: str, enabled: bool) -> SimpleNamespace:
        self.calls.append(("set_enabled", package_id, enabled))
        if not enabled:
            self.active = [item for item in self.active if item.package_id != package_id]
        return SimpleNamespace(status="SUCCESS", error=None)

    async def disable_version(self, package_id: str, version: str) -> SimpleNamespace:
        self.calls.append(("disable_version", package_id, version))
        self.active = [item for item in self.active if item.package_id != package_id]
        return SimpleNamespace(status="SUCCESS", error=None)

    async def uninstall(self, package_id: str, version: str = "") -> SimpleNamespace:
        self.calls.append(("uninstall", package_id, version))
        self.active = [item for item in self.active if item.package_id != package_id]
        return SimpleNamespace(status="SUCCESS", error=None)

    async def remove_standalone_copy(self, package_id: str, version: str) -> SimpleNamespace:
        self.calls.append(("remove_standalone_copy", package_id, version))
        return SimpleNamespace(status="SUCCESS", error=None)

    async def remove_bundle_version(self, bundle_id: str, version: str) -> str:
        self.calls.append(("remove_bundle_version", bundle_id, version))
        return ""


def _standalone(
    store: ExtensionInstallState,
    package_id: str,
    *,
    enabled: bool = True,
    status: str = "pending",
    version: str = "1.0.0",
    claim: bool = True,
) -> ExtensionInstallRecord:
    record = ExtensionInstallRecord(
        package_id=package_id,
        version=version,
        origin="user",
        active=False,
        enabled=enabled,
        domain="gateway",
        status=status,
    )
    store.upsert(record)
    if claim and enabled and status != "failed":
        store.switch_active_instance(
            ActiveInstance(package_id=package_id, version=version, kind="standalone")
        )
        store.mark(package_id, version=version, active=True, enabled=True, status=status)
        record = store.get(package_id, version)
    return record


def _bundle_member(
    store: ExtensionInstallState,
    install_dir: Path,
    package_id: str,
    *,
    member_version: str = "1.0.0",
    status: str = "pending",
    enabled: bool = True,
    active: bool = True,
    enable: bool | None = None,
) -> Path:
    root = install_dir / "demo.bundle" / "2.0.0" / "plugins" / package_id
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "package_type": "extension",
                "schemaVersion": 1,
                "package": {"id": package_id, "version": member_version, "domain": "gateway"},
                **({"enable": enable} if enable is not None else {}),
                "extensions": [{"id": "main", "runtime": "backend", "module": "extension.py"}],
            }
        ),
        encoding="utf-8",
    )
    (root / "extension.py").write_text("def create_extension():\n    return None\n", encoding="utf-8")
    members = (ExtensionBundleMember(package_id, member_version, "gateway"),)
    member_records = (
        BundleMemberRecord(
            package_id=package_id,
            version=member_version,
            domain="gateway",
            root=str(root),
            enabled=enabled,
            active=active,
            status=status,
        ),
    )
    store.upsert_bundle(
        ExtensionBundleRecord(
            bundle_id="demo.bundle",
            version="2.0.0",
            origin="user",
            members=members,
            member_records=member_records,
            status="partial",
            active=True,
        )
    )
    if active:
        store.switch_active_instance(
            ActiveInstance(
                package_id=package_id,
                version=member_version,
                kind="bundle",
                bundle_id="demo.bundle",
                bundle_version="2.0.0",
                root=str(root),
            )
        )
    return root


def _trigger(manager: FakeManager, store: ExtensionInstallState):
    async def trigger(operation: str, package_id: str, version: str, container_id: str, container_version: str):
        manager.calls.append((operation, package_id))
        success = not manager.fail
        if success and operation != "reload":
            manager.active.append(SimpleNamespace(package_id=package_id, version=version))
        request = HostPackageInstallRequest(
            package_id=package_id, version=version, operation=operation,
            container_id=container_id, container_version=container_version,
        )
        return containers.record_install_outcome(
            store, request, event_id="", success=success,
            error="load failed" if not success else "",
        )
    return trigger


@pytest.mark.asyncio
async def test_sync_loads_a_claimed_standalone_instance(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    _standalone(store, "demo.gw")

    results = await containers.sync_members(manager, store, _trigger(manager, store))

    assert [(item.package_id, item.status) for item in results] == [("demo.gw", "success")]
    assert ("install", "demo.gw") in manager.calls
    record = store.get("demo.gw", "1.0.0")
    assert record is not None
    assert (record.status, record.active) == ("active", True)


@pytest.mark.asyncio
async def test_sync_loads_a_claimed_bundle_member_in_place(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    root = _bundle_member(store, install_dir, "demo.gw")

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert manager.managed[("demo.gw", "1.0.0")] == root
    member = store.get_bundle_member("demo.bundle", "2.0.0", "demo.gw")
    assert member is not None and member.status == "active" and member.active is True


@pytest.mark.asyncio
async def test_sync_releases_an_instance_without_a_claim(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    manager.active.append(SimpleNamespace(package_id="demo.gw", version="1.0.0"))
    _standalone(store, "demo.gw", enabled=False, status="active", claim=False)

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert ("disable_version", "demo.gw", "1.0.0") in manager.calls
    record = store.get("demo.gw", "1.0.0")
    assert record is not None and record.active is False


@pytest.mark.asyncio
async def test_sync_reloads_a_pending_claimed_standalone(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    manager.active.append(SimpleNamespace(package_id="demo.gw", version="1.0.0"))
    _standalone(store, "demo.gw", status="pending")

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert ("reload", "demo.gw") in manager.calls
    assert store.get("demo.gw", "1.0.0").status == "active"


@pytest.mark.asyncio
async def test_sync_marks_a_failed_bundle_member(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager(fail=True)
    manager.install_dir = install_dir
    _bundle_member(store, install_dir, "demo.gw")

    results = await containers.sync_members(manager, store, _trigger(manager, store))

    assert results[0].status == "failed"
    member = store.get_bundle_member("demo.bundle", "2.0.0", "demo.gw")
    assert member is not None and (member.status, member.active) == ("failed", False)


@pytest.mark.asyncio
async def test_runtime_sync_container_members_is_local(tmp_path: Path) -> None:
    manager = FakeManager()
    runtime = GatewayRuntime(
        channel=SimpleNamespace(agent_client=None, channel_id="web"),
        config=GatewayExtensionConfig(
            install_dir=tmp_path / "extension_pkgs",
            recovery_data_dir=tmp_path / "extension",
        ),
    )
    runtime._management = manager  # noqa: SLF001 - tests inject the management fake
    store = ExtensionInstallState.for_install_dir(runtime.config.install_dir)

    async def bus_trigger(event):
        request = event.event_input
        manager.active.append(SimpleNamespace(package_id=request.package_id, version=request.version))
        return containers.record_install_outcome(
            store, request, event_id="", success=True,
        )

    runtime.bus.trigger = bus_trigger  # noqa: SLF001 - ops.build has not run in this unit test
    _standalone(store, "demo.gw")

    await runtime.sync_container_members()

    assert [item.package_id for item in manager.active] == ["demo.gw"]
    record = store.get("demo.gw", "1.0.0")
    assert record is not None and record.status == "active"


def test_frontend_status_and_projection_use_member_records(tmp_path: Path) -> None:
    from agent_gateway.channel_manager.web.extension_package_handlers import _project_bundle

    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    members = (
        ExtensionBundleMember("demo.gw", "1.0.0", "gateway"),
        ExtensionBundleMember("demo.as", "1.0.0", "agentserver"),
    )
    store.upsert_bundle(
        ExtensionBundleRecord(
            bundle_id="demo.bundle",
            version="2.0.0",
            origin="user",
            members=members,
            member_records=(
                BundleMemberRecord(
                    package_id="demo.gw", version="1.0.0", domain="gateway",
                    enabled=True, active=True, status="active",
                ),
                BundleMemberRecord(
                    package_id="demo.as", version="1.0.0", domain="agentserver",
                    enabled=True, active=False, status="pending",
                ),
            ),
            status="partial",
            active=True,
        )
    )

    record = store.get_bundle("demo.bundle", "2.0.0")
    assert record is not None
    assert containers.frontend_status(store, record) == "partial"

    projection = {"bundleId": "demo.bundle", "version": "2.0.0", "overallStatus": "partial", "members": []}
    refreshed = _project_bundle(store, projection)
    assert refreshed["overallStatus"] == "partial"
    assert [member["status"] for member in refreshed["members"]] == ["active", "pending"]


@pytest.mark.asyncio
async def test_sync_switches_to_a_new_root_when_the_version_is_equal(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    old_root = _bundle_member(store, install_dir, "demo.gw")
    new_root = install_dir / "demo.bundle" / "2.0.1" / "plugins" / "demo.gw"
    new_root.mkdir(parents=True)
    (new_root / "manifest.json").write_text(
        json.dumps(
            {
                "package_type": "extension",
                "schemaVersion": 1,
                "package": {"id": "demo.gw", "version": "1.0.0", "domain": "gateway"},
                "extensions": [{"id": "main", "runtime": "backend", "module": "extension.py"}],
            }
        ),
        encoding="utf-8",
    )
    (new_root / "extension.py").write_text("def create_extension():\n    return None\n", encoding="utf-8")
    store.upsert_bundle(
        ExtensionBundleRecord(
            bundle_id="demo.bundle",
            version="2.0.1",
            origin="user",
            members=(ExtensionBundleMember("demo.gw", "1.0.0", "gateway"),),
            member_records=(
                BundleMemberRecord(
                    package_id="demo.gw", version="1.0.0", domain="gateway",
                    root=str(new_root), enabled=True, active=True, status="active",
                ),
            ),
            status="active",
            active=True,
        )
    )
    store.switch_active_instance(
        ActiveInstance(
            package_id="demo.gw",
            version="1.0.0",
            kind="bundle",
            bundle_id="demo.bundle",
            bundle_version="2.0.1",
            root=str(new_root),
        )
    )
    manager.active.append(SimpleNamespace(package_id="demo.gw", version="1.0.0", root=old_root))

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert ("disable_version", "demo.gw", "1.0.0") in manager.calls
    assert manager.managed[("demo.gw", "1.0.0")] == new_root


@pytest.mark.asyncio
async def test_sync_leaves_an_agentserver_standalone_record_untouched(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    store.upsert(
        ExtensionInstallRecord(
            package_id="demo.as", version="1.0.0", origin="user",
            active=True, enabled=True, domain="agentserver", status="active",
        )
    )

    await containers.sync_members(manager, store, _trigger(manager, store))

    record = store.get("demo.as", "1.0.0")
    assert record is not None
    assert (record.active, record.enabled, record.status) == (True, True, "active")


@pytest.mark.asyncio
async def test_sync_finalizes_an_uninstalling_gateway_copy(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    store.upsert(
        ExtensionInstallRecord(
            package_id="demo.gw", version="1.0.0", origin="user",
            active=False, enabled=False, domain="gateway", status="uninstalling",
        )
    )

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert ("remove_standalone_copy", "demo.gw", "1.0.0") in manager.calls
    assert store.get("demo.gw", "1.0.0") is None


def test_pending_gateway_work_includes_pending_bundle_members(tmp_path: Path) -> None:
    from agent_gateway.extensions_v2.runtime import _has_pending_gateway_work

    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    _bundle_member(store, install_dir, "demo.gw", status="pending", active=False)

    assert _has_pending_gateway_work(store) is True


@pytest.mark.asyncio
async def test_runtime_schedules_a_retry_for_pending_bundle_members(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = GatewayRuntime(
        channel=SimpleNamespace(agent_client=None, channel_id="web"),
        config=GatewayExtensionConfig(
            install_dir=tmp_path / "extension_pkgs",
            recovery_data_dir=tmp_path / "extension",
        ),
    )
    store = ExtensionInstallState.for_install_dir(runtime.config.install_dir)
    _bundle_member(store, runtime.config.install_dir, "demo.gw", status="pending", active=False)

    async def fake_sync(manager, store, trigger):
        del manager, store, trigger
        return []

    monkeypatch.setattr(containers, "sync_members", fake_sync)

    await runtime.sync_container_members()

    task = runtime._package_retry_task  # noqa: SLF001 - tests inspect the scheduled task
    assert task is not None
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    runtime._package_retry_task = None  # noqa: SLF001


@pytest.mark.asyncio
async def test_gateway_member_install_skips_core_dependency_wait(tmp_path: Path) -> None:
    from openjiuwen.core.events import ExtensionEventBus
    from openjiuwen.core.events.package_lifecycle import host_install_handler

    from agent_gateway.extensions_v2.event_types.package import register_package_lifecycle_events

    calls: list[bool] = []

    class Packages:
        async def enable_version(self, package_id, version, *, skip_dependencies=False):
            del package_id, version
            calls.append(skip_dependencies)
            return SimpleNamespace(status="SUCCESS", error=None)

    runtime = SimpleNamespace(
        config=SimpleNamespace(install_dir=tmp_path / "extension_pkgs"),
        packages=Packages(),
    )
    bus = ExtensionEventBus()
    register_package_lifecycle_events(bus, runtime)
    handler = host_install_handler(bus)
    assert handler is not None

    await handler(HostPackageInstallRequest(package_id="demo.gw", version="1.0.0"))

    assert calls == [True]


@pytest.mark.asyncio
async def test_sync_does_not_load_a_member_of_an_inactive_bundle(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    root = _bundle_member(store, install_dir, "demo.gw")
    store.deactivate_bundle("demo.bundle", "2.0.0")

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert ("install", "demo.gw") not in manager.calls
    member = store.get_bundle_member("demo.bundle", "2.0.0", "demo.gw")
    assert member is not None and member.active is False and member.status == "standby"
    assert root.is_dir()


@pytest.mark.asyncio
async def test_sync_loads_a_pending_bundle_member_during_install(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    root = _bundle_member(store, install_dir, "demo.gw")
    record = store.get_bundle("demo.bundle", "2.0.0")
    assert record is not None
    store.upsert_bundle(replace(record, active=False, status="pending"))

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert manager.managed[("demo.gw", "1.0.0")] == root
    member = store.get_bundle_member("demo.bundle", "2.0.0", "demo.gw")
    assert member is not None and member.status == "active" and member.active is True


@pytest.mark.asyncio
async def test_sync_recovers_an_orphaned_standby_member(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    root = _bundle_member(
        store, install_dir, "demo.gw", status="standby", active=False,
    )

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert manager.managed[("demo.gw", "1.0.0")] == root
    member = store.get_bundle_member("demo.bundle", "2.0.0", "demo.gw")
    assert member is not None and member.status == "active" and member.active is True


@pytest.mark.asyncio
async def test_finalize_uninstall_removes_a_standby_copy_without_unloading_the_member(
    tmp_path: Path,
) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    member_root = _bundle_member(store, install_dir, "demo.gw", status="active", active=True)
    manager.active.append(
        SimpleNamespace(package_id="demo.gw", version="1.0.0", root=str(member_root))
    )
    store.upsert(
        ExtensionInstallRecord(
            package_id="demo.gw", version="1.0.0", origin="hub",
            active=False, enabled=False, domain="gateway", status="uninstalling",
        )
    )

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert ("remove_standalone_copy", "demo.gw", "1.0.0") in manager.calls
    assert not any(call[0] == "uninstall" for call in manager.calls)
    assert store.get("demo.gw", "1.0.0") is None
    member = store.get_bundle_member("demo.bundle", "2.0.0", "demo.gw")
    assert member is not None and member.active is True


@pytest.mark.asyncio
async def test_finalize_uninstall_unloads_the_loaded_standard_copy(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    standard = install_dir / "demo.gw" / "1.0.0"
    manager.active.append(
        SimpleNamespace(package_id="demo.gw", version="1.0.0", root=str(standard))
    )
    store.upsert(
        ExtensionInstallRecord(
            package_id="demo.gw", version="1.0.0", origin="hub",
            active=False, enabled=False, domain="gateway", status="uninstalling",
        )
    )

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert ("uninstall", "demo.gw", "1.0.0") in manager.calls
    assert not any(call[0] == "remove_standalone_copy" for call in manager.calls)
    assert store.get("demo.gw", "1.0.0") is None


@pytest.mark.asyncio
async def test_finalize_uninstall_removes_an_inactive_version(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    manager.active.append(
        SimpleNamespace(
            package_id="demo.gw", version="2.0.0", root=str(install_dir / "demo.gw" / "2.0.0"),
        )
    )
    store.upsert(
        ExtensionInstallRecord(
            package_id="demo.gw", version="1.0.0", origin="hub",
            active=False, enabled=False, domain="gateway", status="uninstalling",
        )
    )

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert ("remove_standalone_copy", "demo.gw", "1.0.0") in manager.calls
    assert not any(call[0] == "uninstall" for call in manager.calls)
    assert store.get("demo.gw", "1.0.0") is None


@pytest.mark.asyncio
async def test_finalize_uninstall_keeps_a_failed_removal_retryable(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    store.upsert(
        ExtensionInstallRecord(
            package_id="demo.gw", version="1.0.0", origin="hub",
            active=False, enabled=False, domain="gateway", status="uninstalling",
        )
    )

    async def fail_remove(package_id: str, version: str) -> SimpleNamespace:
        return SimpleNamespace(
            status="FAILED", error=SimpleNamespace(message="copy is locked"),
        )

    manager.remove_standalone_copy = fail_remove  # type: ignore[method-assign]
    await containers.sync_members(manager, store, _trigger(manager, store))

    record = store.get("demo.gw", "1.0.0")
    assert record is not None
    assert (record.status, record.active, record.enabled, record.error) == (
        "uninstalling", False, False, "copy is locked",
    )


def test_adjust_management_reports_marks_a_failed_gateway_copy(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    store.upsert(
        ExtensionInstallRecord(
            package_id="demo.gw", version="1.0.0", origin="hub",
            active=False, enabled=False, domain="gateway", status="failed", error="boom",
        )
    )
    payload = {
        "reports": [
            {
                "package_id": "demo.gw", "status": "SUCCESS", "plan_id": "",
                "operation": "install", "previous_generation": None,
                "current_generation": None, "error": None,
            },
        ],
        "installed_packages": [
            {"package_id": "demo.gw", "version": "1.0.0", "install_path": "x", "generation": 1},
        ],
    }

    adjusted = containers.adjust_management_reports(store, payload)

    assert adjusted["reports"][0]["status"] == "FAILED"
    assert adjusted["reports"][0]["error"]["message"] == "boom"
    assert adjusted["installed_packages"] == []


def test_adjust_management_reports_ignores_bundle_members(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    _bundle_member(store, install_dir, "demo.gw", status="failed", active=False, enabled=False)
    payload = {
        "reports": [
            {"package_id": "demo.gw", "status": "SUCCESS", "error": None},
        ],
    }

    adjusted = containers.adjust_management_reports(store, payload)

    assert adjusted == payload


def test_failed_outcomes_disable_the_record(tmp_path: Path) -> None:
    from openjiuwen.core.events import HostPackageInstallRequest

    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    store.upsert(
        ExtensionInstallRecord(
            package_id="demo.gw", version="1.0.0", origin="hub",
            active=True, enabled=True, domain="gateway", status="active",
        )
    )
    containers.record_install_outcome(
        store,
        HostPackageInstallRequest(package_id="demo.gw", version="1.0.0"),
        event_id="",
        success=False,
        error="load failed",
    )
    record = store.get("demo.gw", "1.0.0")
    assert record is not None
    assert (record.status, record.active, record.enabled) == ("failed", False, False)

    root = _bundle_member(store, install_dir, "demo.member", status="pending")
    containers.record_install_outcome(
        store,
        HostPackageInstallRequest(
            package_id="demo.member", version="1.0.0",
            container_id="demo.bundle", container_version="2.0.0",
        ),
        event_id="",
        success=False,
        error="member failed",
    )
    member = store.get_bundle_member("demo.bundle", "2.0.0", "demo.member")
    assert member is not None
    assert (member.status, member.active, member.enabled) == ("failed", False, False)
    assert root.is_dir()


@pytest.mark.asyncio
async def test_sync_promotes_a_standalone_when_the_loaded_member_failed(
    tmp_path: Path,
) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    _bundle_member(store, install_dir, "demo.gw", status="failed", active=False, enabled=False)
    _standalone(store, "demo.gw", status="standby", claim=False)

    await containers.sync_members(manager, store, _trigger(manager, store))

    record = store.get("demo.gw", "1.0.0")
    assert record is not None and record.status == "active" and record.active is True
    claim = store.active_instance("demo.gw")
    assert claim is not None and claim.kind == "standalone"


@pytest.mark.asyncio
async def test_sync_does_not_rewrite_a_bundle_without_gateway_members(
    tmp_path: Path,
) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    store.upsert_bundle(
        ExtensionBundleRecord(
            bundle_id="demo.bundle",
            version="2.0.0",
            origin="user",
            members=(ExtensionBundleMember("demo.agent", "1.0.0", "agentserver"),),
            member_records=(
                BundleMemberRecord(
                    package_id="demo.agent", version="1.0.0", domain="agentserver",
                    enabled=True, active=True, status="active",
                ),
            ),
            status="partial",
            active=True,
        )
    )

    await containers.sync_members(manager, store, _trigger(manager, store))

    record = store.get_bundle("demo.bundle", "2.0.0")
    assert record is not None and record.status == "partial"


@pytest.mark.asyncio
async def test_sync_releases_and_deletes_a_tombstoned_bundle(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    root = _bundle_member(
        store, install_dir, "demo.gw", status="disabled", active=False, enabled=False,
    )
    record = store.get_bundle("demo.bundle", "2.0.0")
    assert record is not None
    store.deactivate_bundle("demo.bundle", "2.0.0")
    store.upsert_bundle(replace(record, status="uninstalling", active=False))
    manager.active.append(
        SimpleNamespace(package_id="demo.gw", version="1.0.0", root=str(root))
    )

    await containers.sync_members(manager, store, _trigger(manager, store))

    assert ("disable_version", "demo.gw", "1.0.0") in manager.calls
    assert ("remove_bundle_version", "demo.bundle", "2.0.0") in manager.calls
    assert store.get_bundle("demo.bundle", "2.0.0") is None
    assert store.bundle_members("demo.bundle") == []
    assert manager.active == []


@pytest.mark.asyncio
async def test_sync_keeps_a_tombstoned_bundle_when_deletion_fails(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    _bundle_member(store, install_dir, "demo.gw", status="disabled", active=False, enabled=False)
    record = store.get_bundle("demo.bundle", "2.0.0")
    assert record is not None
    store.deactivate_bundle("demo.bundle", "2.0.0")
    store.upsert_bundle(replace(record, status="uninstalling", active=False))

    async def fail_remove(bundle_id: str, version: str) -> str:
        return "copy is locked"

    manager.remove_bundle_version = fail_remove  # type: ignore[method-assign]
    await containers.sync_members(manager, store, _trigger(manager, store))

    record = store.get_bundle("demo.bundle", "2.0.0")
    assert record is not None
    assert (record.status, record.error) == ("uninstalling", "copy is locked")
    assert store.bundle_members("demo.bundle") != []


def test_pending_gateway_work_includes_tombstoned_bundles(tmp_path: Path) -> None:
    from agent_gateway.extensions_v2.runtime import _has_pending_gateway_work

    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    _bundle_member(store, install_dir, "demo.gw", status="disabled", active=False, enabled=False)
    record = store.get_bundle("demo.bundle", "2.0.0")
    assert record is not None
    store.deactivate_bundle("demo.bundle", "2.0.0")
    store.upsert_bundle(replace(record, status="uninstalling", active=False))

    assert _has_pending_gateway_work(store) is True


def test_adjust_management_reports_marks_a_failed_bundle_uninstall(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    _bundle_member(store, install_dir, "demo.gw", status="disabled", active=False, enabled=False)
    record = store.get_bundle("demo.bundle", "2.0.0")
    assert record is not None
    store.deactivate_bundle("demo.bundle", "2.0.0")
    store.upsert_bundle(
        replace(record, status="uninstalling", active=False, error="copy is locked"),
    )
    payload = {
        "reports": [
            {"package_id": "demo.bundle", "status": "SUCCESS", "error": None},
        ],
    }

    adjusted = containers.adjust_management_reports(store, payload)

    assert adjusted["reports"][0]["status"] == "FAILED"
    assert adjusted["reports"][0]["error"]["message"] == "copy is locked"


def _write_manifest(root: Path, package_id: str, version: str, *, enable: bool) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "manifest.json").write_text(
        json.dumps(
            {
                "package_type": "extension",
                "schemaVersion": 1,
                "package": {"id": package_id, "version": version, "domain": "gateway"},
                "enable": enable,
                "extensions": [{"id": "main", "runtime": "backend", "module": "extension.py"}],
            }
        ),
        encoding="utf-8",
    )
    (root / "extension.py").write_text("def create_extension():\n    return None\n", encoding="utf-8")


@pytest.mark.asyncio
async def test_sync_does_not_resurrect_a_manifest_disabled_standalone(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    _write_manifest(install_dir / "demo.gw" / "1.0.0", "demo.gw", "1.0.0", enable=False)
    _standalone(store, "demo.gw", status="active")

    results = await containers.sync_members(manager, store, _trigger(manager, store))

    assert results == []
    assert ("install", "demo.gw") not in manager.calls
    record = store.get("demo.gw", "1.0.0")
    assert record is not None
    assert (record.enabled, record.active, record.status) == (False, False, "disabled")


@pytest.mark.asyncio
async def test_sync_does_not_resurrect_a_manifest_disabled_member(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    _bundle_member(store, install_dir, "demo.gw", status="active", enable=False)

    results = await containers.sync_members(manager, store, _trigger(manager, store))

    assert results == []
    assert ("install", "demo.gw") not in manager.calls
    member = store.get_bundle_member("demo.bundle", "2.0.0", "demo.gw")
    assert member is not None
    assert (member.enabled, member.active, member.status) == (False, False, "disabled")


@pytest.mark.asyncio
async def test_sync_still_promotes_a_standby_copy_seeded_unloaded(tmp_path: Path) -> None:
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    manager = FakeManager()
    manager.install_dir = install_dir
    _write_manifest(install_dir / "demo.gw" / "1.0.0", "demo.gw", "1.0.0", enable=False)
    _standalone(store, "demo.gw", status="standby", claim=False)

    results = await containers.sync_members(manager, store, _trigger(manager, store))

    assert [(item.package_id, item.status) for item in results] == [("demo.gw", "success")]
    assert ("install", "demo.gw") in manager.calls
    record = store.get("demo.gw", "1.0.0")
    assert record is not None
    assert record.status == "active"
