# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway package management over the shared ``extension_pkgs`` store."""

from __future__ import annotations

import asyncio
import json
import sys
import types
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from openjiuwen.core.common.exception.codes import StatusCode
from openjiuwen.core.extension import ExtensionFailure

from agent_gateway.extensions_v2 import assembly
from agent_gateway.extensions_v2.config import GatewayExtensionConfig
from agent_gateway.extensions_v2.loader import LoadedPackage
from agent_gateway.extensions_v2.management.install_state import ExtensionInstallState
from agent_gateway.extensions_v2.management.registry import GatewayPackageManager
from agent_gateway.extensions_v2.registry import HandlerDeclaration
from agent_gateway.extensions_v2.runtime import GatewayRuntime


class Channel:
    """Minimal channel: methods, responses and connection ids."""

    channel_id = "web"

    def __init__(self) -> None:
        self.methods: dict[str, object] = {}
        self.responses: list[dict] = []

    def register_method(self, name: str, handler) -> None:
        self.methods[name] = handler

    async def ensure_ws_id(self, ws) -> str:
        if not getattr(ws, "_jiuwen_ws_id", ""):
            ws._jiuwen_ws_id = "ws-1"
        return ws._jiuwen_ws_id

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
        self.responses.append(
            {"id": req_id, "ok": ok, "payload": payload or {}, "error": error, "code": code}
        )


ENTRY = """\
class Extension:
    def register(self, registrar):
        async def handler(event, ops, next_handler):
            return await next_handler(event)

        registrar.register_event_handler("gateway.request", handler, {}, "user")


def create_extension():
    return Extension()
"""

BAD_ENTRY = """\
class Extension:
    def register(self, registrar):
        async def handler(event, ops, next_handler):
            return await next_handler(event)

        registrar.register_event_handler("gateway.request", handler, {}, "user")
        registrar.register_event_handler("not.a.gateway.event", handler, {}, "user")


def create_extension():
    return Extension()
"""


AUTH_ENTRY = """\
from agent_gateway.extensions_v2.models import GatewayRequestOutput

class Extension:
    def register(self, registrar):
        async def handler(event, ops, next_handler):
            if event.event_input.headers.get("authorization") != "Bearer valid":
                return GatewayRequestOutput(handled=True, route="deny", error={"code": "AUTH_REQUIRED"})
            return await next_handler(event)

        registrar.register_event_handler("gateway.request", handler)

def create_extension():
    return Extension()
"""


def _package_zip(
    tmp_path: Path,
    *,
    package_id: str = "demo.gateway",
    domain: str = "gateway",
    version: str = "0.1.0",
    entry: str = ENTRY,
    enable: bool | None = None,
    requires: dict | None = None,
) -> Path:
    source = tmp_path / "src" / package_id / version
    source.mkdir(parents=True)
    manifest_document = {
        "package_type": "extension",
        "schemaVersion": 1,
        "package": {"id": package_id, "version": version, "domain": domain},
        "extensions": [{"id": "demo", "runtime": "backend", "module": "extension.py"}],
    }
    if enable is not None:
        manifest_document["enable"] = enable
    if requires is not None:
        manifest_document["requires"] = requires
    manifest = json.dumps(manifest_document)
    (source / "manifest.json").write_text(manifest, encoding="utf-8")
    (source / "extension.py").write_text(entry, encoding="utf-8")
    archive = tmp_path / f"{package_id}-{version}.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for file in source.iterdir():
            bundle.write(file, file.name)
    return archive








async def _runtime(tmp_path: Path) -> GatewayRuntime:
    runtime = GatewayRuntime(
        channel=Channel(),
        config=GatewayExtensionConfig(
            install_dir=tmp_path / "extension_pkgs",
            recovery_data_dir=tmp_path / "extension",
        ),
    )
    await runtime.start()
    return runtime


async def test_gateway_package_install_publish_and_uninstall(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")

    reports = await manager.install([_package_zip(tmp_path)])

    assert [report.status for report in reports] == ["SUCCESS"]
    assert runtime.user_handler_count == 1
    assert [package.package_id for package in runtime.active_packages()] == ["demo.gateway"]
    state = manager.query()
    assert state["installed_packages"][0]["package_id"] == "demo.gateway"
    assert state["installed_packages"][0]["domain"] == "gateway"
    assert state["active"][0]["version"] == "0.1.0"

    report = await manager.uninstall("demo.gateway")

    assert report.status == "SUCCESS"
    assert runtime.user_handler_count == 0
    assert manager.installed() == []


async def test_gateway_install_switches_to_a_new_version_and_rejects_the_same_one(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    await manager.install([_package_zip(tmp_path)])

    upgraded = await manager.install([_package_zip(tmp_path, version="0.2.0")])

    assert [report.status for report in upgraded] == ["SUCCESS"]
    assert (tmp_path / "extension_pkgs" / "demo.gateway" / "0.1.0").is_dir()
    assert (tmp_path / "extension_pkgs" / "demo.gateway" / "0.2.0").is_dir()
    assert runtime.active_record("demo.gateway").version == "0.2.0"

    duplicate = await manager.install([_package_zip(tmp_path / "again", version="0.2.0")])

    assert duplicate[0].status == "FAILED"
    assert duplicate[0].error.code == "EXTENSION_PACKAGE_EXISTS"


async def test_gateway_uninstall_one_version_keeps_the_other(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    await manager.install([_package_zip(tmp_path)])
    await manager.install([_package_zip(tmp_path, version="0.2.0")])

    report = await manager.uninstall("demo.gateway", "0.2.0")

    assert report.status == "SUCCESS"
    assert (tmp_path / "extension_pkgs" / "demo.gateway" / "0.1.0").is_dir()
    assert not (tmp_path / "extension_pkgs" / "demo.gateway" / "0.2.0").exists()
    assert runtime.active_record("demo.gateway") is None


HELPER_ENTRY = """\
from .helpers import ANSWER


class Extension:
    def register(self, registrar):
        async def handler(event, ops, next_handler):
            return await next_handler(event)

        registrar.register_event_handler("gateway.request", handler, {}, "user")


def create_extension():
    return Extension()
"""


def _helper_package_zip(folder: Path, answer: str) -> Path:
    """demo.gateway 0.1.0 whose entry imports a sibling module holding ``answer``."""
    source = folder / "src"
    source.mkdir(parents=True)
    (source / "manifest.json").write_text(json.dumps({
        "schemaVersion": 1, "package_type": "extension",
        "package": {"id": "demo.gateway", "version": "0.1.0", "domain": "gateway"},
        "extensions": [{"id": "demo", "runtime": "backend", "module": "extension.py"}],
    }), encoding="utf-8")
    (source / "extension.py").write_text(HELPER_ENTRY, encoding="utf-8")
    (source / "helpers.py").write_text(f"ANSWER = {answer!r}\n", encoding="utf-8")
    archive = folder / "demo.gateway-0.1.0.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for file in source.iterdir():
            bundle.write(file, file.name)
    return archive


async def test_reinstalling_the_same_version_loads_fresh_submodules(tmp_path):
    """Each load uses new module names, so a reinstall at the same path never reuses cached sibling modules."""
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    try:
        for answer in ("first", "second"):
            reports = await manager.install([_helper_package_zip(tmp_path / answer, answer)])
            assert [report.status for report in reports] == ["SUCCESS"]
            [package] = runtime.active_packages()
            assert package.handlers[0].handler.__globals__["ANSWER"] == answer
            assert (await manager.uninstall("demo.gateway")).status == "SUCCESS"
    finally:
        await runtime.shutdown()


def _package_modules(package) -> list[str]:
    roots = package.module_names
    return [name for name in sys.modules if any(name == root or name.startswith(root + ".") for root in roots)]


async def test_released_loads_leave_sys_modules(tmp_path):
    """Loads no longer accumulate: each release drops the modules of its own load (S_01 invariant 11)."""
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    loads = []
    try:
        for index in range(6):
            reports = await manager.install([_helper_package_zip(tmp_path / f"load{index}", f"answer{index}")])
            assert [report.status for report in reports] == ["SUCCESS"]
            [package] = runtime.active_packages()
            assert len(_package_modules(package)) >= 2  # entry package and the helpers submodule
            loads.append(package)
            assert (await manager.uninstall("demo.gateway")).status == "SUCCESS"
            assert _package_modules(package) == []
        assert all(_package_modules(package) == [] for package in loads)

        await manager.install([_helper_package_zip(tmp_path / "reload", "kept")])
        assert (await manager.reload("demo.gateway")).status == "SUCCESS"
        [current] = runtime.active_packages()
        assert _package_modules(current) and all(_package_modules(package) == [] for package in loads)
    finally:
        await runtime.shutdown()
    assert _package_modules(current) == []


async def test_modules_stay_until_running_handlers_finish(tmp_path):
    runtime = await _runtime(tmp_path)
    started, resume = asyncio.Event(), asyncio.Event()
    root = tmp_path / "slow"
    root.mkdir()
    module_name = "jiuwenswarm_gateway_ext_test_slow"
    sys.modules[module_name] = types.ModuleType(module_name)

    async def slow(event, ops, next_handler):
        started.set()
        await resume.wait()
        return await next_handler(event)

    package = LoadedPackage(package_id="demo.slow", version="1.0.0", root=root, module_names=(module_name,),
                            handlers=(HandlerDeclaration("gateway.request", slow, {}, "slow"),))
    try:
        runtime.publish(package)
        request = asyncio.create_task(runtime.intercept(SimpleNamespace(closed=False), "req", "ping", {}))
        await started.wait()
        runtime.unpublish("demo.slow")
        assert module_name in sys.modules, "a running handler keeps its modules"
        resume.set()
        await request
        assert module_name not in sys.modules
    finally:
        sys.modules.pop(module_name, None)
        await runtime.shutdown()
async def test_gateway_disabled_package_keeps_the_copy_without_publishing(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")

    reports = await manager.install([_package_zip(tmp_path, enable=False)])

    assert [report.status for report in reports] == ["SUCCESS"]
    assert runtime.user_handler_count == 0
    assert manager.owns("demo.gateway")

    enabled = await manager.set_enabled("demo.gateway", True)

    assert enabled.status == "SUCCESS"
    assert runtime.user_handler_count == 1
    disabled = await manager.set_enabled("demo.gateway", False)
    assert disabled.status == "SUCCESS"
    assert runtime.user_handler_count == 0


async def test_installing_a_disabled_copy_releases_an_active_gateway_package(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    await manager.install([_package_zip(tmp_path / "enabled")])
    assert runtime.user_handler_count == 1

    reports = await manager.install([_package_zip(tmp_path / "disabled", version="0.2.0", enable=False)])

    assert [report.status for report in reports] == ["SUCCESS"]
    assert runtime.user_handler_count == 0
    manifest = json.loads(
        (tmp_path / "extension_pkgs" / "demo.gateway" / "0.2.0" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["enable"] is False

    # Enabling again has to activate the newer copy instead of reporting the running one.
    enabled = await manager.set_enabled("demo.gateway", True)
    assert enabled.status == "SUCCESS"
    assert runtime.user_handler_count == 1
    assert runtime.active_record("demo.gateway").version == "0.2.0"


async def test_reload_of_a_disabled_gateway_package_succeeds_without_enabling_it(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    await manager.install([_package_zip(tmp_path, enable=False)])

    report = await manager.reload("demo.gateway")

    assert report.status == "SUCCESS"
    assert report.error is None
    assert runtime.user_handler_count == 0
    manifest = json.loads(
        (tmp_path / "extension_pkgs" / "demo.gateway" / "0.1.0" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["enable"] is False


async def test_agent_package_is_rejected_without_copying(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")

    reports = await manager.install([_package_zip(tmp_path, package_id="demo.agent", domain="agentserver")])

    assert [report.status for report in reports] == ["FAILED"]
    assert reports[0].error.code == "EXTENSION_DOMAIN_MISMATCH"
    assert manager.installed() == []
    assert runtime.user_handler_count == 0


async def test_reload_republishes_the_installed_copy(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    await manager.install([_package_zip(tmp_path)])
    runtime.unpublish("demo.gateway")
    assert runtime.user_handler_count == 0

    report = await manager.reload("demo.gateway")

    assert report.status == "SUCCESS"
    assert runtime.user_handler_count == 1








async def test_partial_registration_failure_rolls_the_package_back(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")

    reports = await manager.install([_package_zip(tmp_path, entry=BAD_ENTRY)])

    assert [report.status for report in reports] == ["FAILED"]
    assert reports[0].error.code == "EXTENSION_LOAD_FAILED"
    assert runtime.user_handler_count == 0
    assert runtime.active_packages() == ()
    assert manager.installed() == []


async def test_uninstall_reports_failure_when_the_copy_cannot_be_deleted(tmp_path, monkeypatch):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    await manager.install([_package_zip(tmp_path)])

    def deny_delete(package_id: str, version: str) -> None:
        raise ExtensionFailure(
            StatusCode.EXTENSION_INSTALL_COPY_FAILED,
            reason="file is locked",
        )

    monkeypatch.setattr(manager._store, "remove_installed", deny_delete)

    report = await manager.uninstall("demo.gateway")

    assert report.status == "FAILED"
    assert report.error.code == "EXTENSION_INSTALL_COPY_FAILED"
    assert runtime.user_handler_count == 0
    assert manager.installed() != []


async def test_uninstall_retries_a_transient_delete_lock(tmp_path, monkeypatch):
    runtime = await _runtime(tmp_path)
    manager = GatewayPackageManager(runtime, tmp_path / "extension_pkgs")
    await manager.install([_package_zip(tmp_path)])
    original = manager._store.remove_installed
    calls = {"count": 0}

    def flaky(package_id: str, version: str) -> None:
        calls["count"] += 1
        if calls["count"] == 1:
            raise ExtensionFailure(StatusCode.EXTENSION_INSTALL_COPY_FAILED, reason="file is locked")
        original(package_id, version)

    monkeypatch.setattr(manager._store, "remove_installed", flaky)

    report = await manager.uninstall("demo.gateway")

    assert report.status == "SUCCESS"
    assert calls["count"] == 2
    assert manager.installed() == []














































# ── shared install report (production path) ──────────────────────────────────


def _report_path(tmp_path: Path) -> Path:
    return tmp_path / "extension" / "extension_install_report.yaml"


async def _reported_manager(tmp_path: Path, runtime: GatewayRuntime | None = None) -> GatewayPackageManager:
    runtime = runtime or await _runtime(tmp_path)
    manager = GatewayPackageManager(
        runtime,
        tmp_path / "extension_pkgs",
        report_path=_report_path(tmp_path),
        poll_interval=0.05,
        dependency_timeout=5.0,
        lock_timeout=2.0,
    )
    await manager.init()
    return manager


async def test_reported_disable_and_enable_keep_copy_and_settle_report(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        assert (await manager.install([_package_zip(tmp_path)]))[0].status == "SUCCESS"
        root = manager.gateway_installed("demo.gateway")[0].root
        for enabled in (False, True):
            report = await manager.set_enabled("demo.gateway", enabled)
            assert report.status == "SUCCESS"
            assert report.operation == ("enable" if enabled else "disable")
            assert runtime.user_handler_count == int(enabled)
            assert root.exists()
            assert json.loads((root / "manifest.json").read_text(encoding="utf-8"))["enable"] is enabled
            state = (await manager._report.read_packages("demo.gateway")).get("demo.gateway")
            if enabled:
                assert state is not None
                assert state.pending is None
                assert state.active_version == "0.1.0"
            else:
                assert state is None
    finally:
        await manager.close()
        await runtime.shutdown()


async def test_reported_install_switches_to_a_new_version_and_rejects_the_same_one(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        assert (await manager.install([_package_zip(tmp_path)]))[0].status == "SUCCESS"

        upgraded = await manager.install([_package_zip(tmp_path, version="0.2.0")])

        assert [report.status for report in upgraded] == ["SUCCESS"]
        assert (tmp_path / "extension_pkgs" / "demo.gateway" / "0.1.0").is_dir()
        assert runtime.active_record("demo.gateway").version == "0.2.0"

        duplicate = await manager.install([_package_zip(tmp_path / "again", version="0.2.0")])

        assert duplicate[0].status == "FAILED"
        assert duplicate[0].error.code == "EXTENSION_PACKAGE_EXISTS"
    finally:
        await manager.close()
        await runtime.shutdown()








async def test_reported_uninstall_one_version_keeps_the_other(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        await manager.install([_package_zip(tmp_path)])
        await manager.install([_package_zip(tmp_path, version="0.2.0")])

        report = await manager.uninstall("demo.gateway", "0.2.0")

        assert report.status == "SUCCESS"
        assert (tmp_path / "extension_pkgs" / "demo.gateway" / "0.1.0").is_dir()
        assert not (tmp_path / "extension_pkgs" / "demo.gateway" / "0.2.0").exists()
        assert runtime.active_record("demo.gateway") is None
    finally:
        await manager.close()
        await runtime.shutdown()


async def test_reported_disable_rejects_active_dependents_without_side_effects(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    released = []
    manager._on_released = released.append
    try:
        sources = [
            _package_zip(tmp_path),
            _package_zip(tmp_path, package_id="demo.consumer", requires={
                "packages": [{"id": "demo.gateway", "version": ">=0.1.0"}],
            }),
        ]
        assert all(report.status == "SUCCESS" for report in await manager.install(sources))
        before = runtime.active_record("demo.gateway")
        root = manager.gateway_installed("demo.gateway")[0].root
        original = (root / "manifest.json").read_bytes()

        report = await manager.set_enabled("demo.gateway", False)

        assert report.status == "FAILED"
        assert report.error.code == "EXTENSION_PACKAGE_IN_USE"
        assert runtime.active_record("demo.gateway") == before
        assert runtime.user_handler_count == 2
        assert (root / "manifest.json").read_bytes() == original
        assert released == []
        assert (await manager._report.read_packages("demo.gateway"))["demo.gateway"].pending is None

        assert (await manager.set_enabled("demo.consumer", False)).status == "SUCCESS"
        assert (await manager.set_enabled("demo.gateway", False)).status == "SUCCESS"
        consumer_root = manager.gateway_installed("demo.consumer")[0].root
        original = (consumer_root / "manifest.json").read_bytes()
        report = await manager.set_enabled("demo.consumer", True)
        assert report.status == "FAILED"
        assert report.error.code == "EXTENSION_DEPENDENCY_NOT_MET"
        assert runtime.user_handler_count == 0
        assert (consumer_root / "manifest.json").read_bytes() == original
        state = (await manager._report.read_packages("demo.consumer"))["demo.consumer"]
        assert state.pending is None
        assert state.active_version is None
    finally:
        await manager.close()
        await runtime.shutdown()


@pytest.mark.parametrize("enabled", [False, True])
async def test_reported_enabled_write_failure_keeps_previous_state(tmp_path, monkeypatch, enabled):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        await manager.install([_package_zip(tmp_path)])
        if enabled:
            assert (await manager.set_enabled("demo.gateway", False)).status == "SUCCESS"
        before = runtime.active_record("demo.gateway")
        root = manager.gateway_installed("demo.gateway")[0].root
        original = (root / "manifest.json").read_bytes()

        def fail_write(*args):
            raise OSError("manifest write failed")

        monkeypatch.setattr(manager._store, "set_enabled", fail_write)
        report = await manager.set_enabled("demo.gateway", enabled)

        assert report.status == "FAILED"
        assert runtime.active_record("demo.gateway") == before
        assert runtime.user_handler_count == int(not enabled)
        assert (root / "manifest.json").read_bytes() == original
        state = (await manager._report.read_packages("demo.gateway"))["demo.gateway"]
        assert state.pending is None
        assert state.active_version == (None if enabled else "0.1.0")
    finally:
        await manager.close()
        await runtime.shutdown()


async def test_reported_failed_enable_restores_disabled_manifest(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        await manager.install([_package_zip(tmp_path)])
        assert (await manager.set_enabled("demo.gateway", False)).status == "SUCCESS"
        root = manager.gateway_installed("demo.gateway")[0].root
        (root / "extension.py").write_text(BAD_ENTRY, encoding="utf-8")

        report = await manager.set_enabled("demo.gateway", True)

        assert report.status == "FAILED"
        assert report.error.code == "EXTENSION_LOAD_FAILED"
        assert runtime.user_handler_count == 0
        assert json.loads((root / "manifest.json").read_text(encoding="utf-8"))["enable"] is False
        state = (await manager._report.read_packages("demo.gateway"))["demo.gateway"]
        assert state.pending is None
        assert state.active_version is None
    finally:
        await manager.close()
        await runtime.shutdown()


async def test_reported_install_records_generation_and_domain(tmp_path):
    manager = await _reported_manager(tmp_path)
    try:
        reports = await manager.install([_package_zip(tmp_path)])

        assert [report.status for report in reports] == ["SUCCESS"]
        assert reports[0].current_generation is not None
        assert manager.query()["active"][0]["generation"] == reports[0].current_generation
        assert await manager.get_package_domain("demo.gateway") == "gateway"
        states = await manager._report.read_packages("demo.gateway")
        assert states["demo.gateway"].active_version == "0.1.0"
        assert states["demo.gateway"].pending is None
    finally:
        await manager.close()


async def test_reported_reload_keeps_the_running_copy_on_failure(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        await manager.install([_package_zip(tmp_path)])
        before = runtime.active_record("demo.gateway")
        root = manager.gateway_installed("demo.gateway")[0].root
        (root / "extension.py").write_text(BAD_ENTRY, encoding="utf-8")

        report = await manager.reload("demo.gateway")

        assert report.status == "FAILED"
        after = runtime.active_record("demo.gateway")
        assert after is not None
        assert after.generation == before.generation
        assert runtime.user_handler_count == 1
        states = await manager._report.read_packages("demo.gateway")
        assert states["demo.gateway"].active_version == "0.1.0"
        assert states["demo.gateway"].pending is None
    finally:
        await manager.close()


async def test_reported_uninstall_is_refused_while_handlers_run(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        await manager.install([_package_zip(tmp_path)])
        started, resume = asyncio.Event(), asyncio.Event()
        original = runtime.intercept

        async def slow(event, ops, next_handler):
            started.set()
            await resume.wait()
            return await next_handler(event)

        root = manager.gateway_installed("demo.gateway")[0].root
        runtime.unpublish("demo.gateway")
        runtime.publish(
            LoadedPackage(package_id="demo.gateway", version="0.1.0", root=root, module_names=(),
                          handlers=(HandlerDeclaration("gateway.request", slow, {}, "slow"),))
        )
        request = asyncio.create_task(original(SimpleNamespace(closed=False), "req", "ping", {}))
        await started.wait()

        report = await manager.uninstall("demo.gateway")

        assert report.status == "FAILED"
        assert report.error.code == "EXTENSION_PACKAGE_BUSY"
        assert manager.gateway_installed("demo.gateway")
        resume.set()
        await request
    finally:
        await manager.close()


async def test_reported_startup_loads_only_gateway_packages(tmp_path):
    first = await _reported_manager(tmp_path)
    await first.install([_package_zip(tmp_path)])
    await first.close()
    agent_root = tmp_path / "extension_pkgs" / "demo.agent" / "0.1.0"
    agent_root.mkdir(parents=True)
    agent_root.joinpath("manifest.json").write_text(json.dumps({
        "package_type": "extension",
        "schemaVersion": 1,
        "package": {"id": "demo.agent", "version": "0.1.0", "domain": "agentserver"},
        "extensions": [{"id": "demo", "runtime": "backend", "module": "extension.py"}],
    }), encoding="utf-8")
    agent_root.joinpath("extension.py").write_text(ENTRY, encoding="utf-8")

    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        reports = await manager.load_installed()

        assert [(report.package_id, report.status) for report in reports] == [("demo.gateway", "SUCCESS")]
        assert [package.package_id for package in runtime.active_packages()] == ["demo.gateway"]
        assert {package.package_id for package in manager.installed()} == {"demo.gateway", "demo.agent"}
    finally:
        await manager.close()


async def test_reported_uninstall_finishes_after_the_caller_is_cancelled(tmp_path, monkeypatch):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        await manager.install([_package_zip(tmp_path)])
        entered = asyncio.Event()
        original = runtime.cleanup_package_data

        async def slow_cleanup(package_id: str) -> None:
            entered.set()
            await asyncio.sleep(0.1)
            await original(package_id)

        monkeypatch.setattr(runtime, "cleanup_package_data", slow_cleanup)
        task = asyncio.create_task(manager.uninstall("demo.gateway"))
        await entered.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert manager.gateway_installed("demo.gateway") == []
        assert runtime.active_record("demo.gateway") is None
        states = await manager._report.read_packages("demo.gateway")
        state = states.get("demo.gateway")
        assert state is None or (state.pending is None and state.active_version is None)
    finally:
        await manager.close()


def _installed_package(tmp_path: Path, package_id: str, domain: str | None) -> Path:
    root = tmp_path / "extension_pkgs" / package_id / "0.1.0"
    root.mkdir(parents=True)
    package: dict[str, str] = {"id": package_id, "version": "0.1.0"}
    if domain is not None:
        package["domain"] = domain
    root.joinpath("manifest.json").write_text(json.dumps({
        "package_type": "extension",
        "schemaVersion": 1,
        "package": package,
        "extensions": [{"id": "demo", "runtime": "backend", "module": "extension.py"}],
    }), encoding="utf-8")
    root.joinpath("extension.py").write_text(ENTRY, encoding="utf-8")
    return root


async def test_reported_startup_rejects_packages_outside_the_host_domains(tmp_path):
    _installed_package(tmp_path, "demo.gateway", "gateway")
    _installed_package(tmp_path, "demo.agent", "agentserver")
    _installed_package(tmp_path, "demo.nodomain", None)
    _installed_package(tmp_path, "demo.other", "other")

    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        reports = await manager.load_installed()

        assert [(report.package_id, report.status) for report in reports] == [("demo.gateway", "SUCCESS")]
        report = manager._report
        data = report._read()
        assert sorted(data["startup_order"]) == ["demo.agent", "demo.gateway"]
        assert {"demo.nodomain", "demo.other"}.isdisjoint(data["packages"])
        assert {package.package_id for package in manager.installed()} == {
            "demo.gateway", "demo.agent", "demo.nodomain", "demo.other",
        }
        assert [package.package_id for package in runtime.active_packages()] == ["demo.gateway"]
    finally:
        await manager.close()


async def test_reported_startup_order_is_shared_across_both_domains(tmp_path):
    _installed_package(tmp_path, "demo.gateway", "gateway")
    _installed_package(tmp_path, "demo.agent", "agentserver")
    manager = await _reported_manager(tmp_path)
    try:
        await manager.load_installed()
    finally:
        await manager.close()

    from openjiuwen.core.extension.install_report import PackageInstallReport
    from openjiuwen.core.extension.package_manager import PackageManager

    agent_report = PackageInstallReport(_report_path(tmp_path), poll_interval=0.05, dependency_timeout=5.0, lock_timeout=2.0)
    agent_store = PackageManager(
        tmp_path / "extension_pkgs", domain="agentserver", allowed_domains=("gateway", "agentserver"),
    )
    agent_store.install_report = agent_report
    await agent_report.open(agent_store.store, domain="agentserver")
    try:
        plan = await agent_report.prepare_startup(agent_store)

        assert [(manifest.package_id, manifest.domain) for manifest in plan.ordered] == [
            ("demo.agent", "agentserver"),
            ("demo.gateway", "gateway"),
        ]
        assert plan.rejected == ()
    finally:
        await agent_report.close()


async def test_gateway_assembly_survives_an_invalid_configuration(tmp_path, monkeypatch):
    monkeypatch.setattr("agent_gateway.common.paths.get_user_workspace_dir", lambda: tmp_path)
    monkeypatch.setenv("GATEWAY_PORT", "19002")
    monkeypatch.setattr(assembly, "_ASSEMBLY", None)
    conflicting = {
        "extensions": {
            "install_dir": str(tmp_path / "shared"),
            "gateway": {"install_dir": str(tmp_path / "legacy")},
        }
    }

    result = await assembly.install_gateway_extensions(Channel(), conflicting)

    assert result is None
    assert assembly._ASSEMBLY is None

    from agent_gateway.extensions_v2.splash_hint import installed_splash_providers

    def broken_config(*args, **kwargs):
        raise ValueError("bad config")

    monkeypatch.setattr("agent_gateway.extensions_v2.splash_hint.load_gateway_config", broken_config)
    assert installed_splash_providers() == []


@pytest.mark.asyncio
async def test_managed_member_root_is_visible_after_the_startup_snapshot(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    try:
        await manager.load_installed()
        root = tmp_path / "extension_pkgs" / "demo.bundle" / "2.0.0" / "plugins" / "demo.gateway"
        root.mkdir(parents=True)
        document = {
            "package_type": "extension",
            "schemaVersion": 1,
            "package": {"id": "demo.gateway", "version": "0.1.0", "domain": "gateway"},
            "extensions": [{"id": "demo", "runtime": "backend", "module": "extension.py"}],
        }
        (root / "manifest.json").write_text(json.dumps(document), encoding="utf-8")
        (root / "extension.py").write_text(ENTRY, encoding="utf-8")

        registered = manager.register_managed_root("demo.gateway", "0.1.0", root)

        assert registered == root.resolve()
        assert [item.root for item in manager.gateway_installed("demo.gateway")] == [root.resolve()]
        manager.unregister_managed_root("demo.gateway", "0.1.0")
        assert manager.gateway_installed("demo.gateway") == []
    finally:
        await manager.close()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_sync_loads_a_bundle_member_after_the_startup_snapshot(tmp_path):
    from openjiuwen.core.events import HostPackageInstallRequest

    from agent_gateway.extensions_v2.management import containers
    from agent_gateway.extensions_v2.management.install_state import (
        ActiveInstance,
        BundleMemberRecord,
        ExtensionBundleMember,
        ExtensionBundleRecord,
        ExtensionInstallState,
    )

    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    try:
        await manager.load_installed()
        root = install_dir / "demo.bundle" / "2.0.0" / "plugins" / "demo.gateway"
        root.mkdir(parents=True)
        document = {
            "package_type": "extension",
            "schemaVersion": 1,
            "package": {"id": "demo.gateway", "version": "0.1.0", "domain": "gateway"},
            "extensions": [{"id": "demo", "runtime": "backend", "module": "extension.py"}],
        }
        (root / "manifest.json").write_text(json.dumps(document), encoding="utf-8")
        (root / "extension.py").write_text(ENTRY, encoding="utf-8")
        store.upsert_bundle(
            ExtensionBundleRecord(
                bundle_id="demo.bundle",
                version="2.0.0",
                origin="user",
                members=(ExtensionBundleMember("demo.gateway", "0.1.0", "gateway"),),
                member_records=(
                    BundleMemberRecord(
                        package_id="demo.gateway",
                        version="0.1.0",
                        domain="gateway",
                        root=str(root),
                        enabled=True,
                        active=True,
                        status="pending",
                    ),
                ),
                status="partial",
                active=True,
            )
        )
        store.switch_active_instance(
            ActiveInstance(
                package_id="demo.gateway",
                version="0.1.0",
                kind="bundle",
                bundle_id="demo.bundle",
                bundle_version="2.0.0",
                root=str(root),
            )
        )

        async def trigger(operation, package_id, version, container_id, container_version):
            report = await manager.enable_version(package_id, version, skip_dependencies=True)
            request = HostPackageInstallRequest(
                package_id=package_id, version=version, operation=operation,
                container_id=container_id, container_version=container_version,
            )
            return containers.record_install_outcome(
                store, request, event_id="", success=report.status == "SUCCESS",
                error="" if report.status == "SUCCESS" else str(report.error),
            )

        await containers.sync_members(manager, store, trigger)

        member = store.get_bundle_member("demo.bundle", "2.0.0", "demo.gateway")
        assert member is not None and member.status == "active" and member.active is True
        assert runtime.user_handler_count == 1
    finally:
        await manager.close()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_sync_loads_a_runtime_standalone_after_the_startup_snapshot(tmp_path):
    from openjiuwen.core.events import HostPackageInstallRequest

    from agent_gateway.extensions_v2.management import containers
    from agent_gateway.extensions_v2.management.install_state import (
        ActiveInstance,
        ExtensionInstallRecord,
        ExtensionInstallState,
    )

    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    try:
        await manager.load_installed()
        root = install_dir / "demo.gateway" / "0.1.0"
        root.mkdir(parents=True)
        document = {
            "package_type": "extension",
            "schemaVersion": 1,
            "package": {"id": "demo.gateway", "version": "0.1.0", "domain": "gateway"},
            "extensions": [{"id": "demo", "runtime": "backend", "module": "extension.py"}],
        }
        (root / "manifest.json").write_text(json.dumps(document), encoding="utf-8")
        (root / "extension.py").write_text(ENTRY, encoding="utf-8")
        store.upsert(
            ExtensionInstallRecord(
                package_id="demo.gateway",
                version="0.1.0",
                origin="hub",
                install_path=str(root),
                active=True,
                enabled=True,
                domain="gateway",
                status="pending",
            )
        )
        store.switch_active_instance(
            ActiveInstance(package_id="demo.gateway", version="0.1.0", kind="standalone")
        )

        async def trigger(operation, package_id, version, container_id, container_version):
            report = await manager.enable_version(package_id, version, skip_dependencies=True)
            request = HostPackageInstallRequest(
                package_id=package_id, version=version, operation=operation,
                container_id=container_id, container_version=container_version,
            )
            return containers.record_install_outcome(
                store, request, event_id="", success=report.status == "SUCCESS",
                error="" if report.status == "SUCCESS" else str(report.error),
            )

        await containers.sync_members(manager, store, trigger)

        record = store.get("demo.gateway", "0.1.0")
        assert record is not None and record.status == "active" and record.active is True
        assert runtime.user_handler_count == 1
    finally:
        await manager.close()
        await runtime.shutdown()


def _write_gateway_copy(root: Path) -> None:
    root.mkdir(parents=True)
    document = {
        "package_type": "extension",
        "schemaVersion": 1,
        "package": {"id": "demo.gateway", "version": "0.1.0", "domain": "gateway"},
        "extensions": [{"id": "demo", "runtime": "backend", "module": "extension.py"}],
    }
    (root / "manifest.json").write_text(json.dumps(document), encoding="utf-8")
    (root / "extension.py").write_text(ENTRY, encoding="utf-8")


@pytest.mark.asyncio
async def test_root_resolution_prefers_the_registered_member(tmp_path):
    from types import SimpleNamespace

    from agent_gateway.extensions_v2.management.registry import GatewayPackageManager

    install_dir = tmp_path / "extension_pkgs"
    standard = install_dir / "demo.gateway" / "0.1.0"
    member = install_dir / "demo.bundle" / "1.0.0" / "plugins" / "demo.gateway"
    _write_gateway_copy(standard)
    _write_gateway_copy(member)
    manager = GatewayPackageManager(
        SimpleNamespace(active_record=lambda package_id: None),
        install_dir,
        recovery_data_dir=tmp_path / "extension",
    )
    manager.register_managed_root("demo.gateway", "0.1.0", member)
    manager._startup_loaded = True
    manager._accepted_roots.add(standard.resolve())

    assert manager._find_root("demo.gateway", "0.1.0") == member.resolve()
    manager._prefer_managed_roots()
    assert [item.root for item in manager.gateway_installed("demo.gateway")] == [member.resolve()]

    manager._accepted_roots.add(standard.resolve())
    report = await manager.remove_standalone_copy("demo.gateway", "0.1.0")

    assert report.status == "SUCCESS"
    assert not standard.exists()
    assert member.is_dir()
    assert [item.root for item in manager.gateway_installed("demo.gateway")] == [member.resolve()]


@pytest.mark.asyncio
async def test_accept_version_replaces_a_same_version_member_root(tmp_path):
    from types import SimpleNamespace

    from agent_gateway.extensions_v2.management.registry import GatewayPackageManager

    install_dir = tmp_path / "extension_pkgs"
    standard = install_dir / "demo.gateway" / "0.1.0"
    member = install_dir / "demo.bundle" / "1.0.0" / "plugins" / "demo.gateway"
    _write_gateway_copy(standard)
    _write_gateway_copy(member)
    manager = GatewayPackageManager(
        SimpleNamespace(active_record=lambda package_id: None),
        install_dir,
        recovery_data_dir=tmp_path / "extension",
    )
    manager.register_managed_root("demo.gateway", "0.1.0", member)

    manager.accept_version("demo.gateway", "0.1.0")

    assert manager._store.is_managed_root(member.resolve()) is False
    assert manager._find_root("demo.gateway", "0.1.0") == standard.resolve()


@pytest.mark.asyncio
async def test_reported_remove_standalone_copy_deletes_only_the_standard_copy(tmp_path):
    runtime = await _runtime(tmp_path)
    manager = await _reported_manager(tmp_path, runtime)
    install_dir = tmp_path / "extension_pkgs"
    try:
        assert (await manager.install([_package_zip(tmp_path)]))[0].status == "SUCCESS"
        assert (await manager.disable_version("demo.gateway", "0.1.0")).status == "SUCCESS"
        standard = install_dir / "demo.gateway" / "0.1.0"
        member = install_dir / "demo.bundle" / "1.0.0" / "plugins" / "demo.gateway"
        _write_gateway_copy(member)
        manager.register_managed_root("demo.gateway", "0.1.0", member)

        report = await manager.remove_standalone_copy("demo.gateway", "0.1.0")

        assert report.status == "SUCCESS"
        assert not standard.exists()
        assert member.is_dir()
    finally:
        await manager.close()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_remove_bundle_version_deletes_only_that_copy(tmp_path):
    from types import SimpleNamespace

    from agent_gateway.extensions_v2.management.registry import GatewayPackageManager

    install_dir = tmp_path / "extension_pkgs"
    first = install_dir / "demo.bundle" / "1.0.0"
    second = install_dir / "demo.bundle" / "1.0.1"
    _write_gateway_copy(first)
    _write_gateway_copy(second)
    manager = GatewayPackageManager(
        SimpleNamespace(active_record=lambda package_id: None),
        install_dir,
        recovery_data_dir=tmp_path / "extension",
    )

    error = await manager.remove_bundle_version("demo.bundle", "1.0.0")

    assert error == ""
    assert not first.exists()
    assert second.is_dir()
