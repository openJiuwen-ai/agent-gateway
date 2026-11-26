# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Keep the Gateway runtime in step with shared package and bundle instances.

Only the instance that owns the package id's active claim is loaded; recovery
promotes exactly one unclaimed same-version instance. Bundle members are loaded
in place through a managed root; standby copies stay on disk. The AgentServer
owns installation and container status; this module only reconciles Gateway
instances with the shared records and finalizes their removals.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from openjiuwen.core.events import PackageInstallOutcome
from openjiuwen.core.extension.manifest import load_manifest
from openjiuwen.core.extension.versions import specifier_matches

from agent_gateway.extensions_v2.management.install_state import (
    ActiveInstance,
    BundleMemberRecord,
    ExtensionBundleRecord,
    ExtensionInstallRecord,
    ExtensionInstallState,
)
from agent_gateway.extensions_v2.manifest import DOMAIN_GATEWAY

logger = logging.getLogger(__name__)

Trigger = Callable[[str, str, str, str, str], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class _Instance:
    package_id: str
    version: str
    domain: str
    kind: str
    root: Path
    bundle_id: str = ""
    bundle_version: str = ""
    enabled: bool = False
    active: bool = False
    status: str = "disabled"


def register_active_managed_roots(manager: Any) -> None:
    """Register claimed bundle member roots before Core startup discovery."""

    store = ExtensionInstallState.for_install_dir(manager.install_dir)
    register = getattr(manager, "register_managed_root", None)
    if register is None:
        return
    reconcile = getattr(store, "reconcile_claims", None)
    if reconcile is not None:
        reconcile()
    for bundle in store.bundles():
        if not bundle.active:
            continue
        for member in bundle.member_records:
            if member.domain != DOMAIN_GATEWAY or not member.active:
                continue
            claim = store.active_instance(member.package_id)
            if (
                claim is None
                or claim.kind != "bundle"
                or claim.bundle_id != bundle.bundle_id
                or claim.bundle_version != bundle.version
                or claim.version != member.version
            ):
                continue
            root = Path(member.root) if member.root else (
                Path(manager.install_dir) / bundle.bundle_id / bundle.version
                / "plugins" / member.package_id
            )
            if root.is_dir():
                register(member.package_id, member.version, root)


async def sync_members(manager: Any, store: ExtensionInstallState, trigger: Trigger) -> list[Any]:
    """Load the claimed Gateway instance and release everything else."""

    await _finalize_uninstalls(manager, store)
    await _finalize_bundle_uninstalls(manager, store, Path(manager.install_dir))
    results: list[Any] = []
    active = {record.package_id: record for record in manager.active_records()}
    instances = _instances(store, Path(manager.install_dir))
    by_package: dict[str, list[_Instance]] = {}
    for instance in instances:
        by_package.setdefault(instance.package_id, []).append(instance)
    package_ids = set(by_package) | set(active)
    for package_id in sorted(package_ids):
        loaded = active.get(package_id)
        claim = store.active_instance(package_id)
        if claim is None:
            claim = _recover_standby_claim(store, by_package.get(package_id, ()))
        target = _instance_for_claim(instances, claim) if claim is not None else None
        if target is not None and target.kind == "bundle":
            bundle = store.get_bundle(target.bundle_id, target.bundle_version)
            # A pending container is the version the AgentServer is installing
            # right now; its member claim is the request to load. Only an
            # inactive container that is no longer pending must not be
            # resurrected from a stale claim.
            if bundle is None or not (bundle.active or bundle.status == "pending"):
                target = None
        if target is None or target.domain != DOMAIN_GATEWAY:
            if loaded is not None:
                await _release_runtime(manager, store, package_id, loaded.version)
            for instance in by_package.get(package_id, ()):
                if instance.kind == "bundle":
                    await asyncio.to_thread(
                        store.mark_bundle_member, instance.bundle_id, instance.bundle_version,
                        package_id, active=False,
                        status="standby" if instance.enabled else "disabled",
                    )
                elif instance.active:
                    await asyncio.to_thread(
                        store.mark, package_id, version=instance.version,
                        active=False, status="standby" if instance.enabled else "disabled",
                    )
            continue

        if not _enabled_on_disk(target) and target.status not in {"standby", "pending"}:
            # Restore/disable writes manifest.enable; never load such a copy
            # from the sync path. Standby/pending copies are seeded unloaded
            # but remain loadable when promoted.
            if loaded is not None:
                await _release_runtime(manager, store, package_id, loaded.version)
            await _mark_instance_disabled(store, target)
            continue

        state = _dependency_state(store, target.root)
        if state == "waiting":
            continue
        if state == "failed":
            await asyncio.to_thread(
                record_install_outcome,
                store,
                _Request(package_id, target.version, target.bundle_id, target.bundle_version),
                event_id="", success=False, error="dependency not met",
            )
            continue

        if loaded is not None and loaded.version == target.version:
            loaded_root = getattr(loaded, "root", None)
            same_root = loaded_root is None or Path(loaded_root).resolve() == target.root.resolve()
            if same_root:
                member = (
                    store.get_bundle_member(target.bundle_id, target.bundle_version, package_id)
                    if target.kind == "bundle"
                    else store.get(package_id, target.version)
                )
                if member is not None and member.status == "pending":
                    outcome = await trigger(
                        "reload", package_id, target.version, target.bundle_id, target.bundle_version,
                    )
                    results.append(outcome)
                continue
            await _release_runtime(manager, store, package_id, target.version)

        if target.kind == "bundle":
            register = getattr(manager, "register_managed_root", None)
            if register is not None and target.root.is_dir():
                await asyncio.to_thread(
                    register, package_id, target.version, target.root,
                )
        else:
            unregister = getattr(manager, "unregister_managed_root", None)
            if unregister is not None:
                await asyncio.to_thread(unregister, package_id, target.version)
            accept = getattr(manager, "accept_version", None)
            if accept is not None:
                await asyncio.to_thread(accept, package_id, target.version)

        outcome = await trigger(
            "install", package_id, target.version, target.bundle_id, target.bundle_version,
        )
        results.append(outcome)
    for bundle in store.bundles():
        if not any(member.domain == DOMAIN_GATEWAY for member in bundle.members):
            # The AgentServer owns the container status of bundles that carry
            # no Gateway member; a second writer here could revert it.
            continue
        await asyncio.to_thread(refresh_container_status, store, bundle.bundle_id, bundle.version)
    return results


async def _finalize_bundle_uninstalls(
    manager: Any,
    store: ExtensionInstallState,
    install_dir: Path,
) -> None:
    """Release this host's members, then delete bundles the AgentServer tombstoned.

    The AgentServer cannot unload this process, so it leaves the bundle record
    and copy as ``uninstalling``. This host releases every gateway member loaded
    from that bundle copy before deleting the directory and the records.
    """

    for bundle in store.bundles():
        if bundle.status != "uninstalling":
            continue
        error = ""
        for member in bundle.member_records:
            if member.domain != DOMAIN_GATEWAY:
                continue
            member_root = Path(member.root) if member.root else (
                install_dir / bundle.bundle_id / bundle.version
                / "plugins" / member.package_id
            )
            active = next(
                (
                    item
                    for item in manager.active_records()
                    if item.package_id == member.package_id
                    and item.version == member.version
                    and Path(item.root).resolve() == member_root.resolve()
                ),
                None,
            )
            if active is None:
                continue
            register = getattr(manager, "register_managed_root", None)
            try:
                if register is not None:
                    await asyncio.to_thread(
                        register, member.package_id, member.version, member_root,
                    )
                report = await manager.disable_version(member.package_id, member.version)
            except Exception as exc:  # noqa: BLE001 - keep the tombstone for a retry
                logger.warning(
                    "[GatewayExtensions] bundle member %s could not be released",
                    member.package_id,
                    exc_info=True,
                )
                error = str(exc)
                break
            if getattr(report, "status", "") != "SUCCESS":
                error = (
                    getattr(getattr(report, "error", None), "message", None)
                    or "member release failed"
                )
                break
            unregister = getattr(manager, "unregister_managed_root", None)
            if unregister is not None:
                await asyncio.to_thread(
                    unregister, member.package_id, member.version, root=member_root,
                )
        if error:
            await asyncio.to_thread(store.upsert_bundle, replace(bundle, error=error))
            continue
        remove_version = getattr(manager, "remove_bundle_version", None)
        error = await remove_version(bundle.bundle_id, bundle.version) if remove_version else ""
        if error:
            await asyncio.to_thread(store.upsert_bundle, replace(bundle, error=error))
            continue
        await asyncio.to_thread(store.remove_bundle_members, bundle.bundle_id, bundle.version)
        await asyncio.to_thread(store.remove_bundle, bundle.bundle_id, bundle.version)


async def _finalize_uninstalls(manager: Any, store: ExtensionInstallState) -> None:
    """Delete Gateway copies the AgentServer marked for uninstall.

    The AgentServer owns the shared store but cannot unload this process, so it
    marks the record ``uninstalling``; the Gateway unloads and deletes through
    its own path, which still honours in-flight call protection. A copy whose
    id is currently loaded from another root (a bundle member, or another
    version) is removed file-only; the running instance keeps its runtime.
    """

    for record in store.packages():
        if record.domain != DOMAIN_GATEWAY or record.status != "uninstalling":
            continue
        standard = Path(manager.install_dir) / record.package_id / record.version
        active = next(
            (item for item in manager.active_records() if item.package_id == record.package_id),
            None,
        )
        loaded_standard = (
            active is not None
            and active.version == record.version
            and Path(active.root).resolve() == standard.resolve()
        )
        try:
            if loaded_standard:
                # Resolve this delete to the standard copy even if a stale
                # member registration still shadows it.
                unregister = getattr(manager, "unregister_managed_root", None)
                if unregister is not None:
                    await asyncio.to_thread(
                        unregister, record.package_id, record.version,
                    )
                report = await manager.uninstall(record.package_id, record.version)
            else:
                remove_copy = getattr(manager, "remove_standalone_copy", None)
                report = (
                    await remove_copy(record.package_id, record.version)
                    if remove_copy is not None
                    else await manager.uninstall(record.package_id, record.version)
                )
        except Exception as exc:  # noqa: BLE001 - keep the record for a retry
            logger.warning(
                "[GatewayExtensions] %s could not be uninstalled",
                record.package_id,
                exc_info=True,
            )
            await asyncio.to_thread(
                store.mark, record.package_id, version=record.version,
                active=False, enabled=False, status="uninstalling", error=str(exc),
            )
            continue
        if getattr(report, "status", "") == "SUCCESS":
            await asyncio.to_thread(store.remove, record.package_id, record.version)
            continue
        error = getattr(getattr(report, "error", None), "message", None) or "uninstall failed"
        # Stay retryable: a transient lock or busy handler must not turn the
        # shared record into a permanent failure the UI cannot recover from.
        await asyncio.to_thread(
            store.mark, record.package_id, version=record.version,
            active=False, enabled=False, status="uninstalling", error=str(error),
        )


def _enabled_on_disk(target: _Instance) -> bool:
    """Return the manifest loading flag; unreadable or mismatched copies stay loadable."""
    try:
        manifest = load_manifest(target.root)
    except Exception:  # noqa: BLE001 - the normal load path reports the real error
        return True
    if manifest.package_id != target.package_id or manifest.version != target.version:
        return True
    return manifest.enable


async def _mark_instance_disabled(store: ExtensionInstallState, target: _Instance) -> None:
    """Persist the store side of a disabled copy so the UI and next sync agree."""
    if target.kind == "bundle":
        await asyncio.to_thread(
            store.mark_bundle_member, target.bundle_id, target.bundle_version,
            target.package_id, enabled=False, active=False, status="disabled", error="",
        )
        return
    await asyncio.to_thread(
        store.mark, target.package_id, version=target.version,
        enabled=False, active=False, status="disabled", error="",
    )


def _instances(store: ExtensionInstallState, install_dir: Path) -> list[_Instance]:
    result: list[_Instance] = []
    for record in store.packages():
        if record.domain != DOMAIN_GATEWAY:
            # A standalone package owned by the other host never enters this runtime.
            continue
        root = Path(record.install_path) if record.install_path else install_dir / record.package_id / record.version
        result.append(
            _Instance(
                package_id=record.package_id,
                version=record.version,
                domain=record.domain,
                kind="standalone",
                root=root,
                enabled=record.enabled,
                active=record.active,
                status=record.status,
            )
        )
    for bundle in store.bundles():
        for member in bundle.member_records:
            if member.domain != DOMAIN_GATEWAY:
                continue
            root = Path(member.root) if member.root else (
                install_dir / bundle.bundle_id / bundle.version
                / "plugins" / member.package_id
            )
            result.append(
                _Instance(
                    package_id=member.package_id,
                    version=member.version,
                    domain=member.domain,
                    kind="bundle",
                    root=root,
                    bundle_id=bundle.bundle_id,
                    bundle_version=bundle.version,
                    enabled=member.enabled,
                    active=member.active,
                    status=member.status,
                )
            )
    return result


def _instance_for_claim(instances: list[_Instance], claim: ActiveInstance) -> _Instance | None:
    for instance in instances:
        if instance.package_id != claim.package_id or instance.version != claim.version:
            continue
        if instance.kind != claim.kind:
            continue
        if claim.kind == "bundle" and (
            instance.bundle_id != claim.bundle_id
            or instance.bundle_version != claim.bundle_version
        ):
            continue
        return instance
    return None


def _recover_standby_claim(
    store: ExtensionInstallState,
    instances: Sequence[_Instance],
) -> ActiveInstance | None:
    """Recover one orphaned same-version standby instance when no claim exists.

    A crash after the AgentServer wrote an enabled instance but before the
    claim survived leaves it on standby with no owner. Recovery accepts a
    bundle member of an active bundle or a standalone copy, and only acts on
    exactly one candidate so an explicit choice is never guessed.
    """

    candidates: list[_Instance] = []
    for instance in instances:
        if not instance.enabled or instance.active or instance.status not in {"pending", "standby"}:
            continue
        if instance.kind == "bundle":
            bundle = store.get_bundle(instance.bundle_id, instance.bundle_version)
            if bundle is None or not bundle.active:
                continue
        candidates.append(instance)
    if len(candidates) != 1:
        return None
    instance = candidates[0]
    if instance.kind == "bundle":
        return ActiveInstance(
            package_id=instance.package_id,
            version=instance.version,
            kind="bundle",
            bundle_id=instance.bundle_id,
            bundle_version=instance.bundle_version,
            root=str(instance.root),
        )
    record = store.get(instance.package_id, instance.version)
    if record is None or record.status not in {"pending", "standby"}:
        return None
    return ActiveInstance(
        package_id=instance.package_id,
        version=instance.version,
        kind="standalone",
        root=str(instance.root),
    )


class _Request:
    """Minimal request shape accepted by ``record_install_outcome``."""

    def __init__(self, package_id: str, version: str, container_id: str = "", container_version: str = "") -> None:
        self.package_id = package_id
        self.version = version
        self.container_id = container_id
        self.container_version = container_version
        self.operation = "install"


def record_install_outcome(
    store: ExtensionInstallState,
    request: Any,
    *,
    event_id: str,
    success: bool,
    error: str = "",
) -> Any:
    """Persist one instance outcome; the event handler calls this."""

    package_id = request.package_id
    version = request.version
    bundle_id = getattr(request, "container_id", "") or ""
    bundle_version = getattr(request, "container_version", "") or ""
    if bundle_id:
        previous = store.get_bundle_member(bundle_id, bundle_version, package_id)
        store.mark_bundle_member(
            bundle_id, bundle_version, package_id,
            enabled=success,
            active=success,
            status="active" if success else "failed",
            error="" if success else error,
        )
        if success:
            store.switch_active_instance(
                ActiveInstance(
                    package_id=package_id,
                    version=version,
                    kind="bundle",
                    bundle_id=bundle_id,
                    bundle_version=bundle_version,
                    root=previous.root if previous is not None else "",
                )
            )
    else:
        previous = store.get(package_id, version)
        record = ExtensionInstallRecord(
            package_id=package_id,
            version=version,
            origin=previous.origin if previous is not None else "user",
            asset_id=previous.asset_id if previous is not None else "",
            install_path=previous.install_path if previous is not None else "",
            active=success,
            enabled=success,
            domain=DOMAIN_GATEWAY,
            status="active" if success else "failed",
            error="" if success else error,
            digest=previous.digest if previous is not None else "",
        )
        store.upsert(record)
        if success:
            store.switch_active_instance(
                ActiveInstance(package_id=package_id, version=version, kind="standalone")
            )
    return PackageInstallOutcome(
        event_id=event_id,
        operation=getattr(request, "operation", "install"),
        package_id=package_id,
        version=version,
        status="success" if success else "failed",
        active=success,
        enabled=True,
        error=error,
        container_id=bundle_id,
        container_version=bundle_version,
    )


def adjust_management_reports(
    store: ExtensionInstallState,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Make Gateway-domain reports match the state this host reached.

    The AgentServer acknowledges Gateway-domain actions before this host has
    loaded or removed the copy. The reply is sent after a sync, so the shared
    record is the final result and can correct an optimistic SUCCESS. Bundle
    members are left alone: the AgentServer already waits for their results.
    """

    reports = payload.get("reports")
    if not isinstance(reports, list):
        return payload
    failed_ids: set[str] = set()
    adjusted: list[Any] = []
    for report in reports:
        if not isinstance(report, dict) or report.get("status") != "SUCCESS":
            adjusted.append(report)
            continue
        package_id = str(report.get("package_id") or "")
        error = _report_mismatch(store, package_id)
        if not error:
            adjusted.append(report)
            continue
        failed = dict(report)
        failed["status"] = "FAILED"
        failed["error"] = {"code": "EXTENSION_LOAD_FAILED", "message": error}
        adjusted.append(failed)
        failed_ids.add(package_id)
    if not failed_ids:
        return payload
    result = dict(payload)
    result["reports"] = adjusted
    installed = result.get("installed_packages")
    if isinstance(installed, list):
        result["installed_packages"] = [
            item
            for item in installed
            if not (isinstance(item, dict) and item.get("package_id") in failed_ids)
        ]
    return result


def _report_mismatch(store: ExtensionInstallState, package_id: str) -> str:
    if not package_id:
        return ""
    bundles = [bundle for bundle in store.bundles() if bundle.bundle_id == package_id]
    if len(bundles) == 1:
        bundle = bundles[0]
        if bundle.status == "uninstalling" and bundle.error:
            return bundle.error
        return ""
    if bundles:
        # Multi-version bundle: the reply carries no version, so do not guess
        # which tombstone produced a failure.
        return ""
    record = store.get(package_id)
    if (
        record is None
        or record.domain != DOMAIN_GATEWAY
        or store.bundle_of(package_id) is not None
    ):
        return ""
    if record.status == "failed":
        return record.error or "package did not load"
    if record.status == "pending":
        return "package did not finish loading"
    if record.status == "uninstalling":
        return record.error or "package uninstall did not finish"
    return ""

def frontend_status(store: ExtensionInstallState, record: ExtensionBundleRecord | None) -> str:
    """Container state in the frontend vocabulary (enabled/partial/disabled/failed)."""

    if record is None or not record.members:
        return "not-installed"
    records = {member.package_id: member for member in record.member_records}
    total = len(record.members)
    active = installed = 0
    pending = standby = failed = 0
    for member in record.members:
        member_record = records.get(member.package_id)
        if member_record is None:
            pending += 1
            continue
        installed += 1
        if member_record.status == "failed":
            failed += 1
        elif member_record.status == "pending":
            pending += 1
        elif member_record.status == "standby":
            standby += 1
        if member_record.active and member_record.enabled and member_record.status == "active":
            active += 1
    if active == total:
        return "enabled"
    if failed and active == 0:
        return "failed"
    if pending or standby or failed:
        return "partial"
    if installed == total:
        return "disabled"
    return "partial" if installed else "failed"


def refresh_container_status(store: ExtensionInstallState, bundle_id: str, version: str) -> None:
    """Recompute and persist one container status from member records."""

    record = store.get_bundle(bundle_id, version)
    if record is None or not record.active:
        return
    status = frontend_status(store, record)
    mapped = "active" if status == "enabled" else status
    if mapped != record.status:
        store.upsert_bundle(replace(record, status=mapped))


def _requirements(root: Path) -> tuple[tuple[str, str], ...]:
    try:
        manifest = load_manifest(root, require_domain=False)
    except Exception:  # noqa: BLE001 - unreadable member is treated as having no requirements
        return ()
    return tuple(
        (dependency.package_id, dependency.version)
        for dependency in manifest.requires_packages
    )


def _dependency_state(store: ExtensionInstallState, root: Path) -> str:
    """Return ready / waiting / failed for one instance's dependencies."""

    for package_id, spec in _requirements(root):
        claim = store.active_instance(package_id)
        if claim is None:
            failed = any(
                record.package_id == package_id and record.status == "failed"
                for record in store.packages()
            ) or any(
                member.package_id == package_id and member.status == "failed"
                for member in store.bundle_members()
            )
            if failed:
                return "failed"
            return "waiting"
        record = (
            store.get(package_id, claim.version)
            if claim.kind == "standalone"
            else store.get_bundle_member(claim.bundle_id, claim.bundle_version, package_id)
        )
        status = record.status if record is not None else ""
        if status == "failed":
            return "failed"
        if record is None or not record.active or status != "active":
            return "waiting"
        try:
            if not specifier_matches(claim.version, spec):
                return "failed"
        except ValueError:
            return "failed"
    return "ready"


async def _release_runtime(
    manager: Any,
    store: ExtensionInstallState,
    package_id: str,
    version: str,
) -> None:
    """Unload one runtime version while keeping package and bundle files."""

    if version:
        try:
            await manager.disable_version(package_id, version)
        except Exception:  # noqa: BLE001 - the record still describes the desired state
            logger.warning("[GatewayExtensions] %s could not be released", package_id, exc_info=True)
    unregister = getattr(manager, "unregister_managed_root", None)
    if unregister is not None and version:
        await asyncio.to_thread(unregister, package_id, version)
