# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from dataclasses import replace

from agent_gateway.extensions_v2.management.install_state import (
    ActiveInstance,
    BundleMemberRecord,
    ExtensionBundleRecord,
    ExtensionInstallRecord,
    ExtensionInstallState,
)


def test_active_instance_reads_one_generation_per_lookup(tmp_path, monkeypatch):
    store = ExtensionInstallState.for_install_dir(tmp_path / "packages")
    package = ExtensionInstallRecord("demo", "1", "user", install_path="standalone-root")
    member = BundleMemberRecord("demo", "2", "gateway", root="member-root", active=True)
    bundle = ExtensionBundleRecord("suite", "3", "user", member_records=(member,))
    # A writer switches the claim from the bundle to the standalone copy.
    snapshots = iter((
        ({"demo@1": replace(package, active=False)}, {"suite@3": bundle}),
        ({"demo@1": package}, {"suite@3": replace(bundle, member_records=(replace(member, active=False),))}),
    ))
    monkeypatch.setattr(store, "_load", lambda: next(snapshots))

    assert store.active_instance("demo") == ActiveInstance("demo", "2", "bundle", "suite", "3", "member-root")
    assert store.active_instance("demo") == ActiveInstance("demo", "1", "standalone", root="standalone-root")


def test_active_instance_preserves_tie_order(tmp_path):
    store = ExtensionInstallState.for_install_dir(tmp_path / "packages")
    stamp = "2026-01-01T00:00:00Z"
    member = BundleMemberRecord("demo", "1", "gateway", active=True, enabled=True, updated_at=stamp)
    for version in ("2", "10"):
        store.upsert_bundle(ExtensionBundleRecord("suite", version, "user", member_records=(member,)))
    assert store.active_instance("demo").bundle_version == "10"

    for version in ("10", "2"):
        store.upsert(ExtensionInstallRecord("demo", version, "user", installed_at=stamp))
    claim = store.active_instance("demo")
    assert (claim.kind, claim.version) == ("standalone", "2")
