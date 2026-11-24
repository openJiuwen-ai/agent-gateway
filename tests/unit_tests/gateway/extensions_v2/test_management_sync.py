# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Management responses must reconcile this host even when they fail."""

from __future__ import annotations

import pytest

from agent_gateway.channel_manager.web import extension_package_handlers as handlers


@pytest.mark.asyncio
@pytest.mark.parametrize("ok", [True, False])
async def test_management_sync_runs_after_failed_responses(monkeypatch, ok: bool) -> None:
    calls: list[int] = []

    async def fake_sync() -> None:
        calls.append(1)

    monkeypatch.setattr(handlers, "_sync_gateway_members", fake_sync)

    await handlers._sync_after_management(ok, {})

    assert calls == [1]


@pytest.mark.asyncio
async def test_container_forward_requires_all_gateway_members(monkeypatch) -> None:
    from e2a.message import ReqMethod

    captured: dict = {}

    async def fake_proxy(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(handlers, "proxy_unary_request", fake_proxy)

    await handlers._forward_with_member_sync(
        channel=object(),
        resolve_client=lambda: object(),
        ws=object(),
        req_id="req-1",
        params={"paths": ["/tmp/demo.zip"]},
        session_id=None,
        user_id=None,
        method=ReqMethod.EXTENSIONS_V2_INSTALL,
    )

    assert captured["metadata"] == {"extensions_v2_require_all_members": True}


def test_bundle_projection_forces_disabled_for_an_inactive_record(tmp_path) -> None:
    from agent_gateway.extensions_v2.management.install_state import (
        BundleMemberRecord,
        ExtensionBundleMember,
        ExtensionBundleRecord,
        ExtensionInstallState,
    )

    store = ExtensionInstallState.for_install_dir(tmp_path / "extension_pkgs")
    store.upsert_bundle(
        ExtensionBundleRecord(
            bundle_id="demo.bundle",
            version="1.0.0",
            origin="user",
            members=(ExtensionBundleMember("demo.gw", "1.0.0", "gateway"),),
            member_records=(
                BundleMemberRecord(
                    package_id="demo.gw", version="1.0.0", domain="gateway",
                    enabled=True, active=True, status="active",
                ),
            ),
            status="partial",
            active=False,
        )
    )

    projection = handlers._project_bundle(
        store, {"bundleId": "demo.bundle", "version": "1.0.0", "overallStatus": "enabled"},
    )

    assert projection["overallStatus"] == "disabled"


def test_management_payload_reflects_a_failed_standalone_load(tmp_path, monkeypatch) -> None:
    from types import SimpleNamespace

    from agent_gateway.extensions_v2 import assembly
    from agent_gateway.extensions_v2.management.install_state import (
        ExtensionInstallRecord,
        ExtensionInstallState,
    )

    install_dir = tmp_path / "extension_pkgs"
    store = ExtensionInstallState.for_install_dir(install_dir)
    store.upsert(
        ExtensionInstallRecord(
            package_id="demo.gw", version="1.0.0", origin="hub",
            active=False, enabled=False, domain="gateway", status="failed", error="load failed",
        )
    )
    monkeypatch.setattr(
        assembly,
        "get_gateway_runtime",
        lambda: SimpleNamespace(config=SimpleNamespace(install_dir=install_dir)),
    )

    refreshed = handlers._refresh_management_payload(
        {"reports": [{"package_id": "demo.gw", "status": "SUCCESS", "error": None}]},
    )

    assert refreshed["reports"][0]["status"] == "FAILED"
    assert refreshed["reports"][0]["error"]["message"] == "load failed"
