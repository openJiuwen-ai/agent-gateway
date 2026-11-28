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


class _FakeChannel:
    """Capture locally answered frames and registered handlers."""

    def __init__(self) -> None:
        self.methods: dict = {}
        self.responses: list[dict] = []

    def register_method(self, name: str, handler) -> None:
        self.methods[name] = handler

    async def send_response(self, ws, req_id: str, *, ok: bool, payload=None, error=None, code=None) -> None:
        self.responses.append({"req_id": req_id, "ok": ok, "payload": payload, "error": error, "code": code})


class _FakeManager:
    """Stand-in for GatewayPackageManager exposing only what the query needs."""

    def __init__(self, state: dict, domains: dict[str, str] | None = None) -> None:
        self.state = state
        self.domains = domains or {}

    def query(self, package_id: str | None = None) -> dict:
        if package_id is None:
            return self.state
        return {
            "active": [item for item in self.state["active"] if item["package_id"] == package_id],
            "package_ids": [package_id] if any(item["package_id"] == package_id for item in self.state["active"]) else [],
            "installed": [
                item["install_path"] for item in self.state["installed_packages"]
                if item["package_id"] == package_id
            ],
            "installed_packages": [
                item for item in self.state["installed_packages"] if item["package_id"] == package_id
            ],
            "inflight": {},
            "published": {},
        }

    async def get_package_domain(self, package_id: str) -> str | None:
        return self.domains.get(package_id)


def _query_fixture() -> tuple[_FakeManager, dict]:
    local_state = {
        "active": [
            {
                "package_id": "workswarm.theme", "version": "2.2.3", "root": "/shared/gw/theme",
                "generation": 1, "requires_packages": [], "domain": "gateway",
            },
        ],
        "package_ids": ["workswarm.theme"],
        "installed": ["/shared/gw/theme", "/shared/gw/freeze"],
        "installed_packages": [
            {
                "package_id": "workswarm.theme", "version": "2.2.3",
                "domain": "gateway", "install_path": "/shared/gw/theme",
            },
            {
                "package_id": "manual.freeze", "version": "1.0.0",
                "domain": "gateway", "install_path": "/shared/gw/freeze",
            },
        ],
        "inflight": {},
        "published": {"workswarm.theme": ["theme"]},
    }
    remote_state = {
        "active": [
            {
                "package_id": "token.weather.glass", "version": "1.2.1", "root": "/shared/as/weather",
                "generation": 1, "requires_packages": [], "domain": "agentserver",
            },
        ],
        "installed": ["/shared/as/weather"],
        "installed_packages": [
            {
                "package_id": "token.weather.glass", "version": "1.2.1",
                "domain": "agentserver", "install_path": "/shared/as/weather",
            },
        ],
        "inflight": {},
        "published": {"token.weather.glass": ["weather"]},
        "packages": [
            {
                "package_id": "token.weather.glass", "domain": "agentserver",
                "active_version": "1.2.1", "status": "success",
            },
        ],
        "package_ids": ["token.weather.glass"],
    }
    return _FakeManager(local_state), remote_state


@pytest.mark.asyncio
async def test_query_merges_gateway_state_into_agentserver_response(monkeypatch) -> None:
    from types import SimpleNamespace

    from e2a.message import ReqMethod

    from agent_gateway.extensions_v2 import assembly

    manager, remote_state = _query_fixture()
    captured: dict = {}

    async def fake_proxy(**kwargs):
        captured.update(kwargs)
        return True

    monkeypatch.setattr(handlers, "proxy_unary_request", fake_proxy)
    monkeypatch.setattr(
        assembly,
        "get_gateway_runtime",
        lambda: SimpleNamespace(
            config=SimpleNamespace(enabled=True, dependency_timeout=300.0),
            packages=manager,
        ),
    )

    channel = _FakeChannel()
    handlers.register_extension_package_handlers(channel, lambda: object())
    await channel.methods[ReqMethod.EXTENSIONS_V2_QUERY.value](None, "req-1", {}, None, None)

    assert captured["transform_payload"] is not None
    merged = captured["transform_payload"]({"state": remote_state})["state"]
    rows = {row["package_id"]: row for row in merged["packages"]}

    assert set(rows) == {"workswarm.theme", "manual.freeze", "token.weather.glass"}
    assert rows["workswarm.theme"]["active_version"] == "2.2.3"
    assert rows["manual.freeze"]["active_version"] is None
    assert rows["token.weather.glass"]["active_version"] == "1.2.1"
    assert merged["installed"][0] == "/shared/gw/theme"
    assert "/shared/as/weather" in merged["installed"]
    assert merged["published"] == {"workswarm.theme": ["theme"], "token.weather.glass": ["weather"]}
    # Installed-but-inactive ids are not successful activations.
    assert set(merged["package_ids"]) == {"workswarm.theme", "token.weather.glass"}


@pytest.mark.asyncio
async def test_query_for_gateway_package_is_answered_without_forwarding(monkeypatch) -> None:
    from types import SimpleNamespace

    from e2a.message import ReqMethod

    from agent_gateway.extensions_v2 import assembly

    manager, _ = _query_fixture()
    manager.domains = {"workswarm.theme": "gateway"}
    forwarded: list[dict] = []

    async def fake_proxy(**kwargs):
        forwarded.append(kwargs)
        return True

    monkeypatch.setattr(handlers, "proxy_unary_request", fake_proxy)
    monkeypatch.setattr(
        assembly,
        "get_gateway_runtime",
        lambda: SimpleNamespace(
            config=SimpleNamespace(enabled=True, dependency_timeout=300.0),
            packages=manager,
        ),
    )

    channel = _FakeChannel()
    handlers.register_extension_package_handlers(channel, lambda: object())
    await channel.methods[ReqMethod.EXTENSIONS_V2_QUERY.value](
        None, "req-2", {"package_id": "workswarm.theme"}, None, None,
    )

    assert forwarded == []
    assert channel.responses[0]["ok"] is True
    state = channel.responses[0]["payload"]["state"]
    assert [row["package_id"] for row in state["packages"]] == ["workswarm.theme"]
    assert state["packages"][0]["active_version"] == "2.2.3"


def test_query_survives_an_unavailable_local_snapshot() -> None:
    _, remote_state = _query_fixture()

    class FailingManager:
        def query(self, package_id=None):
            raise RuntimeError("store unavailable")

    state, records = handlers._read_local_query_state(FailingManager(), None, None)
    assert state is None
    assert records == {}

    merged = handlers._merge_remote_query(state, records)({"state": remote_state})

    assert [row["package_id"] for row in merged["state"]["packages"]] == ["token.weather.glass"]


@pytest.mark.asyncio
async def test_query_for_gateway_package_fails_when_local_state_is_unavailable(monkeypatch) -> None:
    from types import SimpleNamespace

    from e2a.message import ReqMethod

    from agent_gateway.extensions_v2 import assembly

    class FailingManager:
        async def get_package_domain(self, package_id: str) -> str:
            return "gateway"

        def query(self, package_id=None):
            raise RuntimeError("store unavailable")

    monkeypatch.setattr(
        assembly,
        "get_gateway_runtime",
        lambda: SimpleNamespace(
            config=SimpleNamespace(enabled=True, dependency_timeout=300.0),
            packages=FailingManager(),
        ),
    )

    channel = _FakeChannel()
    handlers.register_extension_package_handlers(channel, lambda: object())
    await channel.methods[ReqMethod.EXTENSIONS_V2_QUERY.value](
        None, "req-3", {"package_id": "workswarm.theme"}, None, None,
    )

    assert channel.responses[0]["ok"] is False
    assert channel.responses[0]["code"] == "SERVICE_UNAVAILABLE"


def test_query_preserves_unknown_state_fields() -> None:
    manager, remote_state = _query_fixture()
    remote_state["generation"] = 7
    remote_state["bundles"] = [{"bundle_id": "demo.bundle"}]

    merged = handlers._merge_query_states(manager.state, remote_state)

    assert merged["generation"] == 7
    assert merged["bundles"] == [{"bundle_id": "demo.bundle"}]


def test_query_reports_real_status_for_installed_but_inactive_packages() -> None:
    from agent_gateway.extensions_v2.management.install_state import ExtensionInstallRecord

    manager, _ = _query_fixture()
    state = {
        **manager.state,
        "installed_packages": [
            *manager.state["installed_packages"],
            {
                "package_id": "workswarm.theme.failed", "version": "1.0.0",
                "domain": "gateway", "install_path": "/shared/gw/failed",
            },
            {
                "package_id": "workswarm.theme.standby", "version": "1.0.0",
                "domain": "gateway", "install_path": "/shared/gw/standby",
            },
        ],
    }
    records = {
        "manual.freeze": ExtensionInstallRecord(
            package_id="manual.freeze", version="1.0.0", origin="hub",
            active=False, enabled=False, domain="gateway", status="disabled",
        ),
        "workswarm.theme.failed": ExtensionInstallRecord(
            package_id="workswarm.theme.failed", version="1.0.0", origin="hub",
            domain="gateway", status="failed", error="load failed",
        ),
        "workswarm.theme.standby": ExtensionInstallRecord(
            package_id="workswarm.theme.standby", version="1.0.0", origin="hub",
            active=False, enabled=True, domain="gateway", status="active",
        ),
    }

    rows = {
        row["package_id"]: row
        for row in handlers._gateway_query_packages(state, records)
    }

    assert rows["manual.freeze"]["status"] == "disabled"
    assert rows["manual.freeze"]["reason"] is None
    assert rows["workswarm.theme.failed"]["status"] == "failed"
    assert rows["workswarm.theme.failed"]["reason"] == {"message": "load failed"}
    assert rows["workswarm.theme.standby"]["status"] == "standby"


def test_query_strips_legacy_cross_domain_rows_from_agentserver_state() -> None:
    manager, remote_state = _query_fixture()
    remote_state["active"].append({"package_id": "workswarm.theme", "domain": "gateway"})
    remote_state["package_ids"].append("workswarm.theme")

    merged = handlers._merge_query_states(manager.state, remote_state)

    active_ids = [row["package_id"] for row in merged["active"]]
    assert active_ids.count("workswarm.theme") == 1
    assert "workswarm.theme" in merged["package_ids"]
