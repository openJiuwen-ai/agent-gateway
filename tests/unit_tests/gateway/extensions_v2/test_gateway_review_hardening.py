# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Unit coverage for the hardening behaviours raised during review (PR 263)."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from agent_gateway.extensions_v2.config import GatewayExtensionConfig
from agent_gateway.extensions_v2.management.local_cli import start_local_cli
from agent_gateway.extensions_v2.runtime import GatewayRuntime
from agent_gateway.extensions_v2.webui import transfers as transfers_module
from agent_gateway.extensions_v2.webui.http import attach_browser_routes
from agent_gateway.extensions_v2.webui.models import RenderNode, RenderTarget, UiRenderInput
from agent_gateway.extensions_v2.webui.schema import validate_request

PACKAGE = "test.theme"
SLOT = "application.settings"
LOCAL = ("127.0.0.1", 50000)


def _channel():
    responses, events, sockets = [], [], {}

    async def ensure_ws_id(ws):
        ws._jiuwen_ws_id = getattr(ws, "name", "browser-1")
        sockets[ws._jiuwen_ws_id] = ws
        return ws._jiuwen_ws_id

    async def send_event(ws, event, payload):
        events.append((ws._jiuwen_ws_id, event, payload))

    async def send_response(ws, req_id, *, ok, payload=None, error=None, code=None):
        responses.append({"ok": ok, "payload": payload, "error": error, "code": code})

    return SimpleNamespace(channel_id="web", responses=responses, events=events, ensure_ws_id=ensure_ws_id,
                           connection_user_id=lambda ws: None, ws_by_id=sockets.get,
                           send_event=send_event, send_response=send_response)


async def _start(tmp_path: Path) -> GatewayRuntime:
    runtime = GatewayRuntime(channel=_channel(), config=GatewayExtensionConfig(
        install_dir=tmp_path / "install", data_dir=tmp_path / "data"))
    await runtime.start()
    return runtime


def _client(runtime):
    app = FastAPI()
    attach_browser_routes(app, lambda: runtime)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=LOCAL), base_url="http://test")


class _GatewayPackages:
    """Record mutations so an unknown action is proven to never fall back to uninstall."""

    def __init__(self) -> None:
        self.mutations: list[tuple[str, str]] = []

    def owns(self, package_id: str) -> bool:
        return True

    async def uninstall(self, package_id: str):
        self.mutations.append(("uninstall", package_id))
        raise AssertionError("an unknown action must not uninstall")

    async def reload(self, package_id: str):
        self.mutations.append(("reload", package_id))
        raise AssertionError("an unknown action must not reload")


async def test_unknown_local_cli_action_is_rejected_without_uninstall():
    packages = _GatewayPackages()
    control = await start_local_cli(SimpleNamespace(packages=packages))
    try:
        response = await control._handler("prune", {"package_id": "demo.gateway"})
    finally:
        await control.close()
    assert response["ok"] is False
    assert response["payload"]["code"] == "INVALID_PARAMS"
    assert packages.mutations == []


async def test_cross_site_ticket_requests_are_rejected(tmp_path):
    runtime = await _start(tmp_path)
    try:
        async with _client(runtime) as client:
            upload = f"{transfers_module.UPLOAD_PREFIX}{'0' * 32}/theme.zip"
            cross = {"sec-fetch-site": "cross-site"}
            assert (await client.put(upload, content=b"x", headers=cross)).status_code == 403
            download = f"{transfers_module.DOWNLOAD_PREFIX}{'0' * 32}"
            assert (await client.get(download, headers=cross)).status_code == 403
            # A same-origin request still reaches normal ticket validation.
            same_origin = {"sec-fetch-site": "same-origin"}
            assert (await client.put(upload, content=b"x", headers=same_origin)).status_code == 404
    finally:
        await runtime.shutdown()


async def test_symlinked_upload_directory_is_rejected(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    upload_root = data_dir / transfers_module.UPLOAD_DIR
    try:
        upload_root.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("directory symlinks are not available in this environment")
    manager = transfers_module.TransferManager(SimpleNamespace(package_data_dir=lambda package_id: data_dir))
    target = RenderTarget(session_id="", slot_id=SLOT, package_id=PACKAGE, instance_key="main",
                          scope="application", connection_id="browser-1")
    with pytest.raises(transfers_module.HostRequestError) as excinfo:
        await manager._prepare_upload(target, "0.0", SimpleNamespace(), {"theme.zip": 1}, lambda: True)
    assert excinfo.value.code == transfers_module.CODE_BAD_REQUEST


async def test_close_cancels_background_notifications(tmp_path):
    runtime = await _start(tmp_path)
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def pending():
        started.set()
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.set()
            raise

    try:
        runtime.browser.spawn(pending())
        await asyncio.wait_for(started.wait(), timeout=5)
        await runtime.browser.close()
        assert cancelled.is_set()
        assert not runtime.browser._tasks
    finally:
        await runtime.shutdown()


def _application_surface(src: str):
    target = RenderTarget(session_id="", slot_id="application.presentation", package_id=PACKAGE,
                          instance_key="main", scope="application", connection_id="browser-1")
    root = RenderNode(component="ApplicationSurface", props={"src": src, "title": "演示"})
    return validate_request(UiRenderInput(target=target, root=root))


def test_application_surface_accepts_raw_non_ascii_asset_names():
    report = _application_surface(f"/extension-assets/{PACKAGE}/gen-1/页面.html")
    assert report.ok


@pytest.mark.parametrize("src", [
    f"/extension-assets/{PACKAGE}/gen-1/../other/theme.html",
    f"/extension-assets/{PACKAGE}/gen-1/%2e%2e/other/theme.html",
    f"/extension-assets/{PACKAGE}/gen-1/%2fetc/theme.html",
])
def test_application_surface_rejects_traversal(src):
    assert not _application_surface(src).ok
