# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Render-controlled forms and version-bound physical presentation closes."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from agent_gateway.extensions_v2.config import GatewayExtensionConfig
from agent_gateway.extensions_v2.loader import LoadedPackage
from agent_gateway.extensions_v2.models import GatewayRequestOutput
from agent_gateway.extensions_v2.registry import HandlerDeclaration
from agent_gateway.extensions_v2.runtime import GatewayRuntime
from agent_gateway.extensions_v2.webui.models import RenderNode, RenderTarget, UiActionOutput, UiRenderInput

SLOT = "application.presentation"
PACKAGE = "test.forms"


class Forms:
    def __init__(self):
        self.ops = None
        self.revision = None
        self.closed = []

    async def ready(self, event, ops, next_handler):
        self.ops = ops
        result = await self.render(ops)
        self.revision = result.revision
        return UiActionOutput(accepted=True, status="ready")

    async def render(self, ops):
        src = await ops.ui.asset_url("form.html")
        return await ops.ui.render(UiRenderInput(
            RenderTarget("", SLOT, instance_key="current", scope="application"),
            RenderNode("ApplicationSurface", {"title": "Form", "src": src, "mode": "overlay", "autoStart": True}),
        ))

    async def close(self, event, ops, next_handler):
        self.closed.append(event.event_input)
        return UiActionOutput(accepted=True, status="closed")

    def package(self, root):
        (root / "assets").mkdir(parents=True, exist_ok=True)
        (root / "assets/form.html").write_text("<!doctype html><title>Form</title>")
        match = {"target.package_id": PACKAGE}
        return LoadedPackage(package_id=PACKAGE, version="1.0.0", root=root, client={}, ui={"apiVersion": 1, "items": [
            {"type": "web.slot.contribution", "slot": SLOT},
        ]}, handlers=(HandlerDeclaration("ui.ready", self.ready, match, "ready"),
             HandlerDeclaration("ui.closed", self.close, match, "closed")))


async def setup(tmp_path):
    sockets, events, responses = {}, [], []

    async def ensure_ws_id(ws):
        ws._jiuwen_ws_id = ws.name
        sockets[ws.name] = ws
        return ws.name

    async def send_event(ws, event, payload):
        events.append((ws.name, event, payload))

    async def send_response(ws, request_id, *, ok, payload=None, error=None, code=None):
        responses.append({"ok": ok, "payload": payload, "error": error})

    channel = SimpleNamespace(channel_id="web", ensure_ws_id=ensure_ws_id, ws_by_id=sockets.get,
                              connection_user_id=lambda ws: None, send_event=send_event, send_response=send_response)
    runtime = GatewayRuntime(channel=channel, config=GatewayExtensionConfig(
        install_dir=tmp_path / "install", data_dir=tmp_path / "data"))
    await runtime.start()
    forms = Forms()
    runtime.publish(forms.package(tmp_path / "forms"))
    ws = SimpleNamespace(name="window-1", closed=False)

    async def frame(event, *, revision=None, socket=ws, change=None, presentation_id=1):
        params = {"event": event, "target": {"scope": "application", "slot_id": SLOT,
                  "package_id": PACKAGE, "instance_key": "current"},
                  "payload": {"reason": "closed", "revision": revision, "presentation_id": presentation_id} if event == "ui.closed" else {}}
        if change is not None:
            change(params)
        await runtime.intercept(socket, "request", "webui.dispatch", params)
        return responses[-1]

    await frame("ui.ready")
    return runtime, forms, events, frame, ws


async def test_close_requires_current_connection_package_instance_and_revision(tmp_path):
    runtime, forms, _, frame, _ = await setup(tmp_path)
    try:
        old = forms.revision
        current = (await forms.render(forms.ops)).revision
        other = SimpleNamespace(name="window-2", closed=False)
        assert (await frame("ui.closed", revision=current, socket=other))["payload"]["accepted"] is False
        assert (await frame("ui.closed", revision=old))["payload"]["code"] == "TARGET_INACTIVE"
        for mutate in [
            lambda params: params["target"].update(instance_key="other"),
            lambda params: params["target"].update(package_id="another.package"),
            lambda params: params["target"].update(connection_id="window-1"),
            lambda params: params["payload"].update(reason="replaced"),
            lambda params: params["payload"].update(reason="workspace"),
            lambda params: params["payload"].update(revision=True),
            lambda params: params["payload"].update(revision=0),
            lambda params: params["payload"].update(revision=-1),
            lambda params: params["payload"].update(revision=2**53),
            lambda params: params["payload"].update(presentation_id=True),
            lambda params: params["payload"].pop("presentation_id"),
        ]:
            assert (await frame("ui.closed", revision=current, change=mutate))["payload"]["accepted"] is False
        assert forms.closed == []
        assert (await frame("ui.closed", revision=current))["payload"] == {"accepted": True, "status": "closed"}
        assert len(forms.closed) == 1
        assert dict(forms.closed[0].payload) == {"reason": "closed", "revision": current, "presentation_id": 1}
        assert forms.closed[0].target.connection_id == "window-1"
        assert runtime.registry.get(forms.closed[0].target).revision == current
        assert (await frame("ui.closed", revision=current))["payload"]["code"] == "TARGET_INACTIVE"
        new = (await forms.render(forms.ops)).revision
        assert new > current
        assert (await frame("ui.closed", revision=current))["payload"]["code"] == "TARGET_INACTIVE"
        assert len(forms.closed) == 1
    finally:
        await runtime.shutdown()


async def test_close_releases_only_its_exact_pending_start_before_plugin_notification(tmp_path):
    runtime, forms, _, frame, _ = await setup(tmp_path)
    try:
        pending = asyncio.create_task(runtime.browser.presentations.start({
            "connection_id": "window-1", "package_id": PACKAGE, "instance_key": "current", "revision": forms.revision,
        }))
        await asyncio.sleep(0)
        assert runtime.browser.presentations.pending
        assert (await frame("ui.closed", revision=forms.revision))["payload"]["accepted"]
        assert (await pending)["payload"]["status"] == "unknown"
        assert not runtime.browser.presentations.pending and len(forms.closed) == 1
    finally:
        await runtime.shutdown()


async def test_legacy_package_without_close_handler_keeps_its_candidate_and_can_restart(tmp_path):
    runtime, forms, events, frame, _ = await setup(tmp_path)
    try:
        package = forms.package(tmp_path / "legacy")
        runtime.publish(replace(package, handlers=(package.handlers[0],)))
        await frame("ui.ready")
        revision = forms.revision
        assert (await frame("ui.closed", revision=revision, presentation_id=10))["payload"]["accepted"]
        assert forms.closed == []
        assert len(runtime.browser.presentations.query({})["payload"]["candidates"]) == 1
        pending = asyncio.create_task(runtime.browser.presentations.start({
            "connection_id": "window-1", "package_id": PACKAGE, "instance_key": "current", "revision": revision,
        }))
        await asyncio.sleep(0)
        start = next(event for event in events if event[1] == "webui.presentation.start")
        runtime.browser.presentations.complete(start[0], {"request_id": start[2]["request_id"], "code": "STARTED"})
        assert (await pending)["payload"]["status"] == "started"
        assert (await frame("ui.closed", revision=revision, presentation_id=11))["payload"]["accepted"]
        assert (await frame("ui.closed", revision=revision, presentation_id=10))["payload"]["code"] == "TARGET_INACTIVE"
        assert len(runtime.browser.presentations.query({})["payload"]["candidates"]) == 1
        runtime.browser.drop_connection("window-1")
        assert "window-1" not in runtime.browser._presentation_closes
    finally:
        await runtime.shutdown()


async def test_physical_close_lifecycle_bypasses_request_policy_but_ready_does_not(tmp_path):
    runtime, forms, _, frame, _ = await setup(tmp_path)
    try:
        async def deny(event, ops, next_handler):
            return GatewayRequestOutput(handled=True, route="deny", error={"code": "AUTH_REQUIRED", "message": "login"})

        root = tmp_path / "policy"
        root.mkdir()
        runtime.publish(LoadedPackage(package_id="test.policy", version="1", root=root, client={}, ui={},
                                     handlers=(HandlerDeclaration("gateway.request", deny, {}, "deny"),)))
        assert (await frame("ui.ready"))["ok"] is False
        assert (await frame("ui.closed", revision=forms.revision))["payload"]["accepted"] is True
        assert len(forms.closed) == 1
    finally:
        await runtime.shutdown()


@pytest.mark.parametrize("reason", [
    "closed", "ready_timeout", "render_failed", "load_failed", "renderer_gone",
    "transition_failed", "open_failed",
])
async def test_host_closes_and_open_failures_reach_the_plugin(reason, tmp_path):
    runtime, forms, _, frame, _ = await setup(tmp_path)
    try:
        receipt = await frame("ui.closed", revision=forms.revision,
                              change=lambda params: params["payload"].update(reason=reason))
        assert receipt["payload"] == {"accepted": True, "status": "closed"}
        assert len(forms.closed) == 1 and forms.closed[0].payload["reason"] == reason
        assert runtime.registry.get(forms.closed[0].target) is not None
    finally:
        await runtime.shutdown()
