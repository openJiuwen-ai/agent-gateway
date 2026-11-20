# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway extension runtime lifecycle: start once, stop once, never restart."""

from __future__ import annotations

from pathlib import Path

from agent_gateway.extensions_v2.config import GatewayExtensionConfig
from agent_gateway.extensions_v2.loader import LoadedPackage
from agent_gateway.extensions_v2.registry import HandlerDeclaration
from agent_gateway.extensions_v2.runtime import GatewayRuntime

UI = {"apiVersion": 1, "items": [{"type": "web.slot.contribution", "slot": "application.sidebar.footer"}]}


class Channel:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict]] = []

    async def ensure_ws_id(self, ws) -> str:
        return "ws-1"

    async def send_event(self, ws, event, payload) -> None:
        self.events.append((event, payload))

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None) -> None:
        return None


def _runtime(tmp_path: Path, *, enabled: bool = True) -> GatewayRuntime:
    return GatewayRuntime(
        channel=Channel(),
        config=GatewayExtensionConfig(
            enabled=enabled,
            install_dir=tmp_path / "pkgs",
            data_dir=tmp_path / "data",
            recovery_data_dir=tmp_path / "recovery",
        ),
    )


def _package(root: Path) -> LoadedPackage:
    async def ready(event, ops, next_handler):
        return None

    return LoadedPackage(
        package_id="test.lifecycle",
        version="1.0.0",
        root=root,
        ui=UI,
        handlers=(
            HandlerDeclaration("ui.ready", ready, {"target.package_id": "test.lifecycle"}, "ready"),
        ),
    )


async def test_start_then_shutdown_releases_every_handler(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()
    runtime.publish(_package(tmp_path))

    assert runtime.user_handler_count == 1
    await runtime.shutdown()
    assert runtime.user_handler_count == 0


async def test_shutdown_is_idempotent_and_start_does_not_revive(tmp_path):
    runtime = _runtime(tmp_path)
    await runtime.start()
    await runtime.shutdown()

    await runtime.shutdown()
    await runtime.start()

    assert runtime.user_handler_count == 0


async def test_disabled_runtime_registers_nothing(tmp_path):
    runtime = _runtime(tmp_path, enabled=False)

    await runtime.start()

    assert runtime.user_handler_count == 0

    await runtime.shutdown()
