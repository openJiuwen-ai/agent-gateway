# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway host callbacks for agent-core's package lifecycle events.

The event names and payloads live in agent-core (``package.install``,
``package.install_finish``, ``package.install_failed``). This module only tells
core how the Gateway loads one member and where it records the outcome.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

from openjiuwen.core.events import (
    ExtensionEventBus,
    HostPackageInstallRequest,
    PackageInstallOutcome,
    register_host_package_lifecycle,
)

from agent_gateway.extensions_v2.management import containers
from agent_gateway.extensions_v2.management.install_state import ExtensionInstallState


def register_package_lifecycle_events(bus: ExtensionEventBus, runtime: Any) -> None:
    """Register the Gateway's install/outcome callbacks with core."""

    async def install_member(request: HostPackageInstallRequest) -> PackageInstallOutcome:
        store = ExtensionInstallState.for_install_dir(runtime.config.install_dir)
        event_id = request.event_id or uuid.uuid4().hex
        try:
            if request.operation == "reload":
                report = await runtime.packages.reload(request.package_id)
            else:
                report = await runtime.packages.enable_version(
                    request.package_id, request.version, skip_dependencies=True,
                )
            success = getattr(report, "status", "") == "SUCCESS"
            error = ""
            if not success:
                error = str(getattr(getattr(report, "error", None), "message", None) or "load failed")
        except Exception as exc:  # noqa: BLE001 - report one member
            success = False
            error = str(exc)
        return await asyncio.to_thread(
            containers.record_install_outcome,
            store,
            request,
            event_id=event_id,
            success=success,
            error=error,
        )

    async def outcome_received(outcome: PackageInstallOutcome) -> None:
        if not outcome.container_id:
            return None
        store = ExtensionInstallState.for_install_dir(runtime.config.install_dir)
        await asyncio.to_thread(
            containers.refresh_container_status, store, outcome.container_id, outcome.container_version,
        )
        return None

    register_host_package_lifecycle(
        bus,
        install_handler=install_member,
        outcome_handler=outcome_received,
    )
