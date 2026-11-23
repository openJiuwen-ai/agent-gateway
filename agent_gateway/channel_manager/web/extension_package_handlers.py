# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Forward V2 package-management requests to the AgentServer.

The AgentServer owns install, uninstall, reload, enablement and every catalog
view for both package domains. The Gateway forwards each management frame
unchanged, then reconciles its own domain members from the shared records so
Gateway packages load or unload exactly as they do at install time. Management
frames never enter the extension bus, so an extension cannot intercept its own
uninstall.

The authentication gate applies to every inbound frame, including management
frames: a request without an ``authorization`` header is answered with
``AUTH_REQUIRED`` by the BUILTIN gate before it reaches this module. Before a
login-owning package is published the gate stays inert, so management remains
reachable and the first login package can be installed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from e2a.message import ReqMethod

from agent_gateway.extensions_v2.management.install_state import ExtensionInstallState
from agent_gateway.routing.e2a_proxy import proxy_unary_request

_SYNC_METHODS = frozenset(
    {
        ReqMethod.EXTENSIONS_V2_IMPORT,
        ReqMethod.EXTENSIONS_V2_INSTALL,
        ReqMethod.EXTENSIONS_V2_UNINSTALL,
        ReqMethod.EXTENSIONS_V2_RELOAD,
        ReqMethod.EXTENSIONS_V2_SET_ENABLED,
    }
)

logger = logging.getLogger(__name__)


def register_extension_package_handlers(channel: Any, resolve_client: Callable[[], Any]) -> None:
    """Forward every management frame and sync this host from shared records."""

    def create_handler(method: ReqMethod) -> Callable[..., Awaitable[None]]:
        async def handler(
            ws: Any,
            req_id: str,
            params: dict[str, Any],
            session_id: str | None,
            user_id: str | None = None,
        ) -> None:
            # Install/import (and enabling a container) may return pending
            # members; load this domain while the AgentServer works, then once
            # more before the response is sent.
            enabling = method == ReqMethod.EXTENSIONS_V2_SET_ENABLED and params.get("enable") is True
            if method in (ReqMethod.EXTENSIONS_V2_IMPORT, ReqMethod.EXTENSIONS_V2_INSTALL) or enabling:
                await _forward_with_member_sync(
                    channel=channel,
                    resolve_client=resolve_client,
                    ws=ws,
                    req_id=req_id,
                    params=params,
                    session_id=session_id,
                    user_id=user_id,
                    method=method,
                )
                return
            # The AgentServer owns package management for both domains.
            await proxy_unary_request(
                channel=channel,
                agent_client=resolve_client(),
                ws=ws,
                req_id=req_id,
                params=params,
                session_id=session_id,
                user_id=user_id,
                req_method=method,
                label=method.value,
                timeout_seconds=_request_timeout(),
                drop_params=("session_id",),
                preserve_error_payload=True,
                on_done=_sync_after_management if method in _SYNC_METHODS else None,
                transform_payload=_refresh_management_payload if method in _SYNC_METHODS else None,
            )

        return handler

    for method in (
        ReqMethod.EXTENSIONS_V2_QUERY,
        ReqMethod.EXTENSIONS_V2_CATALOG,
        ReqMethod.EXTENSIONS_V2_SHOW,
        ReqMethod.EXTENSIONS_V2_IMPORT,
        ReqMethod.EXTENSIONS_V2_INSTALL,
        ReqMethod.EXTENSIONS_V2_UNINSTALL,
        ReqMethod.EXTENSIONS_V2_RELOAD,
        ReqMethod.EXTENSIONS_V2_SET_ENABLED,
        ReqMethod.EXTENSIONS_V2_FILE_LIST,
        ReqMethod.EXTENSIONS_V2_FILE_READ,
    ):
        channel.register_method(method.value, create_handler(method))


_SYNC_POLL_SECONDS = 0.5


async def _sync_gateway_members() -> None:
    """Load or unload this host's members without failing the management frame."""

    from agent_gateway.extensions_v2.assembly import get_gateway_runtime

    runtime = get_gateway_runtime()
    if runtime is None or not runtime.config.enabled:
        return
    try:
        await runtime.sync_container_members()
    except Exception:  # noqa: BLE001 - the AgentServer response stays authoritative
        logger.warning("[GatewayExtensions] member sync after management failed", exc_info=True)


async def _sync_after_management(ok: bool, payload: dict[str, Any]) -> None:
    """Reconcile this host from shared records after every management response.

    A failed install or version switch rolls the records back on the AgentServer;
    the Gateway must also release anything it loaded from the temporary claims.
    """

    del ok, payload
    await _sync_gateway_members()


async def _forward_with_member_sync(
    *,
    channel: Any,
    resolve_client: Callable[[], Any],
    ws: Any,
    req_id: str,
    params: dict[str, Any],
    session_id: str | None,
    user_id: str | None,
    method: ReqMethod,
) -> None:
    """Forward a container request and load this domain while the AgentServer works.

    The metadata flag asks the AgentServer to require this host's member
    results, so one container succeeds only when every member loaded.
    """

    task = asyncio.create_task(
        proxy_unary_request(
            channel=channel,
            agent_client=resolve_client(),
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=method,
            label=method.value,
            timeout_seconds=_request_timeout(),
            drop_params=("session_id",),
            preserve_error_payload=True,
            on_done=_sync_after_management,
            transform_payload=_refresh_management_payload,
            metadata={"extensions_v2_require_all_members": True},
        )
    )
    try:
        while True:
            done, _ = await asyncio.wait({task}, timeout=_SYNC_POLL_SECONDS)
            if done:
                break
            await _sync_gateway_members()
        await task
    finally:
        if not task.done():
            task.cancel()


def _refresh_management_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Rewrite results after this host processed its domain's copies."""

    from agent_gateway.extensions_v2.assembly import get_gateway_runtime

    runtime = get_gateway_runtime()
    if runtime is None:
        return payload
    from agent_gateway.extensions_v2.management import containers

    store = ExtensionInstallState.for_install_dir(runtime.config.install_dir)
    refreshed = containers.adjust_management_reports(store, dict(payload))
    single = refreshed.get("bundle")
    if isinstance(single, dict):
        refreshed["bundle"] = _project_bundle(store, single)
    bundles = refreshed.get("bundles")
    if isinstance(bundles, list):
        refreshed["bundles"] = [
            _project_bundle(store, item) if isinstance(item, dict) else item for item in bundles
        ]
    return refreshed


def _project_bundle(store: ExtensionInstallState, projection: dict[str, Any]) -> dict[str, Any]:
    bundle_id = str(projection.get("bundleId") or "")
    version = str(projection.get("version") or "")
    record = store.get_bundle(bundle_id, version)
    if record is None:
        return projection
    members: list[dict[str, Any]] = []
    records = {item.package_id: item for item in record.member_records}
    for member in record.members:
        member_record = records.get(member.package_id)
        members.append(
            {
                "packageId": member.package_id,
                "version": member.version,
                "domain": member.domain,
                "status": member_record.status if member_record is not None else "pending",
                "active": bool(member_record.active) if member_record is not None else False,
                "enabled": bool(member_record.enabled) if member_record is not None else False,
                "error": member_record.error if member_record is not None else "",
            }
        )
    from agent_gateway.extensions_v2.management import containers

    return {
        **projection,
        "overallStatus": (
            containers.frontend_status(store, record) if record.active else "disabled"
        ),
        "members": members,
    }


def _request_timeout() -> float:
    """Client and proxy wait at least the dependency window plus one margin."""

    from agent_gateway.extensions_v2.assembly import get_gateway_runtime

    runtime = get_gateway_runtime()
    if runtime is None:
        return 330.0
    return float(runtime.config.dependency_timeout) + 30.0
