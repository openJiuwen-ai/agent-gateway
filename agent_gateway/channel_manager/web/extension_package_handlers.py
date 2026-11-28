# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Forward V2 package-management requests to the AgentServer.

The AgentServer owns install, uninstall, reload, enablement and every catalog
view for both package domains. The Gateway forwards each management frame
unchanged, then reconciles its own domain members from the shared records so
Gateway packages load or unload exactly as they do at install time. Management
frames never enter the extension bus, so an extension cannot intercept its own
uninstall. Queries are the exception: the AgentServer answers for its own
domain only, so the Gateway merges its local snapshot into every query
response and answers a Gateway package query locally.

The authentication gate applies to every inbound frame, including management
frames: a request without an ``authorization`` header is answered with
``AUTH_REQUIRED`` by the BUILTIN gate before it reaches this module. Before a
login-owning package is published the gate stays inert, so management remains
reachable and the first login package can be installed.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import Any

from e2a.message import ReqMethod

from agent_gateway.extensions_v2.management.install_state import (
    ExtensionInstallRecord,
    ExtensionInstallState,
)
from agent_gateway.extensions_v2.manifest import DOMAIN_GATEWAY
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
            if method == ReqMethod.EXTENSIONS_V2_QUERY:
                await _forward_query_with_local_state(
                    channel=channel,
                    resolve_client=resolve_client,
                    ws=ws,
                    req_id=req_id,
                    params=params,
                    session_id=session_id,
                    user_id=user_id,
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


async def _forward_query_with_local_state(
    *,
    channel: Any,
    resolve_client: Callable[[], Any],
    ws: Any,
    req_id: str,
    params: dict[str, Any],
    session_id: str | None,
    user_id: str | None,
) -> None:
    """Answer one query with both this host's facts and the AgentServer's.

    The AgentServer scopes its own query to the AgentServer domain, so Gateway
    packages reach the front end only when this host merges its snapshot into
    the response. A query for one Gateway package is answered locally: the
    AgentServer rejects it as a domain mismatch instead of returning an empty
    state. Every other failure keeps the AgentServer response as authoritative.
    """

    from agent_gateway.extensions_v2.assembly import get_gateway_runtime

    runtime = get_gateway_runtime()
    manager = None
    install_dir: Path | None = None
    if runtime is not None and runtime.config.enabled:
        try:
            manager = runtime.packages
        except Exception:  # noqa: BLE001 - the AgentServer response stays authoritative
            logger.warning("[GatewayExtensions] local package manager unavailable", exc_info=True)
        raw_install_dir = getattr(runtime.config, "install_dir", None)
        if raw_install_dir:
            install_dir = Path(raw_install_dir)
    package_id = params.get("package_id")
    if not isinstance(package_id, str) or not package_id:
        package_id = None
    state: dict[str, Any] | None = None
    records: dict[str, ExtensionInstallRecord] = {}
    if manager is not None:
        state, records = await asyncio.to_thread(
            _read_local_query_state, manager, install_dir, package_id,
        )
    if manager is not None and package_id is not None:
        try:
            local_only = await manager.get_package_domain(package_id) == DOMAIN_GATEWAY
        except Exception:  # noqa: BLE001 - forward when the domain cannot be confirmed
            logger.warning(
                "[GatewayExtensions] could not resolve %s domain", package_id, exc_info=True,
            )
            local_only = False
        if local_only:
            if state is None:
                await channel.send_response(
                    ws,
                    req_id,
                    ok=False,
                    error="Gateway package state is unavailable",
                    code="SERVICE_UNAVAILABLE",
                )
                return
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload={"state": _merge_query_states(state, {}, records)},
            )
            return

    await proxy_unary_request(
        channel=channel,
        agent_client=resolve_client(),
        ws=ws,
        req_id=req_id,
        params=params,
        session_id=session_id,
        user_id=user_id,
        req_method=ReqMethod.EXTENSIONS_V2_QUERY,
        label=ReqMethod.EXTENSIONS_V2_QUERY.value,
        timeout_seconds=_request_timeout(),
        drop_params=("session_id",),
        preserve_error_payload=True,
        transform_payload=(
            _merge_remote_query(state, records) if manager is not None else None
        ),
    )


def _merge_remote_query(
    local_state: Mapping[str, Any] | None,
    records: Mapping[str, ExtensionInstallRecord],
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Build the successful-response projection that adds this host's facts."""

    def merge(payload: dict[str, Any]) -> dict[str, Any]:
        remote = payload.get("state")
        if not isinstance(remote, Mapping) or local_state is None:
            return payload
        return {**payload, "state": _merge_query_states(local_state, remote, records)}

    return merge


def _read_local_query_state(
    manager: Any,
    install_dir: Path | None,
    package_id: str | None,
) -> tuple[dict[str, Any] | None, dict[str, ExtensionInstallRecord]]:
    """Read this host's snapshot and install records off the event loop.

    ``None`` means the snapshot is unavailable; callers must not report that as
    an empty successful state.
    """

    try:
        state = manager.query(package_id)
    except Exception:  # noqa: BLE001 - a local snapshot must not fail the query
        logger.warning("[GatewayExtensions] local package query failed", exc_info=True)
        return None, {}
    records: dict[str, ExtensionInstallRecord] = {}
    if install_dir is not None:
        try:
            records = {
                record.package_id: record
                for record in ExtensionInstallState.for_install_dir(install_dir).packages()
            }
        except Exception:  # noqa: BLE001 - records only refine the reported status
            logger.warning("[GatewayExtensions] install records unavailable", exc_info=True)
    return state, records


def _merge_query_states(
    local: Mapping[str, Any],
    remote: Mapping[str, Any],
    records: Mapping[str, ExtensionInstallRecord] | None = None,
) -> dict[str, Any]:
    """Merge each host's current facts; do not trust legacy cross-domain AgentServer rows."""

    remote = _agentserver_query_state(remote)
    local_packages = _gateway_query_packages(local, records)
    remote_packages = _mapping_rows(remote.get("packages"))
    packages = _unique_rows(local_packages, remote_packages, key=lambda item: str(item.get("package_id") or ""))
    active = _unique_rows(
        _mapping_rows(local.get("active")),
        _mapping_rows(remote.get("active")),
        key=lambda item: str(item.get("package_id") or item.get("root") or ""),
    )
    installed_packages = _unique_rows(
        _mapping_rows(local.get("installed_packages")),
        _mapping_rows(remote.get("installed_packages")),
        key=lambda item: "\0".join(
            str(item.get(name) or "") for name in ("package_id", "version", "install_path")
        ),
    )
    installed = list(dict.fromkeys([
        *[str(item) for item in local.get("installed") or [] if isinstance(item, (str, Path))],
        *[str(item) for item in remote.get("installed") or [] if isinstance(item, (str, Path))],
    ]))
    # package_ids reports successfully activated packages, so installed-but-inactive
    # ids must not leak in through the projected report rows.
    package_ids = list(dict.fromkeys([
        *[str(item) for item in local.get("package_ids") or [] if isinstance(item, str) and item],
        *[str(item) for item in remote.get("package_ids") or [] if isinstance(item, str) and item],
    ]))
    return {
        **dict(remote),
        "active": active,
        "installed": installed,
        "installed_packages": installed_packages,
        "inflight": {**_mapping_value(remote.get("inflight")), **_mapping_value(local.get("inflight"))},
        "published": {**_mapping_value(remote.get("published")), **_mapping_value(local.get("published"))},
        "packages": packages,
        "package_ids": package_ids,
    }


def _agentserver_query_state(state: Mapping[str, Any]) -> dict[str, Any]:
    """Remove Gateway-owned facts from a legacy shared AgentServer report."""

    gateway_ids: set[str] = set()
    gateway_paths: set[str] = set()
    for field in ("active", "installed_packages", "packages"):
        for item in _mapping_rows(state.get(field)):
            if str(item.get("domain") or "").strip().lower() != DOMAIN_GATEWAY:
                continue
            package_id = str(item.get("package_id") or "")
            if package_id:
                gateway_ids.add(package_id)
            path = str(item.get("install_path") or item.get("root") or "")
            if path:
                gateway_paths.add(path)

    def agent_rows(field: str) -> list[dict[str, Any]]:
        return [
            item for item in _mapping_rows(state.get(field))
            if str(item.get("package_id") or "") not in gateway_ids
            and str(item.get("domain") or "").strip().lower() != DOMAIN_GATEWAY
        ]

    return {
        **dict(state),
        "active": agent_rows("active"),
        "installed": [
            str(item) for item in state.get("installed") or []
            if isinstance(item, (str, Path)) and str(item) not in gateway_paths
        ],
        "installed_packages": agent_rows("installed_packages"),
        "inflight": {
            str(key): value for key, value in _mapping_value(state.get("inflight")).items()
            if str(key) not in gateway_ids
        },
        "published": {
            str(key): value for key, value in _mapping_value(state.get("published")).items()
            if str(key) not in gateway_ids
        },
        "packages": agent_rows("packages"),
        "package_ids": [
            str(item) for item in state.get("package_ids") or []
            if isinstance(item, str) and item and item not in gateway_ids
        ],
    }


def _gateway_query_packages(
    state: Mapping[str, Any],
    records: Mapping[str, ExtensionInstallRecord] | None = None,
) -> list[dict[str, Any]]:
    """Project Gateway runtime and installed rows into the shared query report shape.

    Report rows must not derive a failed status from "not active": an installed
    package that is disabled or waiting uses the install record's real status.
    """

    records = records or {}
    active = {
        str(item.get("package_id")): item
        for item in _mapping_rows(state.get("active"))
        if item.get("package_id")
    }
    identities = dict.fromkeys([
        *active,
        *[
            str(item.get("package_id"))
            for item in _mapping_rows(state.get("installed_packages"))
            if item.get("package_id")
        ],
    ])
    rows: list[dict[str, Any]] = []
    for package_id in identities:
        record = active.get(package_id)
        install = records.get(package_id)
        version = record.get("version") if record is not None else None
        requires = {}
        if record is not None:
            requires = {
                str(item.get("package_id")): str(item.get("version") or "")
                for item in _mapping_rows(record.get("requires_packages"))
                if item.get("package_id")
            }
        status = "success" if record is not None else "unknown"
        reason = None
        if install is not None:
            if record is not None:
                status = "success"
            elif install.error:
                status = "failed"
                reason = {"message": install.error}
            elif not install.enabled:
                status = "disabled"
            elif install.status == "active":
                # Stale "active" on a package the runtime did not load.
                status = "standby"
            else:
                status = install.status
        rows.append({
            "package_id": package_id,
            "domain": DOMAIN_GATEWAY,
            "process_id": None,
            "active_version": str(version) if version is not None else None,
            "requires": requires,
            "status": status,
            "reason": reason,
            "pending": None,
        })
    return rows


def _mapping_rows(value: Any) -> list[dict[str, Any]]:
    return [dict(item) for item in value or [] if isinstance(item, Mapping)]


def _mapping_value(value: Any) -> dict[str, Any]:
    return {str(key): item for key, item in value.items()} if isinstance(value, Mapping) else {}


def _unique_rows(
    first: list[dict[str, Any]],
    second: list[dict[str, Any]],
    *,
    key: Callable[[Mapping[str, Any]], str],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for item in (*first, *second):
        identity = key(item)
        if identity:
            merged.setdefault(identity, item)
    return list(merged.values())


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
