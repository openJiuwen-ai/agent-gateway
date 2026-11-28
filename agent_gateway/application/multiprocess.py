# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Opt-in, personal loopback trial using the fixed Worker group."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import socket

from agent_gateway.application.cron_preflight import require_controlled_cron
from agent_gateway.application.worker_group import WorkerGroup
from agent_gateway.common.agentos_runtime import is_agentos_runtime
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.southbound_routes import MAX_ROUTE_DIRECTORY_BYTES, StaticRouteDirectory
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.enterprise import is_enterprise
from agent_gateway.processes.capabilities import management_preflight

DEFAULT_WORKERS = 2
MAX_WORKERS = 32
logger = logging.getLogger("gateway")


@dataclass(frozen=True)
class MultiprocessOptions:
    workers: int
    web_port: int
    protocol_port: int
    accounts: bool = False
    cron: bool = False
    # Opt-in static multi-AgentServer trial: one non-secret route snapshot.
    southbound_routes: StaticRouteDirectory | None = None

    @classmethod
    def from_cli(cls, *, workers: int | None, web_host: str, web_port: int,
                 web_path: str, web_ws_deflate: bool,
                 required_client_type: str | tuple[str, ...] | None,
                 accounts: bool = False, cron: bool = False,
                 southbound_routes_file: str | None = None) -> MultiprocessOptions:
        count = DEFAULT_WORKERS if workers is None else workers
        if not 1 <= count <= MAX_WORKERS:
            raise ValueError(f"--workers must be between 1 and {MAX_WORKERS}")
        southbound_routes = None
        if southbound_routes_file is not None:
            southbound_routes = load_static_routes(southbound_routes_file, workers=count)
        if type(accounts) is not bool or type(cron) is not bool:
            raise ValueError("--enable-accounts and --enable-cron must be explicit flags")
        if is_enterprise() or is_agentos_runtime() or required_client_type is not None:
            raise ValueError("multiprocess requires a personal deployment")
        if web_host not in {"127.0.0.1", "localhost"} or os.getenv(
            "GATEWAY_HOST", "127.0.0.1",
        ) not in {"127.0.0.1", "localhost"}:
            raise ValueError("multiprocess requires loopback listeners (127.0.0.1)")
        if web_path != "/ws" or web_ws_deflate:
            raise ValueError("multiprocess requires /ws without WebSocket compression")
        if os.getenv("A2A_SERVER_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}:
            raise ValueError("A2A is unavailable in multiprocess mode")
        if not management_preflight().supported or not all((
            hasattr(socket, "SCM_RIGHTS"), hasattr(socket.socket, "sendmsg"),
            hasattr(socket.socket, "recvmsg"),
        )):
            raise ValueError("multiprocess requires Linux or macOS with fd passing")
        protocol_port = int(os.getenv("GATEWAY_PORT", "19001"))
        if not 0 <= web_port <= 65535 or not 0 <= protocol_port <= 65535:
            raise ValueError("listen ports must be between 0 and 65535")
        if web_port == protocol_port != 0:
            raise ValueError("Web and TUI/ACP must use distinct ports")
        return cls(count, web_port, protocol_port, accounts, cron, southbound_routes)


def load_static_routes(path: str, *, workers: int) -> StaticRouteDirectory:
    """Read the static route directory of the Worker group; no other side effect.

    The file is a local, non-secret snapshot. Its manifest must list exactly
    the slots of this run, so every Worker owns the targets fixed for it.
    """
    if type(path) is not str or not path.strip() or type(workers) is not int:
        raise ValueError("--southbound-routes-file requires a path")
    try:
        with open(path.strip(), "rb") as source:
            encoded = source.read(MAX_ROUTE_DIRECTORY_BYTES + 1)
        if len(encoded) > MAX_ROUTE_DIRECTORY_BYTES:
            raise ValueError("static route file exceeds the supported size")
        directory = StaticRouteDirectory.from_json(encoded.decode("utf-8"))
    except (OSError, UnicodeError) as error:
        raise ValueError(f"cannot read --southbound-routes-file: {type(error).__name__}") from error
    except ValueError as error:
        raise ValueError(f"invalid --southbound-routes-file: {error}") from error
    if directory.manifest.slots != tuple(range(workers)):
        raise ValueError("the static route manifest must list exactly the --workers slots")
    return directory


def _channel_enabled(config: object) -> bool:
    if not isinstance(config, dict) or not config:
        return bool(config)
    if "enabled" in config:
        return bool(config["enabled"])
    if "apps" in config:
        return any(_channel_enabled(app) for app in (config["apps"] or ()))
    return True


def _has_enabled_cron_jobs(path: Path) -> bool:
    # Read the atomic snapshot without invoking the store's lazy migration.
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return False
    except (OSError, ValueError) as error:
        raise ValueError("cannot read the Cron job snapshot; use single mode") from error
    jobs = data.get("jobs", []) if isinstance(data, dict) else None
    if not isinstance(jobs, list) or not all(isinstance(job, dict) for job in jobs):
        raise ValueError("invalid Cron job snapshot; use single mode")
    return any(job.get("enabled") for job in jobs)


async def validate_workspace(*, cron: bool = False, workers: int = DEFAULT_WORKERS) -> None:
    """Refuse a workspace the Worker group cannot serve before any listener opens.

    Without ``cron`` an enabled Cron record keeps the trial in single mode, as
    before. With ``cron`` the record must instead pass the read-only preflight.
    Neither path creates, migrates or rewrites the Cron file.
    """
    from agent_gateway.common.config import get_config
    from agent_gateway.common.cron_settings import load_cron_store_settings

    if type(cron) is not bool:
        raise ValueError("cron must be an explicit boolean")

    config = await asyncio.to_thread(get_config)
    gateway = config.get("gateway") or {}
    client_type = (gateway.get("agent_client") or {}).get("type") or "websocket"
    if client_type != "websocket":
        raise ValueError("multiprocess requires the direct WebSocket AgentServer client")
    for name, channel in (config.get("channels") or {}).items():
        if name == "web":
            continue
        enabled = (any(_channel_enabled(bot) for bot in channel.values())
                   if name == "feishu_enterprise" and isinstance(channel, dict)
                   else _channel_enabled(channel))
        if enabled:
            raise ValueError("multiprocess requires optional channels to be disabled")
    if (config.get("proactive_recommendation") or {}).get("enabled"):
        raise ValueError("proactive recommendations are unavailable in multiprocess mode")
    settings = load_cron_store_settings(config)
    if settings.backend != "file":
        raise ValueError("Cron is not supported by this trial; use single mode")
    if not cron:
        if await asyncio.to_thread(_has_enabled_cron_jobs, settings.file_path):
            raise ValueError("Cron is not supported by this trial; use single mode")
        return
    if type(workers) is not int or not 1 <= workers <= MAX_WORKERS:
        raise ValueError(f"--workers must be between 1 and {MAX_WORKERS}")
    await require_controlled_cron(
        CronOwnership(WorkerManifest(tuple(range(workers))), LocalIdentityPolicy()), config=config,
    )


async def run_multiprocess(options: MultiprocessOptions, agent_server_url: str,
                           shutdown_event: asyncio.Event) -> None:
    await validate_workspace(cron=options.cron, workers=options.workers)
    if shutdown_event.is_set():
        return
    auth_service_provider = None
    if options.accounts:
        from agent_gateway.auth.service import get_auth_service

        auth_service_provider = get_auth_service
    group = WorkerGroup(
        None if options.southbound_routes is not None else agent_server_url,
        workers=options.workers, web_port=options.web_port, protocol_port=options.protocol_port,
        auth_service_provider=auth_service_provider, cron=options.cron,
        southbound_routes=options.southbound_routes,
    )
    shutdown = asyncio.create_task(shutdown_event.wait())
    startup = asyncio.create_task(group.start())
    tasks = [shutdown, startup]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if startup in done:
            await startup
        if shutdown.done():
            return
        logger.info("[App] multiprocess_ready workers=%d web_port=%d protocol_port=%d worker_pids=%s%s",
                    options.workers, group.addresses[0][1], group.addresses[1][1],
                    ",".join(str(process.pid) for process in group.pool.processes),
                    f" static_targets={len(options.southbound_routes.targets)}"
                    if options.southbound_routes is not None else "")
        closed = asyncio.create_task(group.wait())
        tasks.append(closed)
        done, _ = await asyncio.wait((closed, shutdown), return_when=asyncio.FIRST_COMPLETED)
        if closed in done:
            await closed
    finally:
        for task in tasks:
            task.cancel()
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await group.stop()
