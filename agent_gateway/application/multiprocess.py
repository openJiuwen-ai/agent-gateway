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

from agent_gateway.application.worker_group import WorkerGroup
from agent_gateway.common.agentos_runtime import is_agentos_runtime
from agent_gateway.enterprise import is_enterprise
from agent_gateway.processes.capabilities import management_preflight

DEFAULT_WORKERS = 2
logger = logging.getLogger("gateway")


@dataclass(frozen=True)
class MultiprocessOptions:
    workers: int
    web_port: int
    protocol_port: int

    @classmethod
    def from_cli(cls, *, workers: int | None, web_host: str, web_port: int,
                 web_path: str, web_ws_deflate: bool,
                 required_client_type: str | tuple[str, ...] | None) -> MultiprocessOptions:
        count = DEFAULT_WORKERS if workers is None else workers
        if not 1 <= count <= 32:
            raise ValueError("--workers must be between 1 and 32")
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
        return cls(count, web_port, protocol_port)


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


async def validate_workspace() -> None:
    from agent_gateway.common.config import get_config
    from agent_gateway.common.cron_settings import load_cron_store_settings

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
    if settings.backend != "file" or await asyncio.to_thread(_has_enabled_cron_jobs, settings.file_path):
        raise ValueError("Cron is not supported by this trial; use single mode")


async def run_multiprocess(options: MultiprocessOptions, agent_server_url: str,
                           shutdown_event: asyncio.Event) -> None:
    await validate_workspace()
    if shutdown_event.is_set():
        return
    group = WorkerGroup(agent_server_url, workers=options.workers, web_port=options.web_port,
                        protocol_port=options.protocol_port)
    shutdown = asyncio.create_task(shutdown_event.wait())
    startup = asyncio.create_task(group.start())
    tasks = [shutdown, startup]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        if startup in done:
            await startup
        if shutdown.done():
            return
        logger.info("[App] multiprocess_ready workers=%d web_port=%d protocol_port=%d worker_pids=%s",
                    options.workers, group.addresses[0][1], group.addresses[1][1],
                    ",".join(str(process.pid) for process in group.pool.processes))
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
