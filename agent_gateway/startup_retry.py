# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Startup retry helpers for Gateway's AgentServer and WebChannel connections."""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger("gateway")


async def _connect_with_retry(
        client,
        uri: str,
        *,
        max_retries: int = 20,
        interval: float = 3.0,
) -> None:
    """连接 AgentServer, 失败重试.

    相比固定 3s 间隔: 先做轻量 TCP 端口探测, 端口通了再 ws 握手, 避免 AgentServer
    尚未 listen 时反复走完整 WS 握手; 重试间隔改为指数退避 (0.2s 起, 翻倍至上限
    ``interval``), 让 AgentServer 一旦 listen 即在亚秒级连上, 而非硬等下一个 3s 节拍.
    """
    parsed = urlparse(uri)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)

    def _tcp_ready(timeout: float = 0.5) -> bool:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            return False

    # Desktop always connects to a loopback AgentServer it has just spawned.
    # Use a short local probe cadence there; retain the existing conservative
    # exponential backoff for remote AgentServer deployments.
    local_agent = host in {"127.0.0.1", "localhost", "::1"}
    backoff = 0.05 if local_agent else 0.2
    max_backoff = min(interval, 0.25) if local_agent else interval
    if local_agent:
        # Allow roughly 60s for the local AgentServer to start at a 0.25s cadence.
        max_retries = max(max_retries, 240)
    for attempt in range(1, max_retries + 1):
        # 先 TCP 探测: 端口未通则不浪费一次 WS 握手, 直接进入退避等待.
        if not _tcp_ready(timeout=0.05 if local_agent else 0.5):
            if attempt >= max_retries:
                logger.error(
                    "[App] connect AgentServer failed after %d tries: port %s not listening  uri=%s",
                    attempt, port, uri,
                )
                raise ConnectionRefusedError(f"AgentServer port {port} not listening")
            logger.info(
                "[App] AgentServer port %s not listening yet (%d/%d), retry in %.2fs...",
                port, attempt, max_retries, backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            continue
        try:
            await client.connect(uri)
            logger.info("[App] connected to AgentServer: %s", uri)
            return
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_retries:
                logger.error(
                    "[App] connect AgentServer failed after %d tries: %s  last=%s",
                    attempt,
                    uri,
                    exc,
                )
                raise
            logger.warning(
                "[App] connect AgentServer failed (%d/%d): %s  retry in %.2fs...",
                attempt,
                max_retries,
                exc,
                backoff,
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


async def _wait_for_web_channel_listening(
    channel: Any,
    task: asyncio.Task[None],
    *,
    timeout: float = 5.0,
) -> None:
    """Wait until WebChannel has actually bound its listening socket.

    ``WebChannel.start`` logs before ``uvicorn.Server.serve`` completes its
    startup, so merely scheduling the task is not sufficient to make 19000
    available.  Desktop and the static proxy both rely on that port during
    cold start; bind it before the potentially expensive channel config pass.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if task.done():
            task.result()
            raise RuntimeError("WebChannel stopped before it began listening")
        uvicorn_server = getattr(channel, "_uvicorn_server", None)
        if bool(getattr(uvicorn_server, "started", False)) or getattr(channel, "_server", None) is not None:
            logger.info(
                "[App] WebChannel listening: ws://%s:%s%s",
                channel.config.host,
                channel.config.port,
                channel.config.path,
            )
            return
        await asyncio.sleep(0.01)
    raise TimeoutError(f"WebChannel did not bind its listening socket within {timeout:.0f}s")
