# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""memory_config 进程入口。

覆盖 ``config.yaml`` 的 ``/memory`` 段（含 ``forbidden_memory_definition``）。
``modes.claw.*.memory`` 与顶层 ``auto_memory_enabled`` 不在本 store，仍走
``common.config``。
"""

from __future__ import annotations

import asyncio
from typing import Any

from agent_gateway.config.memory.repository import MemoryConfigRepository
from agent_gateway.storage.async_bridge import run_awaitable

_repo: MemoryConfigRepository | None = None


def set_memory_config_repository(repo: MemoryConfigRepository | None) -> None:
    global _repo
    _repo = repo


def get_memory_config_repository() -> MemoryConfigRepository | None:
    return _repo


def clear_memory_config_repository() -> None:
    set_memory_config_repository(None)


def schedule_memory_config(awaitable: Any) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        run_awaitable(awaitable)
        return
    loop.create_task(awaitable)


def _require_repository() -> MemoryConfigRepository:
    repo = get_memory_config_repository()
    if repo is None:
        raise RuntimeError("MemoryConfigRepository is not wired")
    return repo


async def get_memory_body_in_config() -> dict[str, Any]:
    return await _require_repository().get_body()


async def replace_memory_in_config(body: dict[str, Any]) -> None:
    """整段替换 ``memory``（EE Manager upsert 语义）。"""
    if not isinstance(body, dict):
        raise ValueError("memory body must be an object")
    await _require_repository().replace(body)


async def merge_memory_in_config(updates: dict[str, Any]) -> None:
    """浅合并 ``memory`` 顶层字段。"""
    if not isinstance(updates, dict):
        raise ValueError("memory updates must be an object")
    await _require_repository().merge(updates)


async def delete_memory_in_config() -> bool:
    """删除 ``memory`` 段 / 企业行（EE Manager delete 语义）。"""
    return await _require_repository().delete()


async def update_memory_forbidden_enabled_in_config(value: bool) -> None:
    await _require_repository().set_forbidden_enabled(value)


async def update_memory_forbidden_description_in_config(
    description: dict[str, str],
) -> None:
    await _require_repository().merge_forbidden_description(description)


async def update_memory_forbidden_in_config(updates: dict[str, Any]) -> None:
    await _require_repository().merge_forbidden(updates)


__all__ = [
    "clear_memory_config_repository",
    "delete_memory_in_config",
    "get_memory_body_in_config",
    "get_memory_config_repository",
    "merge_memory_in_config",
    "replace_memory_in_config",
    "schedule_memory_config",
    "set_memory_config_repository",
    "update_memory_forbidden_description_in_config",
    "update_memory_forbidden_enabled_in_config",
    "update_memory_forbidden_in_config",
]
