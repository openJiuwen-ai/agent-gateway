# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""SectionDocumentRepository 整段 upsert 的共享行为。"""

from __future__ import annotations

from typing import Any

import pytest

from agent_gateway.config.memory.repository import MemoryConfigRepository
from agent_gateway.config.permissions.repository import PermissionsConfigRepository
from agent_gateway.config.section import (
    DbBodySectionCodec,
    SectionDocumentRepository,
    YamlSectionCodec,
)


class _DictPersistentStore:
    def __init__(self) -> None:
        self.rows: dict[str, list[dict[str, Any]]] = {}

    def _table(self, name: str) -> list[dict[str, Any]]:
        return self.rows.setdefault(name, [])

    async def get(self, name: str, key: dict[str, Any]) -> dict[str, Any] | None:
        for row in self._table(name):
            if all(row.get(field) == value for field, value in key.items()):
                return dict(row)
        return None

    async def list(self, name: str, **_: Any) -> list[dict[str, Any]]:
        return [dict(row) for row in self._table(name)]

    async def create(self, name: str, record: dict[str, Any]) -> dict[str, Any]:
        row = dict(record)
        self._table(name).append(row)
        return dict(row)

    async def update(
        self,
        name: str,
        key: dict[str, Any],
        updates: dict[str, Any],
    ) -> dict[str, Any] | None:
        for row in self._table(name):
            if all(row.get(field) == value for field, value in key.items()):
                row.update(updates)
                return dict(row)
        return None

    async def delete(self, name: str, key: dict[str, Any]) -> bool:
        rows = self._table(name)
        for index, row in enumerate(rows):
            if all(row.get(field) == value for field, value in key.items()):
                rows.pop(index)
                return True
        return False


@pytest.mark.asyncio
async def test_section_upsert_body_writes_body_and_increments_revision() -> None:
    store = _DictPersistentStore()
    repo = SectionDocumentRepository(store, DbBodySectionCodec(), "memory_config")

    first = await repo.upsert_body({"enabled": True, "limit": 1})
    second = await repo.upsert_body(
        {"enabled": False, "limit": 2},
        source="manager-config",
    )

    assert first.source == "manager"
    assert first.revision == 1
    assert first.body == {"enabled": True, "limit": 1}
    assert second.revision == 2
    assert second.source == "manager-config"
    assert second.body == {"enabled": False, "limit": 2}
    assert await repo.get_body() == {"enabled": False, "limit": 2}


@pytest.mark.asyncio
async def test_memory_repository_delegates_body_upsert() -> None:
    repo = MemoryConfigRepository(
        _DictPersistentStore(),
        DbBodySectionCodec(),
    )

    saved = await repo.upsert_body({"enabled": True})
    updated = await repo.upsert_body({"enabled": False})

    assert saved.revision == 1
    assert saved.body == {"enabled": True}
    assert saved.source == "manager"
    assert updated.revision == 2
    assert updated.body == {"enabled": False}
    assert await repo.get_body() == {"enabled": False}


@pytest.mark.asyncio
async def test_permissions_repository_delegates_body_upsert() -> None:
    repo = PermissionsConfigRepository(_DictPersistentStore(), YamlSectionCodec())

    await repo.upsert_body({"enabled": True})
    await repo.upsert_body({"enabled": False})

    assert await repo.get_body() == {"enabled": False}
