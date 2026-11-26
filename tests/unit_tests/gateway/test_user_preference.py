# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Gateway user_preference 懒缓存与 Service 行为。"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from agent_gateway.config.user_preference.cache import UserPreferenceCache
from agent_gateway.config.user_preference.constants import DEFAULT_USER_ID
from agent_gateway.config.user_preference.service import UserPreferenceService


class _FakeStore:
    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    async def ensure_ready(self) -> None:
        return None

    async def get(self, name: str, key: dict[str, Any]) -> dict[str, Any] | None:
        assert name == "user_preference"
        return self.rows.get(str(key.get("user_id") or ""))

    async def create(self, name: str, record: dict[str, Any]) -> dict[str, Any]:
        uid = str(record["user_id"])
        self.rows[uid] = dict(record)
        return dict(record)

    async def update(
        self, name: str, key: dict[str, Any], updates: dict[str, Any]
    ) -> dict[str, Any] | None:
        uid = str(key.get("user_id") or "")
        current = self.rows.get(uid)
        if current is None:
            return None
        current.update(updates)
        return dict(current)


@pytest.mark.asyncio
async def test_resolve_order_user_default_yaml(monkeypatch):
    store = _FakeStore()
    cache = UserPreferenceCache(ttl_seconds=60)
    svc = UserPreferenceService(cache=cache)
    monkeypatch.setattr(
        "agent_gateway.config.user_preference.service.get_persistent_store",
        lambda: store,
    )
    monkeypatch.setattr(
        svc,
        "_yaml_fallback_language",
        lambda: "en",
    )

    # 无用户行、无默认行 → yaml；并固化为用户行
    assert await svc.get_language("u1") == "en"
    assert store.rows["u1"]["preferred_language"] == "en"

    # 之后改集群默认，已固化用户不再跟随
    store.rows[DEFAULT_USER_ID] = {
        "user_id": DEFAULT_USER_ID,
        "preferred_language": "zh",
        "revision": 1,
    }
    cache.clear()
    assert await svc.get_language("u1") == "en"

    # 显式用户行仍优先
    store.rows["u1"] = {"user_id": "u1", "preferred_language": "zh", "revision": 2}
    cache.clear()
    assert await svc.get_language("u1") == "zh"


@pytest.mark.asyncio
async def test_get_language_materializes_from_cluster_default(monkeypatch):
    store = _FakeStore()
    cache = UserPreferenceCache(ttl_seconds=60)
    svc = UserPreferenceService(cache=cache)
    monkeypatch.setattr(
        "agent_gateway.config.user_preference.service.get_persistent_store",
        lambda: store,
    )
    store.rows[DEFAULT_USER_ID] = {
        "user_id": DEFAULT_USER_ID,
        "preferred_language": "zh",
        "revision": 1,
    }

    assert await svc.get_language("alice") == "zh"
    assert store.rows["alice"]["preferred_language"] == "zh"
    assert store.rows["alice"].get("created_at") is not None

    store.rows[DEFAULT_USER_ID]["preferred_language"] = "en"
    cache.clear()
    assert await svc.get_language("alice") == "zh"


@pytest.mark.asyncio
async def test_get_language_empty_user_id_does_not_write(monkeypatch):
    store = _FakeStore()
    svc = UserPreferenceService(cache=UserPreferenceCache())
    monkeypatch.setattr(
        "agent_gateway.config.user_preference.service.get_persistent_store",
        lambda: store,
    )
    monkeypatch.setattr(svc, "_yaml_fallback_language", lambda: "en")

    assert await svc.get_language(None) == "en"
    assert await svc.get_language("") == "en"
    assert await svc.get_language(DEFAULT_USER_ID) == "en"
    assert store.rows == {}


@pytest.mark.asyncio
async def test_get_language_materialize_failure_still_returns(monkeypatch):
    store = _FakeStore()
    svc = UserPreferenceService(cache=UserPreferenceCache())
    monkeypatch.setattr(
        "agent_gateway.config.user_preference.service.get_persistent_store",
        lambda: store,
    )
    monkeypatch.setattr(svc, "_yaml_fallback_language", lambda: "en")

    async def boom(*_a, **_k):
        raise RuntimeError("db down")

    monkeypatch.setattr(svc, "_db_insert_if_absent", boom)
    assert await svc.get_language("bob") == "en"
    assert "bob" not in store.rows


@pytest.mark.asyncio
async def test_upsert_user_rejects_default(monkeypatch):
    store = _FakeStore()
    svc = UserPreferenceService(cache=UserPreferenceCache())
    monkeypatch.setattr(
        "agent_gateway.config.user_preference.service.get_persistent_store",
        lambda: store,
    )
    with pytest.raises(ValueError, match="__default__"):
        await svc.upsert_user_language(DEFAULT_USER_ID, "zh")


@pytest.mark.asyncio
async def test_upsert_user_and_default(monkeypatch):
    store = _FakeStore()
    cache = UserPreferenceCache(ttl_seconds=60)
    svc = UserPreferenceService(cache=cache)
    monkeypatch.setattr(
        "agent_gateway.config.user_preference.service.get_persistent_store",
        lambda: store,
    )

    assert await svc.upsert_user_language("alice", "zh") == "zh"
    assert store.rows["alice"]["preferred_language"] == "zh"
    assert store.rows["alice"].get("created_at") is not None
    assert store.rows["alice"].get("updated_at") is not None
    assert store.rows["alice"].get("extras") is None
    assert cache.get("alice") == "zh"

    assert await svc.upsert_default_language("en") == "en"
    assert store.rows[DEFAULT_USER_ID]["preferred_language"] == "en"
    assert cache.get(DEFAULT_USER_ID) == "en"


@pytest.mark.asyncio
async def test_poll_refresh_updates_language_without_touch(monkeypatch):
    store = _FakeStore()
    cache = UserPreferenceCache(ttl_seconds=60)
    svc = UserPreferenceService(cache=cache)
    monkeypatch.setattr(
        "agent_gateway.config.user_preference.service.get_persistent_store",
        lambda: store,
    )

    now = 10.0
    monkeypatch.setattr("agent_gateway.config.user_preference.cache.time", SimpleNamespace(monotonic=lambda: now))
    await svc.upsert_user_language("bob", "zh")
    store.rows["bob"]["preferred_language"] = "en"
    original_get = store.get

    async def read_with_concurrent_touch(name, key):
        nonlocal now
        now = 20.0
        assert cache.get("bob") == "zh"
        now = 30.0
        return await original_get(name, key)

    monkeypatch.setattr(store, "get", read_with_concurrent_touch)
    await svc.poll_refresh_cached()
    assert cache.get("bob", touch=False) == "en"
    assert cache._entries["bob"].last_used_at == 20.0


@pytest.mark.asyncio
async def test_poll_evicts_expired_before_refresh(monkeypatch):
    store = _FakeStore()
    cache = UserPreferenceCache(ttl_seconds=60)
    svc = UserPreferenceService(cache=cache)
    monkeypatch.setattr(
        "agent_gateway.config.user_preference.service.get_persistent_store",
        lambda: store,
    )
    await svc.upsert_user_language("cold", "zh")
    # 直接把使用时间回拨到过期点，避免 sleep 与 TTL 下限（cache.py: max(1.0, ...)）冲突
    cache._entries["cold"].last_used_at -= 61.0
    store.rows["cold"]["preferred_language"] = "en"
    await svc.poll_refresh_cached()
    assert "cold" not in cache.keys()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("old_row", "action", "expected"),
    [("zh", "save", "en"), (None, "save", "en"), ("zh", "evict", None)],
)
async def test_poll_discards_result_if_cache_entry_changed(monkeypatch, old_row, action, expected):
    store = _FakeStore()
    cache = UserPreferenceCache()
    svc = UserPreferenceService(cache=cache)
    monkeypatch.setattr("agent_gateway.config.user_preference.service.get_persistent_store", lambda: store)
    cache.put("alice", "zh")
    if old_row is not None:
        store.rows["alice"] = {"user_id": "alice", "preferred_language": old_row}

    read = asyncio.Event()
    release = asyncio.Event()
    original_get = store.get

    async def delayed_get(name, key):
        row = await original_get(name, key)
        result = dict(row) if row is not None else None
        if not read.is_set():
            read.set()
            await release.wait()
        return result

    monkeypatch.setattr(store, "get", delayed_get)
    async with asyncio.timeout(5), asyncio.TaskGroup() as group:
        group.create_task(svc.poll_refresh_cached())
        await read.wait()
        if action == "save":
            await svc.upsert_user_language("alice", "en")
        else:
            cache.delete("alice")
        release.set()

    assert cache.get("alice", touch=False) == expected
