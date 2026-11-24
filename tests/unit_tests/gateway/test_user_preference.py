# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Gateway user_preference 懒缓存与 Service 行为。"""

from __future__ import annotations

import time
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

    assert await svc.get_language("u1") == "en"

    store.rows[DEFAULT_USER_ID] = {
        "user_id": DEFAULT_USER_ID,
        "preferred_language": "zh",
        "revision": 1,
    }
    assert await svc.get_language("u1") == "zh"

    store.rows["u1"] = {"user_id": "u1", "preferred_language": "en", "revision": 1}
    cache.clear()
    assert await svc.get_language("u1") == "en"


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

    await svc.upsert_user_language("bob", "zh")
    entry_before = cache._entries["bob"].last_used_at
    store.rows["bob"]["preferred_language"] = "en"
    time.sleep(0.01)
    await svc.poll_refresh_cached()
    assert cache.get("bob", touch=False) == "en"
    assert cache._entries["bob"].last_used_at == entry_before


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
