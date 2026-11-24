# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""用户习惯表读写：Gateway DB + 本机懒缓存；poll 仅点查缓存内用户。"""

from __future__ import annotations

import logging
from typing import Any

from agent_gateway.storage.access import get_persistent_store

from .cache import UserPreferenceCache, get_user_preference_cache
from .constants import (
    DEFAULT_USER_ID,
    USER_PREFERENCE_STORE,
    normalize_preferred_language,
)

logger = logging.getLogger(__name__)

_UNSET = object()


def normalize_extras(raw: object) -> dict[str, Any] | None:
    """校验 extras：``None`` 或对象；禁止把 ``preferred_language`` 塞进 extras。"""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError("extras must be a JSON object or null")
    if "preferred_language" in raw:
        raise ValueError("preferred_language must not be stored in extras")
    return dict(raw)


class UserPreferenceService:
    def __init__(self, cache: UserPreferenceCache | None = None) -> None:
        self._cache = cache or get_user_preference_cache()

    def _require_store(self):
        store = get_persistent_store()
        if store is None:
            raise RuntimeError("PersistentStore not wired")
        return store

    @staticmethod
    def _yaml_fallback_language() -> str:
        try:
            from agent_gateway.common.config import get_config

            lang = normalize_preferred_language(
                (get_config() or {}).get("preferred_language")
            )
            if lang:
                return lang
        except Exception:  # noqa: BLE001
            logger.debug("read yaml preferred_language failed", exc_info=True)
        return "en"

    async def get_language(self, user_id: str | None) -> str:
        """用户行 → ``__default__`` → yaml → ``en``；命中缓存会刷新 last_used_at。"""
        uid = str(user_id or "").strip()
        if uid and uid != DEFAULT_USER_ID:
            cached = self._cache.get(uid, touch=True)
            if cached:
                return cached
            row = await self._db_get(uid)
            lang = normalize_preferred_language(
                (row or {}).get("preferred_language") if row else None
            )
            if lang:
                self._cache.put(uid, lang, touch=True)
                return lang

        cached_default = self._cache.get(DEFAULT_USER_ID, touch=True)
        if cached_default:
            return cached_default
        default_row = await self._db_get(DEFAULT_USER_ID)
        default_lang = normalize_preferred_language(
            (default_row or {}).get("preferred_language") if default_row else None
        )
        if default_lang:
            self._cache.put(DEFAULT_USER_ID, default_lang, touch=True)
            return default_lang
        return self._yaml_fallback_language()

    async def upsert_user_language(
        self,
        user_id: str,
        language: str,
        *,
        extras: Any = _UNSET,
    ) -> str:
        """用户写自己的偏好；禁止写 ``__default__``。"""
        uid = str(user_id or "").strip()
        if not uid:
            raise ValueError("user_id is required")
        if uid == DEFAULT_USER_ID:
            raise ValueError("user_id must not be __default__")
        lang = normalize_preferred_language(language)
        if lang is None:
            raise ValueError("preferred_language must be zh or en")
        await self._db_upsert(uid, lang, extras=extras)
        self._cache.put(uid, lang, touch=True)
        return lang

    async def upsert_default_language(
        self,
        language: str,
        *,
        extras: Any = _UNSET,
    ) -> str:
        """仅 Manager 路径写集群默认行。

        ``extras`` 未传时不覆盖库内已有 extras；显式传入（含 ``null``）才更新。
        """
        lang = normalize_preferred_language(language)
        if lang is None:
            raise ValueError("preferred_language must be zh or en")
        await self._db_upsert(DEFAULT_USER_ID, lang, extras=extras)
        self._cache.put(DEFAULT_USER_ID, lang, touch=True)
        return lang

    async def poll_refresh_cached(self) -> None:
        """先按使用 TTL 淘汰，再批量点查仍在缓存中的 user_id 刷新语言（不续命）。"""
        self._cache.evict_expired()
        ids = self._cache.keys()
        if not ids:
            return
        store = get_persistent_store()
        if store is None:
            logger.debug("[UserPreference] poll skipped: PersistentStore not wired")
            return
        await store.ensure_ready()
        # 分片 IN：PersistentStore.list 仅等值 filters，逐条 get 更稳妥
        for user_id in ids:
            try:
                row = await store.get(USER_PREFERENCE_STORE, {"user_id": user_id})
            except Exception:  # noqa: BLE001
                logger.exception(
                    "[UserPreference] poll get failed user_id=%s", user_id
                )
                continue
            lang = normalize_preferred_language(
                (row or {}).get("preferred_language") if row else None
            )
            if lang:
                self._cache.put(user_id, lang, touch=False)
                continue
            # 行缺失：丢掉缓存，避免长期脏值
            self._cache.delete(user_id)

    async def _db_get(self, user_id: str) -> dict[str, Any] | None:
        store = get_persistent_store()
        if store is None:
            return None
        await store.ensure_ready()
        try:
            return await store.get(USER_PREFERENCE_STORE, {"user_id": user_id})
        except Exception:  # noqa: BLE001
            logger.exception("[UserPreference] db get failed user_id=%s", user_id)
            return None

    @staticmethod
    def _utc_now():
        from datetime import datetime, timezone

        return datetime.now(timezone.utc)

    async def _db_upsert(
        self,
        user_id: str,
        language: str,
        *,
        extras: Any = _UNSET,
    ) -> dict[str, Any]:
        store = self._require_store()
        await store.ensure_ready()
        key = {"user_id": user_id}
        now = self._utc_now()
        extras_provided = extras is not _UNSET
        extras_value = normalize_extras(extras) if extras_provided else None
        existing = await store.get(USER_PREFERENCE_STORE, key)
        if existing is None:
            record: dict[str, Any] = {
                "user_id": user_id,
                "preferred_language": language,
                "revision": 1,
                "created_at": now,
                "updated_at": now,
                "extras": extras_value if extras_provided else None,
            }
            return await store.create(USER_PREFERENCE_STORE, record)
        updates: dict[str, Any] = {
            "preferred_language": language,
            "updated_at": now,
        }
        if extras_provided:
            updates["extras"] = extras_value
        try:
            rev = int(existing.get("revision") or 0)
            updates["revision"] = rev + 1
        except (TypeError, ValueError):
            updates["revision"] = 1
        updated = await store.update(USER_PREFERENCE_STORE, key, updates)
        if updated is None:
            # 并发删除后重试 create
            return await store.create(
                USER_PREFERENCE_STORE,
                {
                    "user_id": user_id,
                    "preferred_language": language,
                    "revision": 1,
                    "created_at": now,
                    "updated_at": now,
                    "extras": extras_value if extras_provided else None,
                },
            )
        return updated


_service: UserPreferenceService | None = None


def get_user_preference_service() -> UserPreferenceService:
    global _service
    if _service is None:
        _service = UserPreferenceService()
    return _service


__all__ = (
    "UserPreferenceService",
    "get_user_preference_service",
    "normalize_extras",
)
