# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Gateway 进程内用户偏好懒缓存（``preferred_language`` + ``last_used_at``）。"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .constants import (
    USER_PREFERENCE_CACHE_TTL_SECONDS,
    normalize_preferred_language,
)


@dataclass
class PreferenceCacheEntry:
    preferred_language: str
    last_used_at: float


class UserPreferenceCache:
    """本机懒缓存：只用使用时间做过期，不用 fetched_at。"""

    def __init__(self, *, ttl_seconds: float = USER_PREFERENCE_CACHE_TTL_SECONDS) -> None:
        self._ttl = max(1.0, float(ttl_seconds))
        self._entries: dict[str, PreferenceCacheEntry] = {}
        self._lock = threading.RLock()

    def get(self, user_id: str, *, touch: bool = True) -> str | None:
        key = str(user_id or "").strip()
        if not key:
            return None
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if now - entry.last_used_at > self._ttl:
                self._entries.pop(key, None)
                return None
            if touch:
                entry.last_used_at = now
            return entry.preferred_language

    def put(self, user_id: str, language: str, *, touch: bool = True) -> None:
        key = str(user_id or "").strip()
        lang = normalize_preferred_language(language)
        if not key or lang is None:
            return
        now = time.monotonic()
        with self._lock:
            existing = self._entries.get(key)
            last_used = now if touch or existing is None else existing.last_used_at
            self._entries[key] = PreferenceCacheEntry(
                preferred_language=lang,
                last_used_at=last_used,
            )

    def evict_expired(self, *, now: float | None = None) -> list[str]:
        """删除使用超时的条目，返回被删的 user_id。"""
        ts = time.monotonic() if now is None else float(now)
        removed: list[str] = []
        with self._lock:
            for key, entry in list(self._entries.items()):
                if ts - entry.last_used_at > self._ttl:
                    self._entries.pop(key, None)
                    removed.append(key)
        return removed

    def keys(self) -> list[str]:
        with self._lock:
            return list(self._entries.keys())

    def snapshot_for_refresh(self) -> dict[str, PreferenceCacheEntry]:
        """Capture live entries; a foreground put replaces the entry, a touch does not."""
        with self._lock:
            self.evict_expired()
            return self._entries.copy()

    def apply_refresh(self, user_id: str, expected: PreferenceCacheEntry, raw_language: object) -> None:
        language = normalize_preferred_language(raw_language)
        with self._lock:
            if self._entries.get(user_id) is not expected:
                return
            if language is None:
                del self._entries[user_id]
            else:
                self._entries[user_id] = PreferenceCacheEntry(language, expected.last_used_at)

    def delete(self, user_id: str) -> None:
        key = str(user_id or "").strip()
        if not key:
            return
        with self._lock:
            self._entries.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


_cache = UserPreferenceCache()


def get_user_preference_cache() -> UserPreferenceCache:
    return _cache


__all__ = (
    "PreferenceCacheEntry",
    "UserPreferenceCache",
    "get_user_preference_cache",
)
