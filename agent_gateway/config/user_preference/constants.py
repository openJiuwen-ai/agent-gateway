# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

from typing import Literal

USER_PREFERENCE_STORE = "user_preference"
DEFAULT_USER_ID = "__default__"
PREFERRED_LANGUAGE_VALUES = frozenset({"zh", "en"})
USER_PREFERENCE_CACHE_TTL_SECONDS = 60.0

PreferredLanguage = Literal["zh", "en"]


def normalize_preferred_language(raw: object) -> PreferredLanguage | None:
    if not isinstance(raw, str):
        return None
    lang = raw.strip().lower()
    if lang in PREFERRED_LANGUAGE_VALUES:
        return lang  # type: ignore[return-value]
    return None
