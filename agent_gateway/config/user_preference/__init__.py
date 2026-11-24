# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""企业版用户习惯偏好（Gateway DB ``user_preference``）。"""

from .constants import (
    DEFAULT_USER_ID,
    PREFERRED_LANGUAGE_VALUES,
    USER_PREFERENCE_CACHE_TTL_SECONDS,
    USER_PREFERENCE_STORE,
    normalize_preferred_language,
)
from .service import (
    UserPreferenceService,
    get_user_preference_service,
    normalize_extras,
)

__all__ = (
    "DEFAULT_USER_ID",
    "PREFERRED_LANGUAGE_VALUES",
    "USER_PREFERENCE_CACHE_TTL_SECONDS",
    "USER_PREFERENCE_STORE",
    "UserPreferenceService",
    "get_user_preference_service",
    "normalize_extras",
    "normalize_preferred_language",
)
