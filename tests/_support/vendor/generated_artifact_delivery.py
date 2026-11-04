# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Test-only copy of a stateless delivery helper.

Copied from ``jiuwenswarm/agents/harness/common/rails/permissions/generated_artifact_delivery.py``
at jiuwenswarm ``f6bea5ab3``. The production module pulls in server-side
permission machinery; only this pure normalization function is needed by the
gateway file-fanout test.
"""

from __future__ import annotations

import json
from typing import Any


def normalize_send_file_target_channels(value: Any) -> tuple[str, ...]:
    """Return the canonical delivery target set for a send invocation."""
    parsed = value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ()
        try:
            parsed = json.loads(stripped)
        except (TypeError, ValueError):
            parsed = stripped
    if isinstance(parsed, str):
        candidates = (parsed,)
    elif isinstance(parsed, (list, tuple)):
        candidates = tuple(parsed)
    elif parsed is None:
        candidates = ()
    else:
        candidates = (parsed,)
    return tuple(
        sorted(
            {
                str(candidate).strip()
                for candidate in candidates
                if str(candidate).strip()
            }
        )
    )
