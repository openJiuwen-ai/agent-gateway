# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Small retry helpers shared across gateway components."""

from __future__ import annotations

import asyncio
import random

#: Jitter bounds relative to the caller-supplied base delay. Average delay is
#: unchanged; only the retry instants are spread out.
JITTER_LOW = 0.5
JITTER_HIGH = 1.5


def jittered_delay(
    base: float,
    *,
    low: float = JITTER_LOW,
    high: float = JITTER_HIGH,
) -> float:
    """Return a randomized delay in ``[base * low, base * high]``."""
    value = max(0.0, float(base))
    return value * random.uniform(float(low), float(high))


async def jittered_sleep(base: float) -> None:
    """Sleep for approximately *base* seconds, with ``[0.5, 1.5]`` jitter."""
    await asyncio.sleep(jittered_delay(base))


__all__ = ["JITTER_LOW", "JITTER_HIGH", "jittered_delay", "jittered_sleep"]
