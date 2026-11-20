# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Normalisation shared by every event-name lookup.

``EventType`` is a plain ``Enum`` (not a ``str`` subclass), so a bare
``isinstance(..., str)`` check misses its members; ``str``-mixin enums take the
other path and rely on hash/eq details.  Resolving ``value`` first keeps both
paths behaviourally identical.
"""

from typing import Any


def event_name_text(value: Any) -> str:
    """Return the wire name of an event type, or "" when it has none.

    Accepts plain enums, ``str``-mixin enums and bare strings.
    """
    name = getattr(value, "value", None)
    if isinstance(name, str) and name:
        return name
    return value if isinstance(value, str) else ""
