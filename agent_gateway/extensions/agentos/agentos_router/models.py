# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any


class AgentStatus(str, Enum):
    CREATING = "creating"
    READY = "ready"
    FAILED = "failed"
    DELETED = "deleted"


# Runtime lifecycle transitions that are legal by construction. ``DELETED`` is a
# terminal snapshot (it never stays in ``AgentManager``), and ``FAILED`` has no
# way back to ``READY`` or ``CREATING``.
_ALLOWED_STATUS_TRANSITIONS: dict[AgentStatus, frozenset[AgentStatus]] = {
    AgentStatus.CREATING: frozenset(
        {AgentStatus.READY, AgentStatus.FAILED, AgentStatus.DELETED}
    ),
    AgentStatus.READY: frozenset({AgentStatus.DELETED}),
    AgentStatus.FAILED: frozenset({AgentStatus.DELETED}),
    AgentStatus.DELETED: frozenset(),
}


def is_allowed_status_transition(
    current: AgentStatus, target: AgentStatus,
) -> bool:
    """Return whether ``current -> target`` is an allowed runtime transition."""
    return target in _ALLOWED_STATUS_TRANSITIONS.get(current, frozenset())


@dataclass
class ImageInfo:
    image_name: str
    image_uri: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentInfo:
    user_id: str
    agent_type: str
    agent_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: AgentStatus = AgentStatus.CREATING
    sandbox_id: str | None = None
    public_key: str | None = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def copy(self) -> AgentInfo:
        return replace(self, metadata=dict(self.metadata))
