# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Test-only shim for the stateless part of ``SendFileToolkit``.

Copied from ``jiuwenswarm/agents/harness/common/tools/send_file_to_user.py``
at jiuwenswarm ``f6bea5ab3``. Only ``_normalize_target_channels`` is needed by
the gateway file-fanout test; the real class depends on AgentServer rails.
"""

from __future__ import annotations

from typing import Any

from tests._support.vendor.generated_artifact_delivery import (
    normalize_send_file_target_channels,
)


class SendFileToolkit:
    """Minimal test-only surface used by the gateway file fanout tests."""

    @staticmethod
    def _normalize_target_channels(target_channels: Any) -> list[str]:
        return list(normalize_send_file_target_channels(target_channels))
