# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""advanced_config Manager sync service normalize / upsert shape."""

from __future__ import annotations

import pytest

from agent_gateway.enterprise.config.manager_config.core.application_config.advanced_config import (
    _normalize_payload,
    _require_agent_template_id,
)


def test_require_agent_template_id() -> None:
    assert _require_agent_template_id(" tpl-1 ") == "tpl-1"
    with pytest.raises(ValueError):
        _require_agent_template_id("")
    with pytest.raises(ValueError):
        _require_agent_template_id("x" * 101)


def test_normalize_payload_defaults_and_nested() -> None:
    row = _normalize_payload(
        {
            "advanced_config": {
                "agent_template_id": "tpl-1",
                "a4p_enabled": True,
            }
        }
    )
    assert row["agent_template_id"] == "tpl-1"
    assert row["a4p_enabled"] is True
    assert row["progressive_tool_enabled"] is True
    assert row["progressive_tool_optimize"] is False
    assert row["auto_memory_enabled"] is False
    assert row["enable_kv_cache_affinity"] is False
    assert row["data"] is None


def test_normalize_payload_rejects_bad_bool() -> None:
    with pytest.raises(ValueError, match="a4p_enabled"):
        _normalize_payload({"agent_template_id": "tpl-1", "a4p_enabled": "maybe"})
