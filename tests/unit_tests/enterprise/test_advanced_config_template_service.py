# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved

"""advanced_config_template 开关校验与建模默认值。"""

from __future__ import annotations

import pytest

pytest.importorskip("e2a")

from agent_gateway.enterprise.config.manager_config.core.template.advanced_config_template import (
    _build_row_from_template,
    _merge_switches_for_create,
)
from agent_gateway.enterprise.config.manager_config.schemas.advanced_config_fields import (
    DEFAULT_ADVANCED_CONFIG_SWITCHES,
    normalize_advanced_config_switches,
    validate_advanced_config_data,
)


def test_merge_switches_for_create_defaults() -> None:
    switches = _merge_switches_for_create({})
    assert switches == DEFAULT_ADVANCED_CONFIG_SWITCHES


def test_merge_switches_for_create_overrides() -> None:
    switches = _merge_switches_for_create({"a4p_enabled": True})
    assert switches["a4p_enabled"] is True
    assert switches["progressive_tool_enabled"] is True


def test_merge_switches_rejects_non_bool() -> None:
    with pytest.raises(ValueError, match="a4p_enabled"):
        _merge_switches_for_create({"a4p_enabled": "yes"})


def test_normalize_switches_partial_update_only_validates_provided() -> None:
    out = normalize_advanced_config_switches({"auto_memory_enabled": False})
    assert out == {"auto_memory_enabled": False}


def test_validate_advanced_config_data() -> None:
    assert validate_advanced_config_data(None) is None
    assert validate_advanced_config_data({}) == {}
    with pytest.raises(ValueError, match="data must be"):
        validate_advanced_config_data([])


def test_build_row_from_template_applies_defaults_and_data() -> None:
    row = _build_row_from_template(
        {
            "template_id": " ac-1 ",
            "template_name": " 高级配置 ",
            "a4p_enabled": True,
            "data": {"k": 1},
        },
        now="2026-01-01T00:00:00Z",
    )
    assert row["template_id"] == "ac-1"
    assert row["template_name"] == "高级配置"
    assert row["a4p_enabled"] is True
    assert row["progressive_tool_optimize"] is False
    assert row["data"] == {"k": 1}