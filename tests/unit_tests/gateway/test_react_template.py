# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved
"""react_template 网关 Config Receiver 纯函数单测：body 传输 → data 落库映射。"""

from __future__ import annotations

import pytest

from agent_gateway.enterprise.config.manager_config.core.template.react_template import (
    _build_row_from_template,
    _normalize_data,
    _normalize_template_id,
)

pytestmark = pytest.mark.unit


def test_normalize_data_accepts_dict_and_none():
    assert _normalize_data(None) == {}
    assert _normalize_data({"enable_task_loop": True}) == {"enable_task_loop": True}


def test_normalize_data_rejects_non_dict():
    with pytest.raises(ValueError):
        _normalize_data("not-a-dict")
    with pytest.raises(ValueError):
        _normalize_data(["a", "b"])


def test_normalize_template_id():
    assert _normalize_template_id("tpl-1") == "tpl-1"
    assert _normalize_template_id("  tpl-1  ") == "tpl-1"
    with pytest.raises(ValueError):
        _normalize_template_id("")
    with pytest.raises(ValueError):
        _normalize_template_id("x" * 101)


def test_build_row_maps_body_to_data():
    sentinel = object()
    body = {
        "agent_name": "main_agent",
        "enable_task_loop": True,
        "subagents": {"general_agent": {"enabled": True}},
        "context_engine_config": {"enabled": True},
    }
    row = _build_row_from_template(
        {
            "template_id": "tpl-1",
            "template_name": "react-tpl",
            "description": "desc",
            "body": body,
        },
        now=sentinel,
    )
    assert row["template_id"] == "tpl-1"
    assert row["template_name"] == "react-tpl"
    assert row["description"] == "desc"
    assert row["enabled"] is True
    assert row["data"] == body
    assert row["created_at"] is sentinel
    assert row["updated_at"] is sentinel


def test_build_row_defaults_when_body_missing():
    sentinel = object()
    row = _build_row_from_template(
        {"template_id": "tpl-2", "template_name": "react-tpl", "enabled": False},
        now=sentinel,
    )
    assert row["data"] == {}
    assert row["enabled"] is False
