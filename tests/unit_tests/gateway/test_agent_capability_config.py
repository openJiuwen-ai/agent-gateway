# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Agent 模板能力开关：一张表一个功能，主键是模板编号。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from agent_gateway.config.enterprise.catalog import ENTERPRISE_RECORD_SPECS
from agent_gateway.config.enterprise.tables.table_init import ALL_TABLE_DEFINITIONS
from agent_gateway.enterprise.config.manager_config.core.application_config.agent_capability_config import (
    CAPABILITY_TABLES,
    capability_body,
)
from agent_gateway.enterprise.config.manager_config.schemas.application_config_schemas import (
    SymphonyCapabilityUpsertRequest,
)


def test_capability_tables_are_one_row_per_agent_template() -> None:
    definitions = {table.table_name: table for table in ALL_TABLE_DEFINITIONS}
    assert set(CAPABILITY_TABLES.values()) == {
        "a2ui_config",
        "a4p_config",
        "symphony_config",
        "auto_memory_config",
        "proactive_recommendation_config",
    }
    for table_name in CAPABILITY_TABLES.values():
        assert ENTERPRISE_RECORD_SPECS[table_name].key_fields == ("agent_template_id",)
        columns = {column.name for column in definitions[table_name].columns}
        assert {"agent_template_id", "body", "revision"}.issubset(columns)


def test_capability_body_keeps_switches_only() -> None:
    assert capability_body(
        {"agent_template_id": "tpl-1", "enabled": True, "protocol_version": None}
    ) == {"enabled": True}


def test_capability_body_rejects_template_id_alone() -> None:
    with pytest.raises(ValueError, match="at least one field"):
        capability_body({"agent_template_id": "tpl-1"})


def test_symphony_request_rejects_local_paths() -> None:
    with pytest.raises(ValidationError):
        SymphonyCapabilityUpsertRequest.model_validate(
            {
                "agent_template_id": "tpl-1",
                "enabled": True,
                "paths": {"skills_root": "/tmp/skills"},
            }
        )
