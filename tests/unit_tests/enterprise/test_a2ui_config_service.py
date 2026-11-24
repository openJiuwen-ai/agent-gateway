# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""a2ui_config Manager sync service normalize / delete shape."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent_gateway.enterprise.config.manager_config.core.application_config.a2ui_config import (
    A2UIConfigService,
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
            "a2ui_config": {
                "agent_template_id": "tpl-1",
                "enabled": True,
            }
        }
    )
    assert row["agent_template_id"] == "tpl-1"
    assert row["enabled"] is True
    assert row["protocol_version"] == "0.8"
    assert row["stream_validation_enabled"] is True
    assert row["dev_smoke_tools_enabled"] is False
    assert row["data"] is None


def test_normalize_payload_rejects_bad_protocol() -> None:
    with pytest.raises(ValueError, match="protocol_version"):
        _normalize_payload(
            {"agent_template_id": "tpl-1", "protocol_version": "0.9"}
        )


def test_normalize_payload_rejects_bad_bool() -> None:
    with pytest.raises(ValueError, match="enabled"):
        _normalize_payload({"agent_template_id": "tpl-1", "enabled": "maybe"})


def test_normalize_payload_rejects_bad_data() -> None:
    with pytest.raises(ValueError, match="data"):
        _normalize_payload({"agent_template_id": "tpl-1", "data": ["not", "object"]})


@pytest.mark.asyncio
async def test_delete_not_found() -> None:
    repo = MagicMock()
    repo.delete = AsyncMock(return_value=False)
    with patch(
        "agent_gateway.enterprise.config.manager_config.core.application_config.a2ui_config.require_enterprise_repository",
        return_value=repo,
    ):
        with pytest.raises(ValueError, match="not found"):
            await A2UIConfigService().delete("missing-tpl")
