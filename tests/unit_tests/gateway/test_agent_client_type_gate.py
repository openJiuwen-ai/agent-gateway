# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""已移除的 ``gateway.agent_client.type`` 必须启动即报错，不静默回落 WebSocket."""

from __future__ import annotations

import pytest

from agent_gateway.app_gateway import _reject_removed_agent_client_type


def test_removed_yuanrong_type_raises_with_migration_hint() -> None:
    with pytest.raises(RuntimeError, match=r"type=yuanrong 已移除.*agentos_router") as exc_info:
        _reject_removed_agent_client_type({"type": "yuanrong"})
    assert "websocket" in str(exc_info.value)


def test_removed_type_match_is_case_insensitive_and_trimmed() -> None:
    with pytest.raises(RuntimeError, match="type=yuanrong"):
        _reject_removed_agent_client_type({"type": " YuanRong "})


@pytest.mark.parametrize("client_type", ["websocket", "agentos_router", "adx_router", ""])
def test_supported_types_pass(client_type: str) -> None:
    _reject_removed_agent_client_type({"type": client_type})


def test_absent_type_section_passes() -> None:
    _reject_removed_agent_client_type({})
