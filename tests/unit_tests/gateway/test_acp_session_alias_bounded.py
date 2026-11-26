# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""ACP 外部会话别名映射必须有界，且出站反查不能逐帧全表扫描。

``_acp_session_aliases`` 以客户端给的 external_session_id 为键，只在首次
建立会话时写入，此前既没有删除也没有上限；``_resolve_acp_external_session_id``
又在每条出站 chunk 上线性遍历它做反向查询。长跑网关下内存和 CPU 都随
ACP 会话数线性增长。
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from agent_gateway.message_handler import message_handler as mh
from agent_gateway.message_handler.message_handler import MessageHandler
from tests._support.null_session_login_bindings import NullSessionLoginBindings


def _handler() -> MessageHandler:
    handler = MessageHandler(
        SimpleNamespace(),
        session_login_bindings=NullSessionLoginBindings(),
    )
    # MessageHandler 是单例，逐用例重置别名状态。
    handler._acp_session_aliases.clear()
    handler._acp_session_alias_reverse.clear()
    return handler


def test_alias_map_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mh, "_ACP_SESSION_ALIAS_LIMIT", 3)
    handler = _handler()

    for index in range(5):
        handler._remember_acp_session_alias(f"ext-{index}", f"int-{index}")

    assert list(handler._acp_session_aliases) == ["ext-2", "ext-3", "ext-4"]


def test_rewriting_alias_refreshes_its_position() -> None:
    handler = _handler()
    handler._remember_acp_session_alias("ext-a", "int-a")
    handler._remember_acp_session_alias("ext-b", "int-b")
    handler._remember_acp_session_alias("ext-a", "int-a2")

    assert list(handler._acp_session_aliases) == ["ext-b", "ext-a"]
    assert handler._acp_session_aliases["ext-a"] == "int-a2"


def test_empty_ids_are_not_recorded() -> None:
    handler = _handler()

    handler._remember_acp_session_alias("", "int-a")
    handler._remember_acp_session_alias("ext-a", "")

    assert not handler._acp_session_aliases
    assert not handler._acp_session_alias_reverse


def test_reverse_lookup_returns_original_external_id() -> None:
    handler = _handler()
    handler._remember_acp_session_alias("ext-a", "int-a")

    assert handler._resolve_acp_external_session_id("int-a") == "ext-a"


def test_eviction_drops_stale_reverse_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mh, "_ACP_SESSION_ALIAS_LIMIT", 1)
    handler = _handler()
    handler._remember_acp_session_alias("ext-old", "int-old")
    handler._remember_acp_session_alias("ext-new", "int-new")

    # 已淘汰的内部 id 反查不到，退化成原值返回。
    assert handler._resolve_acp_external_session_id("int-old") == "int-old"
    assert handler._resolve_acp_external_session_id("int-new") == "ext-new"


def test_explicit_metadata_still_wins() -> None:
    handler = _handler()
    handler._remember_acp_session_alias("ext-a", "int-a")

    resolved = handler._resolve_acp_external_session_id(
        "int-a", {mh._ACP_ORIGINAL_SESSION_ID_KEY: "ext-from-metadata"}
    )

    assert resolved == "ext-from-metadata"


def test_direct_write_is_still_resolved_by_fallback_scan() -> None:
    """兼容直接往 dict 写入的旧路径（反查索引里没有时退回全表扫描）。"""
    handler = _handler()
    handler._acp_session_aliases["ext-legacy"] = "int-legacy"

    assert handler._resolve_acp_external_session_id("int-legacy") == "ext-legacy"
