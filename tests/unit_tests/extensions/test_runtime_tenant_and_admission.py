# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""租户 ID 幂等、过期只读准入、个人版 runtime 拒绝。

三个都是「配错/重复执行才暴露」的问题：信封被多次处理时租户 ID 静默漂移、
资源到期后用户连自己的历史都读不了、个人版开 runtime 后配置面板才报错。
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import hashlib

import pytest
from e2a.models import E2AEnvelope

from agent_gateway.extensions.runtime_management_extension import agent_authorization
from agent_gateway.extensions.runtime_management_extension.invoke_ids import (
    _md5_invoke_id,
    apply_invoke_ids_to_envelope,
    is_final_invoke_id,
)

# --------------------------------------------------------------------------- #
# 13：租户 ID 转换幂等
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("value", ["a" * 32, "0123456789abcdef0123456789abcdef", "A" * 32])
def test_final_id_is_recognised(value):
    assert is_final_invoke_id(value) is True


@pytest.mark.parametrize(
    "value", ["my-group", "a" * 31, "a" * 33, "g" * 32, "", "0" * 31 + "z"]
)
def test_non_final_id_is_not_recognised(value):
    assert is_final_invoke_id(value) is False


def test_final_id_is_returned_unchanged():
    final = "a" * 32
    assert _md5_invoke_id(final) == final


def test_logical_id_is_hashed():
    assert _md5_invoke_id("my-group") != "my-group"
    assert len(_md5_invoke_id("my-group")) == 32


def test_conversion_is_idempotent():
    """重复转换不得改变结果——这是原缺陷的核心（二次哈希不可逆）。"""
    once = _md5_invoke_id("my-group")
    assert _md5_invoke_id(once) == once
    assert _md5_invoke_id(_md5_invoke_id(once)) == once


def test_apply_twice_on_same_envelope_is_stable():
    """同一信封被处理两次（重试/重放）时租户 ID 必须不变。"""
    envelope = E2AEnvelope(
        method="chat.send",
        request_id="r",
        session_id="s",
        channel_context={"routing": {"group_id": "g", "bot_id": "b"}, "user_id": "u"},
    )
    apply_invoke_ids_to_envelope(envelope)
    first = (envelope.service_id, envelope.agent_id, envelope.workspace_key)
    apply_invoke_ids_to_envelope(envelope)
    second = (envelope.service_id, envelope.agent_id, envelope.workspace_key)
    assert first == second
    # 第一次的结果本身是 32hex，第二次应原样保留
    assert all(is_final_invoke_id(value) for value in first)


# --------------------------------------------------------------------------- #
# 09：资源过期后只读放行
# --------------------------------------------------------------------------- #


def test_read_only_and_chat_methods_do_not_overlap():
    assert not (agent_authorization.CHAT_METHODS & agent_authorization.READ_ONLY_METHODS)


@pytest.mark.parametrize(
    "method", ["session.rename", "session.fork", "session.delete", "session.switch"]
)
def test_mutating_methods_are_not_read_only(method, enterprise_env):
    """改会话状态的方法不算只读，过期后必须拒绝。"""
    _set_row(expires_at=datetime.now(timezone.utc) - timedelta(days=1))
    with pytest.raises(agent_authorization.FatalRouteError, match="denied"):
        asyncio.run(agent_authorization.authorize_agent(_envelope(method)))


def _envelope(method: str) -> E2AEnvelope:
    return E2AEnvelope(
        method=method,
        request_id="r",
        session_id="s",
        channel_context={"routing": {"group_id": "g", "bot_id": "b"}, "user_id": "u"},
    )


class _Repo:
    def __init__(self, row):
        self._row = row

    async def get(self, resource_id):
        return self._row


@pytest.fixture()
def enterprise_env(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    monkeypatch.setattr(
        agent_authorization, "_enterprise_checkers", lambda: (_repo_getter, _always_match)
    )
    return monkeypatch


_ROW: dict = {}


def _repo_getter(_kind):
    return _Repo(_ROW)


def _always_match(_expr, _identity):
    return True


def _set_row(**values):
    _ROW.clear()
    _ROW.update({"resource_id": "b", "enabled": True, **values})


def test_expired_resource_allows_read_only(enterprise_env):
    _set_row(expires_at=datetime.now(timezone.utc) - timedelta(days=1))
    result = asyncio.run(
        agent_authorization.authorize_agent(_envelope("history.get"))
    )
    assert result == ("g", "b", "u")


def test_expired_resource_rejects_chat(enterprise_env):
    _set_row(expires_at=datetime.now(timezone.utc) - timedelta(days=1))
    with pytest.raises(agent_authorization.FatalRouteError) as caught:
        asyncio.run(
            agent_authorization.authorize_agent(_envelope("chat.send"))
        )
    assert caught.value.code == "FORBIDDEN"


def test_disabled_resource_rejects_even_read_only(enterprise_env):
    """主动禁用与到期语义不同：连历史也不放行。"""
    _set_row(enabled=False)
    with pytest.raises(agent_authorization.FatalRouteError):
        asyncio.run(
            agent_authorization.authorize_agent(_envelope("history.get"))
        )


def test_missing_resource_rejects_read_only(enterprise_env):
    _ROW.clear()
    monkeypatch_repo = _Repo(None)
    enterprise_env.setattr(
        agent_authorization, "_enterprise_checkers", lambda: (lambda _k: monkeypatch_repo, _always_match)
    )
    with pytest.raises(agent_authorization.FatalRouteError):
        asyncio.run(
            agent_authorization.authorize_agent(_envelope("history.get"))
        )


def test_active_resource_allows_both(enterprise_env):
    _set_row(expires_at=datetime.now(timezone.utc) + timedelta(days=1))
    assert asyncio.run(
        agent_authorization.authorize_agent(_envelope("history.get"))
    ) == ("g", "b", "u")
    assert asyncio.run(
        agent_authorization.authorize_agent(_envelope("chat.send"))
    ) == ("g", "b", "u")


def test_unrelated_method_skips_admission(enterprise_env):
    _set_row(expires_at=datetime.now(timezone.utc) - timedelta(days=1))
    assert asyncio.run(
        agent_authorization.authorize_agent(_envelope("skills.list"))
    ) is None


# --------------------------------------------------------------------------- #
# 10：个人版不支持 runtime
# --------------------------------------------------------------------------- #


def test_personal_edition_rejects_runtime(monkeypatch):
    from agent_gateway.agent_client_mode import (
        AgentClientModeError,
        validate_agent_client_configuration,
    )

    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)
    with pytest.raises(AgentClientModeError) as caught:
        validate_agent_client_configuration({}, mode="runtime")
    assert "仅支持企业版" in str(caught.value)


def test_enterprise_edition_allows_runtime(monkeypatch):
    from agent_gateway.agent_client_mode import validate_agent_client_configuration

    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    validate_agent_client_configuration({}, mode="runtime")


def test_personal_edition_allows_websocket(monkeypatch):
    from agent_gateway.agent_client_mode import validate_agent_client_configuration

    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)
    validate_agent_client_configuration({}, mode="")
    validate_agent_client_configuration({}, mode="websocket")


def test_expired_reader_still_requires_matching_identity(enterprise_env):
    _set_row(expires_at=datetime.now(timezone.utc) - timedelta(days=1))
    enterprise_env.setattr(agent_authorization, "_enterprise_checkers",
                           lambda: (_repo_getter, lambda *_args: False))
    with pytest.raises(agent_authorization.FatalRouteError, match="denied"):
        asyncio.run(agent_authorization.authorize_agent(_envelope("history.get")))


def test_generated_32hex_logical_id_retains_existing_workspace_mapping():
    envelope = _envelope("chat.send")
    envelope.channel_context = {"routing": {"group_id": "a" * 30, "bot_id": "b"}, "user_id": "c"}
    apply_invoke_ids_to_envelope(envelope)
    expected = hashlib.md5(("a" * 30 + "bc").encode()).hexdigest()
    assert envelope.workspace_key == expected
    apply_invoke_ids_to_envelope(envelope)
    assert envelope.workspace_key == expected
