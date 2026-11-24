# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""协议枚举成员登记：common 下两个协议模块共用同一份实现。"""

import logging
from enum import Enum

import pytest

from agent_gateway.common import protocol_compat, protocol_methods


class _SampleMethod(Enum):
    EXISTING = "sample.existing"


def test_protocol_methods_reuses_shared_registration_helper():
    assert protocol_methods.ensure_protocol_member is protocol_compat.ensure_protocol_member


def test_register_member_is_idempotent():
    protocol_methods.ensure_protocol_member(_SampleMethod, "ADDED", "sample.added")
    first = _SampleMethod.ADDED

    protocol_methods.ensure_protocol_member(_SampleMethod, "ADDED", "sample.added")

    assert _SampleMethod.ADDED is first
    assert first.value == "sample.added"


def test_register_member_rejects_conflicting_name():
    with pytest.raises(RuntimeError, match="EXISTING"):
        protocol_methods.ensure_protocol_member(_SampleMethod, "EXISTING", "sample.other")


def test_register_member_rejects_duplicate_value():
    with pytest.raises(RuntimeError, match="already registered"):
        protocol_methods.ensure_protocol_member(_SampleMethod, "ALIAS", "sample.existing")


def test_module_level_req_methods_still_registered():
    assert protocol_methods.ReqMethod.SKILLS_UPLOAD_FINALIZE.value == protocol_methods.SKILL_UPLOAD_FINALIZE
    assert protocol_methods.ReqMethod.VIDEO_CONVERSATION_APPEND.value == "video.conversation.append"


def test_ensure_req_method_warns_once_when_registry_full(monkeypatch, caplog):
    """动态配额打满时保持未知方法语义，并且只告警一次，避免刷日志。"""
    full = {f"synthetic.method.{index}" for index in range(protocol_compat.MAX_DYNAMIC_REQ_METHODS)}
    monkeypatch.setattr(protocol_compat, "_dynamic_req_methods", full)
    monkeypatch.setattr(protocol_compat, "_quota_warned", False)

    with caplog.at_level(logging.WARNING, logger=protocol_compat.__name__):
        assert protocol_compat.ensure_req_method("sample.overflow.one") is None
        assert protocol_compat.ensure_req_method("sample.overflow.two") is None

    warnings = [record for record in caplog.records if record.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "full" in warnings[0].getMessage()
