# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""协议枚举成员登记：common 下两个协议模块共用同一份实现。"""

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
