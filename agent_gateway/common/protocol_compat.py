# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""补齐 pinned ``openjiuwen-protocol`` 尚未发布的 ``ReqMethod`` 成员。

拆仓前这些成员由 ``jiuwenswarm.common.schema.message`` 在宿主进程内动态注册；
Gateway 作为独立进程不再导入宿主，必须自行注册同值成员，否则
``app_web_handlers`` 注册 workspace 文件接口时会抛 ``AttributeError``。
协议包发布同名同值成员后，本模块及其调用应一并删除。
"""

from __future__ import annotations

from enum import Enum

from openjiuwen_protocol.message import ReqMethod

GATEWAY_REQ_METHODS: tuple[tuple[str, str], ...] = (
    ("JIUWENMATE_FEISHU_INBOUND", "jiuwenmate.feishu.inbound"),
    ("JIUWENMATE_DINGTALK_INBOUND", "jiuwenmate.dingtalk.inbound"),
    ("JIUWENMATE_DINGTALK_BINDING_CHECK", "jiuwenmate.dingtalk.binding_check"),
    ("JIUWENMATE_DELIVERY_ACK", "jiuwenmate.delivery.ack"),
    ("JIUWENMATE_THINKING_NOTICE_ACK", "jiuwenmate.thinking_notice.ack"),
    ("JIUWENMATE_GROUP_RESPONSE", "jiuwenmate.group.response"),
    ("FILE_READ_UPLOAD_CHUNK", "file.read_upload_chunk"),
    ("WORKSPACE_TREE", "workspace.tree"),
    ("WORKSPACE_PREVIEW", "workspace.preview"),
    ("WORKSPACE_USAGE", "workspace.usage"),
    ("WORKSPACE_ENTRIES_DELETE", "workspace.entries.delete"),
)


def ensure_protocol_member(enum_cls: type[Enum], name: str, value: str) -> None:
    """注册协议包缺失的枚举成员；已有同值成员时幂等返回。"""
    existing = enum_cls.__members__.get(name)
    if existing is not None:
        if existing.value != value:
            raise RuntimeError(
                f"{enum_cls.__name__}.{name} is {existing.value!r}, expected {value!r}"
            )
        return
    if any(item.value == value for item in enum_cls):
        raise RuntimeError(f"{enum_cls.__name__} value {value!r} is already registered")
    # Enum 未暴露公开的成员注册 API，只能写内部注册表。
    member = object.__new__(enum_cls)
    member._name_ = name  # pylint: disable=protected-access
    member._value_ = value  # pylint: disable=protected-access
    type.__setattr__(enum_cls, name, member)
    enum_cls._member_map_[name] = member  # pylint: disable=protected-access
    enum_cls._value2member_map_[value] = member  # pylint: disable=protected-access
    enum_cls._member_names_.append(name)  # pylint: disable=protected-access


def ensure_gateway_req_methods() -> None:
    for name, value in GATEWAY_REQ_METHODS:
        ensure_protocol_member(ReqMethod, name, value)


__all__ = [
    "GATEWAY_REQ_METHODS",
    "ensure_gateway_req_methods",
    "ensure_protocol_member",
]
