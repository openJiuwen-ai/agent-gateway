# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""补齐 pinned ``e2a`` 尚未发布的协议枚举成员。

拆仓前这些成员由宿主进程动态注册；Gateway 作为独立进程不再导入宿主，
必须自行注册同值成员，否则消费对应 RPC 或事件时会抛 ``AttributeError``。
协议包发布同名同值成员后，本模块及其调用应一并删除。
"""

from __future__ import annotations

import logging
import re
from enum import Enum

from e2a.message import EventType, ReqMethod

logger = logging.getLogger(__name__)

GATEWAY_REQ_METHODS: tuple[tuple[str, str], ...] = (
    ("MODE_SET", "mode.set"),
    ("MODE_GET", "mode.get"),
    ("JIUWENMATE_FEISHU_INBOUND", "jiuwenmate.feishu.inbound"),
    ("JIUWENMATE_DINGTALK_INBOUND", "jiuwenmate.dingtalk.inbound"),
    ("JIUWENMATE_DINGTALK_BINDING_CHECK", "jiuwenmate.dingtalk.binding_check"),
    ("JIUWENMATE_WELINK_INBOUND", "jiuwenmate.welink.inbound"),
    ("JIUWENMATE_CONNECTION_ATTACH", "jiuwenmate.connection.attach"),
    ("JIUWENMATE_COLLEAGUE_CONNECTIONS", "jiuwenmate.colleague.connections"),
    ("JIUWENMATE_COLLEAGUE_REMOVE", "jiuwenmate.colleague.remove"),
    ("JIUWENMATE_FEISHU_PREPARE", "jiuwenmate.feishu.prepare"),
    ("JIUWENMATE_CHANNEL_CONFIG_SYNC", "jiuwenmate.channel_config.sync"),
    ("JIUWENMATE_DELIVERY_ACK", "jiuwenmate.delivery.ack"),
    ("JIUWENMATE_THINKING_NOTICE_ACK", "jiuwenmate.thinking_notice.ack"),
    ("JIUWENMATE_GROUP_RESPONSE", "jiuwenmate.group.response"),
    ("JIUWENMATE_COLLEAGUE_ENSURE_DEFAULT", "jiuwenmate.colleague.ensure_default"),
    ("JIUWENMATE_COLLEAGUE_LIST", "jiuwenmate.colleague.list"),
    ("JIUWENMATE_COLLEAGUE_GET", "jiuwenmate.colleague.get"),
    ("JIUWENMATE_COLLEAGUE_CREATE", "jiuwenmate.colleague.create"),
    ("JIUWENMATE_COLLEAGUE_SETTINGS_GET", "jiuwenmate.colleague.settings.get"),
    ("JIUWENMATE_COLLEAGUE_SETTINGS_UPDATE", "jiuwenmate.colleague.settings.update"),
    ("JIUWENMATE_COLLEAGUE_PUBLISH", "jiuwenmate.colleague.publish"),
    ("JIUWENMATE_COLLEAGUE_SET_ENABLED", "jiuwenmate.colleague.set_enabled"),
    ("JIUWENMATE_COLLEAGUE_DELETE", "jiuwenmate.colleague.delete"),
    ("JIUWENMATE_MODELS_LIST", "jiuwenmate.models.list"),
    ("JIUWENMATE_CHAT_SEND", "jiuwenmate.chat.send"),
    ("JIUWENMATE_CHAT_HISTORY", "jiuwenmate.chat.history"),
    ("JIUWENMATE_ARTIFACT_LIST", "jiuwenmate.artifact.list"),
    ("JIUWENMATE_REQUEST_GET", "jiuwenmate.request.get"),
    ("JIUWENMATE_REQUEST_ABORT", "jiuwenmate.request.abort"),
    ("JIUWENMATE_PERMISSION_DECIDE", "jiuwenmate.permission.decide"),
    ("JIUWENMATE_SHARING_LIST", "jiuwenmate.sharing.list"),
    ("JIUWENMATE_SHARING_DECIDE", "jiuwenmate.sharing.decide"),
    ("JIUWENMATE_SHARING_REVOKE", "jiuwenmate.sharing.revoke"),
    ("JIUWENMATE_CONVERSATION_LIST", "jiuwenmate.conversation.list"),
    ("JIUWENMATE_CONVERSATION_RENAME", "jiuwenmate.conversation.rename"),
    ("JIUWENMATE_CONVERSATION_PIN", "jiuwenmate.conversation.pin"),
    ("JIUWENMATE_CONVERSATION_DELETE", "jiuwenmate.conversation.delete"),
    ("JIUWENMATE_ACTIVITY_GET", "jiuwenmate.activity.get"),
    ("JIUWENMATE_KNOWLEDGE_LIST", "jiuwenmate.knowledge.list"),
    ("JIUWENMATE_SCHEDULE_LIST", "jiuwenmate.schedule.list"),
    ("JIUWENMATE_SCHEDULE_CREATE", "jiuwenmate.schedule.create"),
    ("JIUWENMATE_SCHEDULE_SET_ENABLED", "jiuwenmate.schedule.set_enabled"),
    ("JIUWENMATE_SCHEDULE_DELETE", "jiuwenmate.schedule.delete"),
    ("JIUWENMATE_SCHEDULE_UPDATE", "jiuwenmate.schedule.update"),
    ("JIUWENMATE_SCHEDULE_RUN", "jiuwenmate.schedule.run"),
    ("JIUWENMATE_SCHEDULE_HISTORY", "jiuwenmate.schedule.history"),
    ("JIUWENMATE_CHANNEL_LIST", "jiuwenmate.channel.list"),
    ("JIUWENMATE_CHANNEL_MESSAGES", "jiuwenmate.channel.messages"),
    ("JIUWENMATE_CHANNEL_TRACE", "jiuwenmate.channel.trace"),
    ("JIUWENMATE_WELINK_OFFICE_STATUS", "jiuwenmate.welink_office.status"),
    ("JIUWENMATE_WELINK_OFFICE_CONNECT", "jiuwenmate.welink_office.connect"),
    ("JIUWENMATE_WELINK_OFFICE_DISCONNECT", "jiuwenmate.welink_office.disconnect"),
    ("JIUWENMATE_AGENT_MAIL_STATUS", "jiuwenmate.agent_mail.status"),
    ("JIUWENMATE_AGENT_MAIL_AUTH_BEGIN", "jiuwenmate.agent_mail.auth.begin"),
    ("JIUWENMATE_AGENT_MAIL_AUTH_POLL", "jiuwenmate.agent_mail.auth.poll"),
    ("JIUWENMATE_AGENT_MAIL_AUTH_CANCEL", "jiuwenmate.agent_mail.auth.cancel"),
    ("JIUWENMATE_AGENT_MAIL_DISCONNECT", "jiuwenmate.agent_mail.disconnect"),
    ("JIUWENMATE_DINGTALK_AUTH_STATUS", "jiuwenmate.dingtalk_auth.status"),
    ("JIUWENMATE_DINGTALK_AUTH_BEGIN", "jiuwenmate.dingtalk_auth.begin"),
    ("JIUWENMATE_DINGTALK_AUTH_POLL", "jiuwenmate.dingtalk_auth.poll"),
    ("JIUWENMATE_DINGTALK_AUTH_CANCEL", "jiuwenmate.dingtalk_auth.cancel"),
    ("JIUWENMATE_DINGTALK_AUTH_DISCONNECT", "jiuwenmate.dingtalk_auth.disconnect"),
    ("GROUP_EXECUTION_LIST", "group.execution.list"),
    ("GROUP_EXECUTION_GET", "group.execution.get"),
    ("GROUP_FILE_RENAME", "group.file.rename"),
    ("GROUP_FILE_DELETE", "group.file.delete"),
    ("GROUP_HISTORY_CLEAR", "group.history.clear"),
    ("MODELS_REFRESH_STATUS", "models.refresh_status"),
    ("FILE_READ_UPLOAD_CHUNK", "file.read_upload_chunk"),
    ("WORKSPACE_TREE", "workspace.tree"),
    ("WORKSPACE_PREVIEW", "workspace.preview"),
    ("WORKSPACE_USAGE", "workspace.usage"),
    ("WORKSPACE_ENTRIES_DELETE", "workspace.entries.delete"),
    ("WORKSPACE_DOWNLOAD", "workspace.download"),
    ("CLIENT_ATTACH", "client.attach"),
    ("CLIENT_RENEW", "client.renew"),
    ("CLIENT_LIST", "client.list"),
    ("CLIENT_CONVERSATION_MESSAGES", "client.conversation.messages"),
    ("CLIENT_CONVERSATIONS", "client.conversations"),
    ("MCP_APP_LIST_TOOLS", "mcp_app.list_tools"),
    ("MCP_APP_READ_RESOURCE", "mcp_app.read_resource"),
    ("MCP_APP_CALL_TOOL", "mcp_app.call_tool"),
)

GATEWAY_EVENT_TYPES: tuple[tuple[str, str], ...] = (
    ("CHAT_CLIENT_TOOL_REQUEST", "chat.client_tool_request"),
    ("CLIENT_ATTACHMENTS_CHANGED", "client.attachments_changed"),
)

#: WebUI 扩展上行协议与扩展包管理协议（S_10/S_17），由 Gateway 扩展层消费。
EXTENSION_REQ_METHODS: tuple[tuple[str, str], ...] = (
    ("WEBUI_DISPATCH", "webui.dispatch"),
    ("WEBUI_SETTINGS_CATALOG", "webui.settings.catalog"),
    ("WEBUI_RESOURCE_READ", "webui.resource.read"),
    ("WEBUI_CONNECTION_CLOSED", "webui.connection.closed"),
    ("EXTENSIONS_V2_QUERY", "extensions.v2.query"),
    ("EXTENSIONS_V2_CATALOG", "extensions.v2.catalog"),
    ("EXTENSIONS_V2_SHOW", "extensions.v2.show"),
    ("EXTENSIONS_V2_IMPORT", "extensions.v2.import"),
    ("EXTENSIONS_V2_INSTALL", "extensions.v2.install"),
    ("EXTENSIONS_V2_UNINSTALL", "extensions.v2.uninstall"),
    ("EXTENSIONS_V2_RELOAD", "extensions.v2.reload"),
    ("EXTENSIONS_V2_SET_ENABLED", "extensions.v2.set_enabled"),
    ("EXTENSIONS_V2_FILE_LIST", "extensions.v2.file.list"),
    ("EXTENSIONS_V2_FILE_READ", "extensions.v2.file.read"),
)


#: ``ensure_req_method`` 动态补齐的上限；防止任意客户端无限枚举导致进程内存增长。
MAX_DYNAMIC_REQ_METHODS = 64

_dynamic_req_methods: set[str] = set()

#: 配额打满只告警一次，避免被未知方法请求刷爆日志。
_quota_warned = False


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
    for name, value in (*GATEWAY_REQ_METHODS, *EXTENSION_REQ_METHODS):
        ensure_protocol_member(ReqMethod, name, value)


def ensure_gateway_event_types() -> None:
    for name, value in GATEWAY_EVENT_TYPES:
        ensure_protocol_member(EventType, name, value)


def ensure_req_method(value: str) -> ReqMethod | None:
    """按协议值查找 ``ReqMethod``；缺失时运行时补齐并返回该成员。

    新增业务方法不再要求网关随协议包升级：请求先由 Web 通道解析成枚举，
    再按“本地白名单 + 默认透传”决定是否转发。值非法、无法生成合法成员名，
    或动态成员已达 ``MAX_DYNAMIC_REQ_METHODS`` 时返回 ``None``，调用方保持
    未知方法语义。
    """
    global _quota_warned  # pylint: disable=global-statement

    if not isinstance(value, str) or not value or len(value) > 128:
        return None
    for item in ReqMethod:
        if item.value == value:
            return item
    if len(_dynamic_req_methods) >= MAX_DYNAMIC_REQ_METHODS:
        if not _quota_warned:
            _quota_warned = True
            logger.warning(
                "dynamic ReqMethod registry is full (limit=%d); method %r will be treated as unknown",
                MAX_DYNAMIC_REQ_METHODS,
                value,
            )
        return None
    base = re.sub(r"[^0-9A-Za-z]+", "_", value).strip("_").upper()
    if not base:
        return None
    if base[0].isdigit():
        base = f"REQ_{base}"
    name = base
    suffix = 1
    while name in ReqMethod.__members__:
        existing = ReqMethod.__members__[name]
        if existing.value == value:
            return existing
        suffix += 1
        name = f"{base}_{suffix}"
    try:
        ensure_protocol_member(ReqMethod, name, value)
    except RuntimeError:
        return None
    _dynamic_req_methods.add(value)
    return ReqMethod.__members__.get(name)


__all__ = [
    "EXTENSION_REQ_METHODS",
    "GATEWAY_EVENT_TYPES",
    "GATEWAY_REQ_METHODS",
    "MAX_DYNAMIC_REQ_METHODS",
    "ensure_gateway_event_types",
    "ensure_gateway_req_methods",
    "ensure_protocol_member",
    "ensure_req_method",
]
