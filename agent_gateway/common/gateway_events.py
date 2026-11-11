# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway 侧的事件名常量表。

Gateway 是事件的**消费方**：它从 AgentServer 收到线上字符串事件名，按名字
决定各渠道的呈现方式（飞书卡片、ACP session/update、Web 帧、TUI 文本）。
事件名的语义由 AgentServer 定义，Gateway 只按字符串判等，不再依赖
``openjiuwen_protocol.message.EventType`` 的枚举身份。

本枚举仅提供**字符串常量**（``StrEnum``）：成员本身等于线上事件名，可直接
与 ``msg.event_type`` 这类字符串比较、可直接写入 ``Message.event_type``、
也可直接参与 JSON 序列化。除构造 Gateway 自产事件外，判等一律使用字面量，
避免两仓各自持有一份枚举类时身份比较静默失效。
"""

from __future__ import annotations

from enum import StrEnum


class GatewayEvent(StrEnum):
    """线上事件名常量；值必须与 AgentServer 侧事件词汇表逐条一致。"""

    SESSION_ARCHIVED = "session.archived"
    SESSION_UNARCHIVED = "session.unarchived"
    SESSION_DELETED = "session.deleted"
    SESSION_LIFECYCLE_UPDATED = "session.lifecycle.updated"
    PROJECT_LIFECYCLE_UPDATED = "project.lifecycle.updated"
    PROJECT_REMOVED = "project.removed"
    PROJECT_RESTORED = "project.restored"
    CONNECTION_ACK = "connection.ack"
    HELLO = "hello"
    CHAT_DELTA = "chat.delta"
    CHAT_REASONING = "chat.reasoning"
    CHAT_USAGE_METADATA = "chat.usage_metadata"
    CHAT_USAGE_SUMMARY = "chat.usage_summary"
    CHAT_FINAL = "chat.final"
    CHAT_RETRACT = "chat.retract"
    CHAT_MEDIA = "chat.media"
    CHAT_FILE = "chat.file"
    CHAT_TOOL_CALL = "chat.tool_call"
    CHAT_TOOL_UPDATE = "chat.tool_update"
    CHAT_TOOL_RESULT = "chat.tool_result"
    CHAT_SYMPHONY_STATUS = "chat.symphony_status"
    CHAT_MESSAGE_UPDATED = "chat.message_updated"
    CONTEXT_USAGE = "context.usage"
    TODO_UPDATED = "todo.updated"
    CHAT_PROCESSING_STATUS = "chat.processing_status"
    CHAT_ERROR = "chat.error"
    CHAT_INTERRUPT_RESULT = "chat.interrupt_result"
    CHAT_EVOLUTION_STATUS = "chat.evolution_status"
    CHAT_SUBTASK_UPDATE = "chat.subtask_update"
    CHAT_SUBAGENT_ACTIVITY = "chat.subagent_activity"
    CHAT_ASK_USER_QUESTION = "chat.ask_user_question"
    PLAN_APPROVAL_REQUIRED = "plan.approval_required"
    CHAT_SESSION_RESULT = "chat.session_result"
    GOAL_SNAPSHOT = "goal.snapshot"
    GOAL_UPDATED = "goal.updated"
    RUNTIME_ACCEPTED = "runtime.accepted"
    EXECUTION_ERROR = "execution.error"
    TEAM_MEMBER = "team.member"
    TEAM_TASK = "team.task"
    TEAM_MESSAGE = "team.message"
    WORKFLOW_UPDATED = "workflow.updated"
    DESIGNER_RUN_UPDATED = "designer.run.updated"
    DESIGNER_NODE_UPDATED = "designer.node.updated"
    DESIGNER_GRAPH_UPDATED = "designer.graph.updated"
    DESIGNER_LEADER_ACTIVITY = "designer.leader.activity"
    HEALTH_CHECK_RELAY = "health_check.relay"
    HEARTBEAT_RELAY = "health_check.relay"
    HISTORY_GET = "history.message"
    PROACTIVE_RECOMMENDATION = "proactive_recommendation"
    # 由 AgentServer 产出、Gateway 转发；Gateway 早期版本靠运行时补注册才能识别。
    CHAT_CLIENT_TOOL_REQUEST = "chat.client_tool_request"
    CLIENT_ATTACHMENTS_CHANGED = "client.attachments_changed"


# 早期线上别名：``heartbeat.relay`` 在传输层归一为 ``health_check.relay``。
LEGACY_HEALTH_CHECK_RELAY = "heartbeat.relay"

# 已知事件名全集；仅用于诊断与契约测试，判等走字符串。
KNOWN_EVENT_NAMES: frozenset[str] = frozenset(
    member.value for member in GatewayEvent
) | {LEGACY_HEALTH_CHECK_RELAY}


def normalize_event_name(value: object) -> str | None:
    """把任意来源的事件名归一为线上字符串。

    Gateway 不再构造协议枚举：线上字符串原样保留，仅把历史别名
    ``heartbeat.relay`` 归一为 ``health_check.relay``。传入 ``StrEnum`` /
    ``Enum`` 时取其 ``value``，空值与非法类型返回 ``None``。
    """
    name = getattr(value, "value", value)
    if not isinstance(name, str):
        return None
    name = name.strip()
    if not name:
        return None
    if name == LEGACY_HEALTH_CHECK_RELAY:
        return GatewayEvent.HEALTH_CHECK_RELAY
    return name


def event_name_of(value: object) -> str:
    """取 ``Message.event_type`` 的线上字符串；空值返回 ``""``。

    ``event_type`` 现在是字符串（或 ``StrEnum`` 成员），旧调用点的
    ``msg.event_type.value`` 在纯 ``str`` 上会抛 ``AttributeError``，统一经由
    本函数取值。兼容仍传入 ``Enum`` 成员的调用方。
    """
    name = getattr(value, "value", value)
    return name if isinstance(name, str) else ""


__all__ = [
    "KNOWN_EVENT_NAMES",
    "LEGACY_HEALTH_CHECK_RELAY",
    "GatewayEvent",
    "event_name_of",
    "normalize_event_name",
]
