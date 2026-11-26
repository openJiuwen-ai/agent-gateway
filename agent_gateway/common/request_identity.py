# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Transport-level Web request identity helpers.

分层约定（企业版与个人版共用同一结构，按字段必填性区分）：

1. **Wire**（浏览器 → Gateway，非权威）
   - WS query：``user_id`` / ``group_id`` / ``bot_id`` / ``gateway_id``
   - HTTP 头：``X-User-Id`` / ``X-Group-Id`` / ``X-Bot-Id`` / ``X-Gateway-Id``

2. **进站后权威落点**

   - ``metadata["user_id"]``（顶层）—— 谁在说话；并镜像到
     ``Message.user_id`` / ``E2AEnvelope.user_id``
   - ``metadata["routing"] = {group_id, bot_id, gateway_id}``—— 路由维
     （E2A 上对应 ``channel_context.routing``）
   - ``metadata["agent_identity"] = {agent_name, agent_id, runtime_template_id,
     agent_template_id}``—— 平台展示身份（无业务路由含义，仅用于工作区落盘），
     仅企业版入口采信并透传，个人版不写入；其中 ``agent_template_id`` 同时由
     技能上传门禁回查 Manager 校验

   Gateway 入口用 :func:`normalize_routing_identity` + :func:`apply_routing_metadata`
   写一次；下游用 :func:`web_routing_identity` 一次读全（含顶层 ``user_id``）。
   展示身份由 :func:`normalize_agent_identity` + :func:`apply_agent_identity_metadata`
   写入，AgentServer 用 :func:`read_agent_identity` 读取。

3. **不要**：把身份写入业务 ``params``；不要把 ``group_id``/``bot_id``/``gateway_id``
   摊到 metadata 顶层；不要把 ``user_id`` 放进 ``routing``。

4. **必填策略**
   - 个人版 Web：建议有顶层 ``user_id``；其余可缺
   - 企业版 Web：``routing.bot_id`` 建议有；``user_id``/``group_id`` 按策略需要

实现已收口到 ``e2a.identity``（gateway 与 AgentServer 共用的
唯一来源）；本模块只 re-export 并保留 gateway 本地 handler 的
``merge_routing_into_params`` 适配。
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from e2a.identity import (
    ROUTING_METADATA_KEY,
    USER_ID_FIELD,
    WEB_IDENTITY_FIELDS,
    WEB_ROUTING_ID_FIELDS,
    apply_routing_metadata,
    normalize_routing_identity,
    web_routing_identity,
)

AGENT_IDENTITY_METADATA_KEY = "agent_identity"
# 平台展示身份（由 Manager 计算），只在 metadata.agent_identity 内成组出现。
# 其中 agent_template_id 亦供技能上传门禁使用。
AGENT_IDENTITY_FIELDS = (
    "agent_name",
    "agent_id",
    "runtime_template_id",
    "agent_template_id",
)


def _identity_value(value: Any) -> str | None:
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _pick_field(mapping: Mapping[str, Any], field: str) -> str | None:
    return _identity_value(mapping.get(field))


def normalize_agent_identity(
    *sources: Mapping[str, Any] | None,
) -> dict[str, str]:
    """从若干 mapping 归一化平台展示身份（先出现的非空值为准）。

    与路由身份不同，这里没有必填项：旧 Manager 可能只返回 ``agent_name``。
    是否足以落盘由调用方按 ``agent_name`` + ``agent_id`` 判断；整体缺失时
    返回空 dict，由上传门禁按“无法确认”处理。
    """
    identity: dict[str, str] = {}
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        for field in AGENT_IDENTITY_FIELDS:
            if field in identity:
                continue
            value = _pick_field(source, field)
            if value:
                identity[field] = value
    return identity


def normalize_agent_identity_query(
    query: Mapping[str, Any] | None,
) -> dict[str, str]:
    """归一化 WS 握手 query 中的展示身份。

    Query 的展示 id 键是 ``agent_identity_id``，避免与路由层的 ``agent_id``
    （模式内 Agent id）冲突；这里映射回统一的 ``agent_id`` 字段。
    """
    if not isinstance(query, Mapping):
        return {}
    return normalize_agent_identity(
        {
            "agent_name": query.get("agent_name"),
            "agent_id": query.get("agent_identity_id"),
            "runtime_template_id": query.get("runtime_template_id"),
            "agent_template_id": query.get("agent_template_id"),
        }
    )


def apply_agent_identity_metadata(
    metadata: Mapping[str, Any] | None,
    identity: Mapping[str, str] | None,
) -> dict[str, Any]:
    """把平台展示身份写入 ``metadata.agent_identity``（空身份则移除）。"""
    meta = dict(metadata or {})
    cleaned = normalize_agent_identity(identity)
    if cleaned:
        meta[AGENT_IDENTITY_METADATA_KEY] = cleaned
    else:
        meta.pop(AGENT_IDENTITY_METADATA_KEY, None)
    return meta


def read_agent_identity(metadata: Mapping[str, Any] | None) -> dict[str, str]:
    """读取 ``metadata.agent_identity``；缺失或结构不符时返回空 dict。"""
    if not isinstance(metadata, Mapping):
        return {}
    raw = metadata.get(AGENT_IDENTITY_METADATA_KEY)
    if not isinstance(raw, Mapping):
        return {}
    return normalize_agent_identity(raw)


def merge_routing_into_params(
    params: Mapping[str, Any] | None,
    metadata: Mapping[str, Any] | None,
    *,
    override: bool = True,
) -> dict[str, Any]:
    """仅用于 Gateway **本地** handler：handler 签名只有 params。"""
    result = dict(params or {})
    for field, value in web_routing_identity(metadata).items():
        if override or field not in result:
            result[field] = value
    return result


__all__ = (
    "ROUTING_METADATA_KEY",
    "AGENT_IDENTITY_METADATA_KEY",
    "USER_ID_FIELD",
    "WEB_ROUTING_ID_FIELDS",
    "WEB_IDENTITY_FIELDS",
    "AGENT_IDENTITY_FIELDS",
    "normalize_routing_identity",
    "apply_routing_metadata",
    "merge_routing_into_params",
    "web_routing_identity",
    "normalize_agent_identity",
    "normalize_agent_identity_query",
    "apply_agent_identity_metadata",
    "read_agent_identity",
)
