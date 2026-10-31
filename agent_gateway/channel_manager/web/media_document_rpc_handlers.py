# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Media and document RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import base64
import json
import logging
from typing import Any

from openjiuwen_protocol.message import ReqMethod

from agent_gateway.common.attachment_safety import (
    ensure_image_upload_filename,
    image_suffix_for_mime,
    is_forbidden_document,
    safe_session_dirname,
    safe_upload_filename,
    strip_data_uri_prefix,
)
from agent_gateway.routing.agent_http_bridge import (
    E2A_PAYLOAD_MAX_BYTES,
    upload_file_bytes_via_e2a,
)
from agent_gateway.routing.e2a_proxy import proxy_unary_request


logger = logging.getLogger(__name__)


def register_media_document_handlers(channel: Any, agent_client: Any, _resolve: Any) -> None:
    """Register media/document RPC handlers in their original route order."""

    async def _media_persist(ws, req_id, params, session_id, user_id=None):
        """浏览器上传媒体附件落盘（E2A 转发 AgentServer 注入目录 uploads）。

        经统一薄代理 E2A 转发目标 AgentServer（WorkspaceFileAdapter
        MEDIA_PERSIST，base64 小文件由注入目录落盘；大文件在 Gateway 侧解码后
        走受认证 HTTP bridge 上传，避免超内部 WS 帧限制）。
        """
        real_client = _resolve(agent_client)
        if isinstance(params, dict):
            params = await _pre_persist_large_media(
                params,
                session_id=session_id,
                agent_client=real_client,
                user_id=user_id,
            )

        await proxy_unary_request(
            channel=channel,
            agent_client=real_client,
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.MEDIA_PERSIST,
            label="media.persist",
        )

    async def _media_discard(ws, req_id, params, session_id, user_id=None):
        """删除尚未发送的会话 uploads 副本（E2A 转发，路径边界在 AgentServer）。"""
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.MEDIA_DISCARD,
            label="media.discard",
        )

    async def _document_persist(ws, req_id, params, session_id, user_id=None):
        """文档附件落盘（E2A 转发；base64 小文档由 AgentServer 注入目录落盘，
        大文档在 Gateway 侧解码后经受认证 HTTP bridge 上传，避免超内部 WS 帧限制）。"""
        real_client = _resolve(agent_client)
        if isinstance(params, dict):
            params = await _pre_persist_large_documents(
                params,
                session_id=session_id,
                agent_client=real_client,
                user_id=user_id,
            )

        await proxy_unary_request(
            channel=channel,
            agent_client=real_client,
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.DOCUMENT_PERSIST,
            label="document.persist",
        )

    async def _document_formats(ws, req_id, params, session_id, user_id=None):
        """返回文档上传黑名单格式（E2A 转发 AgentServer）。"""
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.DOCUMENT_FORMATS,
            label="document.formats",
        )

    channel.register_method("media.persist", _media_persist)
    channel.register_method("media.discard", _media_discard)
    channel.register_method("document.persist", _document_persist)
    channel.register_method("document.formats", _document_formats)


async def _upload_document_item_via_http(
    item: dict[str, Any],
    data: bytes,
    *,
    session_id: str | None,
    index: int,
    agent_client: Any,
    user_id: str | None,
) -> dict[str, Any] | None:
    """把单个浏览器 base64 文档经 HTTP bridge 上传到 AgentServer 注入目录。

    与 ``_upload_media_item_via_http`` 对称：落盘路径与 AgentServer 侧
    ``_store_document_item`` 一致（``agent/sessions/<safe_session_id>/uploads``），
    返回带 ``_persisted`` 标记的落盘记录，AgentServer 侧直接透传、不重复解码。
    上传失败返回 ``None``（调用方保留原 base64 项）。
    """
    filename = safe_upload_filename(
        str(item.get("filename") or f"document-{index + 1}"),
        fallback=f"document-{index + 1}",
    )
    if is_forbidden_document(filename=filename):
        logger.warning("[document.persist] forbidden document skipped: %s", filename)
        return None
    safe_session_id = safe_session_dirname(session_id)
    rel_path = f"agent/sessions/{safe_session_id}/uploads/{filename}"
    # A1g：legacy/AgentOS 统一走 E2A 分块上传（FILE_UPLOAD_CHUNK 由
    # WorkspaceFileAdapter 处理，任意 AgentServer 形态可用；旧分支的
    # "AgentServer 无 HTTP 上传监听"是 HTTP bridge 时代的前提，已不成立）。
    ok, payload = await upload_file_bytes_via_e2a(
        data,
        rel_path,
        agent_client=agent_client,
        user_id=user_id,
        channel_id="web",
        session_id=session_id,
    )
    if not ok:
        logger.warning("[document.persist] 大文档上传失败: %s", payload.get("error"))
        return None
    return {
        "type": "document",
        "filename": filename,
        "mime_type": str(item.get("mimeType") or item.get("mime_type") or "")
        .lower()
        .strip()
        or "application/octet-stream",
        "path": str(payload.get("path") or ""),
        "original_path": str(payload.get("path") or ""),
        "size_bytes": len(data),
        "_persisted": True,
    }


async def _pre_persist_large_documents(
    params: dict[str, Any], *, session_id: str | None, agent_client: Any, user_id: str | None
) -> dict[str, Any]:
    """转发 document.persist 前，把超预算的 base64 文档改为 HTTP bridge 上传。

    小文档保留 base64 走 E2A（AgentServer 注入目录落盘）；大文档在 Gateway 侧
    解码后经 HTTP 上传并标记 ``_persisted``，AgentServer 侧直接透传落盘记录，
    避免超内部 WS 帧限制。返回处理后的 params（原对象就地修改）。

    扫描范围与 AgentServer 侧 ``_collect_document_items`` 对齐：同时处理
    ``documents`` 列表和 ``media_items`` 中 ``type == "document"`` 的 base64 项，
    否则走 media_items 通道的大文档不会被 HTTP 预上传，原 base64 随 E2A 转发
    可能超内部 WS 帧限制。
    """
    try:
        base_payload = {k: v for k, v in params.items() if k not in ("documents", "media_items")}
        overhead = len(json.dumps(base_payload, ensure_ascii=False, default=str))
    except Exception:  # noqa: BLE001
        overhead = 4096
    remaining = E2A_PAYLOAD_MAX_BYTES - overhead

    async def _maybe_upload(item: dict[str, Any], index: int) -> dict[str, Any]:
        """超预算的 base64 文档走 HTTP 上传；否则原样返回并扣减预算。"""
        nonlocal remaining
        raw = item.get("base64Data") or item.get("base64_data")
        if not isinstance(raw, str) or not raw.strip():
            return item
        try:
            data = base64.b64decode(strip_data_uri_prefix(raw), validate=True)
        except Exception:  # noqa: BLE001
            return item
        if not data:
            return item
        if len(data) > remaining:
            persisted = await _upload_document_item_via_http(
                item,
                data,
                session_id=session_id,
                index=index,
                agent_client=agent_client,
                user_id=user_id,
            )
            if persisted is not None:
                return persisted
            # 上传失败：保留原 base64（若仍超帧限制，由下游链路返回可重试错误）
            return item
        remaining -= len(data)
        return item

    items = params.get("documents")
    if isinstance(items, list):
        new_items: list[Any] = []
        for index, item in enumerate(items):
            if isinstance(item, dict):
                new_items.append(await _maybe_upload(item, index))
            else:
                new_items.append(item)
        params["documents"] = new_items

    media_items = params.get("media_items")
    if isinstance(media_items, list):
        new_media: list[Any] = []
        for index, item in enumerate(media_items):
            if isinstance(item, dict) and item.get("type") == "document":
                new_media.append(await _maybe_upload(item, index))
            else:
                new_media.append(item)
        params["media_items"] = new_media
    return params


async def _upload_media_item_via_http(
    item: dict[str, Any],
    data: bytes,
    *,
    session_id: str | None,
    index: int,
    agent_client: Any,
    user_id: str | None,
) -> dict[str, Any] | None:
    """把单张浏览器图片经 HTTP bridge 上传到 AgentServer 注入目录，返回落盘记录。

    落盘路径与 AgentServer 侧 ``_store_image_item`` 一致
    （``agent/sessions/<safe_session_id>/uploads/<safe_filename>``，同名去重）。
    上传失败返回 ``None``（调用方保留原 base64 项，由下游链路处理）。
    """
    mime_type = str(item.get("mimeType") or item.get("mime_type") or "").lower().strip()
    suffix = image_suffix_for_mime(mime_type)
    if suffix is None:
        return None
    filename = ensure_image_upload_filename(
        safe_upload_filename(
            str(item.get("filename") or f"image-{index + 1}{suffix}"),
            fallback=f"image-{index + 1}{suffix}",
        ),
        suffix,
    )
    safe_session_id = safe_session_dirname(session_id)
    rel_path = f"agent/sessions/{safe_session_id}/uploads/{filename}"
    # A1g：同 document.persist——统一 E2A 分块上传，legacy 不再本地写盘。
    ok, payload = await upload_file_bytes_via_e2a(
        data,
        rel_path,
        agent_client=agent_client,
        user_id=user_id,
        channel_id="web",
        session_id=session_id,
    )
    if not ok:
        logger.warning("[media.persist] 大图上传失败: %s", payload.get("error"))
        return None
    return {
        "type": "image",
        "filename": filename,
        "mime_type": mime_type,
        "path": str(payload.get("path") or ""),
        "size_bytes": len(data),
        "_persisted": True,
    }


async def _pre_persist_large_media(
    params: dict[str, Any], *, session_id: str | None, agent_client: Any, user_id: str | None
) -> dict[str, Any]:
    """转发 media.persist 前，把超预算的 base64 图片改为 HTTP bridge 上传。

    小图保留 base64 走 E2A（AgentServer 注入目录落盘）；大图在 Gateway 侧
    解码后经 HTTP 上传并标记 ``_persisted``，AgentServer 侧直接透传落盘记录，
    不重复解码。返回处理后的 params（原对象就地修改 media_items）。
    """
    items = params.get("media_items")
    if not isinstance(items, list):
        return params
    try:
        base_payload = {k: v for k, v in params.items() if k != "media_items"}
        overhead = len(json.dumps(base_payload, ensure_ascii=False, default=str))
    except Exception:  # noqa: BLE001
        overhead = 4096
    remaining = E2A_PAYLOAD_MAX_BYTES - overhead
    new_items: list[Any] = []
    for index, item in enumerate(items):
        if not isinstance(item, dict) or item.get("type") != "image":
            new_items.append(item)
            continue
        raw = item.get("base64Data") or item.get("base64_data")
        if not isinstance(raw, str) or not raw.strip():
            new_items.append(item)
            continue
        try:
            data = base64.b64decode(strip_data_uri_prefix(raw), validate=True)
        except Exception:  # noqa: BLE001
            new_items.append(item)
            continue
        if not data:
            new_items.append(item)
            continue
        if len(data) > remaining:
            persisted = await _upload_media_item_via_http(
                item,
                data,
                session_id=session_id,
                index=index,
                agent_client=agent_client,
                user_id=user_id,
            )
            if persisted is not None:
                new_items.append(persisted)
                continue
            # 上传失败：保留原 base64（若仍超帧限制，由下游链路返回可重试错误）
            new_items.append(item)
            continue
        remaining -= len(data)
        new_items.append(item)
    params["media_items"] = new_items
    return params
