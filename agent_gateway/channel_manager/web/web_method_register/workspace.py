# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway Web RPC：workspace.tree / usage / entries.delete / preview / download。"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
from dataclasses import dataclass
from typing import Any

from agent_gateway.enterprise import is_enterprise
from agent_gateway.enterprise.capabilities.workspace.quota import (
    disabled_usage_payload,
    resolve_quota_snapshot,
    snapshot_to_dict,
    upsert_usage_cache,
)

logger = logging.getLogger(__name__)

_FEATURE_ENABLED_ENV = "WORKSPACE_QUOTA_ENABLED"

# preview 契约：缺省 512KiB，硬顶 1MiB（本地与转发必须一致）。
_PREVIEW_DEFAULT_MAX_BYTES = 512 * 1024
_PREVIEW_HARD_MAX_BYTES = 1024 * 1024


def parse_preview_max_bytes(raw: Any) -> int:
    """解析 ``max_bytes``：缺省 512KiB；非法非整数 → ``ValueError``；结果 ``min`` 到 1MiB。

    接受正整数或可解析为正整数的十进制字符串；``bool`` / 空串以外的非数字一律拒绝。
    """
    if raw is None:
        return _PREVIEW_DEFAULT_MAX_BYTES
    if isinstance(raw, bool):
        raise ValueError("max_bytes must be a positive integer")
    if isinstance(raw, int):
        n = raw
    elif isinstance(raw, str):
        s = raw.strip()
        if not s:
            return _PREVIEW_DEFAULT_MAX_BYTES
        # 仅接受纯十进制整数字符串（可带可选符号），拒绝 "1.5" / "1e3"。
        if s[0] in "+-" and s[1:].isdigit():
            n = int(s)
        elif s.isdigit():
            n = int(s)
        else:
            raise ValueError("max_bytes must be a positive integer")
    else:
        raise ValueError("max_bytes must be a positive integer")
    if n <= 0:
        raise ValueError("max_bytes must be a positive integer")
    return min(n, _PREVIEW_HARD_MAX_BYTES)


def is_workspace_quota_enabled() -> bool:
    """用户空间配额特性开关；仅企业版生效，默认开启。

    与 AgentServer / Manager 共用 ``WORKSPACE_QUOTA_ENABLED``；未设置默认 true。
    个人版恒为关闭。
    """
    if not is_enterprise():
        return False
    raw = os.environ.get(_FEATURE_ENABLED_ENV)
    if raw is None or not str(raw).strip():
        return True
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _import_local_workspace():
    """个人版本地落盘依赖 jiuwenswarm；企业版 / 拆分部署走 E2A 转发，不需要。"""
    try:
        from jiuwenswarm.common.utils import get_multi_tenant_user_workspace_dir
        from jiuwenswarm.enterprise.capabilities.workspace.quota import (
            get_cached_used,
            mark_usage_active,
            measure_and_cache,
        )
        from jiuwenswarm.enterprise.capabilities.workspace.service import (
            WorkspaceError,
            WorkspaceService,
        )
    except ImportError as exc:  # pragma: no cover - split-deploy personal-only path
        raise RuntimeError(
            "local workspace handlers require jiuwenswarm package in Gateway PYTHONPATH"
        ) from exc
    return (
        get_multi_tenant_user_workspace_dir,
        get_cached_used,
        mark_usage_active,
        measure_and_cache,
        WorkspaceError,
        WorkspaceService,
    )


_local_workspace_available: bool | None = None


def _has_local_workspace() -> bool:
    """Gateway PYTHONPATH 是否含 jiuwenswarm（可做进程内本地盘）。"""
    global _local_workspace_available
    if _local_workspace_available is None:
        try:
            _import_local_workspace()
            _local_workspace_available = True
        except RuntimeError:
            _local_workspace_available = False
    return _local_workspace_available


def _use_local_workspace() -> bool:
    """仅个人版且能导入 jiuwenswarm 时走本地盘；否则转发 Agent（旧代理语义）。

    拆分部署个人版 Gateway 独立进程、无宿主包时，本地路径会 ImportError→RuntimeError；
    此时必须回落到 ``_forward_to_agent``，不能让通用 except 变成 INTERNAL_ERROR 500。
    """
    return (not is_enterprise()) and _has_local_workspace()


@dataclass(frozen=True)
class _AgentForwardCall:
    channel: Any
    agent_client: Any
    ws: Any
    req_id: Any
    params: dict[str, Any]
    session_id: str | None
    req_method: Any
    user_id: str | None = None


class WorkspaceIdentityError(Exception):
    """企业版 workspace 身份缺失或与客户端参数冲突。"""

    def __init__(self, code: str, message: str, *, field: str | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.field = field


def _identity_from_params(params: dict[str, Any] | None) -> tuple[str, str, str]:
    p = params if isinstance(params, dict) else {}
    return (
        str(p.get("user_id") or "").strip(),
        str(p.get("group_id") or "").strip(),
        str(p.get("bot_id") or "").strip(),
    )


def _trusted_user_id(ws: Any, user_id: str | None) -> str:
    """仅从受信来源取 user_id（dispatch / ``_web_routing`` / 连接属性）。"""
    uid = str(user_id or "").strip()
    if uid:
        return uid
    routing = getattr(ws, "_web_routing", None)
    if isinstance(routing, dict):
        uid = str(routing.get("user_id") or "").strip()
        if uid:
            return uid
    try:
        from agent_gateway.channel_manager.web.web_connect import (
            _WEB_CONNECTION_USER_ID_ATTR,
        )

        return str(getattr(ws, _WEB_CONNECTION_USER_ID_ATTR, None) or "").strip()
    except Exception:  # noqa: BLE001
        return ""


def _identity_for_forward(
    ws: Any,
    params: dict[str, Any],
    *,
    user_id: str | None = None,
) -> tuple[str, str, str]:
    """合成转发身份。

    受信来源：dispatch ``user_id`` → ``ws._web_routing`` → 连接级
    ``_WEB_CONNECTION_USER_ID_ATTR``；``group_id`` / ``bot_id`` 只取 ``_web_routing``。

    - **企业版**：禁止把 query/body 身份当权威。受信三元组缺失 →
      ``UNAUTHORIZED``；客户端显式身份与受信不一致 → ``IDENTITY_CONFLICT``。
    - **个人版**：仍可用 params 兜底（本地盘 / 无 routing 握手场景）。
    """
    routing = getattr(ws, "_web_routing", None)
    if not isinstance(routing, dict):
        routing = {}
    p_uid, p_gid, p_bid = _identity_from_params(params)
    trusted_uid = _trusted_user_id(ws, user_id)
    trusted_gid = str(routing.get("group_id") or "").strip()
    trusted_bid = str(routing.get("bot_id") or "").strip()

    if is_enterprise():
        if not trusted_uid or not trusted_gid or not trusted_bid:
            raise WorkspaceIdentityError(
                "UNAUTHORIZED",
                "trusted workspace identity required",
            )
        for field, trusted, client in (
            ("user_id", trusted_uid, p_uid),
            ("group_id", trusted_gid, p_gid),
            ("bot_id", trusted_bid, p_bid),
        ):
            if client and client != trusted:
                raise WorkspaceIdentityError(
                    "IDENTITY_CONFLICT",
                    "Request header and body identity must match",
                    field=field,
                )
        return trusted_uid, trusted_gid, trusted_bid

    return (
        trusted_uid or p_uid,
        trusted_gid or p_gid,
        trusted_bid or p_bid,
    )


def _bind_trusted_identity(
    ws: Any,
    params: dict[str, Any],
    *,
    user_id: str | None = None,
) -> tuple[str, str, str]:
    """把受信身份写回 params；企业版冲突/缺失时抛 ``WorkspaceIdentityError``。"""
    uid, group_id, bot_id = _identity_for_forward(ws, params, user_id=user_id)
    params["user_id"] = uid
    params["group_id"] = group_id
    params["bot_id"] = bot_id
    return uid, group_id, bot_id


async def _reject_identity(
    channel: Any,
    ws: Any,
    req_id: Any,
    exc: WorkspaceIdentityError,
) -> None:
    details = {"field": exc.field} if exc.field else {}
    await channel.send_response(
        ws,
        req_id,
        ok=False,
        error=exc.message,
        code=exc.code,
        payload=details or None,
    )


async def _forward_to_agent(
    call: _AgentForwardCall,
) -> tuple[bool, dict[str, Any], str | None, str | None]:
    """Forward via E2A; return (ok, payload, error, code)."""
    from agent_gateway.common.request_identity import apply_routing_metadata
    from agent_gateway.routing.e2a_proxy import fetch_agent_unary

    agent_client = call.agent_client
    ac = agent_client() if callable(agent_client) else agent_client
    if ac is None or not getattr(ac, "server_ready", False):
        return False, {}, "agent not ready", "INTERNAL_ERROR"

    params = call.params
    user_id, group_id, bot_id = _identity_for_forward(
        call.ws, params, user_id=call.user_id
    )
    # 业务 params 不携带身份三元组（与 request_identity 约定一致）；身份走信封。
    forward_params = {
        k: v
        for k, v in (params or {}).items()
        if k not in {"user_id", "group_id", "bot_id", "gateway_id"}
    }
    routing_meta = apply_routing_metadata(
        {},
        {
            "user_id": user_id,
            "group_id": group_id,
            "bot_id": bot_id,
        },
    )
    ok, pl = await fetch_agent_unary(
        agent_client=ac,
        req_method=call.req_method,
        params=forward_params,
        session_id=call.session_id,
        user_id=user_id or None,
        channel_id=getattr(call.channel, "channel_id", None) or "web",
        label="workspace",
        metadata=routing_meta or None,
    )
    if not ok:
        return (
            False,
            pl if isinstance(pl, dict) else {},
            str((pl or {}).get("error") or (pl or {}).get("message") or "request failed"),
            str((pl or {}).get("code") or "BAD_REQUEST"),
        )
    return True, pl if isinstance(pl, dict) else {}, None, None


def _local_service(params: dict[str, Any] | None) -> Any:
    (
        get_multi_tenant_user_workspace_dir,
        _get_cached_used,
        _mark_usage_active,
        _measure_and_cache,
        _WorkspaceError,
        WorkspaceService,
    ) = _import_local_workspace()
    p = params if isinstance(params, dict) else {}
    if is_enterprise():
        key = str(p.get("workspace_key") or "default").strip() or "default"
        root = get_multi_tenant_user_workspace_dir(key)
    else:
        # 本地个人版无 origin 的 service_/agent_ 多实例布局，落默认用户工作区。
        root = get_multi_tenant_user_workspace_dir()
    return WorkspaceService(tenant_root=root)


def _workspace_error_type() -> type[BaseException]:
    try:
        return _import_local_workspace()[4]
    except Exception:  # noqa: BLE001
        return type("_NoWorkspaceError", (Exception,), {})


def _local_list_tree(params: dict[str, Any], relative_path: Any) -> Any:
    """同步扫盘；须在 worker 线程调用，避免阻塞 asyncio 事件循环。"""
    return _local_service(params).list_tree(relative_path)


def _local_preview_file(
    params: dict[str, Any], relative_path: Any, max_bytes: int | None
) -> Any:
    return _local_service(params).preview_file(relative_path, max_bytes=max_bytes)


def _local_download_payload(params: dict[str, Any], relative_path: Any) -> dict[str, Any]:
    """本地盘下载：小文件 inline base64；大文件签发 verified token（同 Agent 侧）。"""
    from agent_gateway.routing.agent_http_bridge import E2A_PAYLOAD_MAX_BYTES

    blob = _local_service(params).download_payload(relative_path)
    content = blob.get("content") or b""
    if not isinstance(content, (bytes, bytearray)):
        content = bytes(content)
    filename = str(blob.get("filename") or "download.bin")
    content_type = str(blob.get("content_type") or "application/octet-stream")
    size_bytes = len(content)
    if size_bytes <= int(E2A_PAYLOAD_MAX_BYTES):
        return {
            "filename": filename,
            "content_type": content_type,
            "size_bytes": size_bytes,
            "content_b64": base64.b64encode(content).decode("ascii"),
        }
    # 个人版本地大文件：仍经 HTTP 路由回包，但避免把超大 base64 塞进一元帧。
    # 与 AgentServer 一致走 verified download token → /file-api/download。
    import time
    from pathlib import Path

    from jiuwenswarm.common.file_download_token import build_verified_asset_download_info
    from jiuwenswarm.common.verified_download_assets import (
        get_verified_download_asset_owner,
    )

    owner = get_verified_download_asset_owner()
    svc = _local_service(params)
    source = None
    try:
        rel = str(relative_path or "").strip()
        if rel:
            candidate = svc.resolve(rel)
            if candidate.is_file() and candidate.stat().st_size == size_bytes:
                source = candidate
    except Exception:  # noqa: BLE001
        source = None
    if source is not None:
        asset = owner.stage(
            source, file_name=filename, expires_at=0, reference_only=True
        )
    else:
        import tempfile

        suffix = Path(filename).suffix[:32] or ".bin"
        fd, tmp_name = tempfile.mkstemp(prefix="ws-dl-", suffix=suffix)
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(content)
            asset = owner.stage(
                tmp_path,
                file_name=filename,
                expires_at=time.time() + 600,
            )
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
    try:
        info = build_verified_asset_download_info(
            asset,
            filename,
            session_id=str(params.get("session_id") or ""),
            user_id=str(params.get("user_id") or ""),
        )
        owner.commit(asset)
    except Exception:
        owner.revoke(asset)
        raise
    return {
        "filename": filename,
        "content_type": content_type,
        "size_bytes": size_bytes,
        "download_token": info["download_token"],
        "download_url": info["download_url"],
    }


def _local_usage_bytes(params: dict[str, Any]) -> int:
    """同步用量读取（含可能的 du）；须在 worker 线程调用。"""
    (
        _get_root,
        get_cached_used,
        mark_usage_active,
        measure_and_cache,
        _WorkspaceError,
        _WorkspaceService,
    ) = _import_local_workspace()
    svc = _local_service(params)
    uid, gid, bid = _identity_from_params(params)
    raw = params.get("force_refresh")
    force = raw is True or raw == 1
    if isinstance(raw, str) and raw.strip().lower() in {"1", "true", "yes", "on"}:
        force = True
    if force:
        return measure_and_cache(
            svc.tenant_root, user_id=uid, group_id=gid, bot_id=bid
        )
    mark_usage_active(svc.tenant_root, user_id=uid, group_id=gid, bot_id=bid)
    return get_cached_used(svc.tenant_root)


def _local_delete_and_usage(
    params: dict[str, Any], paths: list[str]
) -> tuple[Any, int]:
    """删除 + 立即 du 重算用量（勿用删除前缓存回写），整段放 worker 线程。"""
    svc = _local_service(params)
    data = svc.delete_entries(paths)
    (
        _get_root,
        _get_cached_used,
        _mark_usage_active,
        measure_and_cache,
        _WorkspaceError,
        _WorkspaceService,
    ) = _import_local_workspace()
    uid, gid, bid = _identity_from_params(params)
    # DELETE 路由无 force_refresh；删除后必须 measure，否则 get_cached_used
    # 仍是删前值，upsert 会把过期用量当最新上报写回。
    used = measure_and_cache(
        svc.tenant_root, user_id=uid, group_id=gid, bot_id=bid
    )
    return data, used


def _parse_used_bytes(raw: Any) -> int | None:
    """解析 Agent/本地返回的 used_bytes；缺失、bool、负值、非 int 一律视为非法。

    不可用 ``or 0``：会把缺失/null 静默写成 0 并覆盖用量缓存。
    """
    if isinstance(raw, bool) or not isinstance(raw, int):
        return None
    if raw < 0:
        return None
    return raw


async def _usage_payload_from_used(
    used_bytes: int,
    params: dict[str, Any] | None,
) -> dict[str, Any]:
    user_id, group_id, bot_id = _identity_from_params(params)
    snap = await resolve_quota_snapshot(
        used_bytes,
        user_id=user_id,
        group_id=group_id,
        bot_id=bot_id,
    )
    await upsert_usage_cache(
        user_id=user_id,
        group_id=group_id,
        bot_id=bot_id,
        used_bytes=used_bytes,
    )
    return snapshot_to_dict(snap, user_id=user_id, group_id=group_id, bot_id=bot_id)


def register_workspace_web_methods(channel: Any, *, agent_client: Any) -> None:
    """Register workspace.* WebChannel methods (HTTP mapped routes + WS)."""
    from agent_gateway.common.protocol_methods import ReqMethod

    try:
        from jiuwenswarm.enterprise.capabilities.workspace.usage_reconciler import (
            start_usage_reconciler,
        )

        start_usage_reconciler()
    except Exception:  # noqa: BLE001
        logger.debug("[workspace] usage reconciler start skipped", exc_info=True)

    def _resolve_ac() -> Any:
        if callable(agent_client):
            return agent_client()
        return agent_client

    def _fwd(
        *,
        ws: Any,
        req_id: Any,
        params: dict[str, Any],
        session_id: str | None,
        req_method: Any,
        user_id: str | None = None,
    ) -> _AgentForwardCall:
        return _AgentForwardCall(
            channel=channel,
            agent_client=_resolve_ac,
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            req_method=req_method,
            user_id=user_id,
        )

    async def _tree(ws, req_id, params, session_id, user_id=None):
        p = dict(params) if isinstance(params, dict) else {}
        try:
            uid, gid, bid = _bind_trusted_identity(ws, p, user_id=user_id)
        except WorkspaceIdentityError as exc:
            await _reject_identity(channel, ws, req_id, exc)
            return
        relative_path = p.get("relative_path")
        try:
            if not _use_local_workspace():
                ok, payload, err, code = await _forward_to_agent(
                    _fwd(
                        ws=ws,
                        req_id=req_id,
                        params=p,
                        session_id=session_id,
                        req_method=ReqMethod.WORKSPACE_TREE,
                        user_id=user_id,
                    ),
                )
                if not ok:
                    await channel.send_response(
                        ws, req_id, ok=False, error=err or "failed", code=code or "BAD_REQUEST"
                    )
                    return
                used = _parse_used_bytes(payload.get("used_bytes"))
                if used is not None:
                    await upsert_usage_cache(
                        user_id=uid,
                        group_id=gid,
                        bot_id=bid,
                        used_bytes=used,
                    )
                data = {k: v for k, v in payload.items() if k != "used_bytes"}
                await channel.send_response(ws, req_id, ok=True, payload=data)
                return

            data = await asyncio.to_thread(_local_list_tree, p, relative_path)
            await channel.send_response(ws, req_id, ok=True, payload=data)
        except _workspace_error_type() as exc:
            await channel.send_response(
                ws, req_id, ok=False, error=exc.message, code=exc.code, payload=exc.details or None
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("[workspace.tree] %s", exc)
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR"
            )

    async def _usage(ws, req_id, params, session_id, user_id=None):
        p = dict(params) if isinstance(params, dict) else {}
        try:
            uid, gid, bid = _bind_trusted_identity(ws, p, user_id=user_id)
        except WorkspaceIdentityError as exc:
            await _reject_identity(channel, ws, req_id, exc)
            return
        # 特性开关关闭：不转发、不扫盘，回固定 disabled 载荷（个人版也可关）。
        if not is_workspace_quota_enabled():
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=disabled_usage_payload(user_id=uid, group_id=gid, bot_id=bid),
            )
            return
        try:
            if not _use_local_workspace():
                ok, payload, err, code = await _forward_to_agent(
                    _fwd(
                        ws=ws,
                        req_id=req_id,
                        params=p,
                        session_id=session_id,
                        req_method=ReqMethod.WORKSPACE_USAGE,
                        user_id=user_id,
                    ),
                )
                if not ok:
                    await channel.send_response(
                        ws, req_id, ok=False, error=err or "failed", code=code or "BAD_REQUEST"
                    )
                    return
                used = _parse_used_bytes(payload.get("used_bytes"))
                if used is None:
                    logger.error(
                        "[workspace.usage] agent payload missing/invalid used_bytes: %r",
                        payload.get("used_bytes"),
                    )
                    await channel.send_response(
                        ws,
                        req_id,
                        ok=False,
                        error="invalid used_bytes from agent",
                        code="INTERNAL_ERROR",
                    )
                    return
            else:
                used = await asyncio.to_thread(_local_usage_bytes, p)
                parsed = _parse_used_bytes(used)
                if parsed is None:
                    logger.error("[workspace.usage] local used_bytes invalid: %r", used)
                    await channel.send_response(
                        ws,
                        req_id,
                        ok=False,
                        error="invalid local used_bytes",
                        code="INTERNAL_ERROR",
                    )
                    return
                used = parsed
            out = await _usage_payload_from_used(used, p)
            await channel.send_response(ws, req_id, ok=True, payload=out)
        except Exception as exc:  # noqa: BLE001
            logger.exception("[workspace.usage] %s", exc)
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR"
            )

    async def _delete(ws, req_id, params, session_id, user_id=None):
        p = dict(params) if isinstance(params, dict) else {}
        try:
            uid, gid, bid = _bind_trusted_identity(ws, p, user_id=user_id)
        except WorkspaceIdentityError as exc:
            await _reject_identity(channel, ws, req_id, exc)
            return
        paths = p.get("relative_paths")
        if not isinstance(paths, list) or not paths:
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="relative_paths required",
                code="BAD_REQUEST",
            )
            return
        try:
            if not _use_local_workspace():
                ok, payload, err, code = await _forward_to_agent(
                    _fwd(
                        ws=ws,
                        req_id=req_id,
                        params=p,
                        session_id=session_id,
                        req_method=ReqMethod.WORKSPACE_ENTRIES_DELETE,
                        user_id=user_id,
                    ),
                )
                if not ok:
                    await channel.send_response(
                        ws, req_id, ok=False, error=err or "failed", code=code or "BAD_REQUEST"
                    )
                    return
                used = _parse_used_bytes(payload.get("used_bytes"))
                data = {k: v for k, v in payload.items() if k != "used_bytes"}
                if used is not None:
                    await upsert_usage_cache(
                        user_id=uid,
                        group_id=gid,
                        bot_id=bid,
                        used_bytes=used,
                    )
                await channel.send_response(ws, req_id, ok=True, payload=data)
                return

            path_list = [str(x) for x in paths]
            data, used = await asyncio.to_thread(_local_delete_and_usage, p, path_list)
            await upsert_usage_cache(
                user_id=uid,
                group_id=gid,
                bot_id=bid,
                used_bytes=used,
            )
            await channel.send_response(ws, req_id, ok=True, payload=data)
        except _workspace_error_type() as exc:
            await channel.send_response(
                ws, req_id, ok=False, error=exc.message, code=exc.code
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("[workspace.entries.delete] %s", exc)
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR"
            )

    async def _preview(ws, req_id, params, session_id, user_id=None):
        p = dict(params) if isinstance(params, dict) else {}
        try:
            _bind_trusted_identity(ws, p, user_id=user_id)
        except WorkspaceIdentityError as exc:
            await _reject_identity(channel, ws, req_id, exc)
            return
        relative_path = p.get("relative_path")
        try:
            max_n = parse_preview_max_bytes(p.get("max_bytes"))
        except (TypeError, ValueError):
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="max_bytes must be a positive integer",
                code="BAD_REQUEST",
            )
            return
        # 归一化后再转发，避免本地 / Agent 对缺省与硬顶各自解释。
        p["max_bytes"] = max_n
        try:
            if not _use_local_workspace():
                ok, payload, err, code = await _forward_to_agent(
                    _fwd(
                        ws=ws,
                        req_id=req_id,
                        params=p,
                        session_id=session_id,
                        req_method=ReqMethod.WORKSPACE_PREVIEW,
                        user_id=user_id,
                    ),
                )
                if not ok:
                    await channel.send_response(
                        ws, req_id, ok=False, error=err or "failed", code=code or "BAD_REQUEST"
                    )
                    return
                await channel.send_response(ws, req_id, ok=True, payload=payload)
                return

            data = await asyncio.to_thread(_local_preview_file, p, relative_path, max_n)
            await channel.send_response(ws, req_id, ok=True, payload=data)
        except _workspace_error_type() as exc:
            await channel.send_response(
                ws, req_id, ok=False, error=exc.message, code=exc.code, payload=exc.details or None
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("[workspace.preview] %s", exc)
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR"
            )

    async def _download(ws, req_id, params, session_id, user_id=None):
        """返回 filename / content_type / content_b64；HTTP 层再解包成文件流。"""
        p = dict(params) if isinstance(params, dict) else {}
        try:
            _bind_trusted_identity(ws, p, user_id=user_id)
        except WorkspaceIdentityError as exc:
            await _reject_identity(channel, ws, req_id, exc)
            return
        relative_path = p.get("relative_path")
        try:
            if not _use_local_workspace():
                ok, payload, err, code = await _forward_to_agent(
                    _fwd(
                        ws=ws,
                        req_id=req_id,
                        params=p,
                        session_id=session_id,
                        req_method=ReqMethod.WORKSPACE_DOWNLOAD,
                        user_id=user_id,
                    ),
                )
                if not ok:
                    await channel.send_response(
                        ws, req_id, ok=False, error=err or "failed", code=code or "BAD_REQUEST"
                    )
                    return
                await channel.send_response(ws, req_id, ok=True, payload=payload)
                return

            payload = await asyncio.to_thread(_local_download_payload, p, relative_path)
            await channel.send_response(ws, req_id, ok=True, payload=payload)
        except _workspace_error_type() as exc:
            await channel.send_response(
                ws, req_id, ok=False, error=exc.message, code=exc.code, payload=exc.details or None
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("[workspace.download] %s", exc)
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR"
            )

    channel.register_method(ReqMethod.WORKSPACE_TREE.value, _tree)
    channel.register_method(ReqMethod.WORKSPACE_USAGE.value, _usage)
    channel.register_method(ReqMethod.WORKSPACE_ENTRIES_DELETE.value, _delete)
    channel.register_method(ReqMethod.WORKSPACE_PREVIEW.value, _preview)
    channel.register_method(ReqMethod.WORKSPACE_DOWNLOAD.value, _download)
