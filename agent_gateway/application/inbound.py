# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Message preprocessing and the existing ACP inbound queue."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import logging
from typing import AbstractSet, Any, TYPE_CHECKING

from e2a.message import Message
from agent_gateway.channel_manager.web.req_method_routing import (
    ROUTE_DENIED,
    ROUTE_FORWARD_UNKNOWN,
    ROUTE_LOCAL,
    route_req_method,
)
from agent_gateway.common.protocol_methods import ReqMethod
from agent_gateway.telemetry import trace_stage

logger = logging.getLogger("gateway")
_LOGIN_REFRESH_WAIT_S = 20.0

def _auth_session_id(msg) -> str | None:
    """从入站消息里取调用方的**登录会话** id（不是对话 session_id）。

    浏览器在 HTTP 上用 cookie，但对话走的是 WebSocket，cookie 不会出现在每条
    消息里，所以前端把登录会话 id 放进 ``metadata.auth_session``。它就是
    ``X-Auth-Session`` 头里的那个值——同一个凭据换了条通道，不构成新的信任假设。
    """
    metadata = getattr(msg, "metadata", None)
    if not isinstance(metadata, dict):
        return None
    value = str(metadata.get("auth_session") or "").strip()
    return value or None


def _normalize_model_auth(params: dict[str, Any], msg) -> None:
    """给登录模型挂上本次调用方的凭据。失败不阻断转发。"""
    try:
        from agent_gateway.auth.passthrough import normalize_model_auth

        normalize_model_auth(params, _auth_session_id(msg))
    except Exception:  # noqa: BLE001 — 凭据注入失败不该让整条消息发不出去
        logger.debug("[App] 模型凭据注入失败", exc_info=True)


async def _refresh_expired_login_credential(msg) -> None:
    """用登录模型、而凭据已过期时，先把凭据续好再转发。

    放置超过id_token有效期1小时后发的第一条消息，转发时 token 已过期，挂不上
    凭据，AgentServer会当成未登录；用户点一下账号再回来又好了。
    这里把续期提前到转发之前：放线程里等，不卡事件循环；每条入站消息各自一个 task，
    也不挡同一连接的其他请求；同一账号并发的几次续期在 ``try_refresh`` 里合并成一次。
    续不了（refresh_token失效等）就照旧转发，由AgentServer提示重新登录。
    """
    try:
        from agent_gateway.auth.passthrough import expired_login_session
        from agent_gateway.auth.service import get_auth_service

        params = msg.params if isinstance(msg.params, dict) else {}
        session = expired_login_session(params, _auth_session_id(msg))
        if session is None:
            return
        await asyncio.wait_for(
            asyncio.to_thread(get_auth_service().try_refresh, session),
            timeout=_LOGIN_REFRESH_WAIT_S,
        )
    except asyncio.TimeoutError:
        logger.warning("[App] 转发前续期登录凭据超时，本次按原凭据转发 id=%s", getattr(msg, "id", None))
    except Exception:  # noqa: BLE001 — 续期失败不该让整条消息发不出去
        logger.debug("[App] 转发前续期登录凭据失败", exc_info=True)


def _normalize_gateway_message(msg, *, apply_model_auth: bool = True):

    req_method = getattr(msg, "req_method", None) or ReqMethod.CHAT_SEND
    params = dict(msg.params or {})
    if "query" not in params and "content" in params:
        params["query"] = params["content"]
    if req_method == ReqMethod.CHAT_RESUME:
        req_method = ReqMethod.CHAT_CANCEL
        params.setdefault("intent", "resume")

    method_val = req_method.value
    is_stream = bool(
        msg.is_stream
        or method_val in (ReqMethod.CHAT_SEND.value, ReqMethod.HISTORY_GET.value)
    )

    # 登录模型的凭据在这里挂上，随请求带给AgentServer
    if apply_model_auth:
        _normalize_model_auth(params, msg)

    normalized = Message(
        id=msg.id,
        type=msg.type,
        channel_id=msg.channel_id,
        session_id=msg.session_id,
        params=params,
        timestamp=msg.timestamp,
        ok=msg.ok,
        req_method=req_method,
        mode=msg.mode,
        is_stream=is_stream,
        stream_seq=msg.stream_seq,
        stream_id=msg.stream_id,
        metadata=msg.metadata,
        app_id=getattr(msg, "app_id", None),
        user_id=getattr(msg, "user_id", None),
    )
    from agent_gateway.telemetry import attach_message_trace, get_message_trace
    attach_message_trace(normalized, get_message_trace(msg))
    return normalized


def _inject_session_work_mode(msg: Message) -> None:
    """为 ``session.create`` 请求注入 work_mode 归一化(主路径兜底)。

    与 fallback ``_session_create`` 共用同一 helper
    ``resolve_session_work_mode_params``,保持主路径/fallback 一致。

    成功时写回归一化后的 ``project_id`` / ``project_dir`` / ``work_mode`` 到
    ``msg.params``;失败时(非法 work_mode)不写回,保留原始 params 由后续
    AgentServer 或 fallback ``_session_create`` 返回 BAD_REQUEST。

    本函数做参数归一化 + **TUI project_dir 预解析**(设计文档 §5.3.5):
    - TUI 通道下,若 project_id 为默认项目且 project_dir 非空绝对路径,
      按 work_mode="code" 查找/创建 code 项目,将真实 project_id 写回 params。
    - 非 TUI 通道仅做纯参数归一化,最终 work_mode 以 Project 记录为准的
      校验由 ``_session_create`` / AgentServer 侧完成。

    显式性判定(``has_explicit_work_mode``)由 ``resolve_session_work_mode_params``
    随 binding 返回。但 gateway → AgentServer 是跨进程通信,binding 结果不能
    直接传递。因此 gateway 将 ``has_explicit_work_mode`` 注入 params 的
    ``_work_mode_explicit`` 字段作为传输标记,AgentServer 消费后立即 pop。
    AgentServer 直连调用方(非 gateway 路径)不携带此标记,此时 AgentServer
    通过重新调用 ``resolve_session_work_mode_params`` 获取 binding 中的
    ``has_explicit_work_mode``(直连场景 params 为原始值,计算结果正确)。
    """
    params = getattr(msg, "params", None)
    if not isinstance(params, dict):
        return
    channel_id = getattr(msg, "channel_id", None)
    try:
        from agent_gateway.runtime.work_mode import resolve_session_work_mode_params
        binding = resolve_session_work_mode_params(params, channel_id=channel_id)
    except Exception:  # noqa: BLE001
        # 归一化异常时不写回,保留原始 params 由后续处理
        return
    if binding.error:
        return

    resolved_project_id = binding.project_id
    resolved_project_dir = binding.project_dir
    resolved_work_mode = binding.work_mode

    # 注意:TUI 通道的 session.create 不走 forward 路径(不在 CLI_FORWARD_REQ_METHODS),
    # TUI 的 cwd→code project 绑定由 AgentServer 侧在 SESSION_CREATE 处理时通过
    # find_or_create_code_project_for_tui_params 完成（经 E2A 转发后解析）。
    # 此处仅处理 WEB/ACP 通道的 work_mode 绑定注入。

    params["project_id"] = resolved_project_id
    params["project_dir"] = resolved_project_dir
    params["work_mode"] = resolved_work_mode
    # _work_mode_explicit: 跨进程传输"请求是否显式传了 work_mode"标志。
    # AgentServer 消费后立即 pop,不持久化、不序列化到日志。
    # 该标志区分"用户显式传 work_mode"(需做 Project 一致性校验)与
    # "gateway 注入通道默认 work_mode"(跳过一致性校验,以 Project 记录为准)。
    params["_work_mode_explicit"] = binding.has_explicit_work_mode


class InboundQueue:
    """Gateway internal inbound service for forwarding channel messages to MessageHandler."""

    def __init__(self, inbound_handler):
        self._inbound_handler = inbound_handler
        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._serve_loop(), name="gateway-inbound-server")

    async def stop(self) -> None:
        self._running = False
        task = self._task
        self._task = None
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def handle_message(self, msg) -> bool:
        await self._queue.put(msg)
        return True

    async def _serve_loop(self) -> None:
        while self._running:
            try:
                msg = await self._queue.get()
            except asyncio.CancelledError:
                break
            try:
                handled = self._inbound_handler(msg)
                if asyncio.iscoroutine(handled):
                    await handled
            except Exception:  # noqa: BLE001
                logger.exception("[App] Gateway inbound handling failed: id=%s", getattr(msg, "id", None))



@dataclass(frozen=True)
class MessagePreprocessor:
    """Explicit callbacks shared by the original Web, TUI and ACP paths."""

    refresh_credentials: Callable[[Any], Awaitable[None]]
    normalize: Callable[[Any], Any]
    inject_work_mode: Callable[[Any], None]

    async def _prepare(self, msg: Any) -> Any:
        with trace_stage("preprocess.credentials"):
            await self.refresh_credentials(msg)
        return self.normalize(msg)

    async def forward(self, msg: Any, channel_manager: Any) -> bool:
        normalized = await self._prepare(msg)
        method = getattr(getattr(msg, "req_method", None), "value", None) or ""
        if method == "session.create":
            self.inject_work_mode(normalized)
        await channel_manager.deliver_to_message_handler(normalized)
        logger.info("[App] Gateway inbound -> MessageHandler: id=%s channel_id=%s", msg.id, msg.channel_id)
        return False

    def for_channel(
        self,
        channel_manager: Any,
        forward_methods: set[str] | frozenset[str],
        no_local_methods: set[str] | frozenset[str],
        source_label: str,
        local_methods: AbstractSet[str] | None = None,
    ) -> Callable[[Any], Awaitable[bool]]:
        async def handle(msg: Any) -> bool:
            req_method = getattr(msg, "req_method", None)
            if req_method is None:
                return False
            method = getattr(req_method, "value", None) or ""
            route = route_req_method(
                method,
                forward_methods=forward_methods,
                local_methods=local_methods,
            )
            if route in (ROUTE_LOCAL, ROUTE_DENIED):
                return False
            normalized = await self._prepare(msg)
            if method == "session.create":
                self.inject_work_mode(normalized)
            await channel_manager.deliver_to_message_handler(normalized)
            logger.debug("[App] %s 入站 -> MessageHandler: id=%s channel_id=%s", source_label, msg.id, msg.channel_id)
            return route == ROUTE_FORWARD_UNKNOWN or method in no_local_methods

        return handle
