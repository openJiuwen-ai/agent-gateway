# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Wire the original WebChannel, handlers and application plugins."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import inspect
import logging
import time
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from agent_gateway.auth.credential_access import CredentialUpdateSink
    from agent_gateway.auth.service import AuthService

from e2a.message import (
    Message, PROMPT_SUGGESTION_READY_EVENT,
    TOPIC_STARTERS_READY_EVENT, TOPIC_STARTERS_CONTROL_CHANGED_EVENT,
)
from agent_gateway.channel_manager.web.media import normalize_chat_media_attachments
from agent_gateway.application.inbound import MessagePreprocessor

logger = logging.getLogger("gateway")


def assemble_web(
    *,
    agent_client: Any,
    message_handler: Any,
    channel_manager: Any,
    extension_registry: Any,
    on_config_saved: Callable[..., Any],
    heartbeat_service: Any,
    cron_controller: Any,
    heartbeat_controller: Any,
    updater_service: Any,
    host: str,
    port: int,
    path: str,
    ws_permessage_deflate: bool,
    preprocessor: MessagePreprocessor,
    log_stage: Callable[[str], None],
    auth_service_provider: Callable[[], AuthService] | None = None,
    credential_update_sink: CredentialUpdateSink | None = None,
) -> tuple[Any, asyncio.Event]:
    from agent_gateway.channel_manager.protocol.web_proxy.web_proxy_listen import register_3rdagent_web_method
    from agent_gateway.channel_manager.web.app_web_handlers import (
        _FORWARD_NO_LOCAL_HANDLER_METHODS, _FORWARD_REQ_METHODS,
        WebHandlersBindParams, _DummyBus, _register_web_handlers,
    )
    from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig

    client = agent_client
    web_host, web_port, web_path = host, port, path
    web_ws_deflate = ws_permessage_deflate
    web_config = WebChannelConfig(
        enabled=True,
        host=web_host,
        port=web_port,
        path=web_path,
        ws_permessage_deflate=web_ws_deflate,
    )
    web_channel = WebChannel(
        web_config, _DummyBus(), agent_client=client,
        auth_service_provider=auth_service_provider, credential_update_sink=credential_update_sink,
    )
    register_3rdagent_web_method(web_channel)

    async def show_prompt_suggestion(session_id: str, request_id: str, payload: dict[str, Any]) -> None:
        """Forward a suggestion AgentServer already checked and recorded."""
        if payload.get("event_type") != PROMPT_SUGGESTION_READY_EVENT:
            return
        await web_channel.send(Message(
            id=f"prompt-suggestion:{payload.get('suggestion_id')}",
            type="event",
            channel_id=web_channel.channel_id,
            session_id=session_id,
            params={},
            timestamp=time.time(),
            ok=True,
            payload=payload,
        ))

    async def show_recommendation_event(event_type: str, payload: dict[str, Any]) -> None:
        if event_type in {TOPIC_STARTERS_READY_EVENT, TOPIC_STARTERS_CONTROL_CHANGED_EVENT}:
            await web_channel.broadcast_event(event_type, payload)

    message_handler.on_prompt_suggestion = show_prompt_suggestion
    message_handler.on_recommendation_event = show_recommendation_event

    # 注入 Git diff 监控注册表(设计文档阶段10):
    # 1. 让 ``_mark_git_watcher_dirty`` 能通过 ``channel.git_watcher_registry`` 唤醒轮询
    # 2. 通过 ``set_channel`` 让 registry 拿到 send_event 的发送句柄
    from agent_gateway.channel_manager.web.git_diff_watcher import (
        get_git_diff_watcher_registry,
    )
    _git_watcher_registry = get_git_diff_watcher_registry()
    web_channel.git_watcher_registry = _git_watcher_registry
    _git_watcher_registry.set_channel(web_channel)

    # Git watch 状态计算委托：diff 状态、项目解析与工作目录访问都在目标
    # AgentServer 注入目录完成，Gateway 只做指纹比对与事件推送（方案 §10.6 C）。
    async def _git_diff_status_fetcher(request: dict):
        from agent_gateway.routing.e2a_proxy import fetch_git_diff_status

        hunk_paths = request.get("hunk_paths")
        return await fetch_git_diff_status(
            agent_client=client,
            project_id=request.get("project_id"),
            session_id=request.get("session_id") or None,
            include_files=request.get("include_files"),
            include_hunks=request.get("include_hunks"),
            hunk_paths=list(hunk_paths) if hunk_paths else None,
            user_id=request.get("user_id"),
            channel_id="web",
        )

    _git_watcher_registry.set_diff_status_fetcher(_git_diff_status_fetcher)

    _register_web_handlers(
        WebHandlersBindParams(
            channel=web_channel,
            agent_client=client,
            message_handler=message_handler,
            channel_manager=channel_manager,
            on_config_saved=on_config_saved,
            heartbeat_service=heartbeat_service,
            cron_controller=cron_controller,
            heartbeat_controller=heartbeat_controller,
            updater_service=updater_service,
        )
    )

    async def _media_normalizer(params: dict[str, Any], session_id: str | None) -> None:
        # A1g：小图落盘经 E2A 到 AgentServer，闭包捕获启动装配的 client
        await normalize_chat_media_attachments(
            params, session_id, agent_client=client, user_id=None
        )

    extension_registry.bind_application_plugins(
        web_channel,
        agent_client=client,
        media_attachment_normalizer=_media_normalizer,
    )
    log_stage("web_handlers_registered")
    homepage_request_dispatched = asyncio.Event()
    web_norm_and_forward = preprocessor.for_channel(
        channel_manager,
        _FORWARD_REQ_METHODS,
        _FORWARD_NO_LOCAL_HANDLER_METHODS,
        "Web",
    )
    channel_manager.register_channel_with_inbound(web_channel, web_norm_and_forward)

    async def _mark_homepage_request_dispatched(callback, msg: Message):
        result = callback(msg) if callback is not None else None
        if inspect.isawaitable(result):
            result = await result
        if not homepage_request_dispatched.is_set():
            homepage_request_dispatched.set()
            logger.info(
                "[App] first Web request dispatched; optional channel configuration may proceed"
            )
        return result

    web_channel.wrap_message_callback(_mark_homepage_request_dispatched)
    return web_channel, homepage_request_dispatched
