# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""WebChannel RPC handlers and shared constants (used by app gateway; single source with app.py)."""

from __future__ import annotations

import asyncio
import base64
import importlib.metadata
import importlib.util
import json
import logging
import os
import queue
import re
import secrets
import shutil
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from openjiuwen.core.common.logging import LogManager
from agent_gateway.channel_manager.web.task_asr import (
    TaskAsrError,
    transcribe_task_audio,
)
from agent_gateway.channel_manager.web.channel_config_rpc_handlers import (
    # noqa: F401 — 兼容别名（原模块级符号，实现已下沉）
    _merge_apps_by_id,
    _normalize_feishu_conf,
    _normalize_xiaoyi_conf,
    _validate_wechat_numeric_params,
    register_channel_config_handlers,
)
from agent_gateway.channel_manager.web.updater_rpc_handlers import register_updater_handlers
from agent_gateway.channel_manager.web.external_cli_runtime import (
    _activate_managed_external_cli_paths_if_needed,
    _append_codex_dependency_install_log,
    _append_external_cli_dependency_install_log,
    _build_optional_dependency_install_args,
    _ensure_claude_dependency_available_or_start_install,
    _ensure_codex_dependency_available_or_start_install,
    _ensure_managed_external_cli_runtime_or_start_install,
    _install_claude_dependency_background,
    _install_codex_dependency,
    _install_managed_external_cli_runtime_if_needed,
    _install_optional_dependency,
    _is_frozen_runtime,
    _known_external_cli_agents,
    _managed_external_cli_runtime_status,
    _openjiuwen_url_requirements,
    _remove_custom_external_cli_agent,
    _run_codex_dependency_install_background,
    _run_managed_external_cli_runtime_install,
    _run_optional_dependency_pip_install,
    _snapshot_claude_dependency_install_status,
    _snapshot_codex_dependency_install_status,
    _snapshot_external_cli_dependency_install_status,
    _snapshot_external_cli_dependency_install_status_unlocked,
    _update_codex_dependency_install_status,
    _update_external_cli_dependency_install_status,
)
from agent_gateway.channel_manager.web.locale_rpc_handlers import register_locale_handlers
from agent_gateway.channel_manager.web.heartbeat_rpc_handlers import register_heartbeat_handlers
from agent_gateway.channel_manager.web.path_rpc_handlers import register_path_handlers
from agent_gateway.channel_manager.web.workspace_rpc_handlers import register_workspace_handlers
from agent_gateway.channel_manager.web.cron_rpc_handlers import register_cron_handlers
from agent_gateway.channel_manager.web.memory_rpc_handlers import register_memory_handlers
from agent_gateway.channel_manager.web.media_document_rpc_handlers import (
    register_media_document_handlers,
)
from agent_gateway.channel_manager.web.permissions_harness_rpc_handlers import (
    register_a4p_handlers,
    register_harness_handlers,
    register_permissions_handlers,
)
from agent_gateway.common.config import (
    get_config,
    get_default_models,
    update_browser_in_config,
)
from agent_gateway.common.updater import UpdaterService
from agent_gateway.common.config_panel.config_set_handlers import (  # noqa: F401 — 兼容别名（原模块级符号，实现已下沉）
    build_external_cli_publish_url as _build_external_cli_publish_url,
)
from agent_gateway.common.config_panel.config_set_handlers import (
    detect_external_cli_agent as _detect_external_cli_agent,
)
from agent_gateway.common.config_panel.config_set_handlers import (
    external_cli_managed_cli_path as _external_cli_managed_cli_path,
)
from agent_gateway.common.paths import (
    get_env_file,
    get_root_dir,
)
from agent_gateway.common.work_mode import (
    DEFAULT_PROJECT_ID_CODE,
    DEFAULT_TUI_WORK_MODE,
    DEFAULT_WEB_WORK_MODE,
    SUPPORTED_WORK_MODES,
    is_default_project_id,
)
from agent_gateway.dotenv_early import load_dotenv_runtime

for _jiuwen_log in LogManager.get_all_loggers().values():
    _jiuwen_log.set_level(logging.INFO)

logger = logging.getLogger(__name__)

# A task nobody holds a reference to may be garbage collected mid-flight, and its exception
# would be swallowed silently. Keep every detached fire-and-forget task here until it finishes.
_PENDING_BACKGROUND_TASKS: set[asyncio.Task[Any]] = set()


def spawn_background_task(coro: Any, *, name: str) -> None:
    task = asyncio.create_task(coro, name=name)
    _PENDING_BACKGROUND_TASKS.add(task)

    def _finish(done: asyncio.Task[Any]) -> None:
        _PENDING_BACKGROUND_TASKS.discard(done)
        if done.cancelled():
            return
        error = done.exception()
        if error is not None:
            logger.warning("background task %s failed: %s", name, error, exc_info=error)

    task.add_done_callback(_finish)


_PROJECT_ROOT = get_root_dir()
_ENV_FILE = get_env_file()
load_dotenv_runtime(dotenv_path=_ENV_FILE, override=True)


# 仅满足 Channel 构造所需，不入队、不路由；仅用 channel_manager + message_handler 做入站/出站
class _DummyBus:
    async def publish_user_messages(self, msg):  # noqa: ANN001, ARG002
        pass

    async def route_incoming_message(self, msg):  # noqa: ANN001, ARG002
        pass

    async def route_user_message(self, msg):
        pass


# 仅转发到 Agent 的 Web method
_FORWARD_REQ_METHODS = frozenset({
    "group.create", "group.delete", "group.list", "group.get", "group.update",
    "group.member.add", "group.history.get",
    "initialize",
    "session.switch",
    "session.fork",
    "acp.tool_response",
    "team.delete",
    "command.goal",
    "command.btw",
    "command.compact",
    "command.mcp",
    "chat.send",
    "chat.interrupt",
    "chat.resume",
    "chat.user_answer",
    "chat.swarmflow_reply",
    "swarmflow.pause",
    "swarmflow.resume",
    "swarmflow.stop",
    "command.workflows",
    "history.get",
    # "tts.synthesize",
    "skills.marketplace.list",
    "skills.list",
    "skills.installed",
    "skills.get",
    "skills.versions.list",
    "skills.files.list",
    "skills.files.get",
    "skills.rebuild",
    "skills.toggle",
    "skills.install",
    "skills.pack_member.install",
    "skills.import_local",
    "skills.import_upload",
    "skills.create_from_knowledge",
    "skills.marketplace.add",
    "skills.marketplace.remove",
    "skills.marketplace.toggle",
    "skills.uninstall",
    "skills.online_search.search",
    "skills.online_search.install",
    "skills.skillnet.search",
    "skills.skillnet.install",
    "skills.skillnet.install_status",
    "skills.skillnet.evaluate",
    "skills.clawhub.get_token",
    "skills.clawhub.set_token",
    "skills.clawhub.search",
    "skills.clawhub.download",
    "skills.teamskillshub.info",
    "skills.teamskillshub.init",
    "skills.teamskillshub.validate",
    "skills.teamskillshub.pack",
    "assets.publish.describe",
    "assets.publish.prepare",
    "assets.publish.commit",
    "assets.publish.status",
    "assets.publish.records",
    "assets.publish.local_status",
    "skills.teamskillshub.search",
    "skills.swarmskillshub.recommend",
    "skills.teamskillshub.install",
    "skills.teamskillshub.publish",
    "skills.teamskillshub.delete",
    "skills.swarmskillshub.detail",
    "skills.retrieval.status",
    "skills.retrieval.index_build",
    "skills.retrieval.index_cancel",
    "skills.retrieval.search",
    "skills.retrieval.tree",
    "skills.evolution.status",
    "skills.evolution.get",
    "skills.evolution.save",
    "skills.graph.build",
    "skills.graph.status",
    "skills.graph.get",
    "skills.graph.cancel",
    "skills.experience.list",
    "skills.experience.request",
    "session.assets.list",
    "session.assets.register",
    "session.assets.rename",
    "session.assets.remove",
    "personal_context.runtime.status",
    "personal_context.runtime.start_collection",
    "personal_context.runtime.stop_collection",
    "personal_context.runtime.start_agent_use",
    "personal_context.runtime.stop_agent_use",
    "personal_context.runtime.set_master_enabled",
    "personal_context.runtime.get_config",
    "personal_context.runtime.patch_config",
    "personal_context.runtime.select_model",
    "personal_context.fetch.list_services",
    "personal_context.fetch.create_service",
    "personal_context.fetch.delete_service",
    "personal_context.fetch.patch_service",
    "personal_context.fetch.start_service",
    "personal_context.fetch.stop_service",
    "personal_context.fetch.run_all",
    "personal_context.fetch.run_one",
    "personal_context.fetch.stop_run",
    "personal_context.fetch.get_run_status",
    "personal_context.fetch.get_authorization_status",
    "personal_context.fetch.authorize_provider",
    "personal_context.context.stream_graph",
    "personal_context.context.stream_tree",
    "personal_context.context.search_pages",
    "personal_context.context.get_node",
    "personal_context.context.get_source",
    "plugins.list",
    "plugins.install",
    "plugins.uninstall",
    "plugins.enable",
    "plugins.disable",
    "plugins.reload",
    "agent_groups.list",
    "agent_groups.show",
    "agent_groups.file.list",
    "agent_groups.file.read",
    "agent_groups.create",
    "agent_groups.import_local",
    "agent_groups.install",
    "agent_groups.uninstall",
    "agent_templates.list",
    "agent_templates.show",
    "agent_templates.file.list",
    "agent_templates.file.read",
    "agent_templates.create",
    "agent_templates.update",
    "agent_templates.delete",
    "agent_templates.import_local",
    "agent_templates.install",
    "agent_templates.uninstall",
    "plugin_packages.list",
    "plugin_packages.show",
    "plugin_packages.create",
    "plugin_packages.import_local",
    "plugin_packages.install",
    "plugin_packages.uninstall",
    "mcp.list",
    "mcp.show",
    "mcp.install",
    "mcp.uninstall",
    "mcp.connect",
    "mcp.wait_auth",
    "mcp.oauth.start",
    "mcp.cancel_connect",
    "mcp.disconnect",
    "mcp.register_custom",
    "mcp.delete_custom",
    "mcp.save_credentials",
    "extensions.list",
    "extensions.import",
    "extensions.delete",
    "extensions.toggle",
    "team.templates.list",
    "team.bindings.list",
    "team.binding.create",
    "team.binding.generate",
    "team.session.bind",
    "team.snapshot",
    "team.history.get",
    "team.mq.publish",
    "team.list",
    "org.snapshot",
    # Agent configuration
    "agents.list",
    "agents.get",
    "agents.create",
    "agents.update",
    "agents.delete",
    "agents.enable",
    "agents.disable",
    "agents.tools_list",
    # Schedule task management
    "schedule.check_config",
    "schedule.update_config",
    "schedule.create",
    "schedule.run",
    "schedule.list",
    "schedule.status",
    "schedule.logs",
    "schedule.cancel",
    "schedule.delete",
    "issue.watch_once",
    "issue.state.list",
    "issue.matrix",
    "issue.delete",
    # 主动推荐反馈（点赞/点踩）：经 E2A 转发到 AgentServer 的
    # _handle_proactive_feedback，写入 recommendation.json 的 feedback_buffer。
    "proactive.feedback",
})

_FORWARD_NO_LOCAL_HANDLER_METHODS = frozenset({
    "group.create", "group.delete", "group.list", "group.get", "group.update",
    "group.member.add", "group.history.get",
    "initialize",
    "session.switch",
    "session.fork",
    "acp.tool_response",
    "team.templates.list",
    "team.bindings.list",
    "team.binding.create",
    "team.binding.generate",
    "team.session.bind",
    "team.delete",
    "command.goal",
    "command.btw",
    "command.compact",
    "command.mcp",
    "team.snapshot",
    "team.history.get",
    "team.mq.publish",
    "command.workflows",
    "swarmflow.pause",
    "swarmflow.resume",
    "swarmflow.stop",
    "team.list",
    "org.snapshot",
    "skills.marketplace.list",
    "skills.list",
    "skills.installed",
    "skills.get",
    "skills.versions.list",
    "skills.files.list",
    "skills.files.get",
    "skills.rebuild",
    "skills.toggle",
    "skills.install",
    "skills.pack_member.install",
    "skills.import_local",
    "skills.import_upload",
    "skills.create_from_knowledge",
    "skills.marketplace.add",
    "skills.marketplace.remove",
    "skills.marketplace.toggle",
    "skills.uninstall",
    "skills.online_search.search",
    "skills.online_search.install",
    "skills.skillnet.search",
    "skills.skillnet.install",
    "skills.skillnet.install_status",
    "skills.skillnet.evaluate",
    "skills.clawhub.get_token",
    "skills.clawhub.set_token",
    "skills.clawhub.search",
    "skills.clawhub.download",
    "skills.teamskillshub.info",
    "skills.teamskillshub.init",
    "skills.teamskillshub.validate",
    "skills.teamskillshub.pack",
    "assets.publish.describe",
    "assets.publish.prepare",
    "assets.publish.commit",
    "assets.publish.status",
    "assets.publish.records",
    "assets.publish.local_status",
    "skills.teamskillshub.search",
    "skills.swarmskillshub.recommend",
    "skills.teamskillshub.install",
    "skills.teamskillshub.publish",
    "skills.teamskillshub.delete",
    "skills.swarmskillshub.detail",
    "skills.retrieval.status",
    "skills.retrieval.index_build",
    "skills.retrieval.index_cancel",
    "skills.retrieval.search",
    "skills.retrieval.tree",
    "skills.evolution.status",
    "skills.evolution.get",
    "skills.evolution.save",
    "skills.graph.build",
    "skills.graph.status",
    "skills.graph.get",
    "skills.graph.cancel",
    "skills.experience.list",
    "skills.experience.request",
    "session.assets.list",
    "session.assets.register",
    "session.assets.rename",
    "session.assets.remove",
    "personal_context.runtime.status",
    "personal_context.runtime.start_collection",
    "personal_context.runtime.stop_collection",
    "personal_context.runtime.start_agent_use",
    "personal_context.runtime.stop_agent_use",
    "personal_context.runtime.set_master_enabled",
    "personal_context.runtime.get_config",
    "personal_context.runtime.patch_config",
    "personal_context.runtime.select_model",
    "personal_context.fetch.list_services",
    "personal_context.fetch.create_service",
    "personal_context.fetch.delete_service",
    "personal_context.fetch.patch_service",
    "personal_context.fetch.start_service",
    "personal_context.fetch.stop_service",
    "personal_context.fetch.run_all",
    "personal_context.fetch.run_one",
    "personal_context.fetch.stop_run",
    "personal_context.fetch.get_run_status",
    "personal_context.fetch.get_authorization_status",
    "personal_context.fetch.authorize_provider",
    "personal_context.context.stream_graph",
    "personal_context.context.stream_tree",
    "personal_context.context.search_pages",
    "personal_context.context.get_node",
    "personal_context.context.get_source",
    "plugins.list",
    "plugins.install",
    "plugins.uninstall",
    "plugins.enable",
    "plugins.disable",
    "plugins.reload",
    "agent_groups.list",
    "agent_groups.show",
    "agent_groups.file.list",
    "agent_groups.file.read",
    "agent_groups.create",
    "agent_groups.import_local",
    "agent_groups.install",
    "agent_groups.uninstall",
    "agent_templates.list",
    "agent_templates.show",
    "agent_templates.file.list",
    "agent_templates.file.read",
    "agent_templates.create",
    "agent_templates.update",
    "agent_templates.delete",
    "agent_templates.import_local",
    "agent_templates.install",
    "agent_templates.uninstall",
    "plugin_packages.list",
    "plugin_packages.show",
    "plugin_packages.create",
    "plugin_packages.import_local",
    "plugin_packages.install",
    "plugin_packages.uninstall",
    "mcp.list",
    "mcp.show",
    "mcp.install",
    "mcp.uninstall",
    "mcp.connect",
    "mcp.wait_auth",
    "mcp.oauth.start",
    "mcp.cancel_connect",
    "mcp.disconnect",
    "mcp.register_custom",
    "mcp.delete_custom",
    "mcp.save_credentials",
    "extensions.list",
    "extensions.import",
    "extensions.delete",
    "extensions.toggle",
    # Agent configuration
    "agents.list",
    "agents.get",
    "agents.create",
    "agents.update",
    "agents.delete",
    "agents.enable",
    "agents.disable",
    "agents.tools_list",
    "external_cli.detect",
    "external_cli.codex_install_status",
    "proactive.feedback",
})

async def _clear_agent_config_cache(agent_client=None) -> None:
    """写回 config.yaml 后清除 agent 侧配置缓存，使下次读取时得到最新文件内容。"""
    try:
        if agent_client is not None:
            from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
            from openjiuwen_protocol.message import ReqMethod

            env = e2a_from_agent_fields(
                request_id=f"cfg-reload-{uuid.uuid4().hex[:8]}",
                channel_id="",
                req_method=ReqMethod.AGENT_RELOAD_CONFIG,
            )
            await agent_client.send_request(env)
        else:
            get_config()
    except Exception:  # noqa: BLE001
        pass


async def _restart_agent_browser_runtime(
    agent_client=None,
    *,
    previous_chrome_path: str = "",
    previous_headless: bool = True,
) -> None:
    """Stop active agent-side browser runtimes so the next task uses new config."""
    if agent_client is None:
        return

    from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
    from openjiuwen_protocol.message import ReqMethod

    env = e2a_from_agent_fields(
        request_id=f"browser-restart-{uuid.uuid4().hex[:8]}",
        channel_id="",
        req_method=ReqMethod.BROWSER_RUNTIME_RESTART,
        params={
            "browser_key": "",
            "profile_name": (
                os.getenv("BROWSER_PROFILE_NAME") or "jiuwenclaw"
            ).strip(),
            "display_mode": "headless" if previous_headless else "headed",
            "browser_binary": str(previous_chrome_path or "").strip(),
        },
    )
    response = await agent_client.send_request(env)
    if not response.ok:
        error = response.payload.get("error", "browser runtime restart failed")
        raise RuntimeError(str(error))


def _make_session_id() -> str:
    # 与前端 generateSessionId 保持一致：毫秒时间戳(16进制) + 6位随机16进制
    ts = format(int(time.time() * 1000), "x")
    suffix = secrets.token_hex(3)
    return f"sess_{ts}_{suffix}"


# ---------------------------------------------------------------------------
# 飞书 Feishu / 小艺 Xiaoyi 多应用配置 — 默认值 & 归一化函数
# ---------------------------------------------------------------------------

@dataclass
class WebHandlersBindParams:
    """Named bundle for :func:`_register_web_handlers` (avoids long positional / keyword lists)."""

    channel: Any
    agent_client: Any = None
    message_handler: Any = None
    channel_manager: Any = None
    on_config_saved: Any = None
    heartbeat_service: Any = None
    cron_controller: Any = None
    heartbeat_controller: Any = None
    updater_service: UpdaterService | None = None
    topic_starter_service: Any = None
    topic_history_store: Any = None
    topic_prepare_service: Any = None
    topic_service: Any = None
    topic_outcome_queue: Any = None


_CONTAINER_FILE_API_METHODS = (
    "upload_container_file",
    "download_container_file",
    "list_container_files",
    "mkdir_container_dir",
)


def _supports_container_file_api(client: Any) -> bool:
    """True when *client* can back WebChannel ``/file-api`` (no extensions import)."""
    return all(callable(getattr(client, name, None)) for name in _CONTAINER_FILE_API_METHODS)


def _project_info_payload(
    proj: Any | None,
    *,
    default_id: str | None = None,
    stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Serialize a project item consistently for project.list/info/create."""
    st = stats or {"session_count": 0, "last_message_at": None, "last_user_message_at": None}
    git = getattr(proj, "git", {}) if proj is not None else {}
    git_defaults = {
        "enabled": False,
        "repo_root": "",
        "initialized_by_jiuwenswarm": False,
        "detected_at": 0,
        "status": "disabled",
        "branch": "",
        "error": "",
        "error_code": "",
        "hint": "",
        "is_dirty": False,
    }
    git_payload = {**git_defaults, **dict(git)} if isinstance(git, dict) and git else git_defaults
    if default_id is not None:
        work_mode = DEFAULT_TUI_WORK_MODE if default_id == DEFAULT_PROJECT_ID_CODE else DEFAULT_WEB_WORK_MODE
        return {
            "project_id": default_id,
            "name": "默认项目",
            "project_dir": "",
            "pinned": False,
            "pin_order": 0,
            "is_default": True,
            "work_mode": work_mode,
            "git": git_payload,
            "session_count": st["session_count"],
            "last_message_at": st["last_message_at"],
            "last_user_message_at": st["last_user_message_at"],
            "created_at": 0,
            "updated_at": 0,
        }
    work_mode = getattr(proj, "work_mode", "") or DEFAULT_WEB_WORK_MODE
    return {
        "project_id": proj.project_id,
        "name": proj.name,
        "project_dir": proj.project_dir,
        "pinned": proj.pinned,
        "pin_order": proj.pin_order,
        "is_default": False,
        "work_mode": work_mode,
        "git": git_payload,
        "session_count": st["session_count"],
        "last_message_at": st["last_message_at"],
        "last_user_message_at": st["last_user_message_at"],
        "created_at": proj.created_at,
        "updated_at": getattr(proj, "updated_at", 0),
    }


def _register_web_handlers(bind: WebHandlersBindParams) -> None:
    """注册 Web 前端需要的 method 与 on_connect。
    on_config_saved: 可选，config.set 写回后调用的回调；
        updated_env_keys 为本次改动的键名集合，
        env_updates 为本次变更的环境变量增量（仅包含更新项），
        config_payload 为当前最新配置快照；
        返回 True 表示已热更新未重启，False 表示已安排进程重启。
    heartbeat_service: 可选，GatewayHealthCheckService 实例，用于处理 health_check.get_conf / health_check.set_conf。
    """
    channel = bind.channel
    agent_client = bind.agent_client
    message_handler = bind.message_handler
    channel_manager = bind.channel_manager
    on_config_saved = bind.on_config_saved
    heartbeat_service = bind.heartbeat_service
    cron_controller = bind.cron_controller
    heartbeat_controller = bind.heartbeat_controller
    updater_service = bind.updater_service

    from openjiuwen_protocol.message import EventType, Message

    def _resolve(ref, key="value"):
        """若为 ref 字典则取 key（无则返回 None），否则返回自身。"""
        if isinstance(ref, dict):
            return ref.get(key)
        return ref

    def _schedule_clear_agent_config_cache(name: str) -> None:
        asyncio.create_task(
            _clear_agent_config_cache(_resolve(agent_client)),
            name=f"{name}.clear_agent_config_cache",
        )

    def _schedule_agent_prewarm_sync(name: str) -> None:
        """Reconcile project-derived warm keys without delaying the Web RPC."""
        from agent_gateway.runtime.agent_prewarm import prewarm_enabled_by_env

        if not prewarm_enabled_by_env():
            return

        async def _sync() -> None:
            try:
                from openjiuwen_protocol.e2a.gateway_normalize import (
                    e2a_from_agent_fields,
                )
                from openjiuwen_protocol.message import ReqMethod

                real_client = _resolve(agent_client)
                if real_client is None:
                    return
                cm = _resolve(channel_manager)
                enabled_channels = [
                    channel
                    for channel in list(getattr(cm, "enabled_channels", []) or [])
                    if str(channel).lower() not in {"acp", "a2a"}
                ]
                env = e2a_from_agent_fields(
                    request_id=f"agent-prewarm-project-{uuid.uuid4().hex[:8]}",
                    channel_id="web",
                    req_method=ReqMethod.AGENT_PREWARM_SYNC,
                    params={"enabled_channels": sorted(set(enabled_channels))},
                    is_stream=False,
                    timestamp=time.time(),
                )
                response = await real_client.send_request(env)
                if not getattr(response, "ok", False):
                    logger.warning(
                        "[Project] agent.prewarm.sync rejected: %s",
                        getattr(response, "payload", None),
                    )
            except Exception as exc:  # noqa: BLE001 - maintenance is non-blocking
                logger.warning("[Project] agent.prewarm.sync failed: %s", exc)

        asyncio.create_task(_sync(), name=f"{name}.agent_prewarm_sync")

    def _resolve_env_vars(value: Any) -> Any:
        """Recursively resolve environment variables in config values."""
        if isinstance(value, str):
            pattern = r'\$\{([^:}]+)(?::-([^}]*))?\}'

            def replace_env(match):
                var_name = match.group(1)
                default = match.group(2) if match.group(2) is not None else ""
                return os.getenv(var_name, default)

            return re.sub(pattern, replace_env, value)
        elif isinstance(value, dict):
            return {k: _resolve_env_vars(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [_resolve_env_vars(item) for item in value]
        else:
            return value

    async def _on_connect(ws):
        ac = _resolve(agent_client)
        if ac is None or not getattr(ac, "server_ready", False):
            logger.debug("[_on_connect] Agent 未就绪，跳过 connection.ack")
            return
        # V2: 复用 ws 握手时注册的占位 session_id，而不是另 make 一个新 sid。
        # 原实现凭空生成 sid_B 与 ws 在 _clients_by_key 中注册的 sid_A 不一致，
        # 导致 send() 按 session_id 反查落空、ACK 被丢弃，前端收不到 connection.ack。
        # 复用 sid_A 后，ACK 走标准 send 流程即可命中本 ws，无需特殊路由兜底。
        sid = getattr(ws, "_jiuwen_initial_sid", None) or _make_session_id()

        ack_msg = Message(
            id=f"ack-{sid}",
            type="event",
            channel_id=channel.channel_id,
            session_id=sid,
            params={},
            timestamp=time.time(),
            ok=True,
            event_type=EventType.CONNECTION_ACK,
            payload={
                "session_id": sid,
                "mode": "BUILD",
                "tools": [],
                "protocol_version": "1.0",
            },
        )
        mh = _resolve(message_handler)
        if mh:
            await mh.publish_robot_messages(ack_msg)
        else:
            await channel.send(ack_msg)

    channel.on_connect(_on_connect)

    async def _on_disconnect(ws, _session_ids):
        mh = _resolve(message_handler)
        cleanup = getattr(mh, "unregister_ws_subscriptions", None)
        ws_id = str(getattr(ws, "_jiuwen_ws_id", "") or "").strip()
        if callable(cleanup) and ws_id:
            await cleanup(channel.channel_id, ws_id)

    register_disconnect = getattr(channel, "on_disconnect", None)
    if callable(register_disconnect):
        register_disconnect(_on_disconnect)

    async def _request_zen_warmup() -> None:
        from agent_gateway.zen_query import request_zen_warmup

        await request_zen_warmup(_resolve(agent_client))

    async def _channel_get(ws, req_id, params, session_id):
        """返回已注册的 channel 列表."""
        cm = _resolve(channel_manager)
        if cm is not None:
            channels = [{"channel_id": cid} for cid in cm.enabled_channels]
        else:
            channels = []
        await channel.send_response(ws, req_id, ok=True, payload={"channels": channels})

    async def _task_asr_transcribe(ws, req_id, params, session_id):
        del session_id
        if not isinstance(params, dict):
            await channel.send_response(
                ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST"
            )
            return
        try:
            text = await transcribe_task_audio(params)
        except TaskAsrError as exc:
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code=exc.code
            )
            return
        except Exception as exc:  # noqa: BLE001
            logger.exception("[task.asr.transcribe] unexpected failure")
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR"
            )
            return
        await channel.send_response(ws, req_id, ok=True, payload={"text": text})

    # ── vendors.* handlers ──────────────

    async def _vendors_list(ws, req_id, params, session_id):
        """返回按 plan 分组的厂商预设列表(供前端 Tab+厂商卡片渲染)。

        纯数据,读 ``gateway.common.model_vendor_registry``,无副作用。
        """
        del params, session_id
        try:
            from agent_gateway.common.model_vendor_registry import to_frontend_payload
            await channel.send_response(
                ws, req_id, ok=True,
                payload={"vendors": to_frontend_payload()},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[vendors.list] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _vendors_fetch_models(ws, req_id, params, session_id):
        """按厂商预设拉取远端可用模型列表(供前端"拉取最新"按钮)。

        params: {vendor_key, plan, api_key?, modality?, api_base?}。按预设的 ``models_needs_key``
        决定是否带 Authorization。普通列表失败时回退预设；火山生成模型按
        domain 返回完整远端目录，失败时返回空列表供前端手填。
        """
        del session_id
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        vendor_key = str(params.get("vendor_key") or "").strip()
        plan_raw = str(params.get("plan") or "").strip()
        api_key = str(params.get("api_key") or "").strip()
        modality = str(params.get("modality") or "").strip()
        try:
            from agent_gateway.common.model_vendor_registry import (
                PlanKind,
                get_preset,
            )

            def _fetch_remote_sync(
                endpoint: str, hdrs: dict[str, str], model_domain: str | None = None,
            ) -> tuple[list[str], str]:
                """同步拉取并解析 /models(在线程池里跑,不阻塞事件循环)。

                成功返回 ``(模型 ID 列表, "")``;任何失败(网络/状态码非 200/
                解析失败/空)返回 ``([], 原因)``。原因透传给前端,写进回退响应的
                ``reason`` 字段,便于联调时区分鉴权失败/端点问题/限流/网络异常,
                而非笼统的"拉取失败"。永不抛异常。
                """
                import httpx  # noqa: PLC0415
                from openjiuwen.extensions.external_provider.openai_auth.openai_account_models import (
                    parse_openai_account_model_ids,
                )
                try:
                    resp = httpx.get(endpoint, headers=hdrs, timeout=12.0)
                except Exception as exc:  # noqa: BLE001
                    logger.debug("[vendors.fetch_models] http error: %s", exc)
                    return [], f"remote fetch error: {type(exc).__name__}"
                if resp.status_code != 200:
                    # 401/403 = key 错或鉴权方式不符(如 maas 需华为签名);
                    # 429 = 限流;5xx = 上游故障。把状态码透传给前端。
                    return [], f"remote returned HTTP {resp.status_code}"
                try:
                    payload_json = resp.json()
                except ValueError:
                    return [], "remote returned non-JSON body"
                if not isinstance(payload_json, dict):
                    return [], "remote returned unexpected body shape"
                if model_domain:
                    entries = payload_json.get("data")
                    if not isinstance(entries, list):
                        return [], "remote returned unexpected body shape"
                    ids: list[str] = []
                    seen: set[str] = set()
                    for item in entries:
                        if not isinstance(item, dict) or item.get("domain") != model_domain:
                            continue
                        model_id = item.get("id")
                        if not isinstance(model_id, str):
                            continue
                        model_id = model_id.strip()
                        if model_id and model_id not in seen:
                            seen.add(model_id)
                            ids.append(model_id)
                    return ids, "" if ids else "remote returned empty generation model list"
                try:
                    ids = parse_openai_account_model_ids(payload_json)
                except Exception:  # noqa: BLE001
                    return [], "remote payload parse failed"
                if not ids:
                    return [], "remote returned empty model list"
                return ids, ""

            try:
                plan = PlanKind(plan_raw)
            except ValueError:
                await channel.send_response(
                    ws, req_id, ok=False,
                    error=f"invalid plan: {plan_raw}",
                    code="BAD_REQUEST",
                )
                return

            preset = get_preset(vendor_key, plan)
            if preset is None:
                await channel.send_response(
                    ws, req_id, ok=False,
                    error=f"unknown vendor/plan: {vendor_key}/{plan_raw}",
                    code="BAD_REQUEST",
                )
                return

            model_domain = None
            endpoint = preset.models_endpoint
            if modality:
                if vendor_key != "volcengine" or plan != PlanKind.CUSTOM_API or modality not in {"image", "video"}:
                    await channel.send_response(
                        ws, req_id, ok=False, error="unsupported model modality", code="BAD_REQUEST",
                    )
                    return
                from agent_gateway.common.volcengine_endpoints import normalize_ark_api_base

                try:
                    api_base = normalize_ark_api_base(str(params.get("api_base") or preset.api_base))
                except ValueError as exc:
                    await channel.send_response(
                        ws, req_id, ok=False, error=str(exc), code="BAD_REQUEST",
                    )
                    return
                endpoint = f"{api_base}/models"
                model_domain = "ImageGeneration" if modality == "image" else "VideoGeneration"

            # 无远端端点(Maas 等) -> 直接回退预设
            if not preset.models_endpoint:
                await channel.send_response(
                    ws, req_id, ok=True,
                    payload={
                        "models": list(preset.model_options),
                        "source": "preset",
                        "reason": "no remote models endpoint",
                    },
                )
                return

            headers: dict[str, str] = {"Accept": "application/json"}
            if preset.models_needs_key:
                if not api_key:
                    # 需 key 但未提供 -> 回退预设
                    await channel.send_response(
                        ws, req_id, ok=True,
                        payload={
                            "models": [] if model_domain else list(preset.model_options),
                            "source": "unavailable" if model_domain else "preset",
                            "reason": "api_key required for fetch",
                        },
                    )
                    return
                headers["Authorization"] = f"Bearer {api_key}"

            # 同步 httpx.get 通过 asyncio.to_thread 卸载到线程池,避免阻塞事件循环
            # (与同文件 _openai_account_*_payload / _updater_* 的 to_thread 模式一致)。
            remote_ids, remote_reason = await asyncio.to_thread(
                _fetch_remote_sync, endpoint, headers, model_domain,
            )

            # DashScope's /models catalogue includes unavailable marketplace,
            # retired, and non-chat entries.  For Alibaba, expose only the
            # small plan-specific allowlist whose models have been verified on
            # the corresponding OpenAI-compatible endpoint.  Keep allowlist
            # order so the UI presents recommended models first.
            if vendor_key == "alibaba" and remote_ids:
                unfiltered_count = len(remote_ids)
                remote_model_ids = set(remote_ids)
                remote_ids = [
                    model_id
                    for model_id in preset.model_options
                    if model_id in remote_model_ids
                ]
                filtered_count = unfiltered_count - len(remote_ids)
                if filtered_count:
                    logger.info(
                        "[vendors.fetch_models] filtered %d non-allowlisted Alibaba models",
                        filtered_count,
                    )
                if not remote_ids:
                    remote_reason = "no remote models matched the Alibaba plan allowlist"

            if remote_ids:
                await channel.send_response(
                    ws, req_id, ok=True,
                    payload={"models": remote_ids, "source": "remote"},
                )
                return

            # 远端失败/空 -> 回退预设。把远端状态码/原因透传给前端,联调时能
            # 分辨是鉴权失败(key 错 / 需厂商专属签名)、限流、还是端点不可达,
            # 而非笼统的"拉取报错"。注意:仍回 source="preset" + ok=True,保证
            # 拉取失败不阻塞前端选模型,只是可见地说明为何回退。
            await channel.send_response(
                ws, req_id, ok=True,
                payload={
                    "models": [] if model_domain else list(preset.model_options),
                    "source": "unavailable" if model_domain else "preset",
                    "reason": remote_reason or "remote fetch failed or empty",
                },
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[vendors.fetch_models] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _chat_send(ws, req_id, params, session_id):
        await channel.send_response(
            ws,
            req_id,
            ok=True,
            payload={"accepted": True, "session_id": session_id},
        )

    async def _chat_resume(ws, req_id, params, session_id):
        await channel.send_response(
            ws,
            req_id,
            ok=True,
            payload={"accepted": True, "session_id": session_id},
        )

    async def _chat_interrupt(ws, req_id, params, session_id):
        intent = params.get("intent") if isinstance(params, dict) else None
        payload = {"accepted": True, "session_id": session_id}
        if isinstance(intent, str) and intent:
            payload["intent"] = intent
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _chat_user_answer(ws, req_id, params, session_id):
        payload = {"accepted": True, "session_id": session_id}
        request_id = params.get("request_id") if isinstance(params, dict) else None
        if isinstance(request_id, str) and request_id:
            payload["request_id"] = request_id
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    async def _chat_swarmflow_reply(ws, req_id, params, session_id):
        # Empty-ack shell — standard 3-layer routing forwards the reply to the
        # agent adapter, which builds HumanAgentMessage and calls team_manager.
        await channel.send_response(
            ws, req_id, ok=True, payload={"accepted": True, "session_id": session_id}
        )

    async def _history_get(ws, req_id, params, session_id):
        payload = {"accepted": True, "session_id": session_id}
        if isinstance(params, dict):
            if "session_id" in params:
                payload["session_id"] = params.get("session_id")
            if "page_idx" in params:
                payload["page_idx"] = params.get("page_idx")
            if "cursor" in params:
                payload["cursor"] = params.get("cursor")
            if "limit" in params:
                payload["limit"] = params.get("limit")
        await channel.send_response(ws, req_id, ok=True, payload=payload)

    register_cron_handlers(channel, cron_controller, agent_client, _resolve)

    async def _forward_recommendation(ws, req_id, params, session_id, user_id=None, *, action: str):
        """Send a recommendation panel call to AgentServer and return its payload."""
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        if (
            action == "prompt_suggestion_event"
            and isinstance(params, dict)
            and params.get("event_type") == "input_changed"
        ):
            target_session = params.get("session_id")
            if not isinstance(target_session, str) or not target_session.strip():
                await channel.send_response(
                    ws, req_id, ok=False, error="session_id is required", code="BAD_REQUEST",
                )
                return
        body = dict(params) if isinstance(params, dict) else {}
        if action == "control":
            # The panel verb (enable/disable/delete) shares the name ``action``.
            # Keep it before the RPC action overwrites that field.
            body["control_action"] = body.get("action")
        auth_session = getattr(ws, "_jiuwen_auth_session", "") or ""
        if auth_session:
            body["auth_session"] = auth_session
        body["action"] = action
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=body,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.RECOMMENDATION,
            label="recommendation",
        )

    async def _prompt_suggestion_event(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(
            ws, req_id, params, session_id, user_id, action="prompt_suggestion_event",
        )

    async def _topic_starters_status(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="status")

    async def _topic_starters_select_model(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(
            ws, req_id, params, session_id, user_id, action="select_model",
        )

    async def _topic_starters_profile(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="profile")

    async def _topic_starters_restore_direction(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(
            ws, req_id, params, session_id, user_id, action="restore_direction",
        )

    async def _topic_starters_control(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="control")

    async def _topic_starters_get(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="get")

    async def _topic_starters_foreground(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(
            ws, req_id, params, session_id, user_id, action="foreground",
        )

    async def _topic_starters_event(ws, req_id, params, session_id, user_id=None):
        await _forward_recommendation(ws, req_id, params, session_id, user_id, action="event")

    from agent_gateway.channel_manager.web.external_cli_rpc_handlers import (
        register_external_cli_handlers,
    )

    register_external_cli_handlers(channel)

    async def _proxy_config_request(ws, req_id, params, session_id, user_id=None, *, req_method):
        """Execute configuration RPCs in the target AgentServer's directories.

        MoA readiness always uses E2A. Other configuration methods retain their
        shared-directory handlers through ``_register_config_proxy``.
        """
        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=req_method,
            label="config",
        )

    def _external_cli_install_gate(params: dict[str, Any]) -> dict[str, Any] | None:
        """外部 CLI 依赖安装门控（原 config.set 本地执行的 ensure 回调语义）。

        这是前端"一键安装"的唯一触发通道（``external_cli.install`` 无前端调用
        方），安装器与状态轮询（``external_cli.install_status``）都是 gateway
        本地能力，须随 A4g 保留。命中 codex/claude 相关键且依赖未就绪时启动
        安装并返回状态 payload——调用方据此短路本次 config.set（安装完成前
        不写配置，与原本地语义一致，前端装完后重发）。已就绪/未命中返回
        None，请求正常经 E2A 转发。触发条件为简化版 key 前缀判定：相比本地
        执行少了"当前配置中该 agent 已启用"的前置检查，误触发的代价仅是
        ensure 幂等空转（已装即返回 None）。
        """
        from agent_gateway.common.config_panel.config_set_handlers import (
            team_payload_requests_codex,
        )

        # 键形状与 config_save_all_handler 的展平规则一致：config.set 键在顶层；
        # config.save_all 键嵌套在 params.config（前端外部 CLI 卡片的唯一保存
        # 链路即 save_all 嵌套形状），agents/team 两处皆可出现且**顶层优先**
        # （save_all 的合并顺序：嵌套 config 先建，顶层 agents/team 后写覆盖——
        # 门控若反向合并，碰撞形状下会漏判被顶层覆盖落盘的 team 载荷）。
        raw_config = params.get("config")
        keys_view: dict[str, Any] = (
            dict(raw_config) if isinstance(raw_config, dict) else {}
        )
        for key, value in params.items():
            if key != "config":
                keys_view[key] = value

        codex_touched = any(
            key.startswith("external_cli_agent_codex_") for key in keys_view
        ) or (
            ("agents" in keys_view or "team" in keys_view)
            and team_payload_requests_codex(keys_view)
        )
        claude_touched = any(
            key.startswith("external_cli_agent_claude_") for key in keys_view
        )
        installs: dict[str, Any] = {}
        if codex_touched:
            install = _ensure_codex_dependency_available_or_start_install()
            if install is not None:
                installs["codex"] = install
        if claude_touched:
            install = _ensure_claude_dependency_available_or_start_install()
            if install is not None:
                installs["claude"] = install
        if not installs:
            return None
        return {
            "updated": [],
            "applied_without_restart": False,
            "external_cli_dependency_installs": installs,
        }

    def _register_config_proxy(method_name, req_method, local_handler=None):
        # A4g：配置读写统一经目标 AgentServer 执行（ConfigAdapter 在注入目录
        # 本地执行，自带 in-process reload / zen warmup / config_changed 标记
        # 等后置行为），删除 legacy 单用户的 gateway 本地执行回落。
        # 保留的唯一 gateway 侧门控：外部 CLI 依赖安装——仅共享目录单用户
        # 触发（AgentOS 用户目录在目标沙箱，gateway 主机安装无意义，保持
        # 既有零变化）。local_handler 参数保留为兼容签名，不再使用。
        del local_handler

        async def _handler(ws, req_id, params, session_id, user_id=None):
            from agent_gateway.routing.e2a_proxy import is_legacy_shared_directory_client

            # 登录会话 id 随请求转发（硬规则：客户端传来的键先删，只信连接属性）：
            # models.list 经目标 AgentServer 执行时无法看到 Gateway 连接，登录送的
            # 免费模型合并需要会话 id（AgentOS 既有缺口，A4g 统一后对 legacy 同样关键）。
            if isinstance(params, dict):
                from openjiuwen_protocol.e2a.constants import E2A_AUTH_SESSION_PARAM_KEY

                forwarded = dict(params)
                forwarded.pop(E2A_AUTH_SESSION_PARAM_KEY, None)
                auth_session = str(getattr(ws, "_jiuwen_auth_session", "") or "").strip()
                if auth_session:
                    forwarded[E2A_AUTH_SESSION_PARAM_KEY] = auth_session
                params = forwarded
            if req_method in (
                _ConfigReq.CONFIG_SET,
                _ConfigReq.CONFIG_SAVE_ALL,
            ) and isinstance(params, dict):
                resolved_client = _resolve(agent_client)
                if resolved_client is not None and is_legacy_shared_directory_client(resolved_client):
                    install_payload = _external_cli_install_gate(params)
                    if install_payload is not None:
                        await channel.send_response(ws, req_id, ok=True, payload=install_payload)
                        return
            await _proxy_config_request(
                ws, req_id, params, session_id, user_id, req_method=req_method
            )
        channel.register_method(method_name, _handler)

    from openjiuwen_protocol.message import ReqMethod as _ConfigReq

    async def _config_moa_status(ws, req_id, params, session_id, user_id=None):
        await _proxy_config_request(
            ws, req_id, params, session_id, user_id,
            req_method=_ConfigReq.CONFIG_MOA_STATUS,
        )

    channel.register_method("config.moa_status", _config_moa_status)
    _register_config_proxy("config.get", _ConfigReq.CONFIG_GET)
    _register_config_proxy("config.set", _ConfigReq.CONFIG_SET)
    _register_config_proxy("config.save_all", _ConfigReq.CONFIG_SAVE_ALL)
    _register_config_proxy("config.validate_model", _ConfigReq.CONFIG_VALIDATE_MODEL)
    _register_config_proxy("models.list", _ConfigReq.MODELS_LIST)
    _register_config_proxy("models.get", _ConfigReq.MODELS_GET)
    _register_config_proxy("models.replace_all", _ConfigReq.MODELS_REPLACE_ALL)
    _register_config_proxy("models.validate", _ConfigReq.MODELS_VALIDATE)
    _register_config_proxy("models.upsert", _ConfigReq.MODELS_UPSERT)
    _register_config_proxy("models.delete", _ConfigReq.MODELS_DELETE)
    _register_config_proxy("models.references", _ConfigReq.MODELS_REFERENCES)
    _register_config_proxy("model_groups.list", _ConfigReq.MODEL_GROUPS_LIST)
    _register_config_proxy("model_groups.upsert", _ConfigReq.MODEL_GROUPS_UPSERT)
    _register_config_proxy("model_groups.delete", _ConfigReq.MODEL_GROUPS_DELETE)
    _register_config_proxy("session.selection.set", _ConfigReq.SESSION_SELECTION_SET)

    # vendors.* 为本分支新增的厂商选择接口，不经 config proxy（无 AgentOS 多用户注入目录语义），
    # 直接走本地 handler。
    channel.register_method("vendors.list", _vendors_list)
    channel.register_method("vendors.fetch_models", _vendors_fetch_models)
    channel.register_method("channel.get", _channel_get)
    channel.register_method("task.asr.transcribe", _task_asr_transcribe)
    from agent_gateway.channel_manager.web.openai_account_rpc_handlers import (
        register_openai_account_handlers,
    )
    register_openai_account_handlers(channel)

    from agent_gateway.channel_manager.web.session_rpc_handlers import (
        register_session_handlers,
    )
    register_session_handlers(channel, agent_client, _resolve)

    from agent_gateway.channel_manager.web.designer_handlers import (
        register_designer_handlers,
    )
    register_designer_handlers(channel, lambda: _resolve(agent_client))

    from agent_gateway.channel_manager.web.project_rpc_handlers import (
        register_project_handlers,
        register_project_pinned_sessions_handler,
    )
    register_project_handlers(
        channel,
        agent_client,
        _resolve,
        cron_controller=cron_controller,
        _schedule_agent_prewarm_sync=_schedule_agent_prewarm_sync,
    )
    from agent_gateway.channel_manager.web.lifecycle_handlers import (
        register_lifecycle_handlers,
    )
    register_lifecycle_handlers(
        channel, lambda: _resolve(agent_client), lambda: _resolve(cron_controller),
    )
    register_project_pinned_sessions_handler(channel, agent_client, _resolve)

    # Git RPC handlers (设计文档 §4.1.11-§4.1.15)
    from agent_gateway.channel_manager.web.project_git_rpc_handlers import (
        register_project_git_handlers,
    )
    register_project_git_handlers(channel, agent_client, _resolve)

    register_path_handlers(
        channel,
        agent_client,
        _resolve,
        _clear_agent_config_cache,
        _restart_agent_browser_runtime,
    )

    register_workspace_handlers(channel, agent_client, _resolve)
    channel.register_method("chat.send", _chat_send)
    register_media_document_handlers(channel, agent_client, _resolve)
    channel.register_method("chat.resume", _chat_resume)
    channel.register_method("chat.interrupt", _chat_interrupt)
    channel.register_method("chat.user_answer", _chat_user_answer)
    channel.register_method("chat.swarmflow_reply", _chat_swarmflow_reply)
    channel.register_method("history.get", _history_get)
    channel.register_method("topic_starters.profile", _topic_starters_profile)
    channel.register_method("topic_starters.restore_direction", _topic_starters_restore_direction)
    channel.register_method("topic_starters.control", _topic_starters_control)
    channel.register_method("topic_starters.status", _topic_starters_status)
    channel.register_method("topic_starters.select_model", _topic_starters_select_model)
    channel.register_method("topic_starters.get", _topic_starters_get)
    channel.register_method("topic_starters.foreground", _topic_starters_foreground)
    channel.register_method("topic_starters.event", _topic_starters_event)
    channel.register_method("prompt_suggestion.event", _prompt_suggestion_event)
    register_locale_handlers(channel, agent_client, _resolve)
    register_updater_handlers(channel, updater_service)
    register_channel_config_handlers(
        channel,
        channel_manager,
        heartbeat_service,
        agent_client,
        _resolve,
        _schedule_clear_agent_config_cache,
        _clear_agent_config_cache,
    )
    register_heartbeat_handlers(channel, heartbeat_controller, _resolve)

    register_permissions_handlers(channel, agent_client, _resolve)
    register_a4p_handlers(channel, agent_client, _resolve)
    register_memory_handlers(channel, agent_client, _resolve)
    register_harness_handlers(channel, agent_client, _resolve)

    real_agent_client = _resolve(agent_client)
    # Container file transfer is HTTP on the WebChannel port, not WS JSON-RPC.
    # not WS JSON-RPC. Bind any client that already exposes the container-file
    # methods so build_web_channel_app can mount /file-api/* at channel.start().
    # Do not import AgentOSRouterClient here: this module must not depend on extensions.
    if _supports_container_file_api(real_agent_client):
        channel.container_file_client = real_agent_client
    else:
        channel.container_file_client = None
    # The diagnosis and trajectory HTTP proxies reach the AgentServer through this client.
    channel.diagnosis_agent_client = real_agent_client
    channel.trajectory_agent_client = real_agent_client
