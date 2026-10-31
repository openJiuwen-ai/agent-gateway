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
from openjiuwen.extensions.external_provider.openai_auth.openai_account_auth import (
    OpenAIAccountAuthError,
    OpenAIAccountAuthManager,
    OpenAIAccountDeviceCode,
)
from openjiuwen.extensions.external_provider.openai_auth.openai_account_models import (
    OpenAIAccountModelCatalog,
    OpenAIAccountModelListError,
)

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
from agent_gateway.channel_manager.web.workspace_rpc_handlers import register_workspace_handlers
from agent_gateway.channel_manager.web.cron_rpc_handlers import register_cron_handlers
from agent_gateway.channel_manager.web.memory_rpc_handlers import register_memory_handlers
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


_OPENAI_ACCOUNT_LOGIN_MAX_TTL_SECONDS = 5 * 60
_OPENAI_ACCOUNT_LOGIN_JOBS: dict[str, "_OpenAIAccountLoginJob"] = {}
_OPENAI_ACCOUNT_LOGIN_JOBS_LOCK = threading.RLock()
_OPENAI_ACCOUNT_AUTH_OPERATION_LOCK = threading.RLock()
_OPENAI_ACCOUNT_LOCAL_ERRORS = (OSError, TypeError, ValueError)


@dataclass
class _OpenAIAccountLoginJob:
    device_code: OpenAIAccountDeviceCode
    created_at: float
    expires_at: float


def _cleanup_openai_account_login_jobs(now: float | None = None) -> None:
    current = time.time() if now is None else now
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        expired = [
            login_id
            for login_id, job in _OPENAI_ACCOUNT_LOGIN_JOBS.items()
            if job.expires_at <= current
        ]
        for login_id in expired:
            _OPENAI_ACCOUNT_LOGIN_JOBS.pop(login_id, None)


def _latest_openai_account_login_job(
        now: float | None = None,
) -> tuple[str, "_OpenAIAccountLoginJob"] | None:
    current = time.time() if now is None else now
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _cleanup_openai_account_login_jobs(current)
        if not _OPENAI_ACCOUNT_LOGIN_JOBS:
            return None
        return max(_OPENAI_ACCOUNT_LOGIN_JOBS.items(), key=lambda item: item[1].created_at)


def _get_openai_account_login_job(
        login_id: str,
        now: float | None = None,
) -> "_OpenAIAccountLoginJob" | None:
    current = time.time() if now is None else now
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _cleanup_openai_account_login_jobs(current)
        return _OPENAI_ACCOUNT_LOGIN_JOBS.get(login_id)


def _store_openai_account_login_job(
        login_id: str,
        job: "_OpenAIAccountLoginJob",
        now: float | None = None,
) -> None:
    current = time.time() if now is None else now
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _cleanup_openai_account_login_jobs(current)
        _OPENAI_ACCOUNT_LOGIN_JOBS[login_id] = job


def _remove_openai_account_login_job(login_id: str) -> None:
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _OPENAI_ACCOUNT_LOGIN_JOBS.pop(login_id, None)


def _clear_openai_account_login_jobs() -> None:
    with _OPENAI_ACCOUNT_LOGIN_JOBS_LOCK:
        _OPENAI_ACCOUNT_LOGIN_JOBS.clear()


def _openai_account_auth_status_payload(
        manager: OpenAIAccountAuthManager | None = None,
) -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        auth_manager = manager or OpenAIAccountAuthManager()
        status = auth_manager.status()
        return {
            "authenticated": status.authenticated,
            "auth_path": str(status.auth_path),
            "has_refresh_token": status.has_refresh_token,
            "expires_at": status.expires_at,
            "needs_refresh": status.needs_refresh,
            "error": status.error,
            "base_url": auth_manager.base_url,
        }


def _openai_account_login_payload(
        login_id: str,
        job: "_OpenAIAccountLoginJob",
        manager: OpenAIAccountAuthManager | None = None,
        now: float | None = None,
) -> dict[str, Any]:
    current = time.time() if now is None else now
    device_code = job.device_code
    return {
        "status": "pending",
        "login_id": login_id,
        "user_code": device_code.user_code,
        "verification_uri": device_code.verification_uri,
        "interval": device_code.interval,
        "expires_in": max(0, int(job.expires_at - current)),
        "expires_at": job.expires_at,
        "auth": _openai_account_auth_status_payload(manager),
    }


def _openai_account_pending_login_payload(
        manager: OpenAIAccountAuthManager | None = None,
) -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        current = time.time()
        latest_job = _latest_openai_account_login_job(current)
        if latest_job is None:
            return {
                "status": "none",
                "auth": _openai_account_auth_status_payload(manager),
            }
        login_id, job = latest_job
        return _openai_account_login_payload(login_id, job, manager, current)


def _openai_account_start_login_payload() -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        manager = OpenAIAccountAuthManager()
        now = time.time()
        latest_job = _latest_openai_account_login_job(now)
        if latest_job is not None:
            login_id, job = latest_job
            return _openai_account_login_payload(login_id, job, manager, now)

        device_code = manager.start_device_login()
        now = time.time()
        raw_expires_in = device_code.expires_in or _OPENAI_ACCOUNT_LOGIN_MAX_TTL_SECONDS
        expires_in = min(int(raw_expires_in), _OPENAI_ACCOUNT_LOGIN_MAX_TTL_SECONDS)
        expires_at = now + expires_in
        login_id = uuid.uuid4().hex
        job = _OpenAIAccountLoginJob(
            device_code=device_code,
            created_at=now,
            expires_at=expires_at,
        )
        _store_openai_account_login_job(login_id, job, now)
        return _openai_account_login_payload(login_id, job, manager, now)


def _openai_account_poll_login_payload(login_id: str) -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        now = time.time()
        job = _get_openai_account_login_job(login_id, now)
        if job is None:
            return {"status": "expired", "authenticated": False}

        manager = OpenAIAccountAuthManager()
        try:
            tokens = manager.poll_device_login(job.device_code)
        except OpenAIAccountAuthError as exc:
            if exc.relogin_required:
                _remove_openai_account_login_job(login_id)
            raise
        if tokens is None:
            return {
                "status": "pending",
                "authenticated": False,
                "expires_at": job.expires_at,
            }

        _remove_openai_account_login_job(login_id)
        return {
            "status": "authenticated",
            "authenticated": True,
            "auth": _openai_account_auth_status_payload(manager),
        }


def _openai_account_logout_payload() -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        manager = OpenAIAccountAuthManager()
        _clear_openai_account_login_jobs()
        logged_out = manager.logout()
        return {
            "logged_out": logged_out,
            "auth": _openai_account_auth_status_payload(manager),
        }


def _openai_account_models_payload() -> dict[str, Any]:
    with _OPENAI_ACCOUNT_AUTH_OPERATION_LOCK:
        manager = OpenAIAccountAuthManager()
        catalog = OpenAIAccountModelCatalog(base_url=manager.base_url)
        models = catalog.list_model_ids(auth_manager=manager)
        return {
            "models": models,
            "base_url": manager.base_url,
            "auth": _openai_account_auth_status_payload(manager),
        }


def _openai_account_auth_error_payload(exc: OpenAIAccountAuthError) -> dict[str, Any]:
    return {
        "status": "error",
        "error": str(exc),
        "code": exc.code,
        "status_code": exc.status_code,
        "relogin_required": exc.relogin_required,
    }


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
    from agent_gateway.common.attachment_safety import (
        is_forbidden_document,
        safe_session_dirname,
        safe_upload_filename,
    )
    from agent_gateway.routing.agent_http_bridge import upload_file_bytes_via_e2a

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
    from agent_gateway.common.attachment_safety import (
        strip_data_uri_prefix as _strip_data_uri_prefix,
    )
    from agent_gateway.routing.agent_http_bridge import E2A_PAYLOAD_MAX_BYTES

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
            data = base64.b64decode(_strip_data_uri_prefix(raw), validate=True)
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
    from agent_gateway.common.attachment_safety import (
        ensure_image_upload_filename,
        image_suffix_for_mime,
        safe_session_dirname,
        safe_upload_filename,
    )
    from agent_gateway.routing.agent_http_bridge import upload_file_bytes_via_e2a

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
    import json as _json

    from agent_gateway.common.attachment_safety import (
        strip_data_uri_prefix as _strip_data_uri_prefix,
    )
    from agent_gateway.routing.agent_http_bridge import E2A_PAYLOAD_MAX_BYTES

    try:
        base_payload = {k: v for k, v in params.items() if k != "media_items"}
        overhead = len(_json.dumps(base_payload, ensure_ascii=False, default=str))
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
            data = base64.b64decode(_strip_data_uri_prefix(raw), validate=True)
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

    async def _openai_account_auth_status(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_auth_status_payload)
            await channel.send_response(ws, req_id, ok=True, payload=payload)
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.status] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.status] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_auth_start_login(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_start_login_payload)
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=payload,
            )
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.start_login] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.start_login] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_auth_pending_login(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_pending_login_payload)
            await channel.send_response(ws, req_id, ok=True, payload=payload)
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.pending_login] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.pending_login] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_auth_poll_login(ws, req_id, params, session_id):
        del session_id
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        login_id = str(params.get("login_id") or "").strip()
        if not login_id:
            await channel.send_response(ws, req_id, ok=False, error="login_id is required", code="BAD_REQUEST")
            return
        try:
            payload = await asyncio.to_thread(_openai_account_poll_login_payload, login_id)
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=payload,
            )
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.poll_login] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.poll_login] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_auth_logout(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_logout_payload)
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=payload,
            )
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.auth.logout] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.auth.logout] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _openai_account_models_list(ws, req_id, params, session_id):
        del params, session_id
        try:
            payload = await asyncio.to_thread(_openai_account_models_payload)
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload=payload,
            )
        except OpenAIAccountAuthError as exc:
            logger.warning("[openai_account.models.list] %s", exc)
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(exc),
                code=exc.code,
                payload=_openai_account_auth_error_payload(exc),
            )
        except OpenAIAccountModelListError as exc:
            logger.warning("[openai_account.models.list] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="MODEL_LIST_ERROR")
        except _OPENAI_ACCOUNT_LOCAL_ERRORS as exc:
            logger.warning("[openai_account.models.list] %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")

    async def _project_list(ws, req_id, params, session_id, user_id=None):
        channel.ensure_lifecycle_watch(user_id)
        """获取项目列表(含统计),已排序,包含默认项目。

        filter: ``"all"``(默认) / ``"pinned"`` / ``"unpinned"``
        work_mode: 可选,按工作模式过滤(``"code"`` / ``"work"``),不传则返回全部模式。
            默认项目按 work_mode 拆分:``default``(work)+ ``default_code``(code)。

        统计口径: ``session_count`` / ``last_message_at`` / ``last_user_message_at``
        仅统计该项目的非置顶**普通**会话(``cron_id`` 为空)。置顶会话与 cron 会话
        不计入任何项目统计。
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_LIST,
            label="project.list",
        )

    async def _project_get_sessions(ws, req_id, params, session_id, user_id=None):
        """获取项目下的非置顶普通会话列表（目标 AgentServer 执行）。"""
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_GET_SESSIONS,
            label="project.get_sessions",
        )

    async def _project_get_cron_sessions(ws, req_id, params, session_id, user_id=None):
        """获取项目 cron 会话（目标 AgentServer 执行）。"""
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_GET_CRON_SESSIONS,
            label="project.get_cron_sessions",
        )

    async def _project_create(ws, req_id, params, session_id, user_id=None):
        """Forward project creation to the target AgentServer.

        The AgentServer validates paths and owns project/Git state in its
        injected user directory. Gateway only schedules deployment-side warm
        key reconciliation after a successful mutation.
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        async def after_create(ok, payload):
            if ok:
                _schedule_agent_prewarm_sync("project.create")
                if payload.get("restored"):
                    await _broadcast_project_event("project.restored", payload["project_id"], user_id)

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_CREATE,
            label="project.create",
            # 保留 AgentServer 返回的结构化错误明细。
            preserve_error_payload=True,
            on_done=after_create,
        )

    async def _project_rename(ws, req_id, params, session_id, user_id=None):
        """重命名项目,仅修改展示名,不改动工作目录路径。

        默认项目禁止重命名(``FORBIDDEN``)。``name`` 与已有项目(含隐藏)重复时返回 ``CONFLICT``。
        ``name`` 含文件系统非法字符 / 为保留设备名时返回 ``BAD_REQUEST``。
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_RENAME,
            label="project.rename",
        )

    async def _project_pin(ws, req_id, params, session_id, user_id=None):
        """置顶/取消置顶项目,操作后对所有置顶项目紧凑重编号为 1..N。幂等。

        默认项目禁止置顶(``FORBIDDEN``)。新置顶项目 ``pin_order`` 默认 0,
        重编号后置于置顶区顶部(与 ``session.pin`` 行为一致)。
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_PIN,
            label="project.pin",
        )

    async def _broadcast_project_event(event: str, project_id: str, user_id) -> None:
        """Notify every connection of ``user_id`` that a project was hidden/restored.

        The project's conversations and cron jobs change visibility with it, so
        other tabs (and the archive page) must refresh; a response to the
        originating socket alone would leave them stale.  Broadcast is scoped
        to the owner's connections to avoid making unrelated users refetch.
        """
        if not project_id:
            return
        payload = {"project_id": project_id}
        for client_ws in list(getattr(channel, "clients", ()) or ()):
            try:
                if str(channel.connection_user_id(client_ws) or "") != str(user_id or ""):
                    continue
                await channel.send_event(client_ws, event, payload)
            except Exception:  # noqa: BLE001 - 单条连接异常不得影响其他连接
                logger.debug("project event broadcast failed: %s", event, exc_info=True)

    async def _project_remove(ws, req_id, params, session_id, user_id=None):
        """Forward project soft-deletion; stop its cron jobs, clean Git watchers."""
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import fetch_agent_unary

        project_id = str((params or {}).get("project_id") or "").strip()

        if not project_id or project_id in {"default", "default_code"}:
            await channel.send_response(
                ws, req_id, ok=False,
                error="project_id is required" if not project_id else "default project cannot be removed",
                code="BAD_REQUEST" if not project_id else "FORBIDDEN",
            )
            return

        # Removal is accepted immediately; running sessions no longer block it.
        # The AgentServer hides the project and stops those sessions in the
        # background, so this path goes straight to the cron-safe commit.
        async def _after_remove() -> None:
            registry = getattr(channel, "git_watcher_registry", None)
            if registry is not None and project_id:
                registry.cleanup_project(project_id)
            _schedule_agent_prewarm_sync("project.remove")
            # The AgentServer stops these Sessions in the background.  Settle
            # the Gateway's busy cache now: otherwise a later restore would
            # surface them as "still processing" on session.get_metadata.
            stopped = (
                remove_payload.get("stopped_sessions")
                if isinstance(remove_payload, dict)
                else None
            )
            if isinstance(stopped, list):
                clear_busy = getattr(channel, "clear_session_busy", None)
                if callable(clear_busy):
                    for raw_sid in stopped:
                        sid = str(raw_sid or "")
                        if sid:
                            clear_busy(sid)
            await _broadcast_project_event("project.removed", project_id, user_id)

        class _RemoveCommitError(Exception):
            def __init__(self, payload: dict) -> None:
                self.code = str(payload.get("code") or "BAD_REQUEST")
                super().__init__(str(payload.get("error") or "project.remove failed"))

        remove_payload: dict = {}

        async def commit():
            nonlocal remove_payload
            ok, payload = await fetch_agent_unary(
                agent_client=_resolve(agent_client),
                params=params if isinstance(params, dict) else {},
                session_id=session_id,
                user_id=user_id,
                req_method=ReqMethod.PROJECT_REMOVE,
                channel_id="web",
                label="project.remove",
            )
            if not ok:
                raise _RemoveCommitError(payload)
            remove_payload = payload

        cc = _get_cron()
        if cc is None:
            await channel.send_response(ws, req_id, ok=False, error="cron service unavailable", code="CRON_STOP_FAILED")
            return
        try:
            cron_stop = await cc.hide_project_jobs(project_id, commit=commit)
        except _RemoveCommitError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code=exc.code)
            return
        except Exception as exc:
            logger.warning("project remove failed: %s", exc, exc_info=True)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="CRON_STOP_FAILED")
            return
        # stopped_cron_jobs 是项目下定时任务总数(含移除前已停用的);0 表示
        # 项目下本就没有定时任务,前端据此只提示"项目已移除",不再附带
        # "其定时任务已停止"。
        if isinstance(cron_stop, dict) and "stopped_cron_jobs" in cron_stop:
            remove_payload = {**remove_payload, "stopped_cron_jobs": cron_stop["stopped_cron_jobs"]}
        await channel.send_response(ws, req_id, ok=True, payload=remove_payload)
        await _after_remove()

    async def _project_restore(ws, req_id, params, session_id, user_id=None):
        """Forward project restoration to the target AgentServer."""
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        project_id = str((params or {}).get("project_id") or "").strip()

        async def _after_restore(ok: bool, _payload: object) -> None:
            if not ok:
                return
            _schedule_agent_prewarm_sync("project.restore")
            await _broadcast_project_event("project.restored", project_id, user_id)

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_RESTORE,
            label="project.restore",
            on_done=_after_restore,
        )

    async def _project_info(ws, req_id, params, session_id, user_id=None):
        """获取单个项目详情(含统计),支持虚拟默认项目。

        ``project_id`` 为 ``"default"`` / ``"default_code"`` 时返回对应虚拟默认项目;
        为真实 project_id 时返回该项目的详情。字段与 ``project.list`` 条目一致。

        统计口径同 ``project.list``:仅统计该项目的非置顶普通会话(``cron_id`` 为空)。
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_INFO,
            label="project.info",
        )

    async def _project_pinned_sessions(ws, req_id, params, session_id, user_id=None):
        """获取全部置顶会话,按 ``pin_order`` 升序排列。

        置顶会话已从项目分组中剥离,通过本接口独立获取。``project_dir`` 仍指向
        原归属项目。不接受任何参数。
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PROJECT_PINNED_SESSIONS,
            label="project.pinned_sessions",
        )

    # ── Git RPC thin proxies ────────────────────────────────────────────────
    # Worktree, project-store, and diff-history access executes only in the
    # routed AgentServer. Gateway retains the watcher notification below,
    # because the /ws/git connection lifecycle remains deployment-side.

    def _mark_git_watcher_dirty(project_id: str) -> None:
        """写操作成功后唤醒 /ws/git watcher(阶段 10 注入后生效)。

        阶段 7 时 ``git_watcher_registry`` 属性可能尚未注入,此处防御性调用;
        阶段 10 在 WebChannel 构造后注入 ``git_watcher_registry`` 即自动启用。
        """
        registry = getattr(channel, "git_watcher_registry", None)
        if registry is None:
            return
        try:
            registry.mark_dirty(project_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "[ProjectGit] mark_dirty failed (project=%s): %s",
                project_id, exc,
            )

    from openjiuwen_protocol.message import ReqMethod

    def _make_git_proxy_handler(
        req_method, label, *, doc, mark_dirty=False, mark_dirty_on_failure=False
    ):
        """构造一个 Git RPC 薄代理 handler。
        Git 工作区/项目表/diff 历史访问只发生在目标 AgentServer（E2A 转发）；
        Gateway 仅保留 /ws/git watcher 唤醒（mark_dirty）作为部署侧连接维护。
        统一 ``user_id=None``（AgentOS 下由路由层按连接上下文注入）与
        ``preserve_error_payload=True``（Git 已定义结构化错误明细）。
        """

        async def _handler(ws, req_id, params, session_id, user_id=None):
            from agent_gateway.routing.e2a_proxy import proxy_unary_request

            callbacks = {}
            if mark_dirty or mark_dirty_on_failure:
                project_id = str((params or {}).get("project_id") or "").strip()

                def _on_done(ok, _payload):
                    # 成功与"已发生局部变更的失败"都唤醒 watcher（commit 失败但 index
                    # 已改变时同样需要刷新）；是否行动由调用方在回调内判定。
                    if ok or mark_dirty_on_failure:
                        return _mark_git_watcher_dirty(project_id)
                    return None

                callbacks["on_done"] = _on_done
            await proxy_unary_request(
                channel=channel,
                agent_client=_resolve(agent_client),
                ws=ws,
                req_id=req_id,
                params=params if isinstance(params, dict) else {},
                session_id=session_id,
                user_id=user_id,
                req_method=req_method,
                label=label,
                preserve_error_payload=True,
                **callbacks,
            )

        _handler.__doc__ = doc
        return _handler

    _project_git_status = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_STATUS,
        "project.git.status",
        doc="Forward Git status to the AgentServer that owns the worktree.",
    )
    _project_git_probe = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_PROBE,
        "project.git.probe",
        doc="Probe Git in AgentServer and refresh the Gateway watcher afterwards.",
        mark_dirty=True,
    )
    _project_git_init = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_INIT,
        "project.git.init",
        doc="Initialize Git in the target AgentServer's project worktree.",
        mark_dirty=True,
    )
    _project_git_switch_branch = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_SWITCH_BRANCH,
        "project.git.switch_branch",
        doc="Switch a branch in the target AgentServer's project worktree.",
        mark_dirty=True,
    )
    _project_git_create_branch = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_CREATE_BRANCH,
        "project.git.create_branch",
        doc="Create a branch in the target AgentServer's project worktree.",
        mark_dirty=True,
    )
    _project_git_commit = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_COMMIT,
        "project.git.commit",
        doc=(
            "提交当前工作区改动到当前分支(设计文档 §4.9 ``project.git.commit``)。"
            "``stage_all=true`` 先 ``git add -A`` 暂存全部;``paths`` 显式指定暂存路径"
            "(与 ``stage_all`` 互斥)。``amend=true`` 覆盖最近一次提交。成功/失败后调"
            "``mark_dirty`` 触发 /ws/git 立即重算(HEAD/is_dirty/stats 变化)。"
        ),
        mark_dirty=True,
        mark_dirty_on_failure=True,
    )
    _project_git_push = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_PUSH,
        "project.git.push",
        doc=(
            "推送本地分支到远程(设计文档 §4.10 ``project.git.push``)。"
            "``remote`` 默认 ``origin``;``branch`` 不传时用当前分支(detached HEAD"
            "须显式传 ``branch``)。``force=true`` 使用 ``--force-with-lease``(更安全)。"
            "``delete=true`` 删除远程分支(与 ``set_upstream``/``force`` 互斥)。push"
            "涉及网络,超时独立配置(``GIT_PUSH_TIMEOUT_SEC``,默认 60s)。成功后调"
            "``mark_dirty`` 刷新 /ws/git(主要更新 upstream 字段)。"
        ),
        mark_dirty=True,
    )
    _project_git_diff_status = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_DIFF_STATUS,
        "project.git.diff_status",
        doc=(
            "拉取当前分支 diff 和上一轮对话 diff 的快照(设计文档 §4.1.16)。"
            "``include_files=true`` 返回文件列表;``include_hunks=true`` 隐含"
            "``include_files=true`` 并返回 hunk。transient 状态下 ``current``"
            "为 ``null``,仍成功返回 ``repo.transient=true``。"
        ),
    )
    _project_git_turn_diff_list = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_TURN_DIFF_LIST,
        "project.git.turn_diff_list",
        doc="查询历史 Diff 摘要列表。",
    )
    _project_git_turn_diff = _make_git_proxy_handler(
        ReqMethod.PROJECT_GIT_TURN_DIFF,
        "project.git.turn_diff",
        doc="查询指定轮次 Diff 详情。",
    )

    async def _path_get(ws, req_id, params, session_id, user_id=None):
        """读 browser.chrome_path 并返回给前端（会解析环境变量）。"""
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import (
            is_legacy_shared_directory_client,
            proxy_unary_request,
        )

        resolved_client = _resolve(agent_client)
        if resolved_client is not None and not is_legacy_shared_directory_client(resolved_client):
            await proxy_unary_request(
                channel=channel, agent_client=resolved_client, ws=ws, req_id=req_id,
                params={}, session_id=session_id, user_id=user_id,
                req_method=ReqMethod.PATH_GET, label="path.get",
            )
            return
        try:
            config_base = get_config()
        except FileNotFoundError:
            await channel.send_response(
                ws,
                req_id,
                ok=True,
                payload={"chrome_path": "", "headless": True},
            )
            return

        if not isinstance(config_base, dict):
            config_base = {}

        config = _resolve_env_vars(config_base)
        browser_cfg = config.get("browser", {}) if isinstance(config, dict) else {}
        chrome_path = ""
        headless = True
        if isinstance(browser_cfg, dict):
            value = browser_cfg.get("chrome_path", "")
            if isinstance(value, str):
                chrome_path = value
            raw_headless = browser_cfg.get("headless", True)
            headless = bool(raw_headless) if isinstance(raw_headless, bool) else True

        from agent_gateway.common.browser_config import (
            browser_decision_state,
        )

        await channel.send_response(
            ws, req_id, ok=True,
            payload={
                "chrome_path": chrome_path,
                "headless": headless,
                **browser_decision_state(config),
            },
        )

    async def _path_set(ws, req_id, params, session_id, user_id=None):
        """更新 browser.chrome_path / headless 并写回 config。"""
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return

        chrome_path = params.get("chrome_path")
        if not isinstance(chrome_path, str):
            await channel.send_response(ws, req_id, ok=False, error="chrome_path must be string", code="BAD_REQUEST")
            return
        chrome_path = chrome_path.strip()

        raw_headless = params.get("headless", True)
        headless = bool(raw_headless) if isinstance(raw_headless, bool) else True
        # Older clients omit the decision fields; the saved values are then left untouched.
        set_params = {"chrome_path": chrome_path, "headless": headless}
        for decision_key in (
            "decision_mode", "decision_provider", "decision_model", "decision_api_base", "decision_display_name",
        ):
            if params.get(decision_key) is not None:
                set_params[decision_key] = params[decision_key]

        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import (
            fetch_agent_unary,
            is_legacy_shared_directory_client,
            proxy_unary_request,
        )

        resolved_client = _resolve(agent_client)
        if resolved_client is not None and not is_legacy_shared_directory_client(resolved_client):
            # Fetch current browser config so the restart callback can match the
            # old runtime identity (custom binary / headed mode).
            previous_chrome_path = ""
            previous_headless = True
            try:
                prev_ok, prev_payload = await fetch_agent_unary(
                    agent_client=resolved_client,
                    req_method=ReqMethod.PATH_GET,
                    params=None,
                    session_id=session_id,
                    user_id=user_id,
                    channel_id=channel.channel_id,
                    label="path.get (prev)",
                )
                if prev_ok and isinstance(prev_payload, dict):
                    prev_chrome = prev_payload.get("chrome_path")
                    if isinstance(prev_chrome, str):
                        previous_chrome_path = prev_chrome
                    prev_headless_val = prev_payload.get("headless", True)
                    previous_headless = (
                        bool(prev_headless_val) if isinstance(prev_headless_val, bool) else True
                    )
            except Exception as e:  # noqa: BLE001
                logger.warning("[path.set] failed to fetch previous browser config: %s", e)

            async def _on_path_set_done(ok: bool, _payload: dict) -> None:
                if not ok:
                    return
                try:
                    await _clear_agent_config_cache(resolved_client)
                except Exception as e:  # noqa: BLE001
                    logger.warning("[path.set] AgentServer config cache clear failed: %s", e)
                try:
                    await _restart_agent_browser_runtime(
                        resolved_client,
                        previous_chrome_path=previous_chrome_path,
                        previous_headless=previous_headless,
                    )
                except Exception as e:  # noqa: BLE001
                    logger.warning("[path.set] browser runtime restart failed: %s", e)

            await proxy_unary_request(
                channel=channel, agent_client=resolved_client, ws=ws, req_id=req_id,
                params=set_params,
                session_id=session_id, user_id=user_id,
                req_method=ReqMethod.PATH_SET, label="path.set",
                on_done=_on_path_set_done,
            )
            return

        current_browser_cfg = get_config().get("browser", {})
        if not isinstance(current_browser_cfg, dict):
            current_browser_cfg = {}
        previous_chrome_path = current_browser_cfg.get("chrome_path", "")
        if not isinstance(previous_chrome_path, str):
            previous_chrome_path = ""
        raw_previous_headless = current_browser_cfg.get("headless", True)
        previous_headless = (
            bool(raw_previous_headless)
            if isinstance(raw_previous_headless, bool)
            else True
        )

        from agent_gateway.common.browser_config import (
            browser_decision_state,
            browser_decision_updates,
        )
        from agent_gateway.common.config import update_browser_decision_in_config

        decision_updates, error = browser_decision_updates(
            set_params.get("decision_mode"), set_params.get("decision_provider"), get_config(),
            model=set_params.get("decision_model"), api_base=set_params.get("decision_api_base"),
            display_name=set_params.get("decision_display_name"),
        )
        if error:
            await channel.send_response(ws, req_id, ok=False, error=error, code="BAD_REQUEST")
            return

        try:
            update_browser_in_config({"chrome_path": chrome_path, "headless": headless})
            if decision_updates:
                update_browser_decision_in_config(decision_updates)
            resolved_agent_client = _resolve(agent_client)
            await _clear_agent_config_cache(resolved_agent_client)
        except Exception as e:  # noqa: BLE001
            logger.warning("[path.set] 写回 config.yaml 失败: %s", e)
            await channel.send_response(ws, req_id, ok=False, error=str(e), code="INTERNAL_ERROR")
            return

        try:
            await _restart_agent_browser_runtime(
                resolved_agent_client,
                previous_chrome_path=previous_chrome_path,
                previous_headless=previous_headless,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "[path.set] browser config saved but active runtime reset failed: %s",
                e,
            )

        await channel.send_response(
            ws, req_id, ok=True,
            payload={"chrome_path": chrome_path, "headless": headless, **browser_decision_state(get_config())},
        )

    async def _path_select_directory(ws, req_id, params, session_id, user_id=None):
        """在 AgentServer 注入目录内选择项目目录（决策 D3：返回 AgentServer 侧绝对路径）。

        经统一薄代理 E2A 转发目标 AgentServer（WorkspaceFileAdapter
        PATH_SELECT_DIRECTORY，注入 workspace 内解析/校验，越界返回 BAD_REQUEST）。
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import (
            is_legacy_shared_directory_client,
            proxy_unary_request,
        )

        # A local single-user deployment historically opened a native picker in
        # the Gateway process.  Keep that UI contract there; AgentOS cannot use
        # a deployment-side picker because it would expose the wrong machine and
        # user directory.
        resolved_client = _resolve(agent_client)
        if is_legacy_shared_directory_client(resolved_client):
            if not isinstance(params, dict):
                params = {}
            initial_dir = params.get("initial_dir")
            if initial_dir is not None and not isinstance(initial_dir, str):
                await channel.send_response(
                    ws, req_id, ok=False, error="initial_dir must be string", code="BAD_REQUEST",
                )
                return
            from agent_gateway.channel_manager.web.directory_picker import (
                select_directory_native,
            )
            try:
                selected = await asyncio.to_thread(
                    select_directory_native,
                    initial_dir=initial_dir.strip() if isinstance(initial_dir, str) else None,
                )
            except Exception as exc:  # noqa: BLE001
                await channel.send_response(ws, req_id, ok=False, error=str(exc), code="UNSUPPORTED")
                return
            await channel.send_response(
                ws, req_id, ok=True,
                payload={"path": selected, "cancelled": not bool(selected)},
            )
            return

        await proxy_unary_request(
            channel=channel,
            agent_client=resolved_client,
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PATH_SELECT_DIRECTORY,
            label="path.select_directory",
        )

    async def _path_select_files(ws, req_id, params, session_id, user_id=None):
        """枚举 AgentServer 注入目录下的可上传文件（决策 D3：返回 AgentServer 侧绝对路径）。

        经统一薄代理 E2A 转发目标 AgentServer（WorkspaceFileAdapter
        PATH_SELECT_FILES，注入 workspace 内枚举，返回与桌面端同形元数据）。
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import (
            is_legacy_shared_directory_client,
            proxy_unary_request,
        )

        resolved_client = _resolve(agent_client)
        if is_legacy_shared_directory_client(resolved_client):
            if not isinstance(params, dict):
                params = {}
            initial_dir = params.get("initial_dir")
            allow_multiple = params.get("allow_multiple", True)
            if initial_dir is not None and not isinstance(initial_dir, str):
                await channel.send_response(
                    ws, req_id, ok=False, error="initial_dir must be string", code="BAD_REQUEST",
                )
                return
            if not isinstance(allow_multiple, bool):
                await channel.send_response(
                    ws, req_id, ok=False, error="allow_multiple must be boolean", code="BAD_REQUEST",
                )
                return
            from agent_gateway.channel_manager.web.file_picker import (
                select_and_describe_files,
            )
            try:
                selected = await asyncio.to_thread(
                    select_and_describe_files,
                    allow_multiple=allow_multiple,
                    initial_dir=initial_dir.strip() if isinstance(initial_dir, str) else None,
                )
            except Exception as exc:  # noqa: BLE001
                await channel.send_response(ws, req_id, ok=False, error=str(exc), code="UNSUPPORTED")
                return
            await channel.send_response(
                ws, req_id, ok=True,
                payload={"files": selected or [], "cancelled": selected is None},
            )
            return

        await proxy_unary_request(
            channel=channel,
            agent_client=resolved_client,
            ws=ws,
            req_id=req_id,
            params=params,
            session_id=session_id,
            user_id=user_id,
            req_method=ReqMethod.PATH_SELECT_FILES,
            label="path.select_files",
        )

    async def _path_select_file(ws, req_id, params, _session_id):
        if not isinstance(params, dict):
            params = {}

        initial_dir = params.get("initial_dir")
        initial_path = params.get("initial_path")
        title = params.get("title")
        if initial_dir is not None and not isinstance(initial_dir, str):
            await channel.send_response(
                ws, req_id, ok=False, error="initial_dir must be string", code="BAD_REQUEST",
            )
            return
        if initial_path is not None and not isinstance(initial_path, str):
            await channel.send_response(
                ws, req_id, ok=False, error="initial_path must be string", code="BAD_REQUEST",
            )
            return
        if title is not None and not isinstance(title, str):
            await channel.send_response(
                ws, req_id, ok=False, error="title must be string", code="BAD_REQUEST",
            )
            return

        resolved_initial_dir = initial_dir.strip() if isinstance(initial_dir, str) else None
        if isinstance(initial_path, str) and initial_path.strip():
            resolved_initial_dir = str(Path(initial_path.strip()).expanduser().parent)

        from agent_gateway.channel_manager.web.directory_picker import select_file_native

        try:
            selected = await asyncio.to_thread(
                select_file_native,
                initial_dir=resolved_initial_dir,
                title=title.strip() if isinstance(title, str) else None,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[path.select_file] picker unavailable: %s", exc)
            await channel.send_response(
                ws, req_id, ok=False, error=str(exc), code="UNSUPPORTED",
            )
            return

        if not selected:
            await channel.send_response(ws, req_id, ok=True, payload={"path": None, "cancelled": True})
            return
        await channel.send_response(ws, req_id, ok=True, payload={"path": selected, "cancelled": False})

    async def _chat_send(ws, req_id, params, session_id):
        await channel.send_response(
            ws,
            req_id,
            ok=True,
            payload={"accepted": True, "session_id": session_id},
        )

    async def _media_persist(ws, req_id, params, session_id, user_id=None):
        """浏览器上传媒体附件落盘（E2A 转发 AgentServer 注入目录 uploads）。

        经统一薄代理 E2A 转发目标 AgentServer（WorkspaceFileAdapter
        MEDIA_PERSIST，base64 小文件由注入目录落盘；大文件在 Gateway 侧解码后
        走受认证 HTTP bridge 上传，避免超内部 WS 帧限制）。
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

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
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

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
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

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
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

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
    channel.register_method("openai_account.auth.status", _openai_account_auth_status)
    channel.register_method("openai_account.auth.start_login", _openai_account_auth_start_login)
    channel.register_method("openai_account.auth.pending_login", _openai_account_auth_pending_login)
    channel.register_method("openai_account.auth.poll_login", _openai_account_auth_poll_login)
    channel.register_method("openai_account.auth.logout", _openai_account_auth_logout)
    channel.register_method("openai_account.models.list", _openai_account_models_list)

    from agent_gateway.channel_manager.web.session_rpc_handlers import (
        register_session_handlers,
    )
    register_session_handlers(channel, agent_client, _resolve)

    from agent_gateway.channel_manager.web.designer_handlers import (
        register_designer_handlers,
    )
    register_designer_handlers(channel, lambda: _resolve(agent_client))

    channel.register_method("project.list", _project_list)
    channel.register_method("project.info", _project_info)
    channel.register_method("project.get_sessions", _project_get_sessions)
    channel.register_method("project.get_cron_sessions", _project_get_cron_sessions)
    channel.register_method("project.create", _project_create)
    channel.register_method("project.rename", _project_rename)
    channel.register_method("project.pin", _project_pin)
    channel.register_method("project.remove", _project_remove)
    channel.register_method("project.restore", _project_restore)
    from agent_gateway.channel_manager.web.lifecycle_handlers import (
        register_lifecycle_handlers,
    )
    register_lifecycle_handlers(
        channel, lambda: _resolve(agent_client), lambda: _resolve(cron_controller),
    )
    channel.register_method("project.pinned_sessions", _project_pinned_sessions)

    # Git RPC handlers (设计文档 §4.1.11-§4.1.15)
    channel.register_method("project.git.status", _project_git_status)
    channel.register_method("project.git.probe", _project_git_probe)
    channel.register_method("project.git.init", _project_git_init)
    channel.register_method("project.git.switch_branch", _project_git_switch_branch)
    channel.register_method("project.git.create_branch", _project_git_create_branch)
    channel.register_method("project.git.commit", _project_git_commit)
    channel.register_method("project.git.push", _project_git_push)
    channel.register_method("project.git.diff_status", _project_git_diff_status)
    channel.register_method("project.git.turn_diff_list", _project_git_turn_diff_list)
    channel.register_method("project.git.turn_diff", _project_git_turn_diff)

    channel.register_method("path.get", _path_get)
    channel.register_method("path.set", _path_set)
    channel.register_method("path.select_directory", _path_select_directory)
    channel.register_method("path.select_files", _path_select_files)
    channel.register_method("path.select_file", _path_select_file)

    register_workspace_handlers(channel, agent_client, _resolve)
    channel.register_method("chat.send", _chat_send)
    channel.register_method("media.persist", _media_persist)
    channel.register_method("media.discard", _media_discard)
    channel.register_method("document.persist", _document_persist)
    channel.register_method("document.formats", _document_formats)
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

    # 数字分身 — permissions.owner_scopes：A5 起统一经目标 AgentServer 的
    # ConfigAdapter 执行（写入后 server 侧 in-process reload）。
    # 其余 permissions.*（tools / rules / approval_overrides）走 _forward_permissions_to_agent。

    async def _permissions_owner_scopes_get(ws, req_id, params, session_id, user_id=None):
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(channel=channel, agent_client=_resolve(agent_client), ws=ws, req_id=req_id,
                                  params={}, session_id=session_id, user_id=user_id,
                                  req_method=ReqMethod.PERMISSIONS_OWNER_SCOPES_GET,
                                  label="permissions.owner_scopes.get")

    async def _permissions_owner_scopes_set(ws, req_id, params, session_id, user_id=None):
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error="params must be object", code="BAD_REQUEST")
            return
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        await proxy_unary_request(channel=channel, agent_client=_resolve(agent_client), ws=ws, req_id=req_id,
                                  params=params, session_id=session_id, user_id=user_id,
                                  req_method=ReqMethod.PERMISSIONS_OWNER_SCOPES_SET,
                                  label="permissions.owner_scopes.set")

    channel.register_method("permissions.owner_scopes.get", _permissions_owner_scopes_get)
    channel.register_method("permissions.owner_scopes.set", _permissions_owner_scopes_set)

    async def _forward_permissions_to_agent(ws, req_id, params, session_id, user_id=None, *, req_method):
        """Forward permissions requests; Gateway never falls back to local state."""
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        if not isinstance(req_method, ReqMethod):
            await channel.send_response(ws, req_id, ok=False, error="invalid req_method", code="INTERNAL_ERROR")
            return
        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=params if isinstance(params, dict) else {},
            session_id=session_id,
            user_id=user_id,
            req_method=req_method,
            label="permissions",
        )

    from openjiuwen_protocol.message import ReqMethod as _PermReq

    def _register_perm(method_name: str, rm: Any) -> None:
        async def _handler(ws, req_id, params, session_id, user_id=None):
            await _forward_permissions_to_agent(ws, req_id, params, session_id, user_id, req_method=rm)

        channel.register_method(method_name, _handler)

    _register_perm("permissions.tools.get", _PermReq.PERMISSIONS_TOOLS_GET)
    _register_perm("permissions.tools.set", _PermReq.PERMISSIONS_TOOLS_SET)
    _register_perm("permissions.tools.update", _PermReq.PERMISSIONS_TOOLS_UPDATE)
    _register_perm("permissions.tools.update_user", _PermReq.PERMISSIONS_TOOLS_UPDATE_USER)
    _register_perm("permissions.tools.delete", _PermReq.PERMISSIONS_TOOLS_DELETE)
    _register_perm("permissions.rules.get", _PermReq.PERMISSIONS_RULES_GET)
    _register_perm("permissions.rules.create", _PermReq.PERMISSIONS_RULES_CREATE)
    _register_perm("permissions.rules.update", _PermReq.PERMISSIONS_RULES_UPDATE)
    _register_perm("permissions.rules.delete", _PermReq.PERMISSIONS_RULES_DELETE)
    _register_perm("permissions.approval_overrides.get", _PermReq.PERMISSIONS_APPROVAL_OVERRIDES_GET)
    _register_perm("permissions.approval_overrides.delete", _PermReq.PERMISSIONS_APPROVAL_OVERRIDES_DELETE)

    async def _forward_a4p_to_agent(ws, req_id, params, session_id, *, req_method):
        """Forward an A4P User Authorizer RPC over the session's logical Web route."""
        from openjiuwen_protocol.e2a.gateway_normalize import e2a_from_agent_fields
        from openjiuwen_protocol.message import ReqMethod

        if not isinstance(req_method, ReqMethod):
            await channel.send_response(ws, req_id, ok=False, error="invalid req_method", code="INTERNAL_ERROR")
            return
        ac = _resolve(agent_client)
        if ac is None or not getattr(ac, "server_ready", False):
            await channel.send_response(
                ws, req_id, ok=False, error="Agent server is not ready", code="AGENT_NOT_READY"
            )
            return

        route_metadata: dict[str, Any] = {
            "ws_id": str(getattr(ws, "_jiuwen_ws_id", "") or ""),
        }
        route = None
        try:
            routes = await channel.routing_keys_for_ws(ws)
            route = next(
                (item for item in reversed(routes) if item.session_id == session_id),
                None,
            )
            if route is not None:
                route_metadata["app_id"] = route.app_id
                route_metadata["agent_ref"] = {
                    "mode": route.agent_ref.mode,
                    "id": route.agent_ref.id,
                }
        except Exception:
            logger.warning("[a4p] failed to resolve Web authorizer route", exc_info=True)
        if route is None:
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error="No Web authorizer route is registered for this session",
                code="A4P_WEB_SESSION_REQUIRED",
            )
            return

        env = e2a_from_agent_fields(
            request_id=str(req_id) if req_id else "",
            channel_id="web",
            session_id=session_id,
            req_method=req_method,
            params=dict(params) if isinstance(params, dict) else {},
            metadata=route_metadata,
        )
        authorize_completion = req_method in {
            ReqMethod.A4P_AUTHORIZATION_COMPLETE,
            ReqMethod.A4P_AUTHORIZATION_REJECT,
        }
        if authorize_completion:
            # 刷新后经新连接完成/拒绝授权：把该恢复 turn 的输出改绑到当前连接，
            # 其迟到事件（旧 ws 已注销）据此改投，而不是被当孤儿丢弃。
            channel.record_a4p_rebound(session_id, route_metadata["ws_id"])
        try:
            resp = await ac.send_request(env)
        except Exception as exc:
            if authorize_completion:
                channel.clear_a4p_rebound(session_id)
            logger.exception("[a4p] forward to agent failed: %s", exc)
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code="INTERNAL_ERROR")
            return
        if not resp.ok:
            if authorize_completion:
                channel.clear_a4p_rebound(session_id)
            payload = resp.payload if isinstance(resp.payload, dict) else {}
            await channel.send_response(
                ws,
                req_id,
                ok=False,
                error=str(payload.get("error") or "request failed"),
                code=str(payload.get("code") or "BAD_REQUEST"),
            )
            return
        await channel.send_response(
            ws,
            req_id,
            ok=True,
            payload=resp.payload if isinstance(resp.payload, dict) else {},
        )

    from openjiuwen_protocol.message import ReqMethod as _A4PReq

    def _register_a4p(method_name: str, req_method: Any) -> None:
        async def _handler(ws, req_id, params, session_id):
            await _forward_a4p_to_agent(
                ws, req_id, params, session_id, req_method=req_method
            )

        channel.register_method(method_name, _handler)

    _register_a4p("a4p.config.get", _A4PReq.A4P_CONFIG_GET)
    _register_a4p("a4p.config.update", _A4PReq.A4P_CONFIG_UPDATE)
    _register_a4p("a4p.authorization.reprepare", _A4PReq.A4P_AUTHORIZATION_REPREPARE)
    _register_a4p("a4p.authorization.complete", _A4PReq.A4P_AUTHORIZATION_COMPLETE)
    _register_a4p("a4p.authorization.reject", _A4PReq.A4P_AUTHORIZATION_REJECT)
    _register_a4p("a4p.authorization.pending", _A4PReq.A4P_AUTHORIZATION_PENDING)

    register_memory_handlers(channel, agent_client, _resolve)

    async def _forward_harness_to_agent(ws, req_id, params, session_id, user_id=None, *, req_method):
        """harness.*：经 E2A 转发到 AgentServer（harness 包目录在 AgentServer 注入目录）。

        AgentServer 不可达时：默认本地 WebSocket client（单用户共享目录）由
        ``e2a_proxy`` 的 legacy fallback 在 Gateway 进程内执行同一业务；远程/
        AgentOS client 返回可重试错误，不读部署侧目录。
        """
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        if not isinstance(req_method, ReqMethod):
            await channel.send_response(ws, req_id, ok=False, error="invalid req_method", code="INTERNAL_ERROR")
            return

        request_params = dict(params if isinstance(params, dict) else {})
        if str(req_method.value).startswith("rsi."):
            request_params["session_id"] = session_id
            if req_method is ReqMethod.RSI_ARTIFACT_DOWNLOAD and user_id:
                request_params["_download_user_id"] = user_id

        await proxy_unary_request(
            channel=channel,
            agent_client=_resolve(agent_client),
            ws=ws,
            req_id=req_id,
            params=request_params,
            session_id=session_id,
            user_id=user_id,
            req_method=req_method,
            label="harness",
        )

    from openjiuwen_protocol.message import ReqMethod as _HarnessReq

    def _register_harness(method_name: str, rm: Any) -> None:
        async def _handler(ws, req_id, params, session_id, user_id=None):
            await _forward_harness_to_agent(ws, req_id, params, session_id, user_id, req_method=rm)

        channel.register_method(method_name, _handler)

    _register_harness("harness.packages", _HarnessReq.HARNESS_PACKAGES_GET)
    _register_harness("harness.packages.scan", _HarnessReq.HARNESS_PACKAGES_SCAN)
    _register_harness("harness.activate", _HarnessReq.HARNESS_PACKAGES_ACTIVATE)
    _register_harness("harness.deactivate", _HarnessReq.HARNESS_PACKAGES_DEACTIVATE)
    _register_harness("harness.delete", _HarnessReq.HARNESS_PACKAGES_DELETE)

    # RSI 优化平台 18 个 web method（web 契约 v0.3 §4）：经 E2A 转发到 AgentServer。
    # 与 harness.* 同构（仅注册 + proxy_unary_request，不承载业务）。
    rsi_methods = [
        ("rsi.dataset.validate", _HarnessReq.RSI_DATASET_VALIDATE),
        ("rsi.task.create", _HarnessReq.RSI_TASK_CREATE),
        ("rsi.task.list", _HarnessReq.RSI_TASK_LIST),
        ("rsi.task.get", _HarnessReq.RSI_TASK_GET),
        ("rsi.task.delete", _HarnessReq.RSI_TASK_DELETE),
        ("rsi.training.start", _HarnessReq.RSI_TRAINING_START),
        ("rsi.training.pause", _HarnessReq.RSI_TRAINING_PAUSE),
        ("rsi.training.resume", _HarnessReq.RSI_TRAINING_RESUME),
        ("rsi.training.terminate", _HarnessReq.RSI_TRAINING_TERMINATE),
        ("rsi.report.get", _HarnessReq.RSI_REPORT_GET),
        ("rsi.usage.get", _HarnessReq.RSI_USAGE_GET),
        ("rsi.artifact.download", _HarnessReq.RSI_ARTIFACT_DOWNLOAD),
        ("rsi.artifact.files.list", _HarnessReq.RSI_ARTIFACT_FILES_LIST),
        ("rsi.artifact.files.get", _HarnessReq.RSI_ARTIFACT_FILES_GET),
        ("rsi.tree.get", _HarnessReq.RSI_TREE_GET),
        ("rsi.harness.install", _HarnessReq.RSI_HARNESS_INSTALL),
        ("rsi.harness.versions.list", _HarnessReq.RSI_HARNESS_VERSIONS_LIST),
        ("rsi.harness.rollback", _HarnessReq.RSI_HARNESS_ROLLBACK),
    ]
    for _method_name, _req_method in rsi_methods:
        _register_harness(_method_name, _req_method)

    async def _harness_import_handler(ws, req_id, params, session_id, user_id=None):
        """Import harness archives without exceeding the internal WS frame limit."""
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.agent_http_bridge import (
            E2A_PAYLOAD_MAX_BYTES,
            upload_file_bytes_via_e2a,
        )
        from agent_gateway.routing.e2a_proxy import proxy_unary_request

        request_params = params if isinstance(params, dict) else {}
        resolved_client = _resolve(agent_client)
        raw_content = request_params.get("file_content")
        # The browser link accepts substantially larger payloads than the
        # Gateway--AgentServer WebSocket.  Move larger archives in bounded E2A
        # chunks first; the final import request then contains only a path in
        # the target AgentServer's injected directory.
        if (
            isinstance(raw_content, str)
            and getattr(resolved_client, "server_ready", False)
        ):
            try:
                archive = base64.b64decode(raw_content, validate=True)
            except Exception:
                archive = None
            if archive is not None and len(archive) > E2A_PAYLOAD_MAX_BYTES:
                ok, upload_payload = await upload_file_bytes_via_e2a(
                    archive,
                    f"auto-harness/temp/uploads/harness_{uuid.uuid4().hex}.zip",
                    agent_client=resolved_client,
                    user_id=user_id,
                    channel_id=channel.channel_id,
                    session_id=session_id,
                )
                if not ok:
                    await channel.send_response(
                        ws, req_id, ok=False,
                        error=str(upload_payload.get("error") or "archive upload failed"),
                        code=str(upload_payload.get("code") or "SERVICE_UNAVAILABLE"),
                    )
                    return
                request_params = {
                    **request_params,
                    "uploaded_path": str(upload_payload.get("path") or ""),
                }
                request_params.pop("file_content", None)
        await proxy_unary_request(channel=channel, agent_client=_resolve(agent_client), ws=ws,
                                  req_id=req_id, params=request_params,
                                  session_id=session_id, user_id=user_id,
                                  req_method=ReqMethod.HARNESS_PACKAGES_IMPORT,
                                  label="harness.import", preserve_error_payload=True)

    channel.register_method("harness.import", _harness_import_handler)

    async def _harness_export_handler(ws, req_id, params, session_id, user_id=None):
        from openjiuwen_protocol.message import ReqMethod

        from agent_gateway.routing.e2a_proxy import proxy_unary_request
        await proxy_unary_request(channel=channel, agent_client=_resolve(agent_client), ws=ws,
                                  req_id=req_id, params=params if isinstance(params, dict) else {},
                                  session_id=session_id, user_id=user_id,
                                  req_method=ReqMethod.HARNESS_PACKAGES_EXPORT,
                                  label="harness.export", preserve_error_payload=True)

    channel.register_method("harness.export", _harness_export_handler)

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
