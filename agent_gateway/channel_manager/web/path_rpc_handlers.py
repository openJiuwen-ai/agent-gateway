# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Path and file RPC handlers extracted from app_web_handlers."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from openjiuwen_protocol.message import ReqMethod

from agent_gateway.common.browser_config import (
    browser_decision_state,
    browser_decision_updates,
)
from agent_gateway.common.config import (
    get_config,
    resolve_env_vars,
    update_browser_decision_in_config,
    update_browser_in_config,
)
from agent_gateway.routing.e2a_proxy import (
    fetch_agent_unary,
    is_legacy_shared_directory_client,
    proxy_unary_request,
)
from agent_gateway.channel_manager.web.directory_picker import (
    select_directory_native,
    select_file_native,
)
from agent_gateway.channel_manager.web.file_picker import (
    select_and_describe_files,
)


logger = logging.getLogger(__name__)


def register_path_handlers(
    channel: Any,
    agent_client: Any,
    _resolve: Any,
    _clear_agent_config_cache: Any,
    _restart_agent_browser_runtime: Any,
) -> None:
    """Register path/file RPC handlers in their original route order."""

    async def _path_get(ws, req_id, params, session_id, user_id=None):
        """读 browser.chrome_path 并返回给前端（会解析环境变量）。"""
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

        config = resolve_env_vars(config_base)
        browser_cfg = config.get("browser", {}) if isinstance(config, dict) else {}
        chrome_path = ""
        headless = True
        if isinstance(browser_cfg, dict):
            value = browser_cfg.get("chrome_path", "")
            if isinstance(value, str):
                chrome_path = value
            raw_headless = browser_cfg.get("headless", True)
            headless = bool(raw_headless) if isinstance(raw_headless, bool) else True

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
        resolved_client = _resolve(agent_client)
        if is_legacy_shared_directory_client(resolved_client):
            # A local single-user deployment historically opened a native picker in
            # the Gateway process.  Keep that UI contract there; AgentOS cannot use
            # a deployment-side picker because it would expose the wrong machine and
            # user directory.
            if not isinstance(params, dict):
                params = {}
            initial_dir = params.get("initial_dir")
            if initial_dir is not None and not isinstance(initial_dir, str):
                await channel.send_response(
                    ws, req_id, ok=False, error="initial_dir must be string", code="BAD_REQUEST",
                )
                return
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

    channel.register_method("path.get", _path_get)
    channel.register_method("path.set", _path_set)
    channel.register_method("path.select_directory", _path_select_directory)
    channel.register_method("path.select_files", _path_select_files)
    channel.register_method("path.select_file", _path_select_file)
