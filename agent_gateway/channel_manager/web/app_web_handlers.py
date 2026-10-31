# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Legacy import facade for the WebChannel registration implementation."""

from agent_gateway.channel_manager.web.web_handler_registration import (
    WebHandlersBindParams,
    _clear_agent_config_cache,
    _register_web_handlers,
    _restart_agent_browser_runtime,
    _supports_container_file_api,
)
from agent_gateway.channel_manager.web.web_handler_constants import (
    _DummyBus,
    _FORWARD_NO_LOCAL_HANDLER_METHODS,
    _FORWARD_REQ_METHODS,
    spawn_background_task,
)
from agent_gateway.channel_manager.web.channel_config_rpc_handlers import (
    _merge_apps_by_id,
    _normalize_feishu_conf,
    _normalize_xiaoyi_conf,
    _validate_wechat_numeric_params,
    register_channel_config_handlers,
)
from agent_gateway.channel_manager.web.updater_rpc_handlers import (
    register_updater_handlers,
)
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
from agent_gateway.channel_manager.web.locale_rpc_handlers import (
    register_locale_handlers,
)
from agent_gateway.channel_manager.web.heartbeat_rpc_handlers import (
    register_heartbeat_handlers,
)
from agent_gateway.channel_manager.web.path_rpc_handlers import (
    register_path_handlers,
)
from agent_gateway.channel_manager.web.workspace_rpc_handlers import (
    register_workspace_handlers,
)
from agent_gateway.channel_manager.web.cron_rpc_handlers import (
    register_cron_handlers,
)
from agent_gateway.channel_manager.web.memory_rpc_handlers import (
    register_memory_handlers,
)
from agent_gateway.channel_manager.web.vendor_rpc_handlers import (
    register_vendor_handlers,
)
from agent_gateway.channel_manager.web.media_document_rpc_handlers import (
    register_media_document_handlers,
)
from agent_gateway.channel_manager.web.conversation_rpc_handlers import (
    register_conversation_handlers,
)
from agent_gateway.channel_manager.web.channel_task_rpc_handlers import (
    register_channel_task_handlers,
)
from agent_gateway.channel_manager.web.config_rpc_handlers import (
    register_config_proxy_handlers,
)
from agent_gateway.channel_manager.web.connection_rpc_handlers import (
    register_connection_handlers,
)
from agent_gateway.channel_manager.web.chat_rpc_handlers import (
    register_chat_send_handler,
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
from agent_gateway.common.config_panel.config_set_handlers import (
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
from agent_gateway.dotenv_early import load_dotenv_runtime

__all__ = [
    "WebHandlersBindParams",
    "UpdaterService",
    "register_channel_config_handlers",
    "register_updater_handlers",
    "register_locale_handlers",
    "register_heartbeat_handlers",
    "register_path_handlers",
    "register_workspace_handlers",
    "register_cron_handlers",
    "register_memory_handlers",
    "register_vendor_handlers",
    "register_media_document_handlers",
    "register_conversation_handlers",
    "register_channel_task_handlers",
    "register_config_proxy_handlers",
    "register_connection_handlers",
    "register_chat_send_handler",
    "register_a4p_handlers",
    "register_harness_handlers",
    "register_permissions_handlers",
]
