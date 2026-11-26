"""AgentOS 一体机旧共享配置的启动期迁移。

旧版一体机只有 ``<workspace>/config/config.yaml``，Gateway 和 AS 共用。
AGW 的默认 Gateway 配置目录是 ``<workspace>/config``；一体机部署脚本通过
``JIUWENSWARM_GATEWAY_CONFIG_DIR`` 把 Gateway 配置指向
``<workspace>/infra/config/config.yaml``，与 AS 共享配置分开。如果升级只做了
``pip install`` + ``systemctl restart``，部署脚本不会执行，Gateway 可能读到
旧共享配置（``agent_client.type`` 不是 ``agentos_router``）或找不到新路径。

本模块在 Gateway 启动早期、配置加载之前，把旧共享配置投影到
``resources/config.agentos.yaml`` 模板，并写入部署脚本指定的 Gateway 路径。
这样“部署参数不变”的升级可以零额外步骤完成；部署参数变化时仍应由部署脚本重新渲染。

如果目标路径与旧共享配置解析到同一个文件（没有设置 override），本模块不会
原地重写 AS 共享配置，调用方会 fail fast 要求部署脚本/override 指定独立目录。
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

logger = logging.getLogger(__name__)

_PLACEHOLDER_RE = re.compile(r"<<[A-Z0-9_]+>>")
_MISSING = object()


def _yaml() -> YAML:
    parser = YAML(typ="safe")
    parser.default_flow_style = False
    parser.sort_base_mapping_type_on_output = False
    return parser


def _load(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as stream:
        data = _yaml().load(stream)
    if not isinstance(data, dict):
        raise TypeError(f"config root must be an object: {path}")
    return data


def _dump_atomic(path: Path, data: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    with temp.open("w", encoding="utf-8") as stream:
        _yaml().dump(dict(data), stream)
    os.replace(temp, path)


def _project(template: Any, legacy: Any) -> Any:
    if isinstance(template, dict):
        source = legacy if isinstance(legacy, dict) else {}
        return {
            key: _project(default, source.get(key, _MISSING))
            for key, default in template.items()
        }
    if isinstance(template, list):
        return deepcopy(legacy if isinstance(legacy, list) else template)
    if legacy is _MISSING or legacy is None:
        return deepcopy(template)
    return deepcopy(legacy)


def _contains_placeholder(value: Any) -> bool:
    if isinstance(value, str):
        return bool(_PLACEHOLDER_RE.search(value))
    if isinstance(value, dict):
        return any(_contains_placeholder(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_placeholder(item) for item in value)
    return False


def _is_agentos_router_config(path: Path) -> bool:
    try:
        data = _load(path)
    except (OSError, ValueError, TypeError, YAMLError):
        return False
    gateway = data.get("gateway")
    if not isinstance(gateway, Mapping):
        return False
    client = gateway.get("agent_client")
    return isinstance(client, Mapping) and client.get("type") == "agentos_router"


def _backup(path: Path) -> Path | None:
    if not path.is_file():
        return None
    stamp = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    backup = path.with_name(f"{path.name}.bak.{stamp}")
    backup.write_bytes(path.read_bytes())
    return backup


def migrate_legacy_agentos_config(
    *,
    target: Path,
    legacy: Path,
    template: Path,
    force: bool = False,
) -> bool:
    """Migrate legacy shared config to the Gateway config path.

    Returns ``True`` when a file was written. Raises ``ValueError`` when the
    legacy config cannot fill all template placeholders; the caller should then
    fail fast instead of silently falling back to the stock websocket template.
    """
    if target.is_file() and not force:
        return False
    if not legacy.is_file() or not template.is_file():
        return False

    try:
        legacy_data = _load(legacy)
        template_data = _load(template)
    except (OSError, ValueError, TypeError, YAMLError) as exc:
        raise ValueError(
            "AgentOS Gateway migration input is unreadable; run the deploy script "
            "to re-render config.agentos.yaml "
            f"(legacy={legacy}, template={template}): {exc}"
        ) from exc
    migrated = _project(template_data, legacy_data)
    # 迁移的目标就是 AgentOS；即使旧配置是 stock websocket，也不能把 type 带回来。
    gateway = migrated.setdefault("gateway", {})
    if isinstance(gateway, dict):
        client = gateway.setdefault("agent_client", {})
        if isinstance(client, dict):
            client["type"] = "agentos_router"
    if _contains_placeholder(migrated):
        raise ValueError(
            "legacy AgentOS config cannot resolve all Gateway template placeholders; "
            "run the deploy script to render a fresh config"
        )

    _backup(target)
    _dump_atomic(target, migrated)
    return True


def prepare_agentos_config(
    *,
    config_file: Path,
    template: Path,
    legacy_config: Path,
) -> bool:
    """Ensure an AgentOS Gateway config exists; migrate legacy or fail fast."""
    if config_file.is_file() and _is_agentos_router_config(config_file):
        return False

    if config_file.resolve() == legacy_config.resolve():
        if not config_file.is_file():
            raise RuntimeError(
                "AgentOS Gateway config is missing and the legacy shared config "
                f"path resolves to the same file ({config_file}). Set "
                "AGENT_GATEWAY_CONFIG_DIR (or the legacy JIUWENSWARM_GATEWAY_CONFIG_DIR) "
                "to a dedicated Gateway config directory (for the appliance deploy: "
                "<workspace>/infra/config) and re-run the deploy script before "
                "starting Gateway."
            )
        raise RuntimeError(
            "AgentOS Gateway config path resolves to the legacy shared config "
            f"({config_file}); refusing to rewrite the AgentServer config with "
            "the Gateway template. Set AGENT_GATEWAY_CONFIG_DIR (or the legacy "
            "JIUWENSWARM_GATEWAY_CONFIG_DIR) to a dedicated Gateway config "
            "directory and re-run the deploy script."
        )

    migrated = migrate_legacy_agentos_config(
        target=config_file,
        legacy=legacy_config,
        template=template,
        force=config_file.is_file(),
    )
    if migrated:
        logger.warning(
            "[AgentOS] migrated legacy Gateway config %s -> %s; "
            "run the deploy script if VIP/etcd/function ID changed",
            legacy_config,
            config_file,
        )
        return True

    if not config_file.is_file():
        raise RuntimeError(
            "AgentOS Gateway config is missing and no legacy config was found. "
            "Render config.agentos.yaml via the deploy script before starting Gateway."
        )
    raise RuntimeError(
        "AgentOS Gateway config exists but is not agentos_router, and no legacy "
        "config is available for migration. Run the deploy script."
    )
