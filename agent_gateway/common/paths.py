# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway 侧路径解析（``gateway.common.paths`` 的路径子集副本）。

Gateway 与母仓共用同一份用户工作区与 ``config`` 目录（配置文件共享），因此这里
的返回值必须与母仓实现保持一致；差异仅在于 Gateway 不再通过 import 母仓
``utils`` 触发其模块级 ``setup_logger()``。

缓存为模块级：若同进程内有人调用母仓的 ``set_user_home`` 改变主目录，本模块
缓存不会跟随。生产路径由 ``--dotenv/--name`` 早期设置环境变量
（``JIUWENSWARM_HOME`` / ``JIUWENSWARM_DATA_DIR``）驱动，双方首次解析结果一致。
"""

from __future__ import annotations

import logging
import os
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# gateway.log 独立目录开关（与母仓同名的环境变量）
_GATEWAY_LOG_DIR_ENV = "AGENTOS_GATEWAY_LOG_DIR"


# --- 用户主目录 / 工作区 ---
_user_home: Path | None = None
_workspace_base_dir: Path | None = None


def get_user_home() -> Path:
    """获取用户主目录：缓存 > ``JIUWENSWARM_HOME`` > ``Path.home()``。"""
    global _user_home
    if _user_home is not None:
        return _user_home
    env_home = os.getenv("JIUWENSWARM_HOME")
    if env_home:
        _user_home = Path(env_home)
        return _user_home
    _user_home = Path.home()
    return _user_home


def set_user_home(path: Path, initialized: bool = False) -> None:
    """设置自定义用户主目录；``initialized=False`` 时同时清空路径缓存。"""
    global _user_home, _initialized, _config_dir, _workspace_dir, _root_dir
    _user_home = Path(path)
    if initialized:
        return
    _initialized = False
    _config_dir = None
    _workspace_dir = None
    _root_dir = None


def get_user_workspace_dir() -> Path:
    """获取用户工作区：缓存 > ``JIUWENSWARM_DATA_DIR`` > ``~/.jiuwenswarm``。"""
    global _workspace_base_dir
    if _workspace_base_dir is not None:
        return _workspace_base_dir
    env_workspace = os.getenv("JIUWENSWARM_DATA_DIR")
    if env_workspace:
        _workspace_base_dir = Path(env_workspace)
        return _workspace_base_dir
    _workspace_base_dir = get_user_home() / ".jiuwenswarm"
    return _workspace_base_dir


# --- Business profile under the legacy flat data root ---
# ``profile/`` contains user-owned durable data. System-managed paths remain
# at their established root locations (config/, logs/, runtime/, and so on).
# Gateway computes the profile path locally and does not import JiuwenSwarm.
_PROFILE_DIR_NAME = "profile"
_config_dir: Path | None = None
_workspace_dir: Path | None = None
_root_dir: Path | None = None
_initialized: bool = False


def get_profile_data_dir() -> Path:
    """Return the durable user-data directory shared by enterprise Pods."""
    return get_user_workspace_dir() / _PROFILE_DIR_NAME


def get_application_plugins_dir() -> Path:
    """获取运维部署的应用插件目录：部署期资产、运行期只读；企业版随
    profile 共享卷跨 Pod 存活，各副本共享同一目录。
    """
    return get_profile_data_dir() / "application_plugins"


def get_bundled_resources_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "resources"


def _resolve_paths() -> None:
    """解析并缓存 config / workspace / root 目录。"""
    global _initialized, _config_dir, _workspace_dir, _root_dir

    if _initialized:
        return

    _root_dir = get_user_workspace_dir()
    _config_dir = get_user_workspace_dir() / "config"
    _workspace_dir = get_agent_workspace_dir()

    _initialized = True


def get_config_dir() -> Path:
    """获取配置目录（与母仓共享同一份 config）。"""
    _resolve_paths()
    return _config_dir


def get_workspace_dir() -> Path:
    """获取 agent workspace 目录。"""
    _resolve_paths()
    return _workspace_dir


def get_root_dir() -> Path:
    """获取根目录（已初始化时为用户工作区）。"""
    _resolve_paths()
    return _root_dir


def get_config_file() -> Path:
    """获取 Gateway 配置文件路径。

    默认 ``<config_dir>/config-gateway.yaml``（gateway 专属配置，与 AgentServer
    ``config/config.yaml`` 分离）；拆分部署时可用
    ``JIUWENSWARM_GATEWAY_CONFIG_DIR`` 指定目录，文件名固定为 ``config-gateway.yaml``。

    该环境变量必须在 Gateway 进程启动、config 模块导入前设置，运行中修改不受支持。
    """
    override = (os.getenv("JIUWENSWARM_GATEWAY_CONFIG_DIR") or "").strip()
    if override:
        path = Path(override).expanduser()
        if not path.is_absolute():
            path = get_config_dir() / path
        return path / "config-gateway.yaml"
    return get_config_dir() / "config-gateway.yaml"


def get_env_file() -> Path:
    """获取 ``.env`` 路径。

    优先级：

    1. AgentOS：``<workspace>/config/.env``，与 AgentServer 共用一份；
    2. 个人版：``<config_dir>/.env``，保持原有行为，不跟随
       ``JIUWENSWARM_GATEWAY_CONFIG_DIR``。

    AgentOS 的 Gateway/AS 是独立进程，但仍然共用 AS 配置目录下的 env，
    避免运行期配置面板写入后两边 env 漂移。
    """
    from agent_gateway.common.agentos_runtime import is_agentos_runtime

    if is_agentos_runtime():
        return get_user_workspace_dir() / "config" / ".env"
    return get_config_dir() / ".env"


def env_url(name: str, default: str) -> str:
    """读取环境变量中的 URL，未设置或空白时回退默认值。"""
    return os.environ.get(name, "").strip() or default


# --- agent 工作区子目录 ---
def get_agent_root_dir() -> Path:
    return get_profile_data_dir() / "agent"


def get_agent_home_dir() -> Path:
    return get_agent_root_dir() / "home"


def get_agent_workspace_dir() -> Path:
    """DeepAgent 标准工作区：``~/.jiuwenswarm/profile/agent/workspace``。"""
    return get_agent_root_dir() / "workspace"


def get_agent_memory_dir() -> Path:
    return get_agent_workspace_dir() / "memory"


def get_agent_skills_dir() -> Path:
    return get_agent_workspace_dir() / "skills"


def get_agent_sessions_dir() -> Path:
    return get_agent_root_dir() / "sessions"


def get_deepagent_user_md_path() -> Path:
    return get_agent_workspace_dir() / "USER.md"


def get_interactions_dir() -> Path:
    return get_agent_workspace_dir() / "interactions"


def get_cron_jobs_path() -> Path:
    """``agent/home/cron_jobs.json`` 的规范路径（禁止业务代码硬编码）。"""
    return get_agent_home_dir() / "cron_jobs.json"


def get_xy_tmp_dir() -> Path:
    xy_tmp_dir = get_user_workspace_dir() / "tmp" / "xiaoyi"
    xy_tmp_dir.mkdir(parents=True, exist_ok=True)
    return xy_tmp_dir


# --- checkpoint / 日志目录 ---
_legacy_migration_done: bool = False


def _is_directory_link(path: Path) -> bool:
    try:
        if path.is_symlink():
            return True
        # Junctions are not symbolic links on Windows. Python 3.11 has no
        # Path.is_junction(), so inspect the reparse tag without following it.
        if sys.platform == "win32":
            return getattr(path.lstat(), "st_reparse_tag", None) == getattr(
                stat, "IO_REPARSE_TAG_MOUNT_POINT", 0xA0000003
            )
    except OSError:
        return False
    return False


def _move_if_absent(old: Path, new: Path) -> None:
    """迁移旧路径且不覆盖目标数据；两侧目录并存时递归合并（幂等）。

    并发进程可能已抢先搬迁同一源：源消失视为对方获胜，静默跳过。"""
    if not old.exists() or _is_directory_link(old):
        return
    if _is_directory_link(new):
        return
    if new.exists() and old.is_dir() and new.is_dir():
        for child in list(old.iterdir()):
            _move_if_absent(child, new / child.name)
        try:
            old.rmdir()
        except OSError:
            # A conflicting file remains at the legacy path; never overwrite it.
            return
        return
    if new.exists():
        return
    new.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.move(str(old), str(new))
    except FileNotFoundError:
        pass


def _link_legacy_path(legacy: Path, current: Path) -> bool:
    if legacy.exists() or _is_directory_link(legacy) or not current.exists():
        return True
    try:
        if sys.platform == "win32" and current.is_dir():
            # Native directory junctions do not require symbolic-link privileges.
            # Avoid invoking cmd.exe with user-controlled filesystem paths.
            import _winapi

            _winapi.CreateJunction(str(current.absolute()), str(legacy.absolute()))
        else:
            legacy.symlink_to(current.absolute(), target_is_directory=current.is_dir())
    except FileExistsError:
        pass
    except OSError as exc:
        logger.warning(
            "could not create legacy path link %s -> %s: %s", legacy, current, exc
        )
        return False
    return True


def _legacy_business_paths(root: Path, profile: Path) -> dict[Path, Path]:
    legacy_business_paths = {
        root / "agent": profile / "agent",
        root / ".checkpoint": profile / "agent" / ".checkpoint",
        root / "gateway": profile / "gateway",
        root / "application_plugins": profile / "application_plugins",
        root / ".agent_teams": profile / ".agent_teams",
        root / ".organizations": profile / ".organizations",
        root / "jiuwenmate": profile / "jiuwenmate",
        root / ".trace": profile / ".trace",
        root / ".agent_history": profile / ".agent_history",
        root / "agents": profile / "agents",
        root / ".personal_context": profile / ".personal_context",
        root / "runtime_state": profile / "runtime_state",
    }
    legacy_business_paths.update(
        (path, profile / path.name)
        for path in root.glob("workspace_*")
        if path not in legacy_business_paths
    )
    return legacy_business_paths


def repair_windows_legacy_links() -> None:
    """Repair missing Windows compatibility links after loading runtime dotenv."""
    if sys.platform != "win32":
        return
    from agent_gateway.enterprise.edition import is_enterprise

    profile = get_profile_data_dir()
    if is_enterprise() or _is_directory_link(profile):
        return
    for legacy, current in _legacy_business_paths(get_user_workspace_dir(), profile).items():
        _link_legacy_path(legacy, current)


def _migrate_legacy_checkpoint_and_logs() -> None:
    """把旧平铺布局中的业务数据迁入 profile，并保留旧路径入口。

    系统目录（config、logs、auth、runtime 等）继续位于数据根，不做迁移。
    未发布的 domain/infra 中间布局不属于升级路径。
    """
    global _legacy_migration_done
    if _legacy_migration_done:
        return
    links_ok = True

    root = get_user_workspace_dir()
    profile = get_profile_data_dir()
    legacy_business_paths = _legacy_business_paths(root, profile)

    # 两种早期平铺日志位置都迁入仍处于根目录的 logs/agent。
    current_logs = get_logs_dir() / "agent"
    legacy_logs = root / ".logs"
    legacy_agent_logs = root / "agent" / ".logs"
    had_legacy_logs = legacy_logs.exists() and not _is_directory_link(legacy_logs)
    had_agent_logs = legacy_agent_logs.exists() and not _is_directory_link(legacy_agent_logs)
    _move_if_absent(legacy_logs, current_logs)
    _move_if_absent(legacy_agent_logs, current_logs)
    for legacy, current in legacy_business_paths.items():
        if legacy == root / "agent" and not _is_directory_link(legacy) and (
            legacy_agent_logs.exists() or _is_directory_link(legacy_agent_logs)
        ):
            # A conflicting log must stay Pod-local, not be moved into profile.
            for child in list(legacy.iterdir()):
                if child.name != ".logs":
                    _move_if_absent(child, current / child.name)
            continue
        had_legacy = legacy.exists() and not _is_directory_link(legacy)
        _move_if_absent(legacy, current)
        if had_legacy:
            links_ok = _link_legacy_path(legacy, current) and links_ok

    # A retained legacy agent directory (e.g. conflicting logs) must still
    # expose checkpoint data that was successfully moved into profile.
    legacy_agent = root / "agent"
    if legacy_agent.is_dir() and not _is_directory_link(legacy_agent):
        links_ok = _link_legacy_path(
            legacy_agent / ".checkpoint", profile / "agent" / ".checkpoint"
        ) and links_ok

    if had_legacy_logs:
        links_ok = _link_legacy_path(legacy_logs, current_logs) and links_ok
    if had_agent_logs and _is_directory_link(root / "agent"):
        links_ok = _link_legacy_path(profile / "agent" / ".logs", current_logs) and links_ok
    _legacy_migration_done = links_ok


def get_checkpoint_dir() -> Path:
    return get_agent_root_dir() / ".checkpoint"


def get_logs_dir() -> Path:
    """Return the Pod-local log root; component logs use subdirectories here."""
    return get_user_workspace_dir() / "logs"


def get_gateway_log_dir() -> Optional[Path]:
    """获取 gateway 独立日志目录；未配置 ``AGENTOS_GATEWAY_LOG_DIR`` 时返回 None。"""
    env_dir = os.getenv(_GATEWAY_LOG_DIR_ENV, "").strip()
    if env_dir:
        return Path(env_dir)
    return None


# --- git 分支 ---
_GIT_BRANCH_CACHE: dict[str, tuple[float, str]] = {}
_GIT_BRANCH_TTL_SECONDS = 5.0


def resolve_git_branch(project_dir: str | None) -> str:
    """返回 ``project_dir`` 当前 git 分支，取不到时返回哨兵 ``"HEAD"``。"""
    if not project_dir or not os.path.isdir(project_dir):
        return "HEAD"
    now = time.time()
    cached = _GIT_BRANCH_CACHE.get(project_dir)
    if cached and now - cached[0] < _GIT_BRANCH_TTL_SECONDS:
        return cached[1]
    branch = "HEAD"
    git_bin = shutil.which("git")
    if git_bin:
        try:
            result = subprocess.run(
                [git_bin, "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=5,
                cwd=project_dir,
            )
            if result.returncode == 0:
                branch = result.stdout.strip() or "HEAD"
        except Exception:
            branch = "HEAD"
    _GIT_BRANCH_CACHE[project_dir] = (now, branch)
    return branch


# --- 运行时环境与工作区准备 ---
def apply_free_search_runtime_defaults() -> None:
    """未显式配置时关闭 free-search 引擎（.env / 配置面板 / shell 优先）。"""
    os.environ.setdefault("FREE_SEARCH_DDG_ENABLED", "false")
    os.environ.setdefault("FREE_SEARCH_BING_ENABLED", "false")


def _is_windows_frozen_bundle() -> bool:
    return sys.platform == "win32" and bool(getattr(sys, "frozen", False))


def cleanup_stale_openjiuwen_descs() -> None:
    """删除布局迁移遗留的扁平 OpenJiuwen 工具描述文件（幂等）。"""
    if _is_windows_frozen_bundle():
        logger.info(
            "[Cleanup] Skipping OpenJiuwen description cleanup in frozen Windows "
            "bundle; the installer performs upgrade cleanup."
        )
        return

    loaded = sys.modules.get("openjiuwen")
    package_file = getattr(loaded, "__file__", None)
    if package_file is None:
        # Top-level package discovery does not execute the SDK's __init__.
        # Cleanup must still finish before any SDK description index is built.
        from importlib.util import find_spec

        try:
            spec = find_spec("openjiuwen")
        except ModuleNotFoundError as exc:
            if exc.name == "openjiuwen":
                return
            raise
        package_file = spec.origin if spec is not None else None
    if not package_file:
        return

    descs_dir = (
        Path(package_file).parent / "agent_teams" / "tools" / "locales" / "descs"
    )
    if not descs_dir.is_dir():
        return

    for lang_dir in sorted(path for path in descs_dir.iterdir() if path.is_dir()):
        nested_stems = set()
        for desc_path in lang_dir.rglob("*.md"):
            if desc_path.parent == lang_dir:
                continue
            if "fragments" in desc_path.relative_to(lang_dir).parts:
                continue
            nested_stems.add(desc_path.stem)

        for flat_md in sorted(lang_dir.glob("*.md")):
            if flat_md.stem not in nested_stems:
                continue
            try:
                flat_md.unlink()
                logger.info(
                    f"[Cleanup] Removed stale flat OpenJiuwen description: {flat_md}"
                )
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise RuntimeError(
                    "Failed to remove stale OpenJiuwen description "
                    f"'{flat_md}'. Ensure the Python environment is writable "
                    "or reinstall OpenJiuwen in a clean environment."
                ) from exc


def prepare_runtime_workspace(*, cleanup_stale_descs: bool = True) -> None:
    """Gateway 独立启动时的最小工作区准备。

    完整初始化（配置模板迁移、内置 skills、MCP 种子）由母仓 supervisor /
    ``jiuwenswarm-init`` 完成；Gateway 作为子进程启动时带
    ``JIUWENSWARM_RUNTIME_WORKSPACE_READY=1``，不会走到这里。本函数创建必需目录，
    并在配置缺失时安装 Gateway 的默认模板。
    """
    if cleanup_stale_descs:
        cleanup_stale_openjiuwen_descs()

    for path in (get_agent_workspace_dir(), get_agent_home_dir()):
        path.mkdir(parents=True, exist_ok=True)

    config_file = get_config_file()
    from agent_gateway.common.agentos_runtime import is_agentos_runtime

    if is_agentos_runtime():
        from agent_gateway.common.agentos_migration import prepare_agentos_config

        prepare_agentos_config(
            config_file=config_file,
            template=get_bundled_resources_dir() / "config.agentos.yaml",
            legacy_config=get_user_workspace_dir() / "config" / "config.yaml",
        )
        return

    if not config_file.exists():
        config_file.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=config_file.parent) as temp_dir:
            staged = Path(temp_dir) / config_file.name
            # 模板资源名固定为 config.yaml（未随落盘名 config-gateway.yaml
            # 改名）；staged 用落盘名，硬链接落位后即 config-gateway.yaml
            shutil.copyfile(get_bundled_resources_dir() / "config.yaml", staged)
            try:
                os.link(staged, config_file)
            except FileExistsError:
                pass


__all__ = [
    "apply_free_search_runtime_defaults",
    "cleanup_stale_openjiuwen_descs",
    "env_url",
    "get_agent_home_dir",
    "get_agent_memory_dir",
    "get_agent_root_dir",
    "get_agent_sessions_dir",
    "get_agent_skills_dir",
    "get_agent_workspace_dir",
    "get_bundled_resources_dir",
    "get_checkpoint_dir",
    "get_config_dir",
    "get_config_file",
    "get_cron_jobs_path",
    "get_deepagent_user_md_path",
    "get_user_workspace_dir",
    "get_env_file",
    "get_gateway_log_dir",
    "get_interactions_dir",
    "get_logs_dir",
    "get_profile_data_dir",
    "get_root_dir",
    "get_user_home",
    "get_user_workspace_dir",
    "get_workspace_dir",
    "get_xy_tmp_dir",
    "logger",
    "prepare_runtime_workspace",
    "repair_windows_legacy_links",
    "resolve_git_branch",
    "set_user_home",
]

# 旧平铺 checkpoint/logs 迁移在导入期执行一次：先于本包任何目录创建
# 行为（_resolve_paths 的 mkdir、prepare_runtime_workspace），保证迁移
# 目标位不被抢占。失败仅降级告警：迁移幂等，下次启动重试。
try:
    _migrate_legacy_checkpoint_and_logs()
except Exception:  # noqa: BLE001
    logger.warning(
        "legacy checkpoint/logs migration failed; retried on next start",
        exc_info=True,
    )
