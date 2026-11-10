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


# --- 数据根分区（domain/infra 两区，与 jiuwenswarm.common.utils 同契约）---
# gateway 包自包含：分区根由本模块手工推导（同 get_user_workspace_dir 的
# env/home 语义），不 import jiuwenswarm。
_DOMAIN_DIR_NAME = "domain"
_INFRA_DIR_NAME = "infra"
_config_dir: Path | None = None
_workspace_dir: Path | None = None
_root_dir: Path | None = None
_initialized: bool = False


def get_domain_data_dir() -> Path:
    """获取业务区根：会话/工作区/租户/运行态快照等用户产生、需留存的数据
    一律经此派生；企业版挂共享卷（跨 Pod 存活），个人版为普通子目录。
    """
    return get_user_workspace_dir() / _DOMAIN_DIR_NAME


def get_infra_data_dir() -> Path:
    """获取系统区根：IPC socket/锁、业务 config 的程序副本、本机安装物、
    缓存、日志等程序自维护数据一律经此派生；企业版挂私有卷（随 Pod 生命
    周期），个人版为普通子目录。
    """
    return get_user_workspace_dir() / _INFRA_DIR_NAME


def get_bundled_resources_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "resources"


def _resolve_paths() -> None:
    """解析并缓存 config / workspace / root 目录。"""
    global _initialized, _config_dir, _workspace_dir, _root_dir

    if _initialized:
        return

    _root_dir = get_user_workspace_dir()
    _config_dir = get_infra_data_dir() / "config"
    _workspace_dir = get_domain_data_dir() / "agent" / "workspace"

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
    """获取 ``config.yaml`` 路径。"""
    return get_config_dir() / "config.yaml"


def get_env_file() -> Path:
    """获取 ``.env`` 路径。"""
    return get_config_dir() / ".env"


def env_url(name: str, default: str) -> str:
    """读取环境变量中的 URL，未设置或空白时回退默认值。"""
    return os.environ.get(name, "").strip() or default


# --- agent 工作区子目录 ---
def get_agent_root_dir() -> Path:
    return get_domain_data_dir() / "agent"


def get_agent_home_dir() -> Path:
    return get_agent_root_dir() / "home"


def get_agent_workspace_dir() -> Path:
    """DeepAgent 标准工作区：``~/.jiuwenswarm/domain/agent/workspace``。"""
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
    xy_tmp_dir = get_infra_data_dir() / "tmp" / "xiaoyi"
    xy_tmp_dir.mkdir(parents=True, exist_ok=True)
    return xy_tmp_dir


# --- Workspace layout migration ---
def _move_legacy_entry(source: Path, target: Path) -> None:
    """Move legacy data, preserving existing data when both layouts are present."""
    if source.is_symlink() or not source.exists():
        return
    if source.is_dir() and target.is_dir() and not target.is_symlink():
        for child in source.iterdir():
            _move_legacy_entry(child, target / child.name)
        if not any(source.iterdir()):
            source.rmdir()
        return
    if target.exists() or target.is_symlink():
        original = target
        index = 0
        while target.exists() or target.is_symlink():
            suffix = ".migrated" if index == 0 else f".migrated.{index}"
            target = original.with_name(original.name + suffix)
            index += 1
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(target))


def _move_legacy_sqlite(source: Path, target: Path) -> None:
    """Keep a legacy database and its sidecars separate from a newer database."""
    if source.is_symlink() or not source.is_file():
        return
    sidecars = ("-wal", "-shm", "-journal", "")
    original = target
    index = 0
    while any(os.path.lexists(target.with_name(target.name + suffix)) for suffix in sidecars):
        suffix = ".migrated" if index == 0 else f".migrated.{index}"
        target = original.with_name(original.name + suffix)
        index += 1
    for suffix in sidecars:
        _move_legacy_entry(
            source.with_name(source.name + suffix), target.with_name(target.name + suffix)
        )


def migrate_partitioned_data_root_layout() -> None:
    """Upgrade the data root before Gateway opens its configuration or logs."""
    root = get_user_workspace_dir()
    domain = get_domain_data_dir()
    infra = get_infra_data_dir()
    domain_entries = (
        "agent",
        "application_plugins",
        ".agent_teams",
        ".organizations",
        "jiuwenmate",
        ".trace",
        ".agent_history",
        "agents",
        ".personal_context",
        "gateway",
        "runtime_state",
    )
    infra_entries = (
        "config",
        "run",
        "logs",
        "runtime",
        "tokenizers",
        "auth",
        "rsi",
        "auto-harness",
        ".gateway.lock",
        "last_file_picker_dir.txt",
    )
    has_legacy_data = any(
        (root / name).exists() and not (root / name).is_symlink()
        for name in (*domain_entries, *infra_entries, ".checkpoint", ".logs")
    )
    lock_path = infra / ".workspace-layout.lock"
    if not lock_path.is_file() and not has_legacy_data:
        return

    import portalocker

    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with portalocker.Lock(lock_path, mode="a+b", timeout=60):
        move = _move_legacy_entry
        if not (root / "config").is_symlink():
            _move_legacy_entry(root / "config/runtime_state", domain / "runtime_state")
        if not (root / "agent").is_symlink():
            _move_legacy_entry(root / "agent/.logs", infra / "logs/agent")
            _move_legacy_sqlite(root / "agent/teams/team_control.db", infra / "agent/teams/team_control.db")
        if not (root / "gateway").is_symlink():
            _move_legacy_sqlite(root / "gateway/team_route_bindings.db", infra / "gateway/team_route_bindings.db")
        if not (root / "logs").is_symlink():
            _move_legacy_entry(root / "logs/logs", infra / "logs/core")
        _move_legacy_entry(root / ".logs", infra / "logs/agent")
        _move_legacy_entry(root / ".checkpoint", domain / "agent/.checkpoint")
        for name in domain_entries:
            move(root / name, domain / name)
        for name in infra_entries:
            move(root / name, infra / name)


def get_checkpoint_dir() -> Path:
    return get_agent_root_dir() / ".checkpoint"


def get_logs_dir() -> Path:
    """Return the Pod-local log root; component logs use subdirectories here."""
    return get_infra_data_dir() / "logs"


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

    try:
        import openjiuwen
    except ModuleNotFoundError as exc:
        if exc.name == "openjiuwen":
            return
        raise

    package_file = getattr(openjiuwen, "__file__", None)
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
    migrate_partitioned_data_root_layout()
    if cleanup_stale_descs:
        cleanup_stale_openjiuwen_descs()

    for path in (get_agent_workspace_dir(), get_agent_home_dir()):
        path.mkdir(parents=True, exist_ok=True)

    config_file = get_config_file()
    if not config_file.exists():
        config_file.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=config_file.parent) as temp_dir:
            staged = Path(temp_dir) / config_file.name
            shutil.copyfile(get_bundled_resources_dir() / config_file.name, staged)
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
    "get_domain_data_dir",
    "get_env_file",
    "get_gateway_log_dir",
    "get_infra_data_dir",
    "get_interactions_dir",
    "get_logs_dir",
    "get_root_dir",
    "get_user_home",
    "get_user_workspace_dir",
    "get_workspace_dir",
    "get_xy_tmp_dir",
    "logger",
    "prepare_runtime_workspace",
    "resolve_git_branch",
    "set_user_home",
]

# Startup imports paths before opening configuration or creating log directories.
migrate_partitioned_data_root_layout()
