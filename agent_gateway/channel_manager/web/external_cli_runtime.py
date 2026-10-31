# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from __future__ import annotations

import importlib.metadata
import importlib.util
import logging
import os
import queue
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from openjiuwen.core.common.logging import LogManager

from agent_gateway.common.config import get_config_raw
from agent_gateway.common.config_panel.config_set_handlers import (
    build_external_cli_publish_url as _build_external_cli_publish_url,
    detect_external_cli_agent as _detect_external_cli_agent,
    external_cli_managed_cli_path as _external_cli_managed_cli_path,
)


logger = logging.getLogger(__name__)

_EXTERNAL_CLI_DEPENDENCY_INSTALL_IDLE_STATUS: dict[str, Any] = {
    "status": "idle",
    "phase": "idle",
    "progress_kind": "",
    "error": "",
    "last_log": "",
    "log_tail": [],
    "started_at": 0.0,
    "finished_at": 0.0,
    "updated_at": 0.0,
    "downloaded_bytes": 0,
    "total_bytes": 0,
    "bytes_per_second": 0.0,
    "eta_seconds": 0.0,
    "artifact_index": 0,
    "artifact_count": 0,
    "current_package": "",
    "current_version": "",
    "download_attempt": 0,
    "download_max_attempts": 0,
    "switching_source": False,
}
_CODEX_DEPENDENCY_INSTALL_LOCK = threading.Lock()
_CODEX_DEPENDENCY_INSTALL_STATUS: dict[str, Any] = dict(_EXTERNAL_CLI_DEPENDENCY_INSTALL_IDLE_STATUS)
_CODEX_DEPENDENCY_INSTALL_LOG_TAIL_LIMIT = 8
_OPTIONAL_DEPENDENCY_INSTALL_TIMEOUT_SECONDS = 60 * 60
# Serializes the non-frozen pip installs: parallel pip subprocesses writing the
# same virtualenv are not safe. Frozen managed-runtime installs are per-agent
# (independent target directories and file locks) and stay concurrent.
_OPTIONAL_DEPENDENCY_PIP_INSTALL_LOCK = threading.Lock()
_CLAUDE_DEPENDENCY_INSTALL_LOCK = threading.Lock()
_CLAUDE_DEPENDENCY_INSTALL_STATUS: dict[str, Any] = dict(_EXTERNAL_CLI_DEPENDENCY_INSTALL_IDLE_STATUS)
# Claude and Codex are always present; other managed runtimes (third-party
# harness providers) get their lock and status the first time they are used.
_EXTERNAL_CLI_DEPENDENCY_INSTALL_LOCKS: dict[str, threading.Lock] = {
    "claude": _CLAUDE_DEPENDENCY_INSTALL_LOCK,
    "codex": _CODEX_DEPENDENCY_INSTALL_LOCK,
}
_EXTERNAL_CLI_DEPENDENCY_INSTALL_STATUSES: dict[str, dict[str, Any]] = {
    "claude": _CLAUDE_DEPENDENCY_INSTALL_STATUS,
    "codex": _CODEX_DEPENDENCY_INSTALL_STATUS,
}
_EXTERNAL_CLI_DEPENDENCY_INSTALL_REGISTRY_LOCK = threading.Lock()


def _external_cli_dependency_install_state(cli_agent: str) -> tuple[threading.Lock, dict[str, Any]]:
    """Return the install lock and status of ``cli_agent``, creating idle ones on first use."""
    with _EXTERNAL_CLI_DEPENDENCY_INSTALL_REGISTRY_LOCK:
        lock = _EXTERNAL_CLI_DEPENDENCY_INSTALL_LOCKS.get(cli_agent)
        status = _EXTERNAL_CLI_DEPENDENCY_INSTALL_STATUSES.get(cli_agent)
        if lock is None or status is None:
            lock = threading.Lock()
            status = dict(_EXTERNAL_CLI_DEPENDENCY_INSTALL_IDLE_STATUS)
            _EXTERNAL_CLI_DEPENDENCY_INSTALL_LOCKS[cli_agent] = lock
            _EXTERNAL_CLI_DEPENDENCY_INSTALL_STATUSES[cli_agent] = status
        return lock, status


def _snapshot_external_cli_dependency_install_status(cli_agent: str) -> dict[str, Any]:
    lock, status = _external_cli_dependency_install_state(cli_agent)
    with lock:
        result = dict(status)
        result["cli_agent"] = cli_agent
        result["log_tail"] = list(result.get("log_tail") or [])
        return result


def _update_external_cli_dependency_install_status(cli_agent: str, updates: dict[str, Any]) -> None:
    lock, status = _external_cli_dependency_install_state(cli_agent)
    with lock:
        status.update(updates)
        status["updated_at"] = time.time()


def _external_cli_dependency_install_succeeded_updates() -> dict[str, Any]:
    """Build a terminal success state without stale download progress."""
    return {
        "status": "succeeded",
        "phase": "succeeded",
        "error": "",
        "finished_at": time.time(),
        "downloaded_bytes": 0,
        "total_bytes": 0,
        "bytes_per_second": 0.0,
        "eta_seconds": 0.0,
        "artifact_index": 0,
        "artifact_count": 0,
        "current_package": "",
        "current_version": "",
        "download_attempt": 0,
        "download_max_attempts": 0,
        "switching_source": False,
    }


def _append_external_cli_dependency_install_log(cli_agent: str, line: str) -> None:
    stripped = line.strip()
    if not stripped:
        return
    lock, status = _external_cli_dependency_install_state(cli_agent)
    with lock:
        log_tail = list(status.get("log_tail") or [])
        log_tail.append(stripped)
        status.update({
            "last_log": stripped,
            "log_tail": log_tail[-_CODEX_DEPENDENCY_INSTALL_LOG_TAIL_LIMIT:],
            "updated_at": time.time(),
        })


def _snapshot_claude_dependency_install_status() -> dict[str, Any]:
    return _snapshot_external_cli_dependency_install_status("claude")


def _activate_managed_external_cli_paths_if_needed() -> None:
    """Expose managed SDKs only for an external-CLI configuration request."""
    if not _is_frozen_runtime():
        return
    from agent_gateway.common.external_cli_runtime import (
        activate_external_cli_runtime_paths,
    )

    activate_external_cli_runtime_paths()


def _ensure_claude_dependency_available_or_start_install() -> dict[str, Any] | None:
    _activate_managed_external_cli_paths_if_needed()
    package_available = importlib.util.find_spec("claude_agent_sdk") is not None
    cli_available = bool(_external_cli_managed_cli_path("claude"))
    if package_available and cli_available:
        with _CLAUDE_DEPENDENCY_INSTALL_LOCK:
            _CLAUDE_DEPENDENCY_INSTALL_STATUS.update(_external_cli_dependency_install_succeeded_updates())
            _CLAUDE_DEPENDENCY_INSTALL_STATUS["updated_at"] = time.time()
        return None
    # The SDK package can be importable while its bundled CLI executable is
    # gone (e.g. the user deleted the file): a plain install would end as a pip
    # no-op ("already satisfied"), so repair by force-reinstalling the pinned
    # SDK instead of reporting success — otherwise 一键安装 could never restore
    # a deleted built-in CLI.
    force_reinstall = package_available and not cli_available
    if _is_frozen_runtime():
        return _ensure_managed_external_cli_runtime_or_start_install("claude")
    with _CLAUDE_DEPENDENCY_INSTALL_LOCK:
        if _CLAUDE_DEPENDENCY_INSTALL_STATUS.get("status") == "running":
            return _snapshot_external_cli_dependency_install_status_unlocked("claude")
        _CLAUDE_DEPENDENCY_INSTALL_STATUS.update(
            {
                "status": "running",
                "phase": "installing",
                "progress_kind": "installer_activity",
                "error": "",
                "last_log": "",
                "log_tail": [],
                "started_at": time.time(),
                "finished_at": 0.0,
                "updated_at": time.time(),
                "downloaded_bytes": 0,
                "total_bytes": 0,
                "bytes_per_second": 0.0,
                "eta_seconds": 0.0,
                "artifact_index": 0,
                "artifact_count": 0,
                "current_package": "",
                "current_version": "",
                "download_attempt": 0,
                "download_max_attempts": 0,
                "switching_source": False,
            }
        )
    threading.Thread(
        target=_install_claude_dependency_background,
        args=(force_reinstall,),
        name="claude-dependency-install",
        daemon=True,
    ).start()
    return _snapshot_claude_dependency_install_status()


def _install_claude_dependency_background(force_reinstall: bool = False) -> None:
    try:
        package = _resolve_openjiuwen_extra_package("claude")
        _install_optional_dependency(
            "claude", package, "claude_agent_sdk", force_reinstall=force_reinstall
        )
        if force_reinstall and not _external_cli_managed_cli_path("claude"):
            raise RuntimeError("claude bundled CLI is still missing after reinstall")
        updates = _external_cli_dependency_install_succeeded_updates()
    except Exception as exc:  # noqa: BLE001
        logger.warning("[config.set] Claude dependency installation failed: %s", exc)
        _append_external_cli_dependency_install_log("claude", str(exc))
        updates = {
            "status": "failed",
            "phase": "failed",
            "error": str(exc),
            "finished_at": time.time(),
        }
    _update_external_cli_dependency_install_status("claude", updates)


def _is_frozen_runtime() -> bool:
    return bool(getattr(sys, "frozen", False))


def _snapshot_external_cli_dependency_install_status_unlocked(cli_agent: str) -> dict[str, Any]:
    _lock, status = _external_cli_dependency_install_state(cli_agent)
    result = dict(status)
    result["cli_agent"] = cli_agent
    result["log_tail"] = list(result.get("log_tail") or [])
    return result


def _ensure_managed_external_cli_runtime_or_start_install(cli_agent: str) -> dict[str, Any]:
    lock, status = _external_cli_dependency_install_state(cli_agent)
    with lock:
        if status.get("status") == "running":
            return _snapshot_external_cli_dependency_install_status_unlocked(cli_agent)
        status.update({
            "status": "running",
            "phase": "preparing",
            "progress_kind": "download_metrics",
            "error": "",
            "last_log": "",
            "log_tail": [],
            "started_at": time.time(),
            "finished_at": 0.0,
            "updated_at": time.time(),
            "downloaded_bytes": 0,
            "total_bytes": 0,
            "bytes_per_second": 0.0,
            "eta_seconds": 0.0,
            "artifact_index": 0,
            "artifact_count": 0,
            "current_package": "",
            "current_version": "",
            "download_attempt": 0,
            "download_max_attempts": 0,
            "switching_source": False,
        })
    threading.Thread(
        target=_run_managed_external_cli_runtime_install,
        args=(cli_agent,),
        name=f"{cli_agent}-managed-runtime-install",
        daemon=True,
    ).start()
    return _snapshot_external_cli_dependency_install_status(cli_agent)


def _run_managed_external_cli_runtime_install(cli_agent: str) -> None:
    try:
        from agent_gateway.common.external_cli_runtime import (
            activate_external_cli_runtime_paths,
            install_external_cli_runtime,
        )

        install_external_cli_runtime(
            cli_agent,
            log_callback=lambda line: _append_external_cli_dependency_install_log(cli_agent, line),
            progress_callback=lambda progress: _update_external_cli_dependency_install_status(cli_agent, progress),
        )
        # The installer already verified the runtime's modules and provider
        # entry point against the activated paths.
        activate_external_cli_runtime_paths()
    except Exception as exc:  # noqa: BLE001
        logger.warning("[config.set] %s managed runtime installation failed: %s", cli_agent, exc)
        _append_external_cli_dependency_install_log(cli_agent, str(exc))
        _update_external_cli_dependency_install_status(
            cli_agent,
            {
                "status": "failed",
                "phase": "failed",
                "error": str(exc),
                "finished_at": time.time(),
                "bytes_per_second": 0.0,
                "eta_seconds": 0.0,
            },
        )
        return

    _update_external_cli_dependency_install_status(
        cli_agent,
        _external_cli_dependency_install_succeeded_updates(),
    )


def _known_external_cli_agents() -> tuple[str, ...]:
    """Return Claude, Codex and every runtime the managed-runtime manifest describes."""
    from agent_gateway.common.external_cli_runtime import external_cli_runtime_names

    try:
        managed = external_cli_runtime_names()
    except RuntimeError as exc:
        logger.warning("[external-cli] managed runtime manifest is unavailable: %s", exc)
        managed = ()
    return tuple(dict.fromkeys(("claude", "codex", *managed)))


def _managed_external_cli_runtime_status(cli_agent: str) -> dict[str, Any]:
    """Describe the installed managed runtime; failures become an ``unavailable`` state."""
    from agent_gateway.common.external_cli_runtime import get_external_cli_runtime_status

    try:
        return get_external_cli_runtime_status(cli_agent)
    except (RuntimeError, ValueError) as exc:
        return {"runtime_state": "unavailable", "managed": True, "error": str(exc)}


def _install_managed_external_cli_runtime_if_needed(cli_agent: str) -> dict[str, Any]:
    """Start a managed runtime install unless the current one is already installed.

    Returns:
        The install status snapshot, with the managed runtime state under ``runtime``.
    """
    runtime = _managed_external_cli_runtime_status(cli_agent)
    if runtime.get("runtime_state") == "current":
        _update_external_cli_dependency_install_status(cli_agent, _external_cli_dependency_install_succeeded_updates())
        payload = _snapshot_external_cli_dependency_install_status(cli_agent)
    else:
        payload = _ensure_managed_external_cli_runtime_or_start_install(cli_agent)
    payload["runtime"] = runtime
    return payload


def _remove_custom_external_cli_agent(cli_agent: str) -> None:
    """Drop a custom agent from the team configuration, then uninstall it.

    The configuration goes first: it may only name installed agents.

    Raises:
        ValueError: When ``cli_agent`` is not an installed custom agent.
    """
    from agent_gateway.common.config import (
        EXTERNAL_CLI_AGENTS_CONFIG_PATH,
        update_external_cli_agents_in_config,
    )
    from agent_gateway.common.config_panel.config_set_handlers import (
        get_nested_config_value,
    )
    from agent_gateway.common.custom_agent_runtime import (
        get_custom_agent,
        remove_custom_agent,
    )

    if get_custom_agent(cli_agent) is None:
        raise ValueError(f"custom agent not found: {cli_agent}")
    configured = get_nested_config_value(get_config_raw(), EXTERNAL_CLI_AGENTS_CONFIG_PATH, [])
    if isinstance(configured, list):
        remaining = [
            item
            for item in configured
            if not (isinstance(item, dict) and item.get("cli_agent") == cli_agent) and item != cli_agent
        ]
        if len(remaining) != len(configured):
            update_external_cli_agents_in_config(remaining, _build_external_cli_publish_url())
    remove_custom_agent(cli_agent)


def _snapshot_codex_dependency_install_status() -> dict[str, Any]:
    return _snapshot_external_cli_dependency_install_status("codex")


def _update_codex_dependency_install_status(updates: dict[str, Any]) -> None:
    _update_external_cli_dependency_install_status("codex", updates)


def _append_codex_dependency_install_log(line: str) -> None:
    _append_external_cli_dependency_install_log("codex", line)


def _ensure_codex_dependency_available_or_start_install() -> dict[str, Any] | None:
    _activate_managed_external_cli_paths_if_needed()
    package_available = importlib.util.find_spec("openai_codex") is not None
    cli_available = bool(_external_cli_managed_cli_path("codex"))
    if package_available and cli_available:
        _update_codex_dependency_install_status(_external_cli_dependency_install_succeeded_updates())
        return None
    # Same repair semantics as the claude ensure: the package can be importable
    # while the bundled CLI executable was deleted; force-reinstall the pinned
    # SDK so 一键安装 restores it instead of ending as a pip no-op.
    force_reinstall = package_available and not cli_available

    if _is_frozen_runtime():
        return _ensure_managed_external_cli_runtime_or_start_install("codex")

    with _CODEX_DEPENDENCY_INSTALL_LOCK:
        if _CODEX_DEPENDENCY_INSTALL_STATUS.get("status") == "running":
            already_running = True
        else:
            already_running = False
            _CODEX_DEPENDENCY_INSTALL_STATUS.update({
                "status": "running",
                "phase": "preparing",
                "progress_kind": "installer_activity",
                "error": "",
                "last_log": "",
                "log_tail": [],
                "started_at": time.time(),
                "finished_at": 0.0,
                "updated_at": time.time(),
                "downloaded_bytes": 0,
                "total_bytes": 0,
                "bytes_per_second": 0.0,
                "eta_seconds": 0.0,
                "artifact_index": 0,
                "artifact_count": 0,
                "current_package": "",
                "current_version": "",
                "download_attempt": 0,
                "download_max_attempts": 0,
                "switching_source": False,
            })
    if already_running:
        return _snapshot_codex_dependency_install_status()

    thread = threading.Thread(
        target=_run_codex_dependency_install_background,
        args=(force_reinstall,),
        name="codex-dependency-install",
        daemon=True,
    )
    thread.start()
    return _snapshot_codex_dependency_install_status()


def _run_codex_dependency_install_background(force_reinstall: bool = False) -> None:
    try:
        _install_codex_dependency(force_reinstall=force_reinstall)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[config.set] Codex dependency installation failed: %s", exc)
        _append_codex_dependency_install_log(str(exc))
        _update_codex_dependency_install_status({
            "status": "failed",
            "phase": "failed",
            "error": str(exc),
            "finished_at": time.time(),
        })
        return

    _update_codex_dependency_install_status(_external_cli_dependency_install_succeeded_updates())


def _install_codex_dependency(force_reinstall: bool = False) -> None:
    if _is_frozen_runtime():
        raise RuntimeError("frozen applications must use the managed Codex runtime installer")
    package = _resolve_openjiuwen_codex_package()
    _install_optional_dependency("codex", package, "openai_codex", force_reinstall=force_reinstall)
    if force_reinstall and not _external_cli_managed_cli_path("codex"):
        raise RuntimeError("codex bundled CLI is still missing after reinstall")


def _install_optional_dependency(
    cli_agent: str,
    package: str,
    required_module: str,
    force_reinstall: bool = False,
) -> None:
    if _is_frozen_runtime():
        raise RuntimeError(f"frozen applications must use the managed {cli_agent} runtime installer")
    # Parallel pip subprocesses must not write the same virtualenv at once:
    # queue this install behind any other in-flight pip-based install. The
    # frozen managed-runtime path is per-agent and stays concurrent.
    with _OPTIONAL_DEPENDENCY_PIP_INSTALL_LOCK:
        _run_optional_dependency_pip_install(cli_agent, package, required_module, force_reinstall)


def _run_optional_dependency_pip_install(
    cli_agent: str,
    package: str,
    required_module: str,
    force_reinstall: bool,
) -> None:
    args = _build_optional_dependency_install_args(package, cli_agent, force_reinstall=force_reinstall)
    output_lines: list[str] = []
    logger.info(
        "[external-cli] installing %s dependency: interpreter=%s command=%s",
        cli_agent,
        sys.executable,
        args,
    )
    _update_external_cli_dependency_install_status(
        cli_agent,
        {
            "phase": "installing",
        },
    )
    try:
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        process = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"failed to install {cli_agent} dependency: {exc}") from exc

    output_queue: queue.Queue[str | None] = queue.Queue()

    def read_output() -> None:
        if process.stdout is None:
            output_queue.put(None)
            return
        try:
            for line in process.stdout:
                output_queue.put(line)
        finally:
            output_queue.put(None)

    reader = threading.Thread(target=read_output, name=f"{cli_agent}-dependency-install-output", daemon=True)
    reader.start()
    deadline = time.monotonic() + _OPTIONAL_DEPENDENCY_INSTALL_TIMEOUT_SECONDS
    reader_done = False
    while True:
        try:
            item = output_queue.get(timeout=0.2)
        except queue.Empty:
            item = None
        else:
            if item is None:
                reader_done = True
            else:
                line = item.rstrip()
                if line:
                    output_lines.append(line)
                    _append_external_cli_dependency_install_log(cli_agent, line)

        if process.poll() is not None and reader_done:
            break
        if time.monotonic() >= deadline:
            process.kill()
            process.wait()
            reader.join(timeout=1)
            logger.error(
                "[external-cli] %s dependency install timed out after %ss; output tail:\n%s",
                cli_agent,
                _OPTIONAL_DEPENDENCY_INSTALL_TIMEOUT_SECONDS,
                "\n".join(output_lines[-20:]),
            )
            raise RuntimeError(f"failed to install {cli_agent} dependency: timed out")

    reader.join(timeout=1)
    returncode = process.wait()
    if returncode != 0:
        output = "\n".join(output_lines[-20:])
        logger.error(
            "[external-cli] %s dependency install exited with code %s; output tail:\n%s",
            cli_agent,
            returncode,
            output,
        )
        raise RuntimeError(f"failed to install {cli_agent} dependency: {output}")
    _update_external_cli_dependency_install_status(
        cli_agent,
        {
            "phase": "verifying",
        },
    )
    importlib.invalidate_caches()
    if importlib.util.find_spec(required_module) is None:
        # The installer reported success but the module is not importable from
        # the running interpreter — it almost certainly landed in a different
        # environment. Log the interpreter paths to make that diagnosable.
        logger.error(
            "[external-cli] %s dependency installed but %s is still not importable; "
            "interpreter=%s sys.prefix=%s search paths=%s",
            cli_agent,
            required_module,
            sys.executable,
            sys.prefix,
            sys.path,
        )
        raise RuntimeError(f"failed to install {cli_agent} dependency: {required_module} is still unavailable")
    logger.info("[external-cli] %s dependency installed and verified: %s", cli_agent, required_module)


def _resolve_openjiuwen_codex_package() -> str:
    return _resolve_openjiuwen_extra_package("codex")


def _resolve_openjiuwen_extra_package(extra: str) -> str:
    spec = importlib.util.find_spec("openjiuwen")
    if spec is None:
        return f"openjiuwen[{extra}]"

    candidate_paths: list[Path] = []
    if spec.origin:
        candidate_paths.append(Path(spec.origin))
    if spec.submodule_search_locations:
        candidate_paths.extend(Path(location) for location in spec.submodule_search_locations)

    for candidate_path in candidate_paths:
        package_root = candidate_path if candidate_path.is_dir() else candidate_path.parent
        project_root = package_root.parent if package_root.name == "openjiuwen" else package_root
        if (project_root / "pyproject.toml").is_file() and (project_root / "openjiuwen").is_dir():
            return f"{project_root}[{extra}]"

    return f"openjiuwen[{extra}]"


# Mirror for optional CLI SDK installs. The managed runtime installer (frozen
# apps) already downloads its wheels from Aliyun first; source installs should
# resolve from the same mirror instead of falling back to pypi.org, which is
# slow to unreachable for users in China. Override with PIP_INDEX_URL /
# UV_INDEX_URL to respect a user-configured index.
_OPTIONAL_DEPENDENCY_INDEX_URL = os.getenv("PIP_INDEX_URL") or os.getenv("UV_INDEX_URL") or (
    "https://mirrors.aliyun.com/pypi/simple/"
)


def _openjiuwen_url_requirements(extra: str) -> list[str]:
    """URL-pinned requirements of the installed openjiuwen, restated as direct ones.

    uv pip rejects a URL dependency that only appears transitively (openjiuwen
    pins protocol packages to git), so the extra install must name them itself.
    Requirements gated on a different extra are left out.
    """
    try:
        requires = importlib.metadata.requires("openjiuwen") or []
    except importlib.metadata.PackageNotFoundError:
        return []
    forwarded: list[str] = []
    for raw in requires:
        spec, _, marker = raw.partition(";")
        spec = spec.strip()
        if " @ " not in spec:
            continue
        if "extra" in marker and f'"{extra}"' not in marker and f"'{extra}'" not in marker:
            continue
        forwarded.append(spec)
    return forwarded


def _build_optional_dependency_install_args(
    package: str,
    cli_agent: str,
    force_reinstall: bool = False,
) -> list[str]:
    """Build the pip/uv command that installs an optional CLI agent SDK.

    ``package`` is the ``openjiuwen[extra]`` requirement; the SDK wheels
    themselves are pinned to the versions recorded in the managed-runtime
    manifest so source installs resolve the same versions the frozen-app
    installer ships, instead of the latest release on the index.

    With ``force_reinstall`` (repair mode: the SDK is importable but its
    bundled CLI executable was deleted) only the pinned SDK wheel is
    reinstalled with ``--force-reinstall --no-deps`` — a plain install would
    end as a pip "already satisfied" no-op and never restore the file.
    """
    # Import lazily: only needed on this rare install path.
    from agent_gateway.common.external_cli_runtime import pinned_sdk_requirements

    pinned = pinned_sdk_requirements(cli_agent)
    if force_reinstall:
        requirements = list(pinned) or [package]
    else:
        requirements = [package, *_openjiuwen_url_requirements(cli_agent), *pinned]
    reinstall_flags = ["--force-reinstall", "--no-deps"] if force_reinstall else []
    uv_cmd = shutil.which("uv")
    if uv_cmd and sys.prefix != sys.base_prefix:
        # Pin the target interpreter: without --python, uv resolves its own
        # environment (VIRTUAL_ENV / auto-discovery) which may differ from the
        # running interpreter — the install then "succeeds" into the wrong
        # venv and the follow-up find_spec check reports the SDK as missing.
        return [
            uv_cmd,
            "pip",
            "install",
            "--python",
            sys.executable,
            "--index-url",
            _OPTIONAL_DEPENDENCY_INDEX_URL,
            *reinstall_flags,
            *requirements,
        ]
    return [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--index-url",
        _OPTIONAL_DEPENDENCY_INDEX_URL,
        *reinstall_flags,
        *requirements,
    ]
