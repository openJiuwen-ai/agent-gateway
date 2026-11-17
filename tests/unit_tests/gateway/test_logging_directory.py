"""Explicit process-local log directory contracts."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import agent_gateway.common.logging_setup as logging_setup


_EXPECTED_LOG_FILES = {
    "gateway.log",
    "channel.log",
    "agent_server.log",
    "full.log",
    "permissions.log",
}
_CONFIGURED_ATTR = logging_setup._GATEWAY_LOGGING_CONFIGURED_ATTR
_EXPLICIT_ATTR = "_agent_gateway_explicit_log_directory"
_MISSING = object()


@pytest.fixture(autouse=True)
def isolated_global_logger_state(monkeypatch, caplog):
    monkeypatch.setattr(logging_setup, "_load_logging_config_from_yaml", lambda: {})
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.setattr(logging_setup, "install_runtime_log_filter", lambda: None)
    monkeypatch.setattr(logging_setup, "install_source_record_masking", lambda: None)
    monkeypatch.setattr(logging_setup, "_reconfigure_stdio_utf8", lambda: None)

    states: dict[str, dict[str, Any]] = {}
    for name in ("gateway", "jiuwenswarm"):
        logger = logging.getLogger(name)
        states[name] = {
            "handlers": list(logger.handlers),
            "level": logger.level,
            "propagate": logger.propagate,
            "disabled": logger.disabled,
            _CONFIGURED_ATTR: getattr(logger, _CONFIGURED_ATTR, _MISSING),
            _EXPLICIT_ATTR: getattr(logger, _EXPLICIT_ATTR, _MISSING),
        }
        logger.handlers = []
        logger.level = logging.INFO
        logger.propagate = False
        logger.disabled = False
        for attr in (_CONFIGURED_ATTR, _EXPLICIT_ATTR):
            if hasattr(logger, attr):
                delattr(logger, attr)

    yield

    for name, state in states.items():
        logger = logging.getLogger(name)
        original_handlers = state["handlers"]
        for handler in logger.handlers:
            if handler not in original_handlers:
                handler.close()
        logger.handlers = original_handlers
        logger.level = state["level"]
        logger.propagate = state["propagate"]
        logger.disabled = state["disabled"]
        for attr in (_CONFIGURED_ATTR, _EXPLICIT_ATTR):
            value = state[attr]
            if value is _MISSING:
                if hasattr(logger, attr):
                    delattr(logger, attr)
            else:
                setattr(logger, attr, value)


def _patch_default_paths(monkeypatch, logs_root: Path, gateway_dir: Path | None) -> None:
    monkeypatch.setattr(logging_setup, "get_logs_dir", lambda: logs_root)
    monkeypatch.setattr(logging_setup, "get_gateway_log_dir", lambda: gateway_dir)


def _file_paths(logger: logging.Logger) -> list[Path]:
    return [Path(handler.baseFilename) for handler in logger.handlers if isinstance(handler, logging.FileHandler)]


def _flush(logger: logging.Logger) -> None:
    for handler in logger.handlers:
        handler.flush()


def _clear_capture_handlers() -> None:
    for name in ("gateway", "jiuwenswarm"):
        logging.getLogger(name).handlers.clear()


def test_default_reuses_mother_handlers_and_remains_idempotent(tmp_path, monkeypatch):
    _clear_capture_handlers()
    logs_root = tmp_path / "agent"
    gateway_dir = tmp_path / "gateway-override"
    mother_dir = tmp_path / "mother"
    _patch_default_paths(monkeypatch, tmp_path, gateway_dir)
    mother_dir.mkdir()
    mother_root = logging.getLogger("jiuwenswarm")
    mother_handler = logging.FileHandler(mother_dir / "gateway.log")
    mother_root.addHandler(mother_handler)

    gateway_root = logging_setup.setup_gateway_logger()

    assert gateway_root is logging.getLogger("gateway")
    assert gateway_root is not mother_root
    assert not gateway_root.handlers
    assert logging_setup._is_gateway_logging_configured(gateway_root)
    assert logging_setup.setup_gateway_logger() is gateway_root
    assert not logs_root.exists()
    assert not gateway_dir.exists()


def test_default_without_mother_handlers_uses_default_and_gateway_paths(tmp_path, monkeypatch):
    _clear_capture_handlers()
    logs_root = tmp_path / "agent"
    gateway_dir = tmp_path / "gateway-override"
    _patch_default_paths(monkeypatch, tmp_path, gateway_dir)

    root = logging_setup.setup_gateway_logger()
    paths = _file_paths(root)

    assert {path.name for path in paths} == _EXPECTED_LOG_FILES
    assert {path.parent for path in paths if path.name != "gateway.log"} == {logs_root}
    assert next(path for path in paths if path.name == "gateway.log").parent == gateway_dir
    assert logging_setup.setup_gateway_logger() is root
    assert len(paths) == len(_file_paths(root))


def test_default_gateway_override_falls_back_when_not_writable(tmp_path, monkeypatch):
    _clear_capture_handlers()
    logs_root = tmp_path / "agent"
    blocked_dir = tmp_path / "blocked"
    blocked_dir.write_text("not a directory", encoding="utf-8")
    _patch_default_paths(monkeypatch, tmp_path, blocked_dir)

    root = logging_setup.setup_gateway_logger()
    paths = _file_paths(root)

    assert {path.name for path in paths} == _EXPECTED_LOG_FILES
    assert {path.parent for path in paths} == {logs_root}


def test_explicit_directory_isolates_files_and_keeps_mother_handlers_untouched(tmp_path, monkeypatch):
    _clear_capture_handlers()
    logs_root = tmp_path / "agent"
    gateway_dir = tmp_path / "gateway-override"
    explicit_dir = tmp_path / "explicit-agent"
    mother_dir = tmp_path / "mother"
    _patch_default_paths(monkeypatch, tmp_path, gateway_dir)
    mother_dir.mkdir()
    mother_root = logging.getLogger("jiuwenswarm")
    mother_handler = logging.FileHandler(mother_dir / "gateway.log")
    mother_handler.setLevel(logging.INFO)
    mother_root.addHandler(mother_handler)

    root = logging_setup.setup_gateway_logger(log_directory=explicit_dir)
    paths = _file_paths(root)
    token = "fake_token_p1c_not_a_real_credential"
    root.warning("explicit directory token=%s", token)
    _flush(root)

    assert {path.name for path in paths} == _EXPECTED_LOG_FILES
    assert {path.parent for path in paths} == {explicit_dir}
    assert mother_handler not in root.handlers
    assert not logs_root.exists()
    assert not gateway_dir.exists()
    assert (mother_dir / "gateway.log").read_text(encoding="utf-8") == ""
    assert "explicit directory token=" in (explicit_dir / "gateway.log").read_text(encoding="utf-8")
    for path in paths:
        assert token not in path.read_text(encoding="utf-8")

    gateway_handler = next(path for path in paths if path.name == "gateway.log")
    logging_setup.update_gateway_log_levels(gateway="WARNING")

    assert root.handlers[paths.index(gateway_handler)].level == logging.WARNING
    assert mother_handler.level == logging.INFO


def test_explicit_directory_write_failure_does_not_fall_back(tmp_path, monkeypatch):
    _clear_capture_handlers()
    logs_root = tmp_path / "agent"
    blocked_path = tmp_path / "blocked"
    blocked_path.write_text("not a directory", encoding="utf-8")
    _patch_default_paths(monkeypatch, logs_root, None)

    with pytest.raises(OSError):
        logging_setup.setup_gateway_logger(log_directory=blocked_path)

    assert not logs_root.exists()


def test_default_configuration_rejects_later_explicit_directory(tmp_path, monkeypatch):
    _clear_capture_handlers()
    logs_root = tmp_path / "agent"
    explicit_dir = tmp_path / "explicit-agent"
    _patch_default_paths(monkeypatch, logs_root, None)
    logging_setup.setup_gateway_logger()

    with pytest.raises(RuntimeError, match="already configured"):
        logging_setup.setup_gateway_logger(log_directory=explicit_dir)


def test_explicit_directory_same_reuse_and_different_directory_rejected(tmp_path, monkeypatch):
    _clear_capture_handlers()
    logs_root = tmp_path / "agent"
    first_dir = tmp_path / "first-agent"
    second_dir = tmp_path / "second-agent"
    _patch_default_paths(monkeypatch, logs_root, None)

    root = logging_setup.setup_gateway_logger(log_directory=first_dir)
    first_paths = _file_paths(root)
    assert logging_setup.setup_gateway_logger(log_directory=first_dir) is root
    assert logging_setup.setup_gateway_logger() is root
    assert _file_paths(root) == first_paths

    with pytest.raises(RuntimeError, match="already configured"):
        logging_setup.setup_gateway_logger(log_directory=second_dir)

    assert not second_dir.exists()


def test_explicit_directories_are_isolated_in_separate_processes(tmp_path):
    first_dir = tmp_path / "process-a"
    second_dir = tmp_path / "process-b"
    override_dir = tmp_path / "gateway-override"
    token = "fake_token_p1c_not_a_real_credential"
    repo_root = Path(__file__).resolve().parents[3]
    child_code = """
import logging
import sys
from pathlib import Path

from agent_gateway.common.logging_setup import setup_gateway_logger

root = setup_gateway_logger(log_directory=Path(sys.argv[1]))
root.warning("explicit directory token=fake_token_p1c_not_a_real_credential")
for handler in root.handlers:
    handler.flush()
"""
    child_env = os.environ.copy()
    child_env["JIUWENSWARM_HOME"] = str(tmp_path / "home")
    child_env["JIUWENSWARM_DATA_DIR"] = str(tmp_path / "data")
    child_env["AGENTOS_GATEWAY_LOG_DIR"] = str(override_dir)
    child_env["PYTHONPATH"] = str(repo_root) + os.pathsep + child_env.get("PYTHONPATH", "")

    first_result = subprocess.run(
        [sys.executable, "-W", "ignore:invalid escape sequence:SyntaxWarning", "-c", child_code, str(first_dir)],
        cwd=repo_root,
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert first_result.returncode == 0, first_result.stdout + first_result.stderr
    assert not override_dir.exists()

    second_result = subprocess.run(
        [sys.executable, "-W", "ignore:invalid escape sequence:SyntaxWarning", "-c", child_code, str(second_dir)],
        cwd=repo_root,
        env=child_env,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert second_result.returncode == 0, second_result.stdout + second_result.stderr

    for log_dir in (first_dir, second_dir):
        assert {path.name for path in log_dir.iterdir()} == _EXPECTED_LOG_FILES
        for path in log_dir.iterdir():
            assert token not in path.read_text(encoding="utf-8")
