import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest


def test_bootstrap_import_does_not_prepare_workspace(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-c", "import agent_gateway.startup.bootstrap"],
        env=dict(os.environ, JIUWENSWARM_HOME=str(tmp_path),
                 JIUWENSWARM_DATA_DIR=str(tmp_path / "data")),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "data").exists()


def test_early_environment_loads_selected_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_gateway import dotenv_early
    from agent_gateway.startup.bootstrap import initialize_early_environment

    monkeypatch.setattr(dotenv_early, "_parsed_dotenv", None)
    monkeypatch.setattr(dotenv_early, "_component_name", "agent-gateway")
    env_file = tmp_path / "bootstrap.env"
    workspace = tmp_path / "chosen"
    env_file.write_text(f"JIUWENSWARM_DATA_DIR={workspace}\n", encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["agent-gateway", "--dotenv", str(env_file)])
    stages = []
    initialize_early_environment(stages.append)
    assert os.environ["JIUWENSWARM_DATA_DIR"] == str(workspace)
    assert stages == ["dotenv_parsed"]


@pytest.mark.parametrize("prepared", [False, True])
def test_workspace_preparation_respects_parent_marker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    prepared: bool,
) -> None:
    from agent_gateway.common import paths
    from agent_gateway.startup.bootstrap import prepare_group_workspace

    if prepared:
        monkeypatch.setenv("JIUWENSWARM_RUNTIME_WORKSPACE_READY", "1")
    else:
        monkeypatch.delenv("JIUWENSWARM_RUNTIME_WORKSPACE_READY", raising=False)
    target = tmp_path / "initialized"
    package = tmp_path / "openjiuwen"
    descriptions = package / "agent_teams" / "tools" / "locales" / "descs" / "en"
    nested = descriptions / "nested"
    nested.mkdir(parents=True)
    (nested / "tool.md").write_text("current", encoding="utf-8")
    stale = descriptions / "tool.md"
    stale.write_text("stale", encoding="utf-8")
    monkeypatch.setitem(sys.modules, "openjiuwen", SimpleNamespace(__file__=str(package / "__init__.py")))

    def prepare(*, cleanup_stale_descs: bool) -> None:
        assert cleanup_stale_descs is False
        target.write_text("prepared", encoding="utf-8")

    monkeypatch.setattr(paths, "prepare_runtime_workspace", prepare)
    stages = []
    prepare_group_workspace(stages.append)
    assert target.exists() is (not prepared)
    assert not stale.exists()
    assert stages == ["runtime_workspace_ready"]


def test_group_environment_preserves_explicit_capabilities(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from agent_gateway.common import paths
    from agent_gateway.startup.bootstrap import apply_group_environment

    env_file = tmp_path / "runtime.env"
    env_file.write_text("VISION_ENABLED=false\nAUDIO_ENABLED=true\nVIDEO_ENABLED=false\n", encoding="utf-8")
    monkeypatch.setattr(paths, "get_env_file", lambda: env_file)
    for name in ("FREE_SEARCH_DDG_ENABLED", "FREE_SEARCH_BING_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    for name in ("VISION_ENABLED", "AUDIO_ENABLED", "VIDEO_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    original = env_file.read_bytes()
    stages = []
    apply_group_environment(stages.append)
    assert os.environ["VISION_ENABLED"] == "false"
    assert os.environ["AUDIO_ENABLED"] == "true"
    assert os.environ["VIDEO_ENABLED"] == "false"
    assert env_file.read_bytes() == original
    assert os.environ["FREE_SEARCH_DDG_ENABLED"] == "false"
    assert os.environ["FREE_SEARCH_BING_ENABLED"] == "false"
    assert stages == ["runtime_environment_applied"]


def test_process_logging_writes_in_selected_workspace(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-c", "\n".join([
            "from pathlib import Path",
            "from agent_gateway.startup.bootstrap import configure_process_logging",
            "import logging",
            "stages = []",
            "configure_process_logging(stages.append)",
            "logging.getLogger('gateway').warning('startup-log-contract')",
            "assert stages == ['third_party_logging_pinned', 'logging_ready']",
            "from agent_gateway.common.paths import get_logs_dir",
            "assert 'startup-log-contract' in (get_logs_dir() / 'agent' / 'gateway.log').read_text()",
        ])],
        env=dict(os.environ, JIUWENSWARM_HOME=str(tmp_path),
                 JIUWENSWARM_DATA_DIR=str(tmp_path / "data"), AGENTOS_GATEWAY_LOG_DIR=""),
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr


def test_entrypoint_preserves_startup_phase_order(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, "-c", "\n".join([
            "from agent_gateway.app_gateway import _STARTUP_IMPORT_PHASES",
            "assert [name for name, timestamp in _STARTUP_IMPORT_PHASES] == [",
            "'entry', 'dotenv_parsed', 'third_party_logging_pinned',",
            "'logging_ready', 'runtime_workspace_ready',",
            "'openjiuwen_and_channel_base_imported', 'gateway_core_imports_loaded',",
            "'logging_configured', 'runtime_environment_applied']",
        ])],
        env=dict(os.environ, JIUWENSWARM_HOME=str(tmp_path),
                 JIUWENSWARM_DATA_DIR=str(tmp_path / "data")),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
