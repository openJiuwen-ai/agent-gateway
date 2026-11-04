# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Pytest configuration and shared fixtures for agent-gateway."""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Generator

import pytest


@pytest.fixture(autouse=True)
def _agent_gateway_logs_reach_caplog(
    request: pytest.FixtureRequest,
) -> Generator[None, None, None]:
    """Let ``caplog`` see records from the ``agent_gateway`` logger tree."""
    if "caplog" not in request.fixturenames:
        yield
        return
    logger = logging.getLogger("agent_gateway")
    previous = logger.propagate
    logger.propagate = True
    try:
        yield
    finally:
        logger.propagate = previous


@pytest.fixture(autouse=True)
def _isolate_gateway_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Keep data written by tests out of the real ``~/.jiuwenswarm``.

    The cache attributes are module-level in ``agent_gateway.common.paths``.
    Reset them before and after each test so a previous test cannot leak its
    temporary home into the next one.
    """
    import agent_gateway.common.paths as paths

    monkeypatch.setenv("JIUWENSWARM_HOME", str(tmp_path))
    monkeypatch.setenv("JIUWENSWARM_DATA_DIR", str(tmp_path / ".jiuwenswarm"))
    path_caches = (
        "_initialized",
        "_config_dir",
        "_workspace_dir",
        "_root_dir",
        "_workspace_base_dir",
        "_user_home",
    )
    for attr in path_caches:
        if hasattr(paths, attr):
            setattr(paths, attr, None)
    try:
        yield
    finally:
        for attr in path_caches:
            if hasattr(paths, attr):
                setattr(paths, attr, None)


@pytest.fixture
def temp_workspace() -> Generator[Path, None, None]:
    """Create a temporary workspace directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        workspace = Path(tmpdir)
        (workspace / "config").mkdir(parents=True, exist_ok=True)
        (workspace / "workspace").mkdir(parents=True, exist_ok=True)
        (workspace / "workspace" / "agent").mkdir(parents=True, exist_ok=True)
        (workspace / "workspace" / "agent" / "skills").mkdir(parents=True, exist_ok=True)
        (workspace / "logs").mkdir(parents=True, exist_ok=True)
        yield workspace


@pytest.fixture
def temp_config_file(temp_workspace: Path) -> Generator[Path, None, None]:
    """Create a temporary config.yaml file."""
    config_content = """
# Test configuration
model:
  provider: "test_provider"
  name: "test_model"
  api_base: "https://test.api.com"
  api_key: "${TEST_API_KEY:-default_key}"

channels:
  web:
    enabled: true

react:
  evolution:
    skill_evolution: true
    auto_save: false

heartbeat:
  every: "30 * * * *"
  target: "web"
"""
    config_file = temp_workspace / "config" / "config.yaml"
    config_file.write_text(config_content, encoding="utf-8")
    yield config_file


@pytest.fixture
def mock_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set up mock environment variables."""
    monkeypatch.setenv("MODEL_PROVIDER", "test_provider")
    monkeypatch.setenv("MODEL_NAME", "test_model")
    monkeypatch.setenv("API_BASE", "https://test.api.com")
    monkeypatch.setenv("API_KEY", "test_api_key")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
