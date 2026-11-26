# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from pathlib import Path

import agent_gateway.common.paths as paths
from agent_gateway.common.paths import get_config_file, prepare_runtime_workspace


def test_gateway_config_file_env_override(monkeypatch, tmp_path) -> None:
    target = tmp_path
    monkeypatch.delenv("JIUWENSWARM_RUNTIME_PROFILE", raising=False)
    monkeypatch.setenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", str(target))

    assert get_config_file() == target / "config-gateway.yaml"


def test_gateway_config_file_defaults_to_config_gateway_yaml(monkeypatch) -> None:
    monkeypatch.delenv("JIUWENSWARM_RUNTIME_PROFILE", raising=False)
    monkeypatch.delenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", raising=False)

    assert get_config_file().name == "config-gateway.yaml"


def test_gateway_config_file_relative_override(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(paths, "get_config_dir", lambda: tmp_path)
    monkeypatch.setenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", "gateway-config")

    assert get_config_file() == tmp_path / "gateway-config" / "config-gateway.yaml"


def test_gateway_env_file_ignores_absolute_override(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("JIUWENSWARM_RUNTIME_PROFILE", raising=False)
    monkeypatch.setenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", str(tmp_path / "gateway-config"))
    monkeypatch.setattr(paths, "get_config_dir", lambda: tmp_path / "config")

    assert paths.get_env_file() == tmp_path / "config" / ".env"


def test_gateway_env_file_ignores_relative_override(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("JIUWENSWARM_RUNTIME_PROFILE", raising=False)
    monkeypatch.setattr(paths, "get_config_dir", lambda: tmp_path)
    monkeypatch.setenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", "gateway-config")

    assert paths.get_env_file() == tmp_path / ".env"


def test_agentos_env_file_is_shared_with_agentserver(monkeypatch, tmp_path) -> None:
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("JIUWENSWARM_RUNTIME_PROFILE", "agentos")
    monkeypatch.setattr(paths, "get_user_workspace_dir", lambda: workspace)

    assert paths.get_env_file() == workspace / "config" / ".env"


def test_prepare_runtime_workspace_seeds_config(monkeypatch, tmp_path) -> None:
    target = tmp_path / "config-gateway.yaml"
    monkeypatch.delenv("JIUWENSWARM_RUNTIME_PROFILE", raising=False)
    monkeypatch.delenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", raising=False)
    monkeypatch.setattr(paths, "get_config_file", lambda: target)
    monkeypatch.setattr(paths, "get_agent_workspace_dir", lambda: tmp_path / "workspace")
    monkeypatch.setattr(paths, "get_agent_home_dir", lambda: tmp_path / "home")

    prepare_runtime_workspace(cleanup_stale_descs=False)

    expected = (paths.get_bundled_resources_dir() / "config.yaml").read_text(encoding="utf-8")
    assert target.read_text(encoding="utf-8") == expected


def test_prepare_runtime_workspace_keeps_existing_config(monkeypatch, tmp_path) -> None:
    target = tmp_path / "config-gateway.yaml"
    target.write_text("custom: true\n", encoding="utf-8")
    monkeypatch.delenv("JIUWENSWARM_RUNTIME_PROFILE", raising=False)
    monkeypatch.delenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", raising=False)
    monkeypatch.setattr(paths, "get_config_file", lambda: target)
    monkeypatch.setattr(paths, "get_agent_workspace_dir", lambda: tmp_path / "workspace")
    monkeypatch.setattr(paths, "get_agent_home_dir", lambda: tmp_path / "home")

    prepare_runtime_workspace(cleanup_stale_descs=False)

    assert target.read_text(encoding="utf-8") == "custom: true\n"
