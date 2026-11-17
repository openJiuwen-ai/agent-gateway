# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from pathlib import Path

import agent_gateway.common.paths as paths
from agent_gateway.common.paths import get_config_file, prepare_runtime_workspace


def test_gateway_config_file_env_override(monkeypatch, tmp_path) -> None:
    target = tmp_path
    monkeypatch.setenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", str(target))

    assert get_config_file() == target / "config.yaml"


def test_gateway_config_file_defaults_to_config_yaml(monkeypatch) -> None:
    monkeypatch.delenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", raising=False)

    assert get_config_file().name == "config.yaml"


def test_gateway_config_file_relative_override(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(paths, "get_config_dir", lambda: tmp_path)
    monkeypatch.setenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", "gateway-config")

    assert get_config_file() == tmp_path / "gateway-config" / "config.yaml"


def test_prepare_runtime_workspace_seeds_config(monkeypatch, tmp_path) -> None:
    target = tmp_path / "config.yaml"
    monkeypatch.setattr(paths, "get_config_file", lambda: target)
    monkeypatch.setattr(paths, "get_agent_workspace_dir", lambda: tmp_path / "workspace")
    monkeypatch.setattr(paths, "get_agent_home_dir", lambda: tmp_path / "home")

    prepare_runtime_workspace(cleanup_stale_descs=False)

    expected = (paths.get_bundled_resources_dir() / "config.yaml").read_text(encoding="utf-8")
    assert target.read_text(encoding="utf-8") == expected


def test_prepare_runtime_workspace_keeps_existing_config(monkeypatch, tmp_path) -> None:
    target = tmp_path / "config.yaml"
    target.write_text("custom: true\n", encoding="utf-8")
    monkeypatch.setattr(paths, "get_config_file", lambda: target)
    monkeypatch.setattr(paths, "get_agent_workspace_dir", lambda: tmp_path / "workspace")
    monkeypatch.setattr(paths, "get_agent_home_dir", lambda: tmp_path / "home")

    prepare_runtime_workspace(cleanup_stale_descs=False)

    assert target.read_text(encoding="utf-8") == "custom: true\n"
