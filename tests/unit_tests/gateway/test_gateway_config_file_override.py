# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from pathlib import Path

from agent_gateway.common.paths import get_config_file


def test_gateway_config_file_env_override(monkeypatch, tmp_path) -> None:
    target = tmp_path / "agent-gateway.yaml"
    monkeypatch.setenv("JIUWENSWARM_GATEWAY_CONFIG_FILE", str(target))

    assert get_config_file() == target


def test_gateway_config_file_defaults_to_config_yaml(monkeypatch) -> None:
    monkeypatch.delenv("JIUWENSWARM_GATEWAY_CONFIG_FILE", raising=False)

    assert get_config_file().name == "config.yaml"
