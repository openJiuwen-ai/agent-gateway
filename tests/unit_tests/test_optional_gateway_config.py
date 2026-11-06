# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Standalone Gateway accepts a missing initial config, not an invalid one."""

import pytest
import yaml

from agent_gateway.common import config


def test_missing_config_uses_defaults_without_creating_file(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    monkeypatch.setattr(config, "get_config_file", lambda: path)
    assert isinstance(config.get_config(), dict)
    assert not path.exists()
    path.write_text("extensions:\n  enabled: []\n", encoding="utf-8")
    assert config.get_config()["extensions"]["enabled"] == []


def test_existing_config_preserves_values(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text("extensions:\n  include_package_extensions: false\n", encoding="utf-8")
    monkeypatch.setattr(config, "get_config_file", lambda: path)
    assert config.get_config()["extensions"]["include_package_extensions"] is False


def test_config_permission_error_is_not_hidden(monkeypatch):
    def denied(_path):
        raise PermissionError("unreadable config")

    monkeypatch.setattr(config, "_read_with_retry", denied)
    with pytest.raises(PermissionError):
        config.get_config()


def test_invalid_config_is_not_hidden(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text("extensions: [", encoding="utf-8")
    monkeypatch.setattr(config, "get_config_file", lambda: path)
    with pytest.raises(yaml.YAMLError):
        config.get_config()
