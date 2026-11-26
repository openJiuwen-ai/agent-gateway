# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import os
from unittest.mock import Mock

import pytest

from agent_gateway.common import config


@pytest.fixture
def config_path(tmp_path, monkeypatch):
    path = tmp_path / "config-gateway.yaml"
    path.write_text("label: old\n", encoding="utf-8")
    monkeypatch.setattr(config, "CONFIG_YAML_PATH", path)
    monkeypatch.setattr(config, "_YAML_PARSE_CACHE", {})
    return path


def test_atomic_replacement_with_same_size_and_mtime(config_path):
    assert config.get_config_raw() == {"label": "old"}
    original = config_path.stat()
    replacement = config_path.with_suffix(".tmp")
    replacement.write_text("label: new\n", encoding="utf-8")
    os.utime(replacement, ns=(original.st_atime_ns, original.st_mtime_ns))
    replacement.replace(config_path)

    assert config_path.stat().st_mtime_ns == original.st_mtime_ns
    assert config.get_config_raw() == {"label": "new"}


def test_replacement_between_lookup_and_open_does_not_poison_cache(config_path, monkeypatch):
    original = config_path.with_suffix(".original")
    other = config_path.with_suffix(".other")
    other.write_text("label: replacement\n", encoding="utf-8")

    def swap_then_open(*args, **kwargs):
        config_path.replace(original)
        other.replace(config_path)
        return open(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(config, "open", swap_then_open, raising=False)
        assert config.get_config_raw() == {"label": "replacement"}

    original.replace(config_path)
    assert config.get_config_raw() == {"label": "old"}


def test_unchanged_file_reuses_parse_without_sharing_mutations(config_path, monkeypatch):
    if not config_path.stat().st_ino:
        pytest.skip("filesystem has no file identity for caching")
    load = Mock(wraps=config.yaml.safe_load)
    monkeypatch.setattr(config.yaml, "safe_load", load)

    config.get_config_raw()["label"] = "modified by caller"
    assert config.get_config_raw() == {"label": "old"}
    load.assert_called_once()
