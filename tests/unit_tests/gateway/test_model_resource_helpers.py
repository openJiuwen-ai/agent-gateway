"""Gateway 侧 config 副本的模型资源辅助函数回归。

`agent_gateway/common/config.py` 是从 jiuwenswarm 裁剪出的副本，
`upsert_model_resource()` 依赖的三个私有辅助函数曾在裁剪时丢失，
导致 Web 端 "models.upsert" 直接 NameError。
"""

from __future__ import annotations

from agent_gateway.common import config


class TestModelResourceHelpers:
    @staticmethod
    def test_helpers_exist():
        for name in ("_normalize_model_entry", "_merge_model_update", "_protect_model_key"):
            assert hasattr(config, name), f"{name} is missing from the gateway config copy"

    @staticmethod
    def test_flat_entry_is_promoted_to_nested_config():
        saved = config._normalize_model_entry(
            {
                "model_id": "mdl_1",
                "model_name": "gpt-x",
                "api_base": "https://api.example.com/v1",
                "api_key": "sk-abc",
                "client_provider": "openai",
                "temperature": "0.3",
            }
        )
        assert saved["model_client_config"]["model_name"] == "gpt-x"
        assert saved["model_client_config"]["api_base"] == "https://api.example.com/v1"
        assert saved["model_client_config"]["api_key"] == "sk-abc"
        assert saved["model_client_config"]["client_provider"] == "openai"
        assert saved["model_config_obj"]["temperature"] == 0.3
        assert "model_name" not in saved

    @staticmethod
    def test_nested_entry_is_returned_as_is():
        entry = {
            "model_id": "mdl_1",
            "model_client_config": {"model_name": "gpt-x", "api_key": "sk-abc"},
        }
        assert config._normalize_model_entry(entry) == entry

    @staticmethod
    def test_merge_preserves_omitted_and_drops_write_only_fields():
        merged = config._merge_model_update(
            {"a": 1, "nested": {"x": 1, "y": 2}, "source": "builtin"},
            {"nested": {"y": 3}, "source": "user", "read_only": True},
        )
        assert merged["nested"] == {"x": 1, "y": 3}
        assert merged["source"] == "builtin"
        assert "read_only" not in merged
