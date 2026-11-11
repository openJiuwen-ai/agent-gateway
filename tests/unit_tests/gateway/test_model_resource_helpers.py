"""Gateway 侧 config 副本的模型资源辅助函数回归。

`agent_gateway/common/config.py` 是从 jiuwenswarm 裁剪出的副本，
`upsert_model_resource()` 依赖的三个私有辅助函数曾在裁剪时丢失，
导致 Web 端 "models.upsert" 直接 NameError。
"""

from __future__ import annotations

import pytest

from agent_gateway.common import config


class _FakeCrypto:
    """最小可用的 CryptoProvider 替身：enc(x) 形式便于断言往返。"""

    def __init__(self) -> None:
        self.encrypt_calls: list[str] = []
        self.decrypt_calls: list[str] = []

    def encrypt(self, plaintext: str, **_kwargs) -> str:
        self.encrypt_calls.append(plaintext)
        return f"enc({plaintext})"

    def decrypt(self, ciphertext: str, **_kwargs) -> str:
        self.decrypt_calls.append(ciphertext)
        if ciphertext.startswith("enc(") and ciphertext.endswith(")"):
            return ciphertext[4:-1]
        return ciphertext


def _entry_with_key(api_key: str) -> dict:
    return {"model_id": "mdl_1", "model_client_config": {"api_key": api_key}}


@pytest.fixture()
def fake_crypto(monkeypatch):
    crypto = _FakeCrypto()
    monkeypatch.setattr(config, "get_crypto_provider", lambda: crypto)
    return crypto


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


class TestProtectModelKey:
    """凭据加密/保留契约：api_key 是 write-only 字段，任何分支都不能丢失已存密文。"""

    def test_absent_api_key_is_left_untouched(self, fake_crypto):
        update = {"model_client_config": {"model_name": "gpt-x"}}
        config._protect_model_key(update, _entry_with_key("enc(old)"))
        assert "api_key" not in update["model_client_config"]
        assert fake_crypto.encrypt_calls == []

    def test_unchanged_ciphertext_is_not_reencrypted(self, fake_crypto):
        update = _entry_with_key("enc(old)")
        config._protect_model_key(update, _entry_with_key("enc(old)"))
        assert update["model_client_config"]["api_key"] == "enc(old)"
        assert fake_crypto.encrypt_calls == []

    def test_resubmitted_plaintext_keeps_previous_ciphertext(self, fake_crypto):
        current = _entry_with_key("enc(old-secret)")
        update = _entry_with_key("old-secret")
        config._protect_model_key(update, current)
        assert update["model_client_config"]["api_key"] == "enc(old-secret)"
        assert fake_crypto.encrypt_calls == []
        # 解密走深拷贝，不能把内存里的密文改写成明文
        assert current["model_client_config"]["api_key"] == "enc(old-secret)"

    def test_changed_plaintext_is_encrypted_once(self, fake_crypto):
        update = _entry_with_key("new-secret")
        config._protect_model_key(update, _entry_with_key("enc(old-secret)"))
        assert update["model_client_config"]["api_key"] == "enc(new-secret)"
        assert fake_crypto.encrypt_calls == ["new-secret"]

    def test_blank_api_key_preserves_previous_ciphertext(self, fake_crypto):
        update = _entry_with_key("")
        config._protect_model_key(update, _entry_with_key("enc(old-secret)"))
        assert update["model_client_config"]["api_key"] == "enc(old-secret)"
        assert fake_crypto.encrypt_calls == []

    def test_blank_api_key_without_previous_drops_the_field(self, fake_crypto):
        update = _entry_with_key("")
        config._protect_model_key(update, {})
        assert "api_key" not in update["model_client_config"]

    def test_without_crypto_provider_value_is_stored_as_submitted(self, monkeypatch):
        monkeypatch.setattr(config, "get_crypto_provider", lambda: None)
        update = _entry_with_key("new-secret")
        config._protect_model_key(update, _entry_with_key("enc(old-secret)"))
        assert update["model_client_config"]["api_key"] == "new-secret"

    def test_crypto_provider_failure_degrades_to_submitted_value(self, monkeypatch):
        def _broken():
            raise RuntimeError("provider broken")

        monkeypatch.setattr(config, "get_crypto_provider", _broken)
        update = _entry_with_key("new-secret")
        config._protect_model_key(update, _entry_with_key("enc(old-secret)"))
        assert update["model_client_config"]["api_key"] == "new-secret"


class TestNormalizeModelEntryProviderFallback:
    @pytest.mark.parametrize("blank", [None, ""])
    def test_blank_model_provider_falls_back_to_client_provider(self, blank):
        saved = config._normalize_model_entry(
            {"model_id": "mdl_1", "model_provider": blank, "client_provider": "openai"}
        )
        assert saved["model_client_config"]["client_provider"] == "openai"
        assert "model_provider" not in saved
        assert "client_provider" not in saved

    def test_model_provider_wins_when_present(self):
        saved = config._normalize_model_entry(
            {"model_id": "mdl_1", "model_provider": "azure", "client_provider": "openai"}
        )
        assert saved["model_client_config"]["client_provider"] == "azure"
        assert "client_provider" not in saved


class TestNormalizeModelEntryVerifySsl:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("false", False),
            ("0", False),
            ("off", False),
            ("", False),
            ("true", True),
            (True, True),
            (False, False),
        ],
    )
    def test_verify_ssl_is_parsed_instead_of_coerced(self, raw, expected):
        saved = config._normalize_model_entry({"model_id": "mdl_1", "verify_ssl": raw})
        assert saved["model_client_config"]["verify_ssl"] is expected
