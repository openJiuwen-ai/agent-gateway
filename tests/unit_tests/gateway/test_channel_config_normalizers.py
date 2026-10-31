from agent_gateway.channel_manager.web.channel_config_normalizers import (
    _merge_apps_by_id,
    _normalize_feishu_conf,
    _normalize_xiaoyi_conf,
    _validate_wechat_numeric_params,
)


def test_normalize_feishu_conf_wraps_flat_config_into_apps():
    normalized = _normalize_feishu_conf({"app_id": "app-1", "app_secret": "secret"})

    assert normalized["apps"] == [
        {
            "name": "默认应用",
            "is_default": True,
            "enabled": True,
            "app_id": "app-1",
            "app_secret": "secret",
            "encrypt_key": "",
            "verification_token": "",
            "allow_from": ["0.0.0.0/0"],
            "enable_streaming": True,
            "group_digital_avatar": False,
            "my_user_id": "",
            "bot_name": "",
            "enable_memory": False,
        }
    ]


def test_normalize_xiaoyi_conf_rejects_non_dict():
    assert _normalize_xiaoyi_conf(None) == {"apps": []}


def test_merge_apps_by_id_preserves_existing_sensitive_fields():
    existing = [{"app_id": "app-1", "app_secret": "old"}]
    incoming = [{"app_id": "app-1", "bot_name": "renamed"}]

    merged = _merge_apps_by_id(incoming, existing)

    assert merged == [{"app_id": "app-1", "app_secret": "old", "bot_name": "renamed"}]


def test_validate_wechat_numeric_params_bounds_and_relation():
    assert _validate_wechat_numeric_params({"long_poll_timeout_sec": 0.5}) == (
        "long_poll_timeout_sec 需为整数"
    )
    assert _validate_wechat_numeric_params(
        {"backoff_base_sec": 2.0, "backoff_max_sec": 1.0}
    ) == "backoff_max_sec 不得小于 backoff_base_sec"
