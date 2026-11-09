# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from unittest.mock import Mock
from urllib.parse import urlsplit

import pytest

from agent_gateway.common.obs_files import AttachmentStore, download_object_key, object_prefix


@pytest.fixture
def obs_env(monkeypatch):
    monkeypatch.setenv("OBS_ACCESS_KEY", "test-access")
    monkeypatch.setenv("OBS_SECRET_KEY", "test-secret")
    monkeypatch.setenv("OBS_BUCKET", "artifacts")
    monkeypatch.setenv("OBS_REGION", "cn-north-4")
    monkeypatch.setenv("OBS_SECURE", "true")
    monkeypatch.setenv("OBS_URL", "obs.cn-north-4.myhuaweicloud.com")


@pytest.mark.parametrize(
    ("endpoint", "bucket", "host", "path_prefix"),
    [
        (
            "obs.cn-north-4.myhuaweicloud.com",
            "artifacts",
            "artifacts.obs.cn-north-4.myhuaweicloud.com",
            "/",
        ),
        (
            "https://ARTIFACTS.obs.cn-north-4.myhuaweicloud.com.:443/",
            "artifacts",
            "artifacts.obs.cn-north-4.myhuaweicloud.com",
            "/",
        ),
        (
            "obs.cn-north-4.myhuaweicloud.com:9443",
            "obs",
            "obs.obs.cn-north-4.myhuaweicloud.com:9443",
            "/",
        ),
        ("http://minio.local:9000", "artifacts", "minio.local:9000", "/artifacts/"),
    ],
)
def test_presigned_urls_keep_the_correct_bucket_and_object(
    obs_env, monkeypatch, endpoint, bucket, host, path_prefix
):
    monkeypatch.setenv("OBS_URL", endpoint)
    monkeypatch.setenv("OBS_BUCKET", bucket)
    store = AttachmentStore.from_env()

    key = download_object_key("tenant", "a" * 32, "report 你好.txt")
    signed_url = store.presign_download(key)
    parts = urlsplit(signed_url)
    assert parts.netloc == host
    assert parts.path.startswith(path_prefix + "downloads/")
    assert store.download_key(signed_url, "tenant") == key

    upload_prefix = object_prefix("group", "bot", "user")
    upload_key = f"{upload_prefix}{'b' * 32}/report 你好.txt"
    upload_url = store.client.presigned_get_object(bucket, upload_key)
    delete = Mock()
    monkeypatch.setattr(store.client, "remove_object", delete)
    store.remove_upload(upload_url, upload_prefix)
    delete.assert_called_once_with(bucket, upload_key)


def test_obs_keeps_exact_regional_path_links_compatible(obs_env):
    store = AttachmentStore.from_env()
    key = download_object_key("tenant", "a" * 32, "report.txt")
    legacy_url = f"https://obs.cn-north-4.myhuaweicloud.com/artifacts/{key}"
    assert store.download_key(legacy_url, "tenant") == key


@pytest.mark.parametrize(
    "origin",
    [
        "https://another-bucket.obs.cn-north-4.myhuaweicloud.com",
        "https://artifacts.obs.cn-north-4.myhuaweicloud.com.example.org",
        "https://artifacts.obs.cn-north-4.myhuaweicloud.com:9443",
        "http://artifacts.obs.cn-north-4.myhuaweicloud.com",
        "https://user@artifacts.obs.cn-north-4.myhuaweicloud.com",
    ],
)
def test_only_configured_bucket_origin_may_read_or_delete(obs_env, monkeypatch, origin):
    store = AttachmentStore.from_env()
    key = download_object_key("tenant", "a" * 32, "report.txt")
    delete = Mock()
    monkeypatch.setattr(store.client, "remove_object", delete)
    upload_prefix = object_prefix("group", "bot", "user")

    with pytest.raises(ValueError, match="configured object store"):
        store.download_key(f"{origin}/{key}", "tenant")
    with pytest.raises(ValueError, match="configured object store"):
        store.remove_upload(f"{origin}/{upload_prefix}{'b' * 32}/report.txt", upload_prefix)
    delete.assert_not_called()


def test_bucket_domain_does_not_bypass_scope_checks(obs_env, monkeypatch):
    store = AttachmentStore.from_env()
    key = download_object_key("another-tenant", "a" * 32, "report.txt")
    with pytest.raises(ValueError, match="current tenant"):
        store.download_key(store.presign_download(key), "tenant")

    delete = Mock()
    monkeypatch.setattr(store.client, "remove_object", delete)
    prefix = object_prefix("group", "bot", "user")
    foreign_prefix = object_prefix("group", "bot", "another-user")
    foreign = store.client.presigned_get_object(store.bucket, f"{foreign_prefix}{'b' * 32}/report.txt")
    own = store.client.presigned_get_object(store.bucket, f"{prefix}{'b' * 32}/report.txt")
    traversal = own.replace("/report.txt?", "/%2e%2e/report.txt?", 1)
    for url in (foreign, traversal):
        with pytest.raises(ValueError, match="current context"):
            store.remove_upload(url, prefix)
    delete.assert_not_called()
