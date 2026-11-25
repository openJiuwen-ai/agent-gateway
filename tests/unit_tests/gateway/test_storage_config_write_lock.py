# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
import multiprocessing

import portalocker
import pytest

from agent_gateway.common import config, paths
from agent_gateway.storage_assembly import setup


@pytest.fixture
def personal_store(tmp_path, monkeypatch):
    path = tmp_path / "config-gateway.yaml"
    path.write_text("logging: {level: INFO}\n", encoding="utf-8")
    monkeypatch.setattr(setup, "is_enterprise", lambda: False)
    monkeypatch.setattr(paths, "get_config_file", lambda: path)
    monkeypatch.setattr(paths, "get_profile_data_dir", lambda: tmp_path)
    monkeypatch.setattr(paths, "get_checkpoint_dir", lambda: tmp_path / ".checkpoint")
    store = setup._create_persistent()
    monkeypatch.setattr(store, "_file_lock_timeout", 0)
    return store, path


@pytest.mark.asyncio
async def test_config_transaction_excludes_yaml_writes_but_not_json(personal_store):
    store, path = personal_store
    with config.config_write_lock(config_path=path):
        document = config.load_yaml_round_trip(path)
        with pytest.raises(TimeoutError):
            await store.update("logging_config", {}, {"level": "DEBUG"})
        mapping = {"identity_key": "one", "session_id": "two"}
        assert await store.create("session_map", mapping) == mapping
        document["other"] = "saved by config transaction"
        config.dump_yaml_round_trip(path, document)

    assert await store.update("logging_config", {}, {"level": "DEBUG"}) == {"level": "DEBUG"}
    assert config.load_yaml_round_trip(path) == {
        "logging": {"level": "DEBUG"}, "other": "saved by config transaction",
    }


@pytest.mark.asyncio
async def test_config_store_still_honors_existing_backend_lock(personal_store):
    store, path = personal_store
    with portalocker.Lock(str(path) + ".lock", timeout=0):
        with pytest.raises(portalocker.exceptions.LockException):
            await store.delete("logging_config", {})
    assert await store.get("logging_config", {}) == {"level": "INFO"}
    assert await store.delete("logging_config", {})


def _hold_config_lock(path, ready, release):
    with config.config_write_lock(config_path=path):
        ready.set()
        release.wait(30)


@pytest.mark.asyncio
async def test_config_transaction_excludes_backend_in_another_process(personal_store):
    store, path = personal_store
    ctx = multiprocessing.get_context("spawn")
    ready, release = ctx.Event(), ctx.Event()
    owner = ctx.Process(target=_hold_config_lock, args=(path, ready, release))
    owner.start()
    try:
        assert ready.wait(20), "config writer did not acquire its lock"
        with pytest.raises(portalocker.exceptions.LockException):
            await store.create("channel_config", {"id": "web"})
    finally:
        release.set()
        owner.join(10)
        if owner.is_alive():
            owner.terminate()
            owner.join(10)
    assert owner.exitcode == 0
    assert await store.create("channel_config", {"id": "web"}) == {"id": "web"}
