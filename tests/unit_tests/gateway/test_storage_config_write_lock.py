# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
import asyncio
import multiprocessing
from contextlib import contextmanager

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
async def test_waiting_yaml_write_does_not_block_session_map_json(
    personal_store,
    monkeypatch,
):
    store, path = personal_store
    monkeypatch.setattr(store, "_file_lock_timeout", 2.0)

    waiting = asyncio.Event()
    loop = asyncio.get_running_loop()
    original_extra_lock = store._extra_write_locks[path]

    @contextmanager
    def observed_extra_lock(timeout):
        loop.call_soon_threadsafe(waiting.set)
        with original_extra_lock(timeout):
            yield

    monkeypatch.setitem(store._extra_write_locks, path, observed_extra_lock)
    mapping = {"identity_key": "one", "session_id": "two"}

    async def session_json_ops():
        assert await store.create("session_map", mapping) == mapping
        assert await store.list("session_map") == [mapping]
        assert await store.get(
            "session_map", {"identity_key": "one"}
        ) == mapping

    config_lock = config.config_write_lock(config_path=path)
    config_lock.__enter__()
    yaml_task = None
    json_task = None
    ordered = False
    yaml_result = None
    yaml_error = None
    try:
        yaml_task = asyncio.create_task(
            store.update("logging_config", {}, {"level": "DEBUG"})
        )
        await asyncio.wait_for(waiting.wait(), timeout=2.0)

        json_task = asyncio.create_task(session_json_ops())
        done, _ = await asyncio.wait(
            {yaml_task, json_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        ordered = json_task in done and yaml_task not in done
    finally:
        config_lock.__exit__(None, None, None)

        if json_task is not None:
            await asyncio.wait_for(json_task, timeout=2.0)
        if yaml_task is not None:
            try:
                yaml_result = await asyncio.wait_for(yaml_task, timeout=2.0)
            except Exception as exc:  # noqa: BLE001
                yaml_error = exc

    assert ordered, "session_map JSON must finish while YAML write is waiting"
    assert yaml_error is None
    assert yaml_result == {"level": "DEBUG"}
    assert config.load_yaml_round_trip(path)["logging"]["level"] == "DEBUG"


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
