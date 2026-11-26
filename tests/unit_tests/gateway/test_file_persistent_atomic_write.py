# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
from pathlib import Path

import pytest
from ruamel.yaml.representer import RepresenterError

from agent_gateway.storage.backends.file_persistent import FilePersistentBackend
from agent_gateway.storage.registry.store_registry import FileLayout, StoreLayout, StoreRegistry


@pytest.mark.asyncio
@pytest.mark.parametrize("file_format", ["json", "yaml"])
@pytest.mark.parametrize("failure", ["serialize", "replace"])
async def test_failed_write_preserves_last_committed_record(tmp_path, monkeypatch, file_format, failure):
    path = tmp_path / f"records.{file_format}"
    layout = FileLayout(path=str(path), format=file_format, key_fields=("id",))
    registry = StoreRegistry()
    registry.register("records", StoreLayout(file=layout))
    notifications = []
    backend = FilePersistentBackend(registry=registry, on_write=notifications.append)
    original = {"id": "one", "value": "saved"}
    await backend.create("records", original)
    assert notifications == ([path] if file_format == "yaml" else [])
    notifications.clear()
    before = path.read_bytes()

    if failure == "replace":
        def reject_replace(self, target):
            raise PermissionError("cannot replace destination")

        monkeypatch.setattr(Path, "replace", reject_replace)
        error = PermissionError
        value = "updated"
    else:
        error = TypeError if file_format == "json" else RepresenterError
        value = object()

    with pytest.raises(error):
        await backend.update("records", {"id": "one"}, {"value": value})

    assert path.read_bytes() == before
    assert await backend.get("records", {"id": "one"}) == original
    assert not list(tmp_path.glob("*.tmp"))
    assert not notifications
