# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""WeLink delivery checkpoints must survive restarts and failed writes."""

import hashlib
import json
import os
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

import pytest

from agent_gateway.channel_manager.im_platforms.welink.welink_connect import (
    WelinkChannel,
)
from agent_gateway.channel_manager.im_platforms.welink.welink_messages import (
    WelinkHistoryMessage,
)
from agent_gateway.channel_manager.im_platforms.welink.welink_reader import WelinkReader


@pytest.mark.parametrize("legacy", [False, True])
def test_restart_reads_utf8_checkpoints_independently_of_locale(tmp_path, monkeypatch, legacy):
    message = WelinkHistoryMessage(message_id="1", group_id="g", sender_id="u", sender_name="U", text="café")
    channels = {"ak": SimpleNamespace(config=SimpleNamespace(agent_id="mate"))}
    if legacy:
        digest = hashlib.sha256(b"reader").hexdigest()[:24]
        WelinkChannel._save_pending(tmp_path / f"pending-{digest}.json", [message])
    else:
        jobs = [{"message": asdict(message), "targets": ["ak"], "target_agents": {"ak": "mate"}, "commit": True}]
        (tmp_path / "fanout-pending.json").write_text(json.dumps(jobs, ensure_ascii=False), encoding="utf-8")

    # Model a non-UTF-8 host even when this test runs in Python's UTF-8 mode.
    read_text = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda path, encoding=None: read_text(path, encoding=encoding or "ascii"))

    reader = WelinkReader(object(), "reader", str(tmp_path), 4, channels)
    assert reader._pending[0]["message"]["text"] == message.text
    assert reader._pending[0]["target_agents"] == {"ak": "mate"}


def test_failed_checkpoint_write_leaves_only_last_complete_batch(tmp_path, monkeypatch):
    path = tmp_path / "pending.json"
    original = WelinkHistoryMessage(message_id="1", group_id="g", sender_id="u", sender_name="U", text="sent")
    WelinkChannel._save_pending(path, [original])
    saved = path.read_bytes()

    def fail_sync(fd):
        raise OSError("checkpoint sync failed")

    monkeypatch.setattr(os, "fsync", fail_sync)
    with pytest.raises(OSError, match="checkpoint sync failed"):
        WelinkChannel._save_pending(path, [])
    assert path.read_bytes() == saved
    assert list(tmp_path.iterdir()) == [path]


def test_overlapping_checkpoint_writes_publish_their_own_batch(tmp_path, monkeypatch):
    path = tmp_path / "pending.json"
    replace = Path.replace

    def overlap(staged, target):
        # Complete a second write while the first one is ready to replace the destination.
        monkeypatch.setattr(Path, "replace", replace)
        WelinkReader._save(path, ["second"])
        assert json.loads(path.read_bytes()) == ["second"]
        return replace(staged, target)

    monkeypatch.setattr(Path, "replace", overlap)
    WelinkReader._save(path, ["first"])
    assert json.loads(path.read_bytes()) == ["first"]
    assert list(tmp_path.iterdir()) == [path]
