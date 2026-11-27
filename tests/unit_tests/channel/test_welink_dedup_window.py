# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""WeLink assistant-link dedup marks expire by time window, not by a count cap."""

from __future__ import annotations

import json
import time

from agent_gateway.channel_manager.im_platforms.welink.welink_assistant_link import (
    AssistantLink,
    Mention,
    _DedupAlias,
    _QaSlot,
)


class _Sock:
    def __init__(self) -> None:
        self.frames: list[dict] = []

    def send(self, payload: str) -> None:
        self.frames.append(json.loads(payload))


def _slot(message_id: str = "m1", created_at: float | None = None) -> _QaSlot:
    return _QaSlot(
        message_id=message_id,
        group_id="g1",
        text="hello",
        tool_session_id="ts-1",
        trace_id="tr-1",
        reply_message_id="msg_notify_1",
        part_id="prt_notify_1",
        opened=True,
        created_at=time.monotonic() if created_at is None else created_at,
        sender_id="u1",
    )


def test_delivered_mark_is_dropped_after_window() -> None:
    link = AssistantLink("id", "secret", dedup_window_seconds=60.0)
    key = ("g1", "m1")

    link._delivered[key] = time.monotonic()
    assert link.claim_polled("g1", "m1", "hello") is True

    link._delivered[key] = time.monotonic() - 61.0
    assert link.claim_polled("g1", "m1", "hello") is False
    assert key not in link._delivered


def test_alias_and_delivered_mark_expire_together() -> None:
    link = AssistantLink("id", "secret", dedup_window_seconds=60.0)
    now = time.monotonic()
    link._aliases[("g1", "hist-1")] = _DedupAlias(
        canonical="m1", created_at=now, completed_at=now
    )
    link._delivered[("g1", "m1")] = now

    assert link.claim_polled("g1", "hist-1", "hello") is True

    link._aliases[("g1", "hist-1")].completed_at = now - 61.0
    link._delivered[("g1", "m1")] = now - 61.0

    assert link.claim_polled("g1", "hist-1", "hello") is False
    assert ("g1", "hist-1") not in link._aliases
    assert ("g1", "m1") not in link._delivered


def test_unanswered_alias_expires_from_creation() -> None:
    link = AssistantLink("id", "secret", dedup_window_seconds=60.0)
    now = time.monotonic()
    link._aliases[("g1", "hist-1")] = _DedupAlias(
        canonical="m1", created_at=now - 61.0
    )

    assert link.claim_polled("g1", "hist-1", "hello") is False
    assert ("g1", "hist-1") not in link._aliases


def test_finishing_reply_stamps_alias_with_completion_time() -> None:
    link = AssistantLink("id", "secret", dedup_window_seconds=60.0)
    sock = _Sock()
    link._slots["m1"] = _slot()
    link._aliases[("g1", "hist-1")] = _DedupAlias(
        canonical="m1", created_at=time.monotonic() - 120.0
    )

    link._send_text(sock, "g1", "answer", "m1")

    entry = link._aliases[("g1", "hist-1")]
    assert entry.completed_at is not None
    assert entry.completed_at > entry.created_at
    assert ("g1", "m1") in link._delivered
    assert link.claim_polled("g1", "hist-1", "hello") is True


def test_session_loop_prunes_expired_dedup_entries() -> None:
    link = AssistantLink("id", "secret", dedup_window_seconds=60.0)
    link._delivered[("g1", "m1")] = time.monotonic() - 61.0
    link._aliases[("g1", "hist-1")] = _DedupAlias(
        canonical="m1", created_at=time.monotonic() - 61.0
    )

    link._expire_slots(_Sock())

    assert link._delivered == {}
    assert link._aliases == {}


def test_duplicate_mention_is_still_suppressed_inside_window() -> None:
    link = AssistantLink("id", "secret", dedup_window_seconds=60.0)
    sock = _Sock()
    link._delivered[("g1", "m1")] = time.monotonic()
    mention = Mention(
        message_id="m1",
        group_id="g1",
        sender_id="u1",
        sender_name="U1",
        text="hello",
        tool_session_id="ts-1",
        trace_id="tr-1",
    )

    link._open_mention(sock, mention)

    assert link._slots == {}
    assert [frame["type"] for frame in sock.frames] == ["tool_done"]


def test_stop_keeps_dedup_marks_for_reconnect() -> None:
    link = AssistantLink("id", "secret", dedup_window_seconds=60.0)
    link._delivered[("g1", "m1")] = time.monotonic()

    link.stop()

    assert ("g1", "m1") in link._delivered
