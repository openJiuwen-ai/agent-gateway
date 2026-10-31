# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Pure streaming task state helpers extracted from MessageHandler."""

from __future__ import annotations

from collections.abc import Mapping

from agent_gateway.message_handler.models import ChannelMode


def is_chat_stream(
    stream_emits_processing_status: Mapping[str, bool], rid: str
) -> bool:
    """Return whether a stream counts as a running user chat task."""
    return bool(stream_emits_processing_status.get(rid))


def iter_active_stream_modes(
    stream_modes: Mapping[str, str],
    stream_emits_processing_status: Mapping[str, bool],
    active_chat_tasks: Mapping[str, str],
) -> list[str]:
    """Return a snapshot of modes for running user chat tasks."""
    return [
        mode
        for rid, mode in stream_modes.items()
        if is_chat_stream(stream_emits_processing_status, rid)
    ] + [*active_chat_tasks.values()]


def has_active_streams(
    stream_modes: Mapping[str, str],
    stream_emits_processing_status: Mapping[str, bool],
    active_chat_tasks: Mapping[str, str],
) -> bool:
    """Return whether any non-team user chat task is running."""
    return any(
        not ChannelMode.is_team_mode(mode)
        for mode in iter_active_stream_modes(
            stream_modes,
            stream_emits_processing_status,
            active_chat_tasks,
        )
    )


def active_chat_request_ids(
    stream_modes: Mapping[str, str],
    stream_emits_processing_status: Mapping[str, bool],
    active_chat_tasks: Mapping[str, str],
) -> frozenset[str]:
    """Return request IDs of all running user chat tasks."""
    return frozenset(
        [
            rid
            for rid in stream_modes
            if is_chat_stream(stream_emits_processing_status, rid)
        ]
        + [*active_chat_tasks]
    )


def active_non_team_modes(
    stream_modes: Mapping[str, str],
    stream_emits_processing_status: Mapping[str, bool],
    active_chat_tasks: Mapping[str, str],
) -> list[tuple[str, str]]:
    """Return non-team running user chat task details for diagnostics."""
    result: list[tuple[str, str]] = []
    for rid, mode in stream_modes.items():
        if is_chat_stream(stream_emits_processing_status, rid) and not ChannelMode.is_team_mode(mode):
            result.append((rid, mode))
    for rid, mode in active_chat_tasks.items():
        if not ChannelMode.is_team_mode(mode):
            result.append((rid, mode))
    return result
