# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Tests for shared IM attachment persistence support."""

from __future__ import annotations

import pytest

from agent_gateway.channel_manager.im_platforms.attachment_persistence import (
    AttachmentPersistenceMixin,
    safe_attachment_filename,
)
from agent_gateway.channel_manager.im_platforms.errors import (
    AttachmentPersistError,
)


class _AttachmentService(AttachmentPersistenceMixin):
    def __init__(self) -> None:
        self._initialize_attachment_persistence()


@pytest.mark.asyncio
async def test_persistence_delegates_to_injected_hook() -> None:
    calls: list[tuple[bytes, str, str]] = []

    async def persist(content: bytes, category: str, filename: str) -> dict[str, object]:
        calls.append((content, category, filename))
        return {"path": "/agent/files/report.pdf", "size": len(content)}

    service = _AttachmentService()
    service.set_persist_hook(persist)

    result = await service._persist_downloaded_file(b"report", "files", "report.pdf")

    assert result == {"path": "/agent/files/report.pdf", "size": 6}
    assert calls == [(b"report", "files", "report.pdf")]


@pytest.mark.asyncio
async def test_persistence_requires_agent_server_hook() -> None:
    service = _AttachmentService()

    with pytest.raises(AttachmentPersistError, match="钩子未注入"):
        await service._persist_downloaded_file(b"report", "files", "report.pdf")


@pytest.mark.asyncio
async def test_persistence_wraps_unexpected_hook_errors() -> None:
    async def persist(_content: bytes, _category: str, _filename: str) -> dict[str, object]:
        raise RuntimeError("AgentServer is unavailable")

    service = _AttachmentService()
    service.set_persist_hook(persist)

    with pytest.raises(AttachmentPersistError, match="AgentServer is unavailable"):
        await service._persist_downloaded_file(b"report", "files", "report.pdf")


@pytest.mark.asyncio
async def test_persistence_preserves_classified_errors() -> None:
    classified = AttachmentPersistError("retry attachment delivery")

    async def persist(_content: bytes, _category: str, _filename: str) -> dict[str, object]:
        raise classified

    service = _AttachmentService()
    service.set_persist_hook(persist)

    with pytest.raises(AttachmentPersistError) as raised:
        await service._persist_downloaded_file(b"report", "files", "report.pdf")

    assert raised.value is classified


def test_safe_filename_replaces_platform_unsafe_characters_and_bounds_length() -> None:
    unsafe = 'report<>:"/\\|?*\x00\x1f' + "x" * 100

    result = safe_attachment_filename(unsafe)

    assert result.startswith("report__________")
    assert len(result) == 100
