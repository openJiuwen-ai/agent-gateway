# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Shared persistence support for downloaded IM attachments."""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from typing import Any

from agent_gateway.channel_manager.im_platforms.errors import (
    AttachmentPersistError,
)

AttachmentPersistHook = Callable[
    [bytes, str, str],
    Awaitable[dict[str, Any]],
]


class AttachmentPersistenceMixin:
    """Delegate attachment storage to the current AgentServer context."""

    _persist_hook: AttachmentPersistHook | None

    def _initialize_attachment_persistence(self) -> None:
        self._persist_hook = None

    def set_persist_hook(self, hook: AttachmentPersistHook | None) -> None:
        """Set the hook used to persist downloaded attachment bytes."""
        self._persist_hook = hook

    async def _persist_downloaded_file(
        self,
        content: bytes,
        category: str,
        filename: str,
    ) -> dict[str, Any]:
        """Persist downloaded bytes through the injected AgentServer hook."""
        if self._persist_hook is None:
            raise AttachmentPersistError(
                "IM 附件落盘钩子未注入（AgentServer 上下文缺失）"
            )
        try:
            return await self._persist_hook(content, category, filename)
        except AttachmentPersistError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise AttachmentPersistError(str(exc)) from exc


def safe_attachment_filename(name: str, *, max_length: int = 100) -> str:
    """Replace platform-unsafe filename characters and bound the result."""
    safe = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    return safe[:max_length]


__all__ = [
    "AttachmentPersistHook",
    "AttachmentPersistenceMixin",
    "safe_attachment_filename",
]
