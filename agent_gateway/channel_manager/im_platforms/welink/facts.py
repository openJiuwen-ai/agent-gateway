"""Gateway wire facts and reader credential location for WeLink."""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True, kw_only=True)
class WelinkInboundEvent:
    account: str
    expected_agent_id: str
    message_id: str
    chat_id: str
    sender_id: str
    sender_name: str
    text: str
    received_at: datetime
    chat_type: str = "group"
    local_files: tuple[str, ...] = ()
    assistant_credential_id: str = ""
    mentioned: bool = False
    sender_kind: str = "human"
    first_observation: bool = False
    visible_from: datetime | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "account": self.account,
            "assistant_credential_id": self.assistant_credential_id,
            "mentioned": self.mentioned,
            "sender_kind": self.sender_kind,
            "first_observation": self.first_observation,
            "visible_from": self.visible_from.isoformat() if self.visible_from else None,
            "expected_agent_id": self.expected_agent_id,
            "message_id": self.message_id,
            "chat_id": self.chat_id,
            "chat_type": self.chat_type,
            "sender_id": self.sender_id,
            "sender_name": self.sender_name,
            "text": self.text,
            "received_at": self.received_at.isoformat(),
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> WelinkInboundEvent:
        if not isinstance(payload, Mapping):
            raise ValueError("WeLink event payload must be a mapping")
        received_at = payload.get("received_at")
        if isinstance(received_at, str):
            received_at = datetime.fromisoformat(received_at.replace("Z", "+00:00"))
        if not isinstance(received_at, datetime):
            received_at = datetime.now(timezone.utc)
        if received_at.tzinfo is None:
            received_at = received_at.replace(tzinfo=timezone.utc)
        visible_from = payload.get("visible_from")
        if isinstance(visible_from, str):
            visible_from = datetime.fromisoformat(visible_from.replace("Z", "+00:00"))
        if visible_from is not None and (not isinstance(visible_from, datetime) or visible_from.utcoffset() is None):
            raise ValueError("invalid WeLink readable boundary")
        raw_files = payload.get("local_files") or ()
        if isinstance(raw_files, str):
            raw_files = (raw_files,)
        local_files = tuple(str(item) for item in raw_files if isinstance(item, str) and item.strip())
        return cls(
            account=str(payload.get("account") or ""),
            expected_agent_id=str(payload.get("expected_agent_id") or ""),
            message_id=str(payload.get("message_id") or ""),
            chat_id=str(payload.get("chat_id") or ""),
            sender_id=str(payload.get("sender_id") or ""),
            sender_name=str(payload.get("sender_name") or ""),
            text=str(payload.get("text") or ""),
            received_at=received_at,
            chat_type=str(payload.get("chat_type") or "group"),
            local_files=local_files,
            assistant_credential_id=str(payload.get("assistant_credential_id") or ""),
            mentioned=payload.get("mentioned") is True,
            sender_kind=str(payload.get("sender_kind") or "human"),
            first_observation=payload.get("first_observation") is True,
            visible_from=visible_from,
        )


def welink_connection_key(account: str, agent_id: str = "") -> str:
    """The account as a connection key; login treats spellings that differ only in case as one account."""
    key = account.strip().casefold()
    return f"{key}:{agent_id.strip()}" if agent_id.strip() else key


def credential_dir_for(agent_id: str, configured: str = "") -> Path:
    if isinstance(configured, str) and configured.strip():
        return Path(configured).expanduser()
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", agent_id)[:80] or "account"
    return Path.home() / ".jiuwenswarm" / "welink" / safe
