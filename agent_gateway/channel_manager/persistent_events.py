"""Normalized platform facts transported from Gateway to the persistent agent service."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


class InboundDisposition(str, Enum):
    """Whether the persistent path claimed, accepted, or rejected an event."""

    UNCLAIMED = "unclaimed"
    HANDLED = "handled"
    REJECTED = "rejected"
    RETRYABLE = "retryable"


@dataclass(frozen=True, kw_only=True)
class FeishuInboundEvent:
    """The minimum normalized Feishu facts needed to accept a Request."""

    receiving_app_id: str
    expected_agent_id: str | None = None
    message_id: str | None
    chat_id: str | None
    chat_type: str | None
    sender_type: str | None
    sender_id: str | None
    sender_name: str | None
    sender_app_id: str | None
    received_at: datetime | None
    text: str | None
    mention_ids: tuple[str, ...] = ()
    receiver_bot_mentioned: bool = False
    reply_to: str | None = None
    attachment_refs: tuple[str, ...] = ()
    event_kind: str = "message"
    event_id: str | None = None
    changed_at: datetime | None = None
    group_name: str | None = None
    # The delivery archive may already hold this platform id with the raw handoff token.
    allow_existing_message: bool = False

    def to_payload(self) -> dict[str, Any]:
        """Return JSON-safe normalized facts for the internal Gateway E2A request."""
        return {
            "receiving_app_id": self.receiving_app_id,
            "expected_agent_id": self.expected_agent_id,
            "message_id": self.message_id,
            "chat_id": self.chat_id,
            "chat_type": self.chat_type,
            "sender_type": self.sender_type,
            "sender_id": self.sender_id,
            "sender_name": self.sender_name,
            "sender_app_id": self.sender_app_id,
            "received_at": self.received_at.isoformat() if isinstance(self.received_at, datetime) else None,
            "text": self.text,
            "mention_ids": list(self.mention_ids),
            "receiver_bot_mentioned": self.receiver_bot_mentioned,
            "reply_to": self.reply_to,
            "attachment_refs": list(self.attachment_refs),
            "event_kind": self.event_kind,
            "event_id": self.event_id,
            "changed_at": self.changed_at.isoformat() if self.changed_at else None,
            "group_name": self.group_name,
        }


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"missing DingTalk {field}")
    return value.strip()


@dataclass(frozen=True, kw_only=True)
class DingTalkInboundEvent:
    receiving_app_id: str
    expected_agent_id: str
    message_id: str
    conversation_id: str
    sender_staff_id: str
    sender_name: str
    text: str
    received_at: datetime
    conversation_type: str = "1"
    receiver_bot_mentioned: bool = False
    mention_ids: tuple[str, ...] = ()
    group_name: str | None = None
    robot_code: str | None = None
    sender_corp_id: str | None = None
    chatbot_corp_id: str | None = None
    platform_created_at: datetime | None = None

    def __post_init__(self) -> None:
        for field in (
            "receiving_app_id", "expected_agent_id", "message_id", "conversation_id",
            "sender_staff_id", "sender_name", "text",
        ):
            _text(getattr(self, field), field)
        if not isinstance(self.received_at, datetime) or self.received_at.tzinfo is None:
            raise ValueError("DingTalk received_at must be timezone-aware")
        if self.platform_created_at is not None and (
            not isinstance(self.platform_created_at, datetime) or self.platform_created_at.utcoffset() is None
            or self.platform_created_at > self.received_at + timedelta(minutes=5)
        ):
            raise ValueError("invalid DingTalk platform creation time")
        if self.conversation_type not in {"1", "2"}:
            raise ValueError("invalid DingTalk conversation type")
        if not isinstance(self.receiver_bot_mentioned, bool):
            raise ValueError("invalid DingTalk mention flag")
        if not isinstance(self.mention_ids, tuple) or any(not isinstance(item, str) or not item.strip()
                                                          for item in self.mention_ids):
            raise ValueError("invalid DingTalk mention IDs")
        if self.group_name is not None and not isinstance(self.group_name, str):
            raise ValueError("invalid DingTalk group name")
        for field in ("robot_code", "sender_corp_id", "chatbot_corp_id"):
            if getattr(self, field) is not None:
                _text(getattr(self, field), field)

    @classmethod
    def from_callback(cls, data: Mapping[str, Any], *, app_id: str, agent_id: str) -> DingTalkInboundEvent:
        conversation_type = str(data.get("conversationType", "")) if isinstance(data, Mapping) else ""
        if (
            not isinstance(data, Mapping) or conversation_type not in {"1", "2"} or data.get("msgtype") != "text"
        ):
            raise ValueError("only DingTalk direct and group text messages are supported")
        content = data.get("text")
        if not isinstance(content, Mapping):
            raise ValueError("invalid DingTalk text")
        staff_id = _text(data.get("senderStaffId"), "senderStaffId")
        mentions = data.get("atUsers", ())
        mention_ids = tuple(dict.fromkeys(
            item.get("dingtalkId") or item.get("staffId")
            for item in mentions
            if isinstance(item, Mapping)
            and isinstance(item.get("dingtalkId") or item.get("staffId"), str)
            and (item.get("dingtalkId") or item.get("staffId")).strip()
        )) if isinstance(mentions, (list, tuple)) else ()
        chatbot_user_id = data.get("chatbotUserId")
        mentions_everyone = any(item.casefold() in {"all", "@_all"} for item in mention_ids)
        # DingTalk's Stream bot-message callback is only delivered for an @ to that bot in a group.
        # Trust this app-scoped callback when its mention metadata is missing or inconsistent.
        bot_mentioned = not mentions_everyone and (
            conversation_type == "2"
            or data.get("isInAtList") is True
            or (isinstance(chatbot_user_id, str) and chatbot_user_id in mention_ids)
        )
        received_at = datetime.now(timezone.utc)
        platform_created_at = None
        created = data.get("createAt")
        if not isinstance(created, bool) and (
            isinstance(created, int) or (isinstance(created, str) and created.isdecimal())
        ):
            try:
                candidate = datetime.fromtimestamp(int(created) / 1000, timezone.utc)
                if candidate.year >= 2000 and candidate <= received_at + timedelta(minutes=5):
                    platform_created_at = candidate
            except (ValueError, OverflowError, OSError):
                pass
        return cls(
            receiving_app_id=app_id, expected_agent_id=agent_id,
            message_id=_text(data.get("msgId"), "msgId"),
            conversation_id=_text(data.get("conversationId"), "conversationId"),
            sender_staff_id=staff_id, sender_name=data.get("senderNick") or staff_id,
            text=_text(content.get("content"), "text"), received_at=received_at,
            conversation_type=conversation_type, receiver_bot_mentioned=bot_mentioned,
            mention_ids=mention_ids,
            group_name=data.get("conversationTitle") if isinstance(data.get("conversationTitle"), str) else None,
            robot_code=data.get("robotCode"), sender_corp_id=data.get("senderCorpId"),
            chatbot_corp_id=data.get("chatbotCorpId"),
            platform_created_at=platform_created_at,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            **asdict(self), "received_at": self.received_at.isoformat(),
            "platform_created_at": self.platform_created_at.isoformat() if self.platform_created_at is not None else None,
        }

    @classmethod
    def from_payload(cls, payload: Any) -> DingTalkInboundEvent:
        if not isinstance(payload, dict):
            raise ValueError("invalid DingTalk inbound payload")
        fields = dict(payload)
        fields["received_at"] = datetime.fromisoformat(fields["received_at"])
        fields["mention_ids"] = tuple(fields.get("mention_ids", ()))
        if fields.get("platform_created_at") is not None:
            fields["platform_created_at"] = datetime.fromisoformat(fields["platform_created_at"])
        return cls(**fields)
