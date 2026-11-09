"""Typed, app-bound DingTalk Stream group facts; no chat task is created here."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

GROUP_EVENT_TYPES = frozenset({
    "chat_add_member", "chat_remove_member", "chat_quit", "chat_update_title", "chat_disband",
})


def _text(value, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 512:
        raise ValueError(f"invalid DingTalk group {name}")
    return value.strip()


@dataclass(frozen=True, kw_only=True)
class DingTalkGroupEvent:
    receiving_app_id: str
    expected_agent_id: str
    event_type: str
    event_id: str
    conversation_id: str
    changed_at: datetime
    received_at: datetime
    corp_id: str
    title: str | None = None
    operator: str = "lifecycle"

    def __post_init__(self):
        for name in ("receiving_app_id", "expected_agent_id", "event_id", "conversation_id", "corp_id", "operator"):
            _text(getattr(self, name), name)
        if self.event_type not in GROUP_EVENT_TYPES:
            raise ValueError("unsupported DingTalk group event")
        for stamp in (self.changed_at, self.received_at):
            if not isinstance(stamp, datetime) or stamp.utcoffset() is None or stamp.year < 2000:
                raise ValueError("invalid DingTalk group event time")
        if self.changed_at > self.received_at + timedelta(minutes=5):
            raise ValueError("future DingTalk group event")
        if self.title is not None:
            _text(self.title, "title")
        if self.event_type == "chat_update_title" and self.title is None:
            raise ValueError("missing DingTalk group title")

    @property
    def sender_staff_id(self) -> str:
        """Transport metadata only; never a permission approver or user request."""
        return self.operator

    @classmethod
    def from_stream(cls, raw, *, app_id: str, agent_id: str, headers=None):
        if not isinstance(raw, Mapping):
            raise ValueError("invalid DingTalk event data")
        body = raw.get("data", raw)
        if not isinstance(body, Mapping):
            raise ValueError("invalid DingTalk group event body")
        header_type = getattr(headers, "event_type", None)
        event_type = raw.get("eventType") or header_type
        if header_type is not None and header_type != event_type:
            raise ValueError("conflicting DingTalk group event type")
        if body.get("eventType", event_type) != event_type:
            raise ValueError("conflicting DingTalk group event body")
        corp_ids = [value for value in (raw.get("eventCorpId"), getattr(headers, "event_corp_id", None),
                                       body.get("corpId")) if value is not None]
        if not corp_ids or any(value != corp_ids[0] for value in corp_ids):
            raise ValueError("conflicting DingTalk group event corporation")
        event_id = raw.get("eventId") or getattr(headers, "event_id", None)
        if getattr(headers, "event_id", None) not in (None, event_id):
            raise ValueError("conflicting DingTalk group event identity")
        stamp = body.get("timeStamp", raw.get("eventBornTime", getattr(headers, "event_born_time", None)))
        if isinstance(stamp, bool) or not str(stamp).isdecimal():
            raise ValueError("invalid DingTalk group event timestamp")
        try:
            changed_at = datetime.fromtimestamp(int(stamp) / 1000, timezone.utc)
        except (ValueError, OverflowError, OSError) as exc:
            raise ValueError("invalid DingTalk group event timestamp") from exc
        return cls(
            receiving_app_id=app_id, expected_agent_id=agent_id, event_type=event_type,
            event_id=_text(event_id, "event_id"), conversation_id=_text(body.get("openConversationId"), "group"),
            changed_at=changed_at, received_at=datetime.now(timezone.utc), corp_id=_text(corp_ids[0], "corp"),
            title=body.get("title"), operator=body.get("operator") or "lifecycle",
        )

    def to_payload(self) -> dict:
        return {**asdict(self), "event_kind": "group_lifecycle", "changed_at": self.changed_at.isoformat(),
                "received_at": self.received_at.isoformat()}

    @classmethod
    def from_payload(cls, payload):
        """Decode host payloads with a consistent validation error contract."""
        if not isinstance(payload, dict) or payload.get("event_kind") != "group_lifecycle":
            raise ValueError("invalid DingTalk lifecycle payload")
        try:
            values = {key: value for key, value in payload.items() if key != "event_kind"}
            values["changed_at"] = datetime.fromisoformat(values["changed_at"])
            values["received_at"] = datetime.fromisoformat(values["received_at"])
            return cls(**values)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("invalid DingTalk lifecycle payload") from exc
