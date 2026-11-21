# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

from e2a.message import Message
from agent_gateway.channel_manager.web.web_connect import WebChannel


def test_macro_events_preserve_complete_payload() -> None:
    """macro.* frames keep the routing fields the UI needs."""
    payload = {
        "event_type": "macro.routing",
        "mode": "team",
        "mode_label": "Cluster Mode",
        "confidence": 0.91,
        "rationale": "query needs several roles",
        "source": "rules",
    }
    message = Message(
        id="macro-routing",
        type="event",
        channel_id="web",
        session_id="macro-session",
        params={},
        timestamp=0.0,
        ok=True,
        payload=payload,
    )

    assert WebChannel._build_event_payload(message, "macro.routing") == {
        **payload,
        "session_id": "macro-session",
    }
