# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Session observers only receive events with a usable conversation identity."""

from types import SimpleNamespace

import pytest

from agent_gateway.channel_manager.web.web_connect import WebChannel


@pytest.mark.parametrize("event", ["chat.processing_status", "chat.ask_user_question"])
@pytest.mark.parametrize("envelope_sid,payload_sid,expected", [
    (None, None, None), ("", "", None), ("fallback", None, "fallback"),
    ("envelope", "payload", "payload"),
])
def test_session_observers_require_session_identity(event, envelope_sid, payload_sid, expected):
    channel = WebChannel.__new__(WebChannel)
    channel._session_busy = {}
    channel._expert_processing_requests = {}
    channel._session_event_listeners = []
    seen = []
    channel.add_session_event_listener(lambda *args: seen.append(args))
    payload = {"event_type": event, "session_id": payload_sid, "is_processing": True}
    channel._track_session_busy(SimpleNamespace(
        type="event", event_type=None, session_id=envelope_sid, payload=payload,
    ))
    assert seen == ([(event, expected, payload)] if expected else [])
    assert None not in channel._session_busy and "" not in channel._session_busy
