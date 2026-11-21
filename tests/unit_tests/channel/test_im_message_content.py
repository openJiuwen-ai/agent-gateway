import pytest

from agent_gateway.channel_manager.im_platforms.message_content import (
    select_message_content,
)


@pytest.mark.parametrize(
    ("params", "payload", "expected"),
    [
        ({"content": " params "}, {"content": "payload"}, " params "),
        ({}, {"content": "payload"}, "payload"),
        ({"content": {"output": "rendered"}}, {}, "rendered"),
        ({"content": {"value": 1}}, {}, '{"value": 1}'),
        ({}, {"content": {"output": "x"}}, "x"),
        ({}, {"content": {"value": "\u4e2d\u6587"}}, '{"value": "\u4e2d\u6587"}'),
        ({}, {"content": {"output": None}}, None),
        ({"content": {"value": b"raw"}}, {}, "{'value': b'raw'}"),
        ({"content": ""}, {"content": "fallback"}, "fallback"),
        (None, {"content": "payload"}, "payload"),
        ({"content": "ignored"}, None, "ignored"),
    ],
)
def test_select_message_content(params, payload, expected) -> None:
    assert select_message_content(params, payload) == expected
