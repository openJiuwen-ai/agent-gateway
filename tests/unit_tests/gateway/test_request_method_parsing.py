"""Tests for shared request method parsing."""

from e2a.message import ReqMethod

from agent_gateway.common.request_method import parse_request_method


def test_parses_known_method():
    assert parse_request_method(ReqMethod.CHAT_SEND.value) == ReqMethod.CHAT_SEND


def test_returns_none_for_unknown_or_empty_method():
    assert parse_request_method("not.a.protocol.method") is None
    assert parse_request_method("") is None
