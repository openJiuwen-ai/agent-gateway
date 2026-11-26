"""MessageHandler 的 session 上下文缓存必须有上限。

``_session_last_user_query`` 每个 session 最多保留 8000 字符，``_session_last_originator``
保留发起者身份，两者都只写不删，内存随 session 总数增长。
"""

from collections import OrderedDict

from e2a.message import Message, ReqMethod

from agent_gateway.message_handler.message_handler import (
    _SESSION_CONTEXT_LIMIT,
    MessageHandler,
)


def _handler() -> MessageHandler:
    handler = object.__new__(MessageHandler)
    handler._session_last_user_query = OrderedDict()
    handler._session_last_originator = OrderedDict()
    return handler


def _chat(session_id: str, query: str = "hi", member: str = "") -> Message:
    return Message(
        id=f"req-{session_id}",
        type="req",
        channel_id="web",
        session_id=session_id,
        params={"query": query},
        timestamp=0,
        ok=True,
        req_method=ReqMethod.CHAT_SEND,
        is_stream=True,
        metadata={"member_name": member} if member else None,
    )


def test_session_query_cache_stays_bounded():
    handler = _handler()
    total = _SESSION_CONTEXT_LIMIT * 2
    for i in range(total):
        handler._remember_user_query_context(_chat(f"s-{i}"))

    assert len(handler._session_last_user_query) == _SESSION_CONTEXT_LIMIT
    assert "s-0" not in handler._session_last_user_query
    assert f"s-{total - 1}" in handler._session_last_user_query


def test_rewrite_refreshes_recency():
    handler = _handler()
    keep = "s-keep"
    handler._remember_user_query_context(_chat(keep, query="keep"))
    for i in range(_SESSION_CONTEXT_LIMIT - 1):
        handler._remember_user_query_context(_chat(f"s-{i}"))
    handler._remember_user_query_context(_chat(keep, query="keep-2"))
    handler._remember_user_query_context(_chat("s-new"))

    assert len(handler._session_last_user_query) == _SESSION_CONTEXT_LIMIT
    assert handler._session_last_user_query[keep] == "keep-2"
    assert "s-0" not in handler._session_last_user_query
    assert handler._get_session_last_user_query(keep) == "keep-2"


def test_long_query_is_truncated():
    handler = _handler()
    handler._remember_user_query_context(_chat("s-long", query="x" * 20000))

    assert len(handler._session_last_user_query["s-long"]) == 8000


def test_originator_cache_stays_bounded():
    handler = _handler()
    total = _SESSION_CONTEXT_LIMIT * 2
    for i in range(total):
        handler._remember_user_query_context(_chat(f"s-{i}", member=f"m-{i}"))

    assert len(handler._session_last_originator) == _SESSION_CONTEXT_LIMIT
    assert "s-0" not in handler._session_last_originator
    assert handler.get_session_last_originator(f"s-{total - 1}") == ("web", f"m-{total - 1}")


def test_originator_clear_keeps_query_cache():
    handler = _handler()
    handler._remember_user_query_context(_chat("s-1", member="m-1"))
    handler._remember_user_query_context(_chat("s-1", query="next"))

    assert "s-1" not in handler._session_last_originator
    assert handler.get_session_last_originator("s-1") is None
    assert handler._get_session_last_user_query("s-1") == "next"
