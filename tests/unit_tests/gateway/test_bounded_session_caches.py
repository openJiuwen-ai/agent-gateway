"""Bounded LRU caches for per-session fallback state."""

from types import SimpleNamespace

import pytest

from agent_gateway.common.bounded_cache import BoundedLruCache
from agent_gateway.message_handler.message_handler import MessageHandler
from tests._support.null_session_login_bindings import NullSessionLoginBindings


def _chat_send(session_id: str, query: str, member_name: str | None = None) -> SimpleNamespace:
    metadata = {"member_name": member_name} if member_name else {}
    return SimpleNamespace(
        req_method="chat.send",
        session_id=session_id,
        channel_id="feishu",
        params={"query": query},
        metadata=metadata,
    )


def _bare_handler(query_capacity: int = 2, originator_capacity: int = 2) -> MessageHandler:
    handler = object.__new__(MessageHandler)
    handler._session_last_user_query = BoundedLruCache(query_capacity)
    handler._session_last_originator = BoundedLruCache(originator_capacity)
    return handler


def test_capacity_must_be_positive() -> None:
    with pytest.raises(ValueError):
        BoundedLruCache(0)


def test_evicts_least_recently_used_entry() -> None:
    cache: BoundedLruCache[str, int] = BoundedLruCache(2)
    cache["a"] = 1
    cache["b"] = 2
    assert cache["a"] == 1  # touch "a" so "b" becomes the LRU entry
    cache["c"] = 3

    assert len(cache) == 2
    assert list(cache) == ["a", "c"]
    assert cache.get("b") is None


def test_get_does_not_insert_missing_key() -> None:
    cache: BoundedLruCache[str, str] = BoundedLruCache(4)
    assert cache.get("missing", "") == ""
    assert len(cache) == 0


def test_delete_pop_clear_and_contains() -> None:
    cache: BoundedLruCache[str, int] = BoundedLruCache(4)
    cache["a"] = 1
    cache["b"] = 2

    assert "a" in cache
    assert cache.pop("a") == 1
    assert cache.pop("a", None) is None
    del cache["b"]
    assert len(cache) == 0

    cache["c"] = 3
    cache.clear()
    assert len(cache) == 0


def test_message_handler_evicts_old_sessions() -> None:
    handler = _bare_handler()
    for session_id in ("s1", "s2", "s3"):
        handler._remember_user_query_context(_chat_send(session_id, f"query-{session_id}"))

    assert len(handler._session_last_user_query) == 2
    assert handler._get_session_last_user_query("s1") == ""
    assert handler._get_session_last_user_query("s3") == "query-s3"


def test_message_handler_keeps_recently_used_session() -> None:
    handler = _bare_handler()
    handler._remember_user_query_context(_chat_send("s1", "first"))
    handler._remember_user_query_context(_chat_send("s2", "second"))
    assert handler._get_session_last_user_query("s1") == "first"  # marks s1 as MRU
    handler._remember_user_query_context(_chat_send("s3", "third"))

    assert handler._get_session_last_user_query("s2") == ""
    assert handler._get_session_last_user_query("s1") == "first"


def test_originator_cache_is_bounded_and_cleared_for_anonymous_sender() -> None:
    handler = _bare_handler()
    for session_id in ("s1", "s2", "s3"):
        handler._remember_user_query_context(_chat_send(session_id, "hi", member_name="alice"))

    assert handler.get_session_last_originator("s1") is None
    assert handler.get_session_last_originator("s3") == ("feishu", "alice")

    handler._remember_user_query_context(_chat_send("s3", "hi again"))
    assert handler.get_session_last_originator("s3") is None


def test_constructor_wires_bounded_caches() -> None:
    MessageHandler._instance = None
    try:
        handler = MessageHandler(
            object(),
            session_login_bindings=NullSessionLoginBindings(),
        )
        assert isinstance(handler._session_last_user_query, BoundedLruCache)
        assert isinstance(handler._session_last_originator, BoundedLruCache)
        assert handler._session_last_user_query.capacity > 0
        assert handler._session_last_originator.capacity > 0
    finally:
        MessageHandler._instance = None
