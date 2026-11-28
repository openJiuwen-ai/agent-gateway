"""Session deletion must release the per-session context MessageHandler keeps."""

import pytest

from agent_gateway.channel_manager.web import lifecycle_handlers
from agent_gateway.channel_manager.web.lifecycle_handlers import (
    register_lifecycle_handlers,
    release_session_context_hook,
)
from agent_gateway.message_handler.message_handler import MessageHandler


class _CacheOnlyHandler:
    """The two per-session dicts MessageHandler owns, without its dependencies."""

    forget_session_context = MessageHandler.forget_session_context

    def __init__(self) -> None:
        self._session_last_user_query = {
            "session-gone": "why did the build fail",
            "session-kept": "keep me",
        }
        self._session_last_originator = {
            "session-gone": ("web", "alice"),
            "session-kept": ("web", "bob"),
        }


class _FakeChannel:
    def __init__(self) -> None:
        self.channel_id = "web"
        self.clients = []
        self.methods = {}
        self.responses = []
        self.ensure_lifecycle_watch = None

    def register_method(self, method, handler) -> None:
        self.methods[method] = handler

    async def send_response(self, ws, req_id, ok, payload=None, error=None, code=None) -> None:
        self.responses.append({"req_id": req_id, "ok": ok, "payload": payload})

    def connection_user_id(self, ws):
        return ""


def _wire(handler):
    channel = _FakeChannel()
    register_lifecycle_handlers(
        channel,
        resolve_client=lambda: None,
        resolve_cron=lambda: None,
        on_session_deleted=release_session_context_hook(lambda: handler),
    )
    return channel


@pytest.fixture
def agent_reply(monkeypatch):
    calls = []

    async def fake_fetch_agent_unary(**kwargs):
        calls.append(kwargs)
        return True, {"session_id": kwargs["params"].get("session_id") or "batch"}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_fetch_agent_unary)
    return calls


def test_forget_session_context_drops_only_the_target_session() -> None:
    handler = _CacheOnlyHandler()
    handler.forget_session_context("session-gone")

    assert handler._session_last_user_query == {"session-kept": "keep me"}
    assert handler._session_last_originator == {"session-kept": ("web", "bob")}


def test_forget_session_context_ignores_blank_ids() -> None:
    handler = _CacheOnlyHandler()
    handler.forget_session_context("")
    handler.forget_session_context("   ")
    handler.forget_session_context(None)

    assert set(handler._session_last_user_query) == {"session-gone", "session-kept"}
    assert set(handler._session_last_originator) == {"session-gone", "session-kept"}


@pytest.mark.asyncio
async def test_session_delete_releases_context_after_agent_confirms(agent_reply) -> None:
    handler = _CacheOnlyHandler()
    channel = _wire(handler)

    await channel.methods["session.delete"](
        object(), "delete-1", {"session_id": "session-gone"}, "current"
    )

    assert channel.responses[-1]["ok"] is True
    assert agent_reply[0]["params"] == {"session_id": "session-gone"}
    assert handler._session_last_user_query == {"session-kept": "keep me"}
    assert handler._session_last_originator == {"session-kept": ("web", "bob")}


@pytest.mark.asyncio
async def test_session_archive_keeps_context(agent_reply) -> None:
    handler = _CacheOnlyHandler()
    channel = _wire(handler)

    await channel.methods["session.archive"](
        object(), "archive-1", {"session_id": "session-kept"}, "current"
    )

    assert channel.responses[-1]["ok"] is True
    assert handler._session_last_user_query["session-kept"] == "keep me"
    assert handler._session_last_originator["session-kept"] == ("web", "bob")


@pytest.mark.asyncio
async def test_unknown_delete_target_leaves_context_untouched(agent_reply) -> None:
    handler = _CacheOnlyHandler()
    channel = _wire(handler)

    await channel.methods["session.delete"](
        object(), "delete-2", {"session_id": "session-other"}, "current"
    )

    assert channel.responses[-1]["ok"] is True
    assert set(handler._session_last_user_query) == {"session-gone", "session-kept"}
