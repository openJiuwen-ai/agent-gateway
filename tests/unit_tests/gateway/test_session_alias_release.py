"""Deleting an internal Session must release the ACP/a2a alias entries for it."""

import asyncio

import pytest
from e2a.message import Message

from agent_gateway.channel_manager.web import lifecycle_handlers
from agent_gateway.channel_manager.web.lifecycle_handlers import (
    register_lifecycle_handlers,
    release_session_alias_hook,
)
from agent_gateway.message_handler.message_handler import MessageHandler


class _AliasOnlyHandler:
    """The alias maps and release path, without MessageHandler's dependencies."""

    forget_session_aliases = MessageHandler.forget_session_aliases
    _resolve_acp_internal_session_id = MessageHandler._resolve_acp_internal_session_id
    _resolve_acp_external_session_id = MessageHandler._resolve_acp_external_session_id
    _resolve_external_channel_session = MessageHandler._resolve_external_channel_session

    def __init__(self) -> None:
        self._session_last_user_query: dict[str, str] = {}
        self._session_last_originator: dict[str, tuple[str, str]] = {}
        self._acp_session_aliases: dict[str, str] = {}
        self._acp_alias_by_internal: dict[str, dict[str, None]] = {}
        self._acp_session_alias_lock = asyncio.Lock()
        self._external_session_aliases: dict[tuple[str, str], str] = {}
        self._external_alias_by_internal: dict[str, dict[tuple[str, str], None]] = {}
        self._external_session_alias_lock = asyncio.Lock()
        self._created = 0

    async def _ensure_acp_agent_session(self) -> str:
        self._created += 1
        return f"internal-{self._created}"

    async def _allocate_channel_session(self, msg, state) -> str:
        self._created += 1
        return f"internal-{self._created}"


def _a2a_message(external_id: str) -> Message:
    return Message(
        id=f"msg-{external_id}",
        type="req",
        channel_id="a2a",
        session_id=external_id,
        params={"mode": "agent"},
        timestamp=0.0,
        ok=True,
    )


@pytest.mark.asyncio
async def test_forget_session_aliases_drops_acp_alias() -> None:
    handler = _AliasOnlyHandler()

    internal, aliased = await handler._resolve_acp_internal_session_id("ide-session-1")
    assert (internal, aliased) == ("internal-1", True)
    assert handler._acp_session_aliases == {"ide-session-1": "internal-1"}

    handler.forget_session_aliases("internal-1")

    assert handler._acp_session_aliases == {}
    assert handler._acp_alias_by_internal == {}
    # No alias left: the caller's id is passed through unchanged.
    assert handler._resolve_acp_external_session_id("internal-1") == "internal-1"


@pytest.mark.asyncio
async def test_forget_session_aliases_drops_a2a_alias() -> None:
    handler = _AliasOnlyHandler()
    msg = _a2a_message("context-1")

    await handler._resolve_external_channel_session(msg)
    assert msg.session_id == "internal-1"
    assert handler._external_session_aliases == {("a2a", "context-1"): "internal-1"}

    handler.forget_session_aliases("internal-1")

    assert handler._external_session_aliases == {}
    assert handler._external_alias_by_internal == {}


@pytest.mark.asyncio
async def test_forget_session_aliases_keeps_aliases_of_other_sessions() -> None:
    handler = _AliasOnlyHandler()
    await handler._resolve_acp_internal_session_id("ide-a")
    await handler._resolve_acp_internal_session_id("ide-b")
    a2a = _a2a_message("context-b")
    await handler._resolve_external_channel_session(a2a)

    handler.forget_session_aliases("internal-1")

    assert handler._acp_session_aliases == {"ide-b": "internal-2"}
    assert handler._external_session_aliases == {("a2a", "context-b"): "internal-3"}
    assert handler._resolve_acp_external_session_id("internal-2") == "ide-b"


def test_forget_session_aliases_ignores_blank_ids() -> None:
    handler = _AliasOnlyHandler()
    handler._acp_session_aliases["ide-a"] = "internal-1"
    handler._acp_alias_by_internal["internal-1"] = {"ide-a": None}

    handler.forget_session_aliases("")
    handler.forget_session_aliases("   ")
    handler.forget_session_aliases(None)

    assert handler._acp_session_aliases == {"ide-a": "internal-1"}
    assert handler._acp_alias_by_internal == {"internal-1": {"ide-a": None}}


@pytest.mark.asyncio
async def test_reverse_lookup_keeps_first_alias_when_several_share_a_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _AliasOnlyHandler()

    async def shared_session() -> str:
        return "shared-internal"

    monkeypatch.setattr(handler, "_ensure_acp_agent_session", shared_session)
    await handler._resolve_acp_internal_session_id("ide-first")
    await handler._resolve_acp_internal_session_id("ide-second")

    assert handler._resolve_acp_external_session_id("shared-internal") == "ide-first"

    handler.forget_session_aliases("shared-internal")

    assert handler._acp_session_aliases == {}
    assert handler._acp_alias_by_internal == {}


@pytest.mark.asyncio
async def test_original_session_metadata_still_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    handler = _AliasOnlyHandler()
    await handler._resolve_acp_internal_session_id("ide-session-1")

    resolved = handler._resolve_acp_external_session_id(
        "internal-1", {"acp_original_session_id": "ide-session-1"}
    )

    assert resolved == "ide-session-1"


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


@pytest.mark.asyncio
async def test_session_delete_releases_aliases_after_agent_confirms(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _AliasOnlyHandler()
    await handler._resolve_acp_internal_session_id("ide-session-1")

    async def fake_fetch_agent_unary(**kwargs):
        return True, {"session_id": kwargs["params"].get("session_id") or "batch"}

    monkeypatch.setattr(lifecycle_handlers, "fetch_agent_unary", fake_fetch_agent_unary)

    channel = _FakeChannel()
    register_lifecycle_handlers(
        channel,
        resolve_client=lambda: None,
        resolve_cron=lambda: None,
        on_session_deleted=release_session_alias_hook(lambda: handler),
    )

    await channel.methods["session.delete"](
        object(), "delete-1", {"session_id": "internal-1"}, "current"
    )

    assert channel.responses[-1]["ok"] is True
    assert handler._acp_session_aliases == {}
    assert handler._external_session_aliases == {}
