"""With IAM on, a Web or TUI connection may only declare a user the token belongs to.

The connection's identity is the user_id it declares at handshake (query
``user_id`` or ``X-User-Id``); AgentServer checks Session ownership against that
name. The handshake therefore binds the declared name to the IAM token: it must
be the token's username (``USER_MISMATCH`` otherwise). ``/file-api`` and the web
proxy also accept the IAM user id; a connection does not, because Sessions and
workspaces are keyed by username.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from agent_gateway.channel_manager.base import RobotMessageRouter
from agent_gateway.channel_manager.web.web_channel_app import _serve_channel_websocket
from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig
from agent_gateway.extensions.auth.credential_authenticator import AuthResult
from tests.unit_tests.extensions.test_agentos_router import FakeYuanRongClient, _router_client

_IAM_USER_ID = "d8e63255-9dfd-4aa0-8b59-2b9096442154"


class _Iam:
    """IAM that accepts the token for ``alice``; ``username`` may be withheld."""

    def __init__(self, *, username: str | None = "alice") -> None:
        self.username = username
        self.contexts = []

    async def authenticate(self, context) -> AuthResult:
        self.contexts.append(context)
        return AuthResult(
            success=True,
            user_id=_IAM_USER_ID,
            extensions={"username": self.username, "role": "user", "auth_method": "token"},
        )


async def _authenticate(*, path: str, headers: dict | None = None, channel: str = "web",
                        iam: _Iam | None = None) -> AuthResult:
    client = _router_client(FakeYuanRongClient(), auth_client=iam)
    try:
        return await client.authenticate_http(path=path, headers=headers or {}, channel=channel)
    finally:
        await client.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["web", "tui"])
@pytest.mark.parametrize("path, headers", [
    ("/ws?token=t&user_id=alice", {}),
    ("/ws?token=t", {"X-User-Id": "alice"}),
    ("/ws?token=t&user_id=alice", {"x-user-id": "alice"}),
    ("/ws?token=t", {}),
])
async def test_declared_user_belonging_to_the_token_is_admitted(channel, path, headers) -> None:
    result = await _authenticate(path=path, headers=headers, channel=channel, iam=_Iam())
    assert result.success is True
    assert result.user_id == _IAM_USER_ID


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["web", "tui"])
@pytest.mark.parametrize("path, headers", [
    ("/ws?token=t&user_id=bob", {}),
    ("/ws?token=t", {"X-User-Id": "bob"}),
    # Web reads the query first and the TUI the header first; every declaration must match.
    ("/ws?token=t&user_id=alice", {"X-User-Id": "bob"}),
    ("/ws?token=t&user_id=bob", {"X-User-Id": "alice"}),
    # The token's own IAM user id is not a connection identity: this user's Sessions are owned by "alice".
    (f"/ws?token=t&user_id={_IAM_USER_ID}", {}),
    ("/ws?token=t", {"X-User-Id": _IAM_USER_ID}),
    ("/ws?token=t&user_id=alice", {"X-User-Id": _IAM_USER_ID}),
])
async def test_declared_user_not_belonging_to_the_token_is_refused(channel, path, headers) -> None:
    result = await _authenticate(path=path, headers=headers, channel=channel, iam=_Iam())
    assert result.success is False
    assert result.extensions.get("error_code") == "USER_MISMATCH"


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["web", "tui"])
@pytest.mark.parametrize("path, headers", [
    ("/ws?token=t&user_id=alice", {}),
    # Not even the token's own IAM user id: the workspace it would route to is keyed by username.
    (f"/ws?token=t&user_id={_IAM_USER_ID}", {}),
    ("/ws?token=t", {"X-User-Id": _IAM_USER_ID}),
])
async def test_without_a_username_from_iam_every_declared_user_is_refused(channel, path, headers) -> None:
    result = await _authenticate(path=path, headers=headers, channel=channel, iam=_Iam(username=None))
    assert result.success is False
    assert result.extensions.get("error_code") == "USER_MISMATCH"


@pytest.mark.asyncio
async def test_without_a_username_from_iam_an_undeclared_connection_is_admitted() -> None:
    result = await _authenticate(path="/ws?token=t", iam=_Iam(username=None))
    assert result.success is True
    assert result.extensions["connection_user_id"] == ""


@pytest.mark.asyncio
async def test_without_iam_the_declared_user_is_kept() -> None:
    """Single-user and auth-off deployments have no token to bind to; behaviour is unchanged."""
    result = await _authenticate(path="/ws?user_id=anyone", headers={"X-User-Id": "anyone"})
    assert result.success is True
    assert result.user_id == "anyone"


@pytest.mark.asyncio
async def test_other_channels_keep_their_own_identity_checks() -> None:
    """/file-api and the web proxy apply USER_MISMATCH themselves, after authenticate_http."""
    result = await _authenticate(path="/file-api/list-files?token=t&user_id=bob", channel="file-api", iam=_Iam())
    assert result.success is True


@pytest.mark.asyncio
async def test_web_handshake_declaring_another_user_is_not_accepted() -> None:
    """End to end through the real Web upgrade: the connection is refused before it is accepted."""
    client = _router_client(FakeYuanRongClient(), auth_client=_Iam())
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.set_handshake_auth(client.authenticate_http)
    channel.handle_connection = AsyncMock()
    websocket = MagicMock()
    websocket.headers = {}
    websocket.url.path = "/ws"
    websocket.url.query = "token=t&user_id=bob"
    websocket.client = MagicMock(host="127.0.0.1", port=9)
    websocket.send_denial_response = AsyncMock()
    websocket.accept = AsyncMock()
    try:
        await _serve_channel_websocket(channel, websocket)
    finally:
        await client.shutdown()
    websocket.accept.assert_not_called()
    channel.handle_connection.assert_not_awaited()
    websocket.send_denial_response.assert_awaited_once()
    assert websocket.send_denial_response.await_args.args[0].status_code == 401


# --- One declaration per source, and the connection uses what was checked ----------

def _starlette_headers(*pairs: tuple[str, str]):
    from starlette.datastructures import Headers

    return Headers(raw=[(name.lower().encode(), value.encode()) for name, value in pairs])


def _websockets_headers(*pairs: tuple[str, str]):
    from websockets.datastructures import Headers

    return Headers(list(pairs))


@pytest.mark.asyncio
@pytest.mark.parametrize("channel", ["web", "tui"])
@pytest.mark.parametrize("path, headers", [
    # Starlette (Web) reads the first value, a dict view keeps the last: alice must not vouch for bob.
    ("/ws?token=t", _starlette_headers(("X-User-Id", "bob"), ("X-User-Id", "alice"))),
    ("/ws?token=t", _starlette_headers(("X-User-Id", "alice"), ("X-User-Id", "alice"))),
    # websockets (TUI) cannot read a repeated header at all.
    ("/ws?token=t", _websockets_headers(("X-User-Id", "alice"), ("X-User-Id", "bob"))),
    ("/ws?token=t", {"X-User-Id": "alice", "x-user-id": "bob"}),
    ("/ws?token=t&user_id=bob&user_id=alice", {}),
    ("/ws?token=t&user_id=alice&user_id=alice", {}),
])
async def test_repeated_user_declaration_is_refused(channel, path, headers) -> None:
    result = await _authenticate(path=path, headers=headers, channel=channel, iam=_Iam())
    assert result.success is False
    assert result.extensions.get("error_code") == "AMBIGUOUS_USER_ID"


@pytest.mark.asyncio
@pytest.mark.parametrize("path, headers, expected", [
    ("/ws?token=t&user_id=alice", {}, "alice"),
    ("/ws?token=t", _starlette_headers(("X-User-Id", "alice")), "alice"),
    ("/ws?token=t", {}, ""),
])
async def test_web_handshake_records_the_identity_it_verified(path, headers, expected) -> None:
    result = await _authenticate(path=path, headers=headers, channel="web", iam=_Iam())
    assert result.success is True
    assert result.extensions["connection_user_id"] == expected


def _upgrade(query: str, headers) -> MagicMock:
    websocket = MagicMock()
    websocket.headers = headers
    websocket.url.path = "/ws"
    websocket.url.query = query
    websocket.client = MagicMock(host="127.0.0.1", port=9)
    websocket.scope = {}
    websocket.send_denial_response = AsyncMock()
    websocket.accept = AsyncMock()
    return websocket


@pytest.mark.asyncio
async def test_web_handshake_with_a_repeated_identity_header_is_not_accepted() -> None:
    client = _router_client(FakeYuanRongClient(), auth_client=_Iam())
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.set_handshake_auth(client.authenticate_http)
    channel.handle_connection = AsyncMock()
    websocket = _upgrade("token=t", _starlette_headers(("X-User-Id", "bob"), ("X-User-Id", "alice")))
    try:
        await _serve_channel_websocket(channel, websocket)
    finally:
        await client.shutdown()
    websocket.accept.assert_not_called()
    channel.handle_connection.assert_not_awaited()


@pytest.mark.asyncio
async def test_web_connection_uses_the_identity_the_handshake_verified() -> None:
    client = _router_client(FakeYuanRongClient(), auth_client=_Iam())
    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    channel.set_handshake_auth(client.authenticate_http)
    seen = []

    async def handle_connection(adapter, *, path):
        seen.append((adapter.verified_user_id, channel._resolve_connection_user_id({"user_id": "alice"}, adapter)))

    channel.handle_connection = handle_connection
    websocket = _upgrade("token=t&user_id=alice", _starlette_headers(("X-User-Id", "alice")))
    try:
        await _serve_channel_websocket(channel, websocket)
    finally:
        await client.shutdown()
    websocket.accept.assert_awaited_once()
    assert seen == [("alice", "alice")]
