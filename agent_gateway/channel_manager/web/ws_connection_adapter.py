# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Adapt Starlette WebSocket to the websockets-like surface WebChannel expects."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from starlette.websockets import WebSocket, WebSocketDisconnect, WebSocketState
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK
from websockets.frames import Close


class StarletteWsAdapter:
    """Thin adapter so existing WebChannel handlers keep using ``send`` / ``async for`` / ``closed``.

    Future HTTP routes on the same uvicorn app do not go through this adapter.
    """

    def __init__(
        self,
        websocket: WebSocket,
        *,
        authenticated_user_id: str | None = None,
        verified_user_id: str | None = None,
        frame_allowed: Callable[[str | bytes], bool] | None = None,
    ) -> None:
        self._ws = websocket
        self._frame_allowed = frame_allowed
        self.authenticated_user_id = authenticated_user_id
        # The declared user the handshake bound to the token ("" when none was
        # declared); None when no check ran. WebChannel uses it as the identity.
        self.verified_user_id = verified_user_id
        self._closed = False
        self._close_code = 1006
        self._close_reason = ""
        # Match websockets: path includes query string when present.
        path = websocket.url.path or ""
        query = websocket.url.query or ""
        self.path = f"{path}?{query}" if query else path
        client = websocket.client
        self.remote_address: tuple[str, int] | None = (
            (client.host, client.port) if client is not None else None
        )
        self.local_address: tuple[str, int] | None = None
        scope_server = websocket.scope.get("server")
        if isinstance(scope_server, (list, tuple)) and len(scope_server) >= 2:
            self.local_address = (str(scope_server[0]), int(scope_server[1]))
        # Headers containers used by WebChannel._extract_ws_header_user_id / Origin helpers.
        self.request_headers = websocket.headers
        self.request = type("Request", (), {"headers": websocket.headers, "path": self.path})()

    @property
    def closed(self) -> bool:
        if self._closed:
            return True
        return self._ws.client_state == WebSocketState.DISCONNECTED

    @property
    def state(self) -> Any:
        return self._ws.client_state

    def _closed_exc(self) -> ConnectionClosed:
        frame = Close(self._close_code, self._close_reason or "")
        if self._close_code == 1000:
            return ConnectionClosedOK(frame, None)
        return ConnectionClosedError(frame, None)

    def _mark_closed(self, code: int | None = None, reason: str | None = None) -> None:
        self._closed = True
        if code is not None:
            self._close_code = int(code)
        if reason is not None:
            self._close_reason = str(reason)

    async def accept(self, **kwargs: Any) -> None:
        await self._ws.accept(**kwargs)

    async def send(self, data: str | bytes) -> None:
        if self.closed:
            raise self._closed_exc()
        try:
            if isinstance(data, bytes):
                await self._ws.send_bytes(data)
            else:
                await self._ws.send_text(data)
        except WebSocketDisconnect as exc:
            self._mark_closed(exc.code, exc.reason)
            raise self._closed_exc() from exc

    async def recv(self) -> str | bytes:
        if self.closed:
            raise self._closed_exc()
        try:
            message = await self._ws.receive()
        except WebSocketDisconnect as exc:
            self._mark_closed(exc.code, exc.reason)
            raise self._closed_exc() from exc

        msg_type = message.get("type")
        if msg_type == "websocket.disconnect":
            self._mark_closed(message.get("code", 1006), message.get("reason", ""))
            raise self._closed_exc()
        data = message.get("text")
        if data is None:
            data = message.get("bytes")
        if data is not None:
            if self._frame_allowed is not None and not self._frame_allowed(data):
                await self.close(code=1008, reason="unsupported Worker request")
                raise self._closed_exc()
            return data
        self._mark_closed(1006, "empty websocket frame")
        raise self._closed_exc()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        if self.closed:
            return
        self._mark_closed(code, reason)
        try:
            await self._ws.close(code=code, reason=reason or "")
        except Exception:  # noqa: BLE001 — best-effort close
            return

    def __aiter__(self) -> StarletteWsAdapter:
        return self

    async def __anext__(self) -> str | bytes:
        try:
            return await self.recv()
        except ConnectionClosed:
            raise StopAsyncIteration from None
