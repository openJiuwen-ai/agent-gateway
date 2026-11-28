# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Test-local loopback AgentServer stand-ins for static routed Workers.

One instance plays one physical target: it answers unary requests, holds
streams open on demand, answers session probes from its own session set and
lets a test inject ``send_push`` frames on the real gateway connection.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from e2a.agent_models import AgentResponse, AgentResponseChunk
from e2a.constants import E2A_WIRE_SERVER_PUSH_KEY
from e2a.wire_codec import encode_agent_chunk_for_wire, encode_agent_response_for_wire
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed


def connection_ack(readiness: str = "AGENT_READY") -> str:
    return json.dumps({"type": "event", "event": "connection.ack", "payload": {"status": "ready", "readiness": readiness}})


def push_wire(
    request_id: str, session_id: str, *, event: str = "chat.request_completed", channel: str = "web",
    user: str | None = None, metadata: dict[str, Any] | None = None,
    payload_fields: dict[str, Any] | None = None, **payload: Any,
) -> dict[str, Any]:
    """Build a legacy server-push frame with the same codec as the real peer.

    ``payload_fields`` are merged last, for keys that share a name with the
    arguments of this helper (such as ``request_id``).
    """
    payload = {**payload, **(payload_fields or {})}
    chunk = AgentResponseChunk(
        request_id=request_id, channel_id=channel, payload={"event_type": event, **payload}, is_complete=False,
    )
    wire = encode_agent_chunk_for_wire(chunk, response_id=f"push-{request_id}-{event}", sequence=0)
    wire["session_id"] = session_id
    wire["metadata"] = {E2A_WIRE_SERVER_PUSH_KEY: True, **(metadata or {})}
    if user is not None:
        wire["user_id"] = user
    return wire


def legacy_push_wire(
    request_id: str, session_id: str, *, event: str = "chat.request_completed", channel: str = "web",
    user: str | None = None, metadata: dict[str, Any] | None = None,
    payload_fields: dict[str, Any] | None = None, **payload: Any,
) -> dict[str, Any]:
    """Build the deprecated legacy push shape that keeps every payload field."""
    wire: dict[str, Any] = {
        "request_id": request_id, "channel_id": channel, "session_id": session_id,
        "metadata": {E2A_WIRE_SERVER_PUSH_KEY: True, **(metadata or {})},
        "payload": {"event_type": event, **payload, **(payload_fields or {})}, "is_complete": False,
    }
    if user is not None:
        wire["user_id"] = user
    return wire


class RoutedAgentServer:
    """One loopback AgentServer with holdable streams and injectable pushes."""

    def __init__(self, name: str, *, readiness: str = "AGENT_READY", ack: bool = True) -> None:
        self.name = name
        self._readiness = readiness
        self._ack = ack
        self.requests: list[dict[str, Any]] = []
        self.sessions: set[str] = set()
        self.missing: set[str] = set()
        self.hold: set[str] = set()
        self.received = asyncio.Event()
        self._released: dict[str, asyncio.Event] = {}
        self._connections: set[Any] = set()
        self._server: Any = None

    async def start(self) -> RoutedAgentServer:
        self._server = await serve(self._handle, "127.0.0.1", 0)
        return self

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self._server.sockets[0].getsockname()[1]}/agent"

    async def stop(self) -> None:
        for connection in list(self._connections):
            try:
                await connection.close(code=1001)
            except Exception:  # noqa: BLE001 - the peer may already be gone
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def drop(self) -> None:
        """Close every connection without stopping the listener."""
        for connection in list(self._connections):
            connection.transport.abort()
        self._connections.clear()

    def release(self, request_id: str) -> None:
        self._released[request_id].set()

    async def push(self, wire: dict[str, Any]) -> None:
        for connection in list(self._connections):
            await connection.send(json.dumps(wire))

    def methods(self) -> list[str]:
        return [str(item.get("method") or "") for item in self.requests]

    def has_request(self, request_id: str) -> bool:
        return any(str(item.get("request_id") or "") == request_id for item in self.requests)

    async def wait_request(self, request_id: str, timeout: float = 5.0) -> None:
        """Wait until a named request reached this AgentServer, not merely a probe."""
        async with asyncio.timeout(timeout):
            while not self.has_request(request_id):
                await asyncio.sleep(0.01)

    async def _handle(self, websocket: Any) -> None:
        self._connections.add(websocket)
        pending: set[asyncio.Task[None]] = set()
        try:
            if self._ack:
                await websocket.send(connection_ack(self._readiness))
            async for raw in websocket:
                envelope = json.loads(raw)
                self.requests.append(envelope)
                self.received.set()
                # A real AgentServer answers each request concurrently; a held
                # stream must not stop this connection from reading the next one.
                task = asyncio.create_task(self._answer(websocket, envelope))
                pending.add(task)
                task.add_done_callback(pending.discard)
        except ConnectionClosed:
            pass
        finally:
            self._connections.discard(websocket)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    async def _answer(self, websocket: Any, envelope: dict[str, Any]) -> None:
        request_id = str(envelope.get("request_id") or "")
        method = str(envelope.get("method") or "")
        channel = str(envelope.get("channel_id") or envelope.get("channel") or "web")
        params = envelope.get("params") if isinstance(envelope.get("params"), dict) else {}
        session_id = str(envelope.get("session_id") or params.get("session_id") or "")
        # The real AgentServer names a session at the top level of the payload;
        # the gateway's own handlers read ``payload["session_id"]`` there.
        if method == "session.get_metadata":
            found = session_id in self.sessions and session_id not in self.missing
            payload = {"session_id": session_id} if found else {"error": "not found", "code": "NOT_FOUND"}
            await self._send(websocket, AgentResponse(request_id=request_id, channel_id=channel, payload=payload, ok=found))
            return
        if method == "session.create":
            created = f"session-{self.name}-{len(self.sessions)}"
            self.sessions.add(created)
            await self._send(websocket, AgentResponse(
                request_id=request_id, channel_id=channel, payload={"session_id": created}, ok=True,
            ))
            return
        if envelope.get("is_stream"):
            await self._chunk(websocket, request_id, channel, {"event_type": "chat.delta", "content": f"{self.name}-delta"}, 0)
            if request_id in self.hold:
                self._released[request_id] = asyncio.Event()
                await self._released[request_id].wait()
            await self._chunk(
                websocket, request_id, channel,
                {"event_type": "chat.final", "content": f"{self.name}-final"}, 1, complete=True,
            )
            return
        await self._send(websocket, AgentResponse(
            request_id=request_id, channel_id=channel, payload={"echo": self.name, "method": method}, ok=True,
        ))

    @staticmethod
    async def _send(websocket: Any, response: AgentResponse) -> None:
        await websocket.send(json.dumps(encode_agent_response_for_wire(response, response_id=response.request_id)))

    @staticmethod
    async def _chunk(websocket: Any, request_id: str, channel: str, payload: dict[str, Any], sequence: int, *, complete: bool = False) -> None:
        chunk = AgentResponseChunk(request_id=request_id, channel_id=channel, payload=payload, is_complete=complete)
        await websocket.send(json.dumps(encode_agent_chunk_for_wire(chunk, response_id=request_id, sequence=sequence)))
