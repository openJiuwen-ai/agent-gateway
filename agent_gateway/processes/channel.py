# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Async framing channel for parent-worker management traffic."""

import asyncio
import socket
from typing import Any

from agent_gateway.processes.protocol import (
    DEFAULT_FRAME_LIMIT,
    ProtocolError,
    decode_message,
    encode_message,
)


class ChannelClosed(ConnectionError):
    """Raised when a management channel is closed or reaches EOF."""


class ManagementChannel:
    """Serialized async channel for bounded management frames."""

    def __init__(self, sock: socket.socket, *, frame_limit: int = DEFAULT_FRAME_LIMIT) -> None:
        if type(frame_limit) is not int or frame_limit < 5:
            raise ProtocolError("frame_limit must be an integer of at least 5 bytes")

        self._sock = sock
        self._frame_limit = frame_limit
        self._closed = False
        self._reader_active = False
        self._send_lock = asyncio.Lock()
        self._sock.setblocking(False)

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self._sock.close()
        except OSError:
            pass

    async def send(self, message: dict[str, Any], *, timeout: float | None = None) -> None:
        if self._closed:
            raise ChannelClosed("management channel is closed")

        loop = asyncio.get_running_loop()
        try:
            async with asyncio.timeout(timeout):
                async with self._send_lock:
                    if self._closed:
                        raise ChannelClosed("management channel is closed")

                    frame = encode_message(message, frame_limit=self._frame_limit)
                    await loop.sock_sendall(self._sock, frame)
        except TimeoutError:
            self.close()
            raise
        except asyncio.CancelledError:
            self.close()
            raise
        except OSError as error:
            self.close()
            raise ChannelClosed("failed to send management frame") from error

    async def receive(self, *, timeout: float | None = None) -> dict[str, Any]:
        if self._closed:
            raise ChannelClosed("management channel is closed")
        if self._reader_active:
            raise RuntimeError("ManagementChannel supports a single reader")

        self._reader_active = True
        loop = asyncio.get_running_loop()
        try:
            async with asyncio.timeout(timeout):
                header = await self._recv_exact(4)
                payload_length = int.from_bytes(header, "big")
                if payload_length == 0:
                    raise ProtocolError("management frame payload is empty")
                if payload_length > self._frame_limit - 4:
                    raise ProtocolError(
                        f"management frame payload exceeds the {self._frame_limit}-byte frame limit"
                    )
                payload = await self._recv_exact(payload_length, eof_is_truncation=True)
                return decode_message(payload)
        except ChannelClosed:
            self.close()
            raise
        except (ProtocolError, TimeoutError):
            self.close()
            raise
        except asyncio.CancelledError:
            self.close()
            raise
        except OSError as error:
            self.close()
            raise ChannelClosed("failed to receive management frame") from error
        finally:
            self._reader_active = False

    async def _recv_exact(self, size: int, *, eof_is_truncation: bool = False) -> bytes:
        loop = asyncio.get_running_loop()
        received = bytearray()
        remaining = size

        while remaining:
            chunk = await loop.sock_recv(self._sock, remaining)
            if not chunk:
                if received or eof_is_truncation:
                    raise ProtocolError("truncated management frame")
                raise ChannelClosed("management channel reached EOF")
            received.extend(chunk)
            remaining -= len(chunk)

        return bytes(received)
