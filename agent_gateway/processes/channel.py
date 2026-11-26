# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Async framing channel for parent-worker management traffic."""

import asyncio
import socket
from typing import Any

from agent_gateway.processes.management_protocol import (
    DEFAULT_FRAME_LIMIT,
    MIN_FRAME_LIMIT,
    ProtocolError,
    decode_message,
    encode_message,
)


class ChannelClosed(ConnectionError):
    """Raised when a management channel is closed or reaches EOF."""


class ManagementChannel:
    """Serialized async channel for bounded management frames."""

    def __init__(self, sock: socket.socket, *, frame_limit: int = DEFAULT_FRAME_LIMIT) -> None:
        if type(frame_limit) is not int or frame_limit < MIN_FRAME_LIMIT:
            raise ProtocolError(
                f"frame_limit must be an integer of at least {MIN_FRAME_LIMIT} bytes"
            )

        self._sock = sock
        self._frame_limit = frame_limit
        self._closed = False
        self._reader_active = False
        self._send_lock = asyncio.Lock()
        self._io_tasks: set[asyncio.Task[object]] = set()
        self._sock.setblocking(False)

    @property
    def closed(self) -> bool:
        return self._closed

    def _track_io_task(self) -> asyncio.Task[object] | None:
        task = asyncio.current_task()
        if task is not None:
            self._io_tasks.add(task)
        return task

    def _untrack_io_task(self, task: asyncio.Task[object] | None) -> None:
        if task is not None:
            self._io_tasks.discard(task)

    def _cancel_io_tasks(self) -> None:
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        for task in tuple(self._io_tasks):
            if task is not current:
                task.cancel()

    def close(self) -> None:
        if self._closed:
            return

        self._closed = True
        self._cancel_io_tasks()
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
                    io_task = self._track_io_task()
                    try:
                        await loop.sock_sendall(self._sock, frame)
                    finally:
                        self._untrack_io_task(io_task)
        except ChannelClosed:
            self.close()
            raise
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
        io_task = self._track_io_task()
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
            self._untrack_io_task(io_task)
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
