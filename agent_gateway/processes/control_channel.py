# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Async framing channel for the private control service protocol."""

import asyncio
import math
import socket
from typing import Any

from agent_gateway.processes.control_protocol import (
    DEFAULT_FRAME_LIMIT,
    ControlProtocolError,
    decode_message,
    encode_message,
)


class ControlChannelClosed(ConnectionError):
    """Raised when a control channel is closed or reaches EOF."""


class ControlChannelSendQueueFull(ControlProtocolError):
    """Raised when the bounded send queue is full; the channel stays usable."""


_MAX_SEND_TASKS = 32


class ControlServiceChannel:
    """Bounded single-reader channel with serialized complete-frame writes."""

    def __init__(
        self,
        sock: socket.socket,
        *,
        frame_limit: int = DEFAULT_FRAME_LIMIT,
        send_timeout: float = 1.0,
        read_timeout: float = 5.0,
    ) -> None:
        if type(frame_limit) is not int or frame_limit < 5:
            raise ControlProtocolError(
                "frame_limit must be an integer of at least 5 bytes"
            )
        if not _is_positive_finite_number(send_timeout):
            raise ControlProtocolError("send_timeout must be a positive finite number")
        if not _is_positive_finite_number(read_timeout):
            raise ControlProtocolError("read_timeout must be a positive finite number")

        self._sock = sock
        self._frame_limit = frame_limit
        self._send_timeout = send_timeout
        self._read_timeout = read_timeout
        self._closed = False
        self._reader_active = False
        self._reader_task: asyncio.Task[dict[str, Any]] | None = None
        self._send_lock = asyncio.Lock()
        self._send_tasks: set[asyncio.Task[None]] = set()
        self._sock.setblocking(False)

    @property
    def closed(self) -> bool:
        return self._closed

    async def send(self, message: dict[str, Any]) -> None:
        if self._closed:
            raise ControlChannelClosed("control channel is closed")

        frame = encode_message(message, frame_limit=self._frame_limit)
        if self._closed:
            raise ControlChannelClosed("control channel is closed")
        if len(self._send_tasks) >= _MAX_SEND_TASKS:
            raise ControlChannelSendQueueFull("control channel send queue is full")

        loop = asyncio.get_running_loop()
        send_task = asyncio.create_task(
            self._send_frame(frame, deadline=loop.time() + self._send_timeout)
        )
        self._send_tasks.add(send_task)
        send_task.add_done_callback(self._finish_send_task)

        try:
            await asyncio.shield(send_task)
        except asyncio.CancelledError:
            if self._closed:
                raise ControlChannelClosed("control channel is closed") from None
            raise

    async def receive(self) -> dict[str, Any]:
        if self._closed:
            raise ControlChannelClosed("control channel is closed")
        if self._reader_active:
            raise RuntimeError("ControlServiceChannel supports a single reader")

        self._reader_active = True
        self._reader_task = asyncio.current_task()
        loop = asyncio.get_running_loop()
        try:
            first_chunk = await loop.sock_recv(self._sock, 4)
            if self._closed:
                raise ControlChannelClosed("control channel is closed")
            if not first_chunk:
                raise ControlChannelClosed("control channel reached EOF")

            try:
                async with asyncio.timeout_at(
                    loop.time() + self._read_timeout
                ):
                    header = first_chunk + await self._recv_exact(
                        4 - len(first_chunk)
                    )
                    payload_length = int.from_bytes(header, "big")
                    if payload_length == 0:
                        raise ControlProtocolError("control frame payload is empty")
                    if payload_length > self._frame_limit - 4:
                        raise ControlProtocolError(
                            "control frame payload exceeds the frame limit"
                        )
                    payload = await self._recv_exact(payload_length)
            except TimeoutError:
                raise ControlProtocolError("control frame completion timed out") from None

            return decode_message(payload)
        except ControlChannelClosed:
            self.close()
            raise
        except ControlProtocolError:
            self.close()
            raise
        except asyncio.CancelledError:
            if self._closed:
                raise ControlChannelClosed("control channel is closed") from None
            self.close()
            raise
        except (OSError, ValueError):
            self.close()
            raise ControlChannelClosed("failed to receive control frame") from None
        finally:
            self._reader_task = None
            self._reader_active = False

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

        current_task = asyncio.current_task()
        if self._reader_task is not None and self._reader_task is not current_task:
            self._reader_task.cancel()
        for send_task in tuple(self._send_tasks):
            if send_task is not current_task:
                send_task.cancel()

    async def stop(self) -> None:
        """Close the channel and wait for every task it still owns."""
        reader_task = self._reader_task
        self.close()
        owned_tasks = tuple(self._send_tasks)
        # A task that awaits itself can neither finish nor be cancelled.
        if reader_task is not None and reader_task is not asyncio.current_task():
            owned_tasks += (reader_task,)
        if owned_tasks:
            await asyncio.gather(*owned_tasks, return_exceptions=True)

    async def _send_frame(self, frame: bytes, *, deadline: float) -> None:
        loop = asyncio.get_running_loop()
        try:
            async with asyncio.timeout_at(deadline):
                async with self._send_lock:
                    if self._closed:
                        raise ControlChannelClosed("control channel is closed")
                    await loop.sock_sendall(self._sock, frame)
        except TimeoutError:
            self.close()
            raise TimeoutError("control frame send timed out") from None
        except asyncio.CancelledError:
            raise
        except OSError:
            self.close()
            raise ControlChannelClosed("failed to send control frame") from None

    async def _recv_exact(self, size: int) -> bytes:
        loop = asyncio.get_running_loop()
        received = bytearray()
        remaining = size

        while remaining:
            if self._closed:
                raise ControlChannelClosed("control channel is closed")
            chunk = await loop.sock_recv(self._sock, remaining)
            if self._closed:
                raise ControlChannelClosed("control channel is closed")
            if not chunk:
                raise ControlProtocolError("truncated control frame")
            received.extend(chunk)
            remaining -= len(chunk)

        return bytes(received)

    def _finish_send_task(self, send_task: asyncio.Task[None]) -> None:
        self._send_tasks.discard(send_task)
        if send_task.cancelled():
            return
        try:
            send_task.exception()
        except asyncio.CancelledError:
            return


def _is_positive_finite_number(value: object) -> bool:
    # Every integer is finite; converting a large one to float would overflow.
    if type(value) is int:
        return value > 0
    return type(value) is float and value > 0 and math.isfinite(value)
