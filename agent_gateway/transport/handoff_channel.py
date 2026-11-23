# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Nonblocking, single-reader Unix stream channel with ancillary fd ownership."""

from __future__ import annotations

import array
import asyncio
from dataclasses import dataclass, field
import os
import socket
from typing import Any
import warnings

from agent_gateway.transport.handoff_protocol import (
    FRAME_LIMIT, HandoffAck, HandoffOffer, HandoffProtocolError, decode_frame, encode_frame,
)


class HandoffChannelClosed(ConnectionError):
    """The private channel is no longer usable; never retry on another target."""


@dataclass
class ReceivedHandoff:
    message: HandoffOffer | HandoffAck
    sock: socket.socket | None = field(default=None, repr=False)

    def close(self) -> None:
        """Release only the local reference; do not shutdown a shared client socket."""
        if self.sock is not None:
            self.sock.close()
            self.sock = None


class HandoffChannel:
    """Own one Unix socket endpoint; receive every byte using recvmsg only."""

    def __init__(self, sock: socket.socket) -> None:
        if sock.family != socket.AF_UNIX or sock.type != socket.SOCK_STREAM:
            raise ValueError('handoff channel requires a Unix stream socket')
        self._sock = sock
        self._sock.setblocking(False)
        self._sock.set_inheritable(False)
        self._closed = False
        self._reader_active = False
        self._send_lock = asyncio.Lock()
        self._waiters: dict[str, asyncio.Future] = {}

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for mode, future in tuple(self._waiters.items()):
            loop = future.get_loop()
            remove = loop.remove_reader if mode == 'read' else loop.remove_writer
            remove(self._sock.fileno())
            if not future.done():
                future.set_result(None)
        # This endpoint is a private control socket, not a transferred client.
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    async def _wait_io(self, mode: str) -> None:
        if self._closed:
            raise HandoffChannelClosed('handoff channel is closed')
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        descriptor = self._sock.fileno()
        add = loop.add_reader if mode == 'read' else loop.add_writer
        remove = loop.remove_reader if mode == 'read' else loop.remove_writer

        def ready() -> None:
            if not future.done():
                future.set_result(None)

        self._waiters[mode] = future
        add(descriptor, ready)
        try:
            await future
            if self._closed:
                raise HandoffChannelClosed('handoff channel is closed')
        finally:
            # A new socket may have reused this fd after close; don't remove its watcher.
            if self._sock.fileno() == descriptor:
                remove(descriptor)
            if self._waiters.get(mode) is future:
                self._waiters.pop(mode, None)

    def _sendmsg(self, data: memoryview, ancillary: list) -> int:
        return self._sock.sendmsg([data], ancillary)

    def _recvmsg(self, size: int) -> tuple[bytes, list, int, Any]:
        # CPython may warn before returning MSG_CTRUNC. Retrieve the delivered fds
        # even when applications turn warnings into exceptions, then close them.
        # Linux/macOS cap one SCM_RIGHTS message below 256 descriptors; size
        # the control buffer to receive and reject all fds without truncation.
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', message='received malformed or improperly-truncated ancillary data',
                category=RuntimeWarning,
            )
            return self._sock.recvmsg(size, socket.CMSG_SPACE(256 * array.array('i').itemsize))

    async def send(
        self, message: HandoffOffer | HandoffAck, descriptor: socket.socket | None = None,
        *, timeout: float | None = None,
    ) -> None:
        frame = encode_frame(message)
        if isinstance(message, HandoffOffer) != (descriptor is not None):
            raise HandoffProtocolError('only offers require exactly one descriptor')
        if descriptor is not None and descriptor.fileno() < 0:
            raise HandoffProtocolError('descriptor is closed')
        try:
            async with asyncio.timeout(timeout):
                async with self._send_lock:
                    ancillary = [] if descriptor is None else [
                        (socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [descriptor.fileno()])),
                    ]
                    offset = 0
                    while offset < len(frame):
                        if self._closed:
                            raise HandoffChannelClosed('handoff channel is closed')
                        try:
                            sent = self._sendmsg(memoryview(frame)[offset:], ancillary)
                        except (BlockingIOError, InterruptedError):
                            await self._wait_io('write')
                            continue
                        if sent <= 0:
                            raise HandoffChannelClosed('handoff channel short send')
                        offset += sent
                        ancillary = []
        except BaseException:
            self.close()
            raise

    async def receive(self, *, timeout: float | None = None) -> ReceivedHandoff:
        if self._closed:
            raise HandoffChannelClosed('handoff channel is closed')
        if self._reader_active:
            raise RuntimeError('HandoffChannel supports a single reader')
        self._reader_active = True
        descriptors: list[int] = []
        owned_socket = None
        try:
            async with asyncio.timeout(timeout):
                header = await self._recv_exact(4, descriptors)
                payload_length = int.from_bytes(header, 'big')
                if not 4 < payload_length <= FRAME_LIMIT - 4:
                    raise HandoffProtocolError('handoff frame exceeds size limit')
                payload = await self._recv_exact(payload_length, descriptors)
                message = decode_frame(header + payload)
                expected = 1 if isinstance(message, HandoffOffer) else 0
                if len(descriptors) != expected:
                    raise HandoffProtocolError('wrong descriptor count')
                if descriptors:
                    owned_socket = socket.socket(fileno=descriptors[0])
                    descriptors.pop(0)
                    if owned_socket.type != socket.SOCK_STREAM:
                        raise HandoffProtocolError('client descriptor is not a stream socket')
                    owned_socket.getpeername()
                    owned_socket.setblocking(False)
                result = ReceivedHandoff(message, owned_socket)
                owned_socket = None
                return result
        except BaseException:
            self.close()
            raise
        finally:
            for descriptor in descriptors:
                os.close(descriptor)
            if owned_socket is not None:
                owned_socket.close()
            self._reader_active = False

    async def _recv_exact(self, size: int, descriptors: list[int]) -> bytes:
        received = bytearray()
        while len(received) < size:
            if self._closed:
                raise HandoffChannelClosed('handoff channel is closed')
            try:
                data, ancillary, flags, _address = self._recvmsg(size - len(received))
            except (BlockingIOError, InterruptedError):
                await self._wait_io('read')
                continue
            invalid_control = False
            for level, kind, contents in ancillary:
                if level != socket.SOL_SOCKET or kind != socket.SCM_RIGHTS:
                    invalid_control = True
                    continue
                values = array.array('i')
                remainder = len(contents) % values.itemsize
                values.frombytes(contents[:len(contents) - remainder])
                descriptors.extend(values)
                if remainder:
                    invalid_control = True
            # Record all delivered fds before any operation that can fail.
            for descriptor in descriptors:
                os.set_inheritable(descriptor, False)
            if flags & (socket.MSG_CTRUNC | socket.MSG_TRUNC) or invalid_control or len(descriptors) > 1:
                raise HandoffProtocolError('invalid ancillary descriptor data')
            if not data:
                raise HandoffChannelClosed('handoff channel reached EOF')
            received.extend(data)
        return bytes(received)
