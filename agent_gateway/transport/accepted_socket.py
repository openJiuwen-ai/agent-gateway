# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Adopt an existing connection and replay its prefix before socket reads."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import socket

from agent_gateway.transport.handoff_protocol import PREFIX_LIMIT


class _ReplayTransport(asyncio.Transport):
    def __init__(self, transport: asyncio.Transport, protocol: asyncio.Protocol) -> None:
        self._transport = transport
        self._protocol = protocol
        self._replaying = True
        self._reading_requested = True

    def get_extra_info(self, name, default=None):
        return self._transport.get_extra_info(name, default)

    def is_closing(self):
        return self._transport.is_closing()

    def close(self):
        self._transport.close()

    def abort(self):
        self._transport.abort()

    def get_protocol(self):
        return self._protocol

    def set_protocol(self, protocol):
        self._protocol = protocol
        if not self._replaying:
            self._transport.set_protocol(protocol)

    def is_reading(self):
        """Reading as the inner protocol asked for it; the real pause only serves replay."""
        return self._reading_requested if self._replaying else self._transport.is_reading()

    def pause_reading(self):
        self._reading_requested = False
        self._transport.pause_reading()

    def resume_reading(self):
        self._reading_requested = True
        if not self._replaying:
            self._transport.resume_reading()

    def set_write_buffer_limits(self, high=None, low=None):
        self._transport.set_write_buffer_limits(high=high, low=low)

    def get_write_buffer_limits(self):
        return self._transport.get_write_buffer_limits()

    def get_write_buffer_size(self):
        return self._transport.get_write_buffer_size()

    def write(self, data):
        self._transport.write(data)

    def writelines(self, lines):
        self._transport.writelines(lines)

    def can_write_eof(self):
        return self._transport.can_write_eof()

    def write_eof(self):
        self._transport.write_eof()

    def finish_replay(self) -> None:
        self._transport.set_protocol(self._protocol)
        self._replaying = False
        if self._reading_requested and not self._transport.is_closing():
            self._transport.resume_reading()


class _AcceptedProtocol(asyncio.Protocol):
    def __init__(self, protocol: asyncio.Protocol, prefix: bytes) -> None:
        self.protocol = protocol
        self.prefix = prefix
        self.replay: _ReplayTransport | None = None
        self.transport: asyncio.Transport | None = None
        self.ready = asyncio.get_running_loop().create_future()

    def current_protocol(self) -> asyncio.Protocol:
        """The inner protocol that last received an event, even once the transport closed."""
        return self.protocol if self.replay is None else self.replay.get_protocol()

    def connection_made(self, transport: asyncio.Transport) -> None:
        self.transport = transport
        transport.pause_reading()
        replay = self.replay = _ReplayTransport(transport, self.protocol)
        try:
            self.protocol.connection_made(replay)
            if self.prefix:
                replay.get_protocol().data_received(self.prefix)
            replay.finish_replay()
        except BaseException as error:
            transport.abort()
            if not self.ready.done():
                self.ready.set_exception(error)
        else:
            if not self.ready.done():
                self.ready.set_result(None)
        finally:
            self.prefix = b''

    def data_received(self, data):
        # Bytes a loop delivers before the replay finishes still belong to the inner protocol.
        self.current_protocol().data_received(data)

    def connection_lost(self, error):
        self.current_protocol().connection_lost(error)

    def eof_received(self):
        return self.current_protocol().eof_received()

    def pause_writing(self):
        self.current_protocol().pause_writing()

    def resume_writing(self):
        self.current_protocol().resume_writing()


async def adopt_connection(
    sock: socket.socket, prefix: bytes, protocol_factory: Callable[[], asyncio.Protocol],
) -> tuple[asyncio.Transport, asyncio.Protocol]:
    """Take ownership and return only after connection_made and ordered replay."""
    bootstrap = None
    try:
        if not isinstance(prefix, bytes) or len(prefix) > PREFIX_LIMIT:
            raise ValueError('invalid accepted connection prefix')
        protocol = protocol_factory()
        bootstrap = _AcceptedProtocol(protocol, prefix)
        transport, _wrapper = await asyncio.get_running_loop().connect_accepted_socket(
            lambda: bootstrap, sock,
        )
        await bootstrap.ready
        return transport, bootstrap.current_protocol()
    except BaseException:
        if bootstrap is not None and bootstrap.transport is not None:
            bootstrap.transport.abort()
        else:
            sock.close()
        if bootstrap is not None and bootstrap.ready.done() and not bootstrap.ready.cancelled():
            bootstrap.ready.exception()
        raise
