# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""One-way socket ownership transfer with a single runtime-adoption ACK."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
import socket
from typing import Protocol

from agent_gateway.transport.handoff_channel import HandoffChannel
from agent_gateway.transport.handoff_protocol import (
    HandoffAck, HandoffIdentity, HandoffOffer, HandoffProtocolError, encode_frame, sequence_for,
)


class ConnectionAdopter(Protocol):
    """Success transfers ownership; raising must leave no active protocol tasks."""

    async def adopt(self, sock: socket.socket, prefix: bytes) -> None: ...


class HandoffRejected(ConnectionError):
    """The receiver confirmed that no runtime adopted this connection."""


class HandoffSender:
    """Serialize handoffs; caller transfers socket ownership on invocation."""

    def __init__(self, channel: HandoffChannel, identity: HandoffIdentity) -> None:
        self.channel = channel
        self.identity = identity
        self._sequence = 0
        self._lock = asyncio.Lock()

    async def handoff(
        self, sock: socket.socket, runtime_id: str, prefix: bytes, *, timeout: float = 2.0,
    ) -> HandoffAck:
        """Close our client reference once the peer has taken it; never shutdown or retry.

        The reference has to outlive the peer's read of the offer. On macOS a socket that
        is still queued in an unread SCM_RIGHTS message can be delivered dead, reading EOF
        at once, once the sender has closed its last reference.

        A handoff that fails or is cancelled after the send still releases its reference at
        once. An offer the peer has not read by then reaches it dead on macOS; that
        connection was abandoned with the channel, and the runtime only sees it drop.
        """
        active = False
        try:
            async with asyncio.timeout(timeout):
                async with self._lock:
                    self._sequence += 1
                    offer = HandoffOffer(self.identity, f'{self.identity.slot_id}:{self._sequence}', runtime_id, prefix)
                    # Invalid local inputs cannot corrupt an otherwise healthy channel.
                    encode_frame(offer)
                    if sock.fileno() < 0:
                        raise HandoffProtocolError('descriptor is closed')
                    active = True
                    await self.channel.send(offer, sock)
                    received = await self.channel.receive()
                    try:
                        acknowledgement = received.message
                        if (
                            not isinstance(acknowledgement, HandoffAck)
                            or acknowledgement.identity != self.identity
                            or acknowledgement.handoff_id != offer.handoff_id
                        ):
                            raise HandoffProtocolError('uncorrelated handoff acknowledgement')
                        if acknowledgement.status == 'REJECTED':
                            raise HandoffRejected(acknowledgement.error_code)
                        return acknowledgement
                    finally:
                        received.close()
        except HandoffRejected:
            raise
        except BaseException:
            if active:
                self.channel.close()
            raise
        finally:
            sock.close()


class HandoffReceiver:
    """One reader owns each received descriptor until a registered runtime adopts it."""

    def __init__(
        self, channel: HandoffChannel, identity: HandoffIdentity,
        runtimes: Mapping[str, ConnectionAdopter],
    ) -> None:
        self.channel = channel
        self.identity = identity
        self._runtimes = dict(runtimes)
        self._last_sequence = 0

    async def receive_one(self) -> HandoffAck:
        received = await self.channel.receive()
        try:
            offer = received.message
            if not isinstance(offer, HandoffOffer) or offer.identity != self.identity:
                raise HandoffProtocolError('invalid handoff target')
            sequence = sequence_for(self.identity, offer.handoff_id)
            if sequence <= self._last_sequence:
                raise HandoffProtocolError('replayed handoff sequence')
            self._last_sequence = sequence
            runtime = self._runtimes.get(offer.runtime_id)
            error_code = 'UNKNOWN_RUNTIME'
            if runtime is not None:
                try:
                    await runtime.adopt(received.sock, offer.prefix)
                except Exception:
                    error_code = 'ADOPTION_FAILED'
                else:
                    # The runtime now owns the socket, even if sending ACK later fails.
                    received.sock = None
                    error_code = None
            received.close()
            acknowledgement = HandoffAck(
                self.identity, offer.handoff_id,
                'ADOPTED' if error_code is None else 'REJECTED', error_code,
            )
            await self.channel.send(acknowledgement, timeout=2.0)
            return acknowledgement
        except BaseException:
            self.channel.close()
            raise
        finally:
            received.close()
