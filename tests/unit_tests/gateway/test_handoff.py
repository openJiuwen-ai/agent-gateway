# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""One ownership transfer, one correlated acknowledgement, and no redelivery."""

import asyncio
import socket
import sys

import pytest

from agent_gateway.transport.handoff import HandoffReceiver, HandoffRejected, HandoffSender
from agent_gateway.transport.handoff_channel import HandoffChannel, HandoffChannelClosed, ReceivedHandoff
from agent_gateway.transport.handoff_protocol import (
    HandoffAck, HandoffIdentity, HandoffOffer, HandoffProtocolError,
)


pytestmark = pytest.mark.skipif(sys.platform not in {'linux', 'darwin'}, reason='Unix descriptor transfer')


class HoldingRuntime:
    def __init__(self) -> None:
        self.connections = []

    async def adopt(self, sock: socket.socket, prefix: bytes) -> None:
        self.connections.append((sock, prefix))

    def close(self) -> None:
        for sock, _ in self.connections:
            sock.close()


def setup(runtime=None):
    left, right = socket.socketpair()
    identity = HandoffIdentity('group', 0, 1)
    sender = HandoffSender(HandoffChannel(left), identity)
    receiver = HandoffReceiver(HandoffChannel(right), identity, {'web': runtime} if runtime else {})
    return sender, receiver


async def test_ack_after_runtime_adoption_and_parent_ref_closed() -> None:
    runtime = HoldingRuntime()
    sender, receiver = setup(runtime)
    client, accepted = socket.socketpair()
    task = asyncio.create_task(receiver.receive_one())
    try:
        ack = await sender.handoff(accepted, 'web', b'prefix')
        await task
        assert ack.status == 'ADOPTED'
        assert accepted.fileno() == -1
        child, prefix = runtime.connections[0]
        assert prefix == b'prefix'
        client.sendall(b'later')
        assert await asyncio.get_running_loop().sock_recv(child, 5) == b'later'
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        runtime.close()
        client.close()
        sender.channel.close()
        receiver.channel.close()


async def test_unknown_runtime_is_closed_and_explicitly_rejected_without_breaking_channel() -> None:
    sender, receiver = setup()
    client, accepted = socket.socketpair()
    task = asyncio.create_task(receiver.receive_one())
    try:
        with pytest.raises(HandoffRejected, match='UNKNOWN_RUNTIME'):
            await sender.handoff(accepted, 'missing', b'')
        await task
        assert not sender.channel.closed
        assert accepted.fileno() == -1
        client.setblocking(False)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        client.close()
        sender.channel.close()
        receiver.channel.close()


async def test_adopter_failure_rejects_without_leaking_or_printing_secret() -> None:
    class BrokenRuntime:
        async def adopt(self, sock, prefix):
            raise RuntimeError('secret-request-prefix')

    sender, receiver = setup(BrokenRuntime())
    client, accepted = socket.socketpair()
    task = asyncio.create_task(receiver.receive_one())
    try:
        with pytest.raises(HandoffRejected) as error:
            await sender.handoff(accepted, 'web', b'secret-request-prefix')
        await task
        assert 'secret' not in str(error.value)
        client.setblocking(False)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        client.close()
        sender.channel.close()
        receiver.channel.close()


@pytest.mark.parametrize('mismatch', ['id', 'generation', 'slot', 'group'])
async def test_bad_ack_closes_channel_and_never_redelivers(mismatch: str) -> None:
    sender, receiver = setup()
    client, accepted = socket.socketpair()

    async def wrong_ack():
        received = await receiver.channel.receive()
        received.close()
        identity = sender.identity
        identity = HandoffIdentity(
            'wrong' if mismatch == 'group' else identity.group_id,
            1 if mismatch == 'slot' else 0,
            2 if mismatch == 'generation' else 1,
        )
        sequence = '2' if mismatch == 'id' else '1'
        await receiver.channel.send(HandoffAck(identity, f'{identity.slot_id}:{sequence}', 'ADOPTED'))

    task = asyncio.create_task(wrong_ack())
    try:
        with pytest.raises(HandoffProtocolError):
            await sender.handoff(accepted, 'web', b'', timeout=1)
        await task
        assert sender.channel.closed
        assert accepted.fileno() == -1
    finally:
        sender.channel.close()
        receiver.channel.close()
        client.close()


async def test_sender_timeout_closes_channel_and_client_copy() -> None:
    sender, receiver = setup()
    client, accepted = socket.socketpair()
    try:
        with pytest.raises(TimeoutError):
            await sender.handoff(accepted, 'web', b'', timeout=0.02)
        assert sender.channel.closed
        assert accepted.fileno() == -1
        received = await receiver.channel.receive()
        received.close()
    finally:
        sender.channel.close()
        receiver.channel.close()
        client.close()


async def test_cancel_after_fd_sent_cannot_be_retried() -> None:
    sender, receiver = setup()
    client, accepted = socket.socketpair()
    handing = asyncio.create_task(sender.handoff(accepted, 'web', b''))
    received = await receiver.channel.receive(timeout=1)
    try:
        handing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await handing
        assert sender.channel.closed
        assert accepted.fileno() == -1
        assert received.sock.fileno() >= 0
    finally:
        received.close()
        sender.channel.close()
        receiver.channel.close()
        client.close()


async def test_duplicate_offer_is_rejected_before_second_adoption() -> None:
    runtime = HoldingRuntime()
    sender, receiver = setup(runtime)
    client, accepted = socket.socketpair()
    try:
        for index in range(2):
            await sender.channel.send(HandoffOffer(sender.identity, '0:1', 'web', b''), accepted)
            if index == 0:
                await receiver.receive_one()
                assert (await sender.channel.receive()).message.status == 'ADOPTED'
            else:
                with pytest.raises(HandoffProtocolError):
                    await receiver.receive_one()
        assert len(runtime.connections) == 1
        assert receiver.channel.closed
    finally:
        runtime.close()
        sender.channel.close()
        receiver.channel.close()
        accepted.close()
        client.close()


async def test_waiting_sender_cancellation_does_not_corrupt_active_handoff() -> None:
    runtime = HoldingRuntime()
    sender, receiver = setup(runtime)
    clients = [socket.socketpair(), socket.socketpair()]
    first = asyncio.create_task(sender.handoff(clients[0][1], 'web', b'first'))
    incoming = await receiver.channel.receive(timeout=1)
    second = asyncio.create_task(sender.handoff(clients[1][1], 'web', b'second'))
    await asyncio.sleep(0)
    second.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await second
        assert not sender.channel.closed
        await receiver.channel.send(HandoffAck(sender.identity, incoming.message.handoff_id, 'ADOPTED'))
        await first
        assert clients[1][1].fileno() == -1
    finally:
        incoming.close()
        sender.channel.close()
        receiver.channel.close()
        for client, accepted in clients:
            client.close()
            accepted.close()


async def test_cancel_during_adoption_closes_unadopted_descriptor() -> None:
    class WaitingRuntime:
        def __init__(self):
            self.started = asyncio.Event()
            self.sock = None
            self.cancelled = False

        async def adopt(self, sock, prefix):
            self.sock = sock
            self.started.set()
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled = True

    runtime = WaitingRuntime()
    sender, receiver = setup(runtime)
    client, accepted = socket.socketpair()
    client.setblocking(False)
    receiving = asyncio.create_task(receiver.receive_one())
    sending = asyncio.create_task(sender.handoff(accepted, 'web', b'', timeout=1))
    try:
        await asyncio.wait_for(runtime.started.wait(), 1)
        receiving.cancel()
        with pytest.raises(asyncio.CancelledError):
            await receiving
        with pytest.raises(HandoffChannelClosed):
            await sending
        assert runtime.cancelled
        assert runtime.sock.fileno() == -1
        assert accepted.fileno() == -1
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        for task in (receiving, sending):
            task.cancel()
        await asyncio.gather(receiving, sending, return_exceptions=True)
        sender.channel.close()
        receiver.channel.close()
        client.close()
        accepted.close()


async def test_ack_send_failure_does_not_close_or_shutdown_adopted_connection() -> None:
    runtime = HoldingRuntime()
    sender, receiver = setup(runtime)
    client, accepted = socket.socketpair()
    client.setblocking(False)
    try:
        # Deliberately lose the ACK path after a complete offer has reached the peer.
        await sender.channel.send(HandoffOffer(sender.identity, '0:1', 'web', b'once'), accepted)
        sender.channel.close()
        with pytest.raises(OSError):
            await receiver.receive_one()
        # Keep our reference until the peer has read the offer: on macOS a socket still queued
        # in an unread SCM_RIGHTS message arrives dead once the sender closed it and its endpoint.
        accepted.close()
        child, prefix = runtime.connections[0]
        assert prefix == b'once'
        loop = asyncio.get_running_loop()
        await loop.sock_sendall(client, b'still-open')
        assert await asyncio.wait_for(loop.sock_recv(child, 10), 1) == b'still-open'
        await loop.sock_sendall(child, b'direct')
        assert await asyncio.wait_for(loop.sock_recv(client, 6), 1) == b'direct'
        assert receiver.channel.closed
    finally:
        runtime.close()
        client.close()
        accepted.close()
        sender.channel.close()
        receiver.channel.close()


async def test_closed_client_socket_is_rejected_without_closing_a_healthy_channel() -> None:
    runtime = HoldingRuntime()
    sender, receiver = setup(runtime)
    dead, dead_peer = socket.socketpair()
    dead.close()
    live, live_peer = socket.socketpair()
    task = asyncio.create_task(receiver.receive_one())
    try:
        with pytest.raises(HandoffProtocolError, match='closed'):
            await sender.handoff(dead, 'web', b'')
        assert not sender.channel.closed
        assert not receiver.channel.closed
        assert (await sender.handoff(live, 'web', b'prefix')).status == 'ADOPTED'
        await task
        assert [prefix for _sock, prefix in runtime.connections] == [b'prefix']
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        runtime.close()
        for peer in (dead_peer, live_peer):
            peer.close()
        sender.channel.close()
        receiver.channel.close()



async def test_sender_keeps_its_client_reference_until_the_worker_takes_the_offer() -> None:
    # When a closed queued descriptor turns dead is kernel timing that a test cannot
    # control, so observe the reference that prevents it instead of the symptom.
    class LateReadingChannel:
        """A worker endpoint that takes the descriptor only from inside receive()."""

        def __init__(self, descriptor: socket.socket) -> None:
            self.descriptor = descriptor
            self.offered: HandoffOffer | None = None
            self.descriptor_at_send = -1
            self.descriptor_at_read = -1
            self.closed = False

        async def send(self, message: HandoffOffer, descriptor=None, *, timeout=None) -> None:
            self.offered = message
            self.descriptor_at_send = descriptor.fileno()

        async def receive(self, *, timeout=None) -> ReceivedHandoff:
            # The worker's recvmsg takes the descriptor now.
            self.descriptor_at_read = self.descriptor.fileno()
            return ReceivedHandoff(HandoffAck(
                self.offered.identity, self.offered.handoff_id, 'ADOPTED',
            ))

        def close(self) -> None:
            self.closed = True

    client, accepted = socket.socketpair()
    channel = LateReadingChannel(accepted)
    identity = HandoffIdentity('group', 0, 1)
    try:
        ack = await HandoffSender(channel, identity).handoff(accepted, 'web', b'')
        assert ack.status == 'ADOPTED'
        assert channel.descriptor_at_send >= 0
        assert channel.descriptor_at_read == channel.descriptor_at_send
        assert not channel.closed
        assert accepted.fileno() == -1
    finally:
        client.close()

async def test_client_connection_stays_usable_when_the_worker_reads_the_offer_late() -> None:
    runtime = HoldingRuntime()
    sender, receiver = setup(runtime)
    client, accepted = socket.socketpair()
    client.setblocking(False)
    sending = asyncio.create_task(sender.handoff(accepted, 'web', b'head', timeout=3))
    receiving = None
    try:
        # The worker event loop is still busy when the offer reaches the channel.
        await asyncio.sleep(.05)
        receiving = asyncio.create_task(receiver.receive_one())
        assert (await sending).status == 'ADOPTED'
        await receiving
        child, prefix = runtime.connections[0]
        assert prefix == b'head'
        loop = asyncio.get_running_loop()
        await loop.sock_sendall(client, b'late-but-alive')
        assert await asyncio.wait_for(loop.sock_recv(child, 32), 1) == b'late-but-alive'
    finally:
        for task in (sending, receiving):
            if task is not None:
                task.cancel()
        await asyncio.gather(*(task for task in (sending, receiving) if task), return_exceptions=True)
        runtime.close()
        client.close()
        sender.channel.close()
        receiver.channel.close()


@pytest.mark.parametrize('mismatch', ['group', 'slot', 'generation'])
async def test_offer_for_another_target_is_rejected_before_adoption(mismatch: str) -> None:
    runtime = HoldingRuntime()
    sender, receiver = setup(runtime)
    identity = sender.identity
    identity = HandoffIdentity(
        'other' if mismatch == 'group' else identity.group_id,
        1 if mismatch == 'slot' else identity.slot_id,
        2 if mismatch == 'generation' else identity.generation,
    )
    client, accepted = socket.socketpair()
    try:
        await sender.channel.send(HandoffOffer(identity, f'{identity.slot_id}:1', 'web', b''), accepted)
        with pytest.raises(HandoffProtocolError, match='invalid handoff target'):
            await receiver.receive_one()
        assert runtime.connections == []
        assert receiver.channel.closed
        accepted.close()
        client.setblocking(False)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        runtime.close()
        accepted.close()
        client.close()
        sender.channel.close()
        receiver.channel.close()
