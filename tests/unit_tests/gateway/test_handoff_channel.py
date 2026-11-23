# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Real ancillary descriptor transfer and bounded stream framing."""

import array
import asyncio
import os
import socket
import struct
import sys

import pytest

from agent_gateway.transport.handoff_channel import HandoffChannel, HandoffChannelClosed
from agent_gateway.transport.handoff_protocol import (
    HandoffAck, HandoffIdentity, HandoffOffer, HandoffProtocolError, encode_frame,
)

pytestmark = pytest.mark.skipif(sys.platform not in {'linux', 'darwin'}, reason='Unix descriptor transfer')


def offer(prefix: bytes = b'headers-and-first-frame') -> HandoffOffer:
    return HandoffOffer(HandoffIdentity('group', 0, 1), '0:1', 'web', prefix)


def pair() -> tuple[HandoffChannel, HandoffChannel]:
    left, right = socket.socketpair()
    return HandoffChannel(left), HandoffChannel(right)


async def test_passed_descriptor_is_the_same_connection_not_its_number() -> None:
    parent, worker = pair()
    client, accepted = socket.socketpair()
    received = None
    try:
        await parent.send(offer(), accepted)
        received = await worker.receive(timeout=1)
        assert received.message == offer()
        assert received.sock is not None
        assert received.sock.fileno() != accepted.fileno()
        assert received.sock.getblocking() is False
        assert received.sock.get_inheritable() is False
        accepted.close()
        client.sendall(b'remainder')
        assert await asyncio.get_running_loop().sock_recv(received.sock, 9) == b'remainder'
        await asyncio.get_running_loop().sock_sendall(received.sock, b'direct')
        assert client.recv(6) == b'direct'
    finally:
        if received is not None:
            received.close()
        client.close()
        accepted.close()
        parent.close()
        worker.close()


async def test_ack_requires_no_descriptor() -> None:
    parent, worker = pair()
    try:
        ack = HandoffAck(offer().identity, '0:1', 'ADOPTED')
        await worker.send(ack)
        received = await parent.receive(timeout=1)
        assert received.message == ack
        assert received.sock is None
    finally:
        parent.close()
        worker.close()


async def test_short_write_attaches_rights_exactly_once(monkeypatch: pytest.MonkeyPatch) -> None:
    parent, worker = pair()
    client, accepted = socket.socketpair()
    original = parent._sendmsg
    attachments = []

    def short_write(data, ancillary):
        attachments.append(bool(ancillary))
        return original(data[:3], ancillary)

    monkeypatch.setattr(parent, '_sendmsg', short_write)
    try:
        sending = asyncio.create_task(parent.send(offer(b'x' * 500), accepted, timeout=2))
        received = await worker.receive(timeout=2)
        await sending
        assert received.message == offer(b'x' * 500)
        assert attachments.count(True) == 1
        assert len(attachments) > 1
        received.close()
    finally:
        accepted.close()
        client.close()
        parent.close()
        worker.close()


async def test_every_read_uses_recvmsg_and_accumulates_short_frames(monkeypatch: pytest.MonkeyPatch) -> None:
    parent, worker = pair()
    client, accepted = socket.socketpair()
    original = worker._recvmsg
    reads = []

    def short_read(size):
        reads.append(size)
        return original(min(size, 2))

    monkeypatch.setattr(worker, '_recvmsg', short_read)
    try:
        await parent.send(offer(), accepted)
        received = await worker.receive(timeout=1)
        assert received.message == offer()
        assert len(reads) > 4
        received.close()
    finally:
        client.close()
        accepted.close()
        parent.close()
        worker.close()


async def test_multiple_frames_are_not_consumed_as_one() -> None:
    parent, worker = pair()
    try:
        ack = HandoffAck(offer().identity, '0:1', 'ADOPTED')
        await worker.send(ack)
        await worker.send(ack)
        assert (await parent.receive()).message == ack
        assert (await parent.receive()).message == ack
    finally:
        parent.close()
        worker.close()


@pytest.mark.parametrize('count', [0, 2])
async def test_offer_rejects_wrong_number_of_fds_and_closes_received_copies(count: int) -> None:
    left, right = socket.socketpair()
    worker = HandoffChannel(right)
    client, accepted = socket.socketpair()
    accepted.setblocking(False)
    try:
        ancillary = [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [accepted.fileno()] * count))] if count else []
        left.sendmsg([encode_frame(offer())], ancillary)
        accepted.close()
        with pytest.raises(HandoffProtocolError):
            await worker.receive(timeout=1)
        assert worker.closed
        client.setblocking(False)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        left.close()
        worker.close()
        accepted.close()
        client.close()


async def test_oversized_ancillary_closes_all_delivered_fds() -> None:
    left, right = socket.socketpair()
    worker = HandoffChannel(right)
    client, accepted = socket.socketpair()
    try:
        left.sendmsg([encode_frame(offer())], [
            (socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [accepted.fileno()] * 40)),
        ])
        accepted.close()
        with pytest.raises(HandoffProtocolError):
            await worker.receive(timeout=1)
        client.setblocking(False)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        worker.close()
        left.close()
        accepted.close()
        client.close()


async def test_malformed_frame_closes_descriptor_before_raising() -> None:
    left, right = socket.socketpair()
    worker = HandoffChannel(right)
    client, accepted = socket.socketpair()
    try:
        left.sendmsg([struct.pack('>I', 999999)], [
            (socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [accepted.fileno()])),
        ])
        accepted.close()
        with pytest.raises(HandoffProtocolError):
            await worker.receive(timeout=1)
        client.setblocking(False)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        worker.close()
        left.close()
        accepted.close()
        client.close()


async def test_receive_cancellation_closes_already_received_fd() -> None:
    left, right = socket.socketpair()
    worker = HandoffChannel(right)
    client, accepted = socket.socketpair()
    left.sendmsg([encode_frame(offer())[:4]], [
        (socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [accepted.fileno()])),
    ])
    accepted.close()
    try:
        receiving = asyncio.create_task(worker.receive())
        await asyncio.sleep(0)
        receiving.cancel()
        with pytest.raises(asyncio.CancelledError):
            await receiving
        assert worker.closed
        client.setblocking(False)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        left.close()
        worker.close()
        client.close()


async def test_close_wakes_pending_reader_and_is_idempotent() -> None:
    parent, worker = pair()
    reading = asyncio.create_task(worker.receive())
    await asyncio.sleep(0)
    worker.close()
    worker.close()
    with pytest.raises(HandoffChannelClosed):
        await asyncio.wait_for(reading, 1)
    parent.close()


async def test_only_one_reader_and_timeout_closes_channel() -> None:
    parent, worker = pair()
    receiving = asyncio.create_task(worker.receive(timeout=0.05))
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match='single reader'):
        await worker.receive()
    with pytest.raises(TimeoutError):
        await receiving
    assert worker.closed
    parent.close()


async def test_wrong_outgoing_fd_does_not_send_and_leaves_ownership_with_caller() -> None:
    parent, worker = pair()
    client, accepted = socket.socketpair()
    try:
        with pytest.raises(HandoffProtocolError):
            await parent.send(HandoffAck(offer().identity, '0:1', 'ADOPTED'), accepted)
        assert accepted.fileno() >= 0
        assert not parent.closed
    finally:
        parent.close()
        worker.close()
        client.close()
        accepted.close()


async def test_truncation_flag_rejects_and_closes_delivered_fds(monkeypatch: pytest.MonkeyPatch) -> None:
    parent, worker = pair()
    client, accepted = socket.socketpair()
    original = worker._recvmsg

    def marked_truncated(size):
        data, control, flags, address = original(size)
        return data, control, flags | socket.MSG_CTRUNC, address

    monkeypatch.setattr(worker, '_recvmsg', marked_truncated)
    try:
        await parent.send(offer(), accepted)
        accepted.close()
        with pytest.raises(HandoffProtocolError):
            await worker.receive(timeout=1)
        client.setblocking(False)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        client.close()
        accepted.close()
        parent.close()
        worker.close()


async def test_non_socket_descriptor_is_closed_on_rejection() -> None:
    left, right = socket.socketpair()
    worker = HandoffChannel(right)
    read_fd, write_fd = os.pipe()
    original_read_fd = read_fd
    try:
        left.sendmsg([encode_frame(offer())], [
            (socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array('i', [write_fd])),
        ])
        os.close(write_fd)
        write_fd = -1
        with pytest.raises((HandoffProtocolError, OSError)):
            await worker.receive(timeout=1)
        os.set_blocking(read_fd, False)
        assert os.read(read_fd, 1) == b''
    finally:
        left.close()
        worker.close()
        os.close(original_read_fd)
        if write_fd >= 0:
            os.close(write_fd)
