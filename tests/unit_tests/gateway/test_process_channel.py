# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Unit tests for the async process management channel."""

import asyncio
import socket
import struct

import pytest

from agent_gateway.processes.channel import ChannelClosed, ManagementChannel
from agent_gateway.processes.management_protocol import ManagementProtocolError


def init_message(slot_id: int = 0) -> dict[str, object]:
    return {
        "protocol_version": 1,
        "group_id": "gateway-group",
        "slot_id": slot_id,
        "generation": 1,
        "type": "INIT",
    }


def create_pair(
    frame_limit: int = 16384,
) -> tuple[ManagementChannel, ManagementChannel]:
    parent_socket, worker_socket = socket.socketpair()
    return (
        ManagementChannel(parent_socket, frame_limit=frame_limit),
        ManagementChannel(worker_socket, frame_limit=frame_limit),
    )


def fill_send_buffer(channel: ManagementChannel) -> None:
    while True:
        try:
            channel._sock.send(b"x" * 4096)
        except BlockingIOError:
            break


async def test_send_receive_round_trips_a_frame() -> None:
    parent, worker = create_pair()
    message = init_message()

    await parent.send(message)
    assert await worker.receive() == message

    parent.close()
    worker.close()


async def test_constructor_sets_socket_nonblocking_and_closed_is_read_only() -> None:
    parent, worker = create_pair()

    assert parent._sock.getblocking() is False
    assert worker._sock.getblocking() is False
    assert parent.closed is False
    with pytest.raises(AttributeError):
        parent.closed = True

    parent.close()
    worker.close()


async def test_send_serializes_concurrent_calls() -> None:
    parent, worker = create_pair()
    first = init_message(0)
    second = init_message(1)

    await asyncio.gather(parent.send(first), parent.send(second))
    received = [await worker.receive(), await worker.receive()]

    assert received == [first, second]

    parent.close()
    worker.close()


async def test_send_timeout_includes_waiting_for_send_lock() -> None:
    parent, worker = create_pair()
    message = init_message()
    fill_send_buffer(parent)

    blocked_send = asyncio.create_task(parent.send(message))
    await asyncio.sleep(0)
    timed_send = asyncio.create_task(parent.send(message, timeout=0.01))

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(timed_send, timeout=0.2)

    assert parent.closed is True
    blocked_send.cancel()
    with pytest.raises((asyncio.CancelledError, ChannelClosed)):
        await blocked_send

    worker.close()


async def test_receive_rejects_concurrent_readers() -> None:
    parent, worker = create_pair()
    reader = asyncio.create_task(worker.receive(timeout=0.1))
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="single reader"):
        await worker.receive()

    with pytest.raises(TimeoutError):
        await reader

    assert worker.closed is True
    parent.close()


async def test_close_shuts_down_socket_and_is_idempotent() -> None:
    parent, worker = create_pair()
    socket_fileno = parent._sock.fileno()

    parent.close()
    parent.close()

    assert parent.closed is True
    assert parent._sock.fileno() == -1
    assert socket_fileno >= 0
    with pytest.raises(ChannelClosed):
        await parent.send(init_message())
    with pytest.raises(ChannelClosed):
        await parent.receive()

    worker.close()


async def test_receive_reports_eof_as_channel_closed() -> None:
    parent, worker = create_pair()
    parent.close()

    with pytest.raises(ChannelClosed):
        await worker.receive()

    assert worker.closed is True


async def test_receive_reports_truncated_frame_as_protocol_error() -> None:
    parent, worker = create_pair()
    parent._sock.send(struct.pack(">I", 10) + b"{")
    parent.close()

    with pytest.raises(ManagementProtocolError, match="truncated"):
        await worker.receive()

    assert worker.closed is True


async def test_receive_reports_missing_body_as_truncated_frame() -> None:
    parent, worker = create_pair()
    parent._sock.send(struct.pack(">I", 10))
    parent.close()

    with pytest.raises(ManagementProtocolError, match="truncated"):
        await worker.receive()

    assert worker.closed is True


async def test_receive_rejects_zero_length_payload() -> None:
    parent, worker = create_pair()
    parent._sock.send(struct.pack(">I", 0))
    parent.close()

    with pytest.raises(ManagementProtocolError, match="empty"):
        await worker.receive()

    assert worker.closed is True


async def test_receive_rejects_oversize_frame_before_reading_body() -> None:
    parent, worker = create_pair(frame_limit=64)
    parent._sock.send(struct.pack(">I", 65))
    parent.close()

    with pytest.raises(ManagementProtocolError, match="frame limit"):
        await worker.receive()

    assert worker.closed is True


async def test_receive_timeout_closes_channel() -> None:
    parent, worker = create_pair()

    with pytest.raises(TimeoutError):
        await worker.receive(timeout=0.01)

    assert worker.closed is True
    parent.close()


async def test_receive_cancellation_closes_channel() -> None:
    parent, worker = create_pair()
    reader = asyncio.create_task(worker.receive())
    await asyncio.sleep(0)
    reader.cancel()

    with pytest.raises(asyncio.CancelledError):
        await reader

    assert worker.closed is True
    parent.close()


async def test_send_rejects_invalid_message_without_writing() -> None:
    parent, worker = create_pair()
    message = init_message()
    message["type"] = "PING"

    with pytest.raises(ManagementProtocolError):
        await parent.send(message)

    with pytest.raises(TimeoutError):
        await worker.receive(timeout=0.01)

    parent.close()
    worker.close()


async def test_receive_rejects_invalid_json_and_closes_channel() -> None:
    parent, worker = create_pair()
    payload = b"{"
    parent._sock.send(struct.pack(">I", len(payload)) + payload)
    parent.close()

    with pytest.raises(ManagementProtocolError, match="JSON"):
        await worker.receive()

    assert worker.closed is True


async def test_send_reports_peer_close_as_channel_closed() -> None:
    parent, worker = create_pair()
    worker.close()

    with pytest.raises(ChannelClosed):
        await parent.send(init_message())

    assert parent.closed is True


async def test_send_cancellation_closes_channel() -> None:
    parent, worker = create_pair()
    fill_send_buffer(parent)
    blocked_send = asyncio.create_task(parent.send(init_message()))
    await asyncio.sleep(0)

    blocked_send.cancel()

    with pytest.raises(asyncio.CancelledError):
        await blocked_send

    assert parent.closed is True
    worker.close()


async def test_close_cancels_inflight_receive() -> None:
    parent, worker = create_pair()
    reader = asyncio.create_task(worker.receive())
    await asyncio.sleep(0)

    worker.close()

    with pytest.raises(asyncio.CancelledError):
        await reader

    assert worker.closed is True
    parent.close()


async def test_close_cancels_inflight_send() -> None:
    parent, worker = create_pair()
    fill_send_buffer(parent)
    blocked_send = asyncio.create_task(parent.send(init_message()))
    await asyncio.sleep(0)

    parent.close()

    with pytest.raises(asyncio.CancelledError):
        await blocked_send

    assert parent.closed is True
    worker.close()


async def test_send_waiting_for_lock_reports_channel_closed() -> None:
    parent, worker = create_pair()
    fill_send_buffer(parent)
    blocked_send = asyncio.create_task(parent.send(init_message()))
    await asyncio.sleep(0)
    waiting_send = asyncio.create_task(parent.send(init_message()))
    await asyncio.sleep(0)

    parent.close()

    with pytest.raises(ChannelClosed, match="closed"):
        await waiting_send

    with pytest.raises((asyncio.CancelledError, ChannelClosed)):
        await blocked_send

    assert parent.closed is True
    worker.close()
