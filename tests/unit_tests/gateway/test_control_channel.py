"""Unit tests for the control service transport channel."""

import asyncio
import json
import socket
import struct

import pytest

from agent_gateway.processes.control_channel import (
    ControlChannelClosed,
    ControlServiceChannel,
)
from agent_gateway.processes.control_protocol import ControlProtocolError


def base_message(message_type: str = "HELLO") -> dict[str, object]:
    message: dict[str, object] = {
        "protocol_version": 1,
        "group_id": "gateway-group",
        "slot_id": 0,
        "generation": 1,
        "type": message_type,
    }
    if message_type == "HELLO":
        message["pid"] = 123
    elif message_type == "REQUEST":
        message["request_id"] = "0123456789abcdef0123456789abcdef"
        message["operation"] = "credentials.request_model_auth"
        message["arguments"] = {"model_name": "test-model"}
    return message


def large_message() -> dict[str, object]:
    message = base_message("REQUEST")
    message["arguments"] = {"values": ["x" * 200 for _ in range(250)]}
    return message


def create_pair(
    *,
    frame_limit: int = 65536,
    send_timeout: float = 1.0,
    read_timeout: float = 5.0,
) -> tuple[ControlServiceChannel, ControlServiceChannel]:
    left_socket, right_socket = socket.socketpair()
    return (
        ControlServiceChannel(
            left_socket,
            frame_limit=frame_limit,
            send_timeout=send_timeout,
            read_timeout=read_timeout,
        ),
        ControlServiceChannel(
            right_socket,
            frame_limit=frame_limit,
            send_timeout=send_timeout,
            read_timeout=read_timeout,
        ),
    )


def encoded_frame(message: dict[str, object]) -> bytes:
    payload = json.dumps(message, separators=(",", ":")).encode("utf-8")
    return struct.pack(">I", len(payload)) + payload


async def test_send_receive_round_trips_a_control_frame() -> None:
    sender, receiver = create_pair()
    message = base_message()

    await sender.send(message)
    assert await receiver.receive() == message

    await sender.stop()
    await receiver.stop()


async def test_receive_has_no_deadline_while_idle() -> None:
    sender, receiver = create_pair(read_timeout=0.01)
    reader = asyncio.create_task(receiver.receive())
    await asyncio.sleep(0.05)

    assert not reader.done()
    assert receiver.closed is False

    await sender.send(base_message())
    assert await reader == base_message()

    await sender.stop()
    await receiver.stop()


async def test_receive_completion_timeout_starts_after_first_byte() -> None:
    sender, receiver = create_pair(read_timeout=0.01)
    sender._sock.send(b"\x00")
    await asyncio.sleep(0.03)

    with pytest.raises(ControlProtocolError, match="timed out"):
        await receiver.receive()

    assert receiver.closed is True
    assert sender.closed is False
    await sender.stop()


async def test_receive_rejects_truncated_header_and_payload() -> None:
    sender, receiver = create_pair()
    sender._sock.send(b"\x00")
    sender.close()

    with pytest.raises(ControlProtocolError, match="truncated"):
        await receiver.receive()
    assert receiver.closed is True

    sender, receiver = create_pair()
    sender._sock.send(struct.pack(">I", 10) + b"{")
    sender.close()

    with pytest.raises(ControlProtocolError, match="truncated"):
        await receiver.receive()
    assert receiver.closed is True


async def test_receive_reports_idle_eof_as_closed() -> None:
    sender, receiver = create_pair()
    sender.close()

    with pytest.raises(ControlChannelClosed):
        await receiver.receive()

    assert receiver.closed is True


async def test_receive_rejects_oversize_frame_before_reading_payload() -> None:
    sender, receiver = create_pair(frame_limit=64)
    sender._sock.send(struct.pack(">I", 65))
    sender.close()

    with pytest.raises(ControlProtocolError, match="frame limit"):
        await receiver.receive()

    assert receiver.closed is True


async def test_receive_rejects_concurrent_readers_without_closing_channel() -> None:
    sender, receiver = create_pair()
    first_reader = asyncio.create_task(receiver.receive())
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="single reader"):
        await receiver.receive()

    assert receiver.closed is False
    await sender.send(base_message())
    assert await first_reader == base_message()

    await sender.stop()
    await receiver.stop()


async def test_receive_cancellation_closes_channel() -> None:
    sender, receiver = create_pair()
    reader = asyncio.create_task(receiver.receive())
    await asyncio.sleep(0)
    reader.cancel()

    with pytest.raises(asyncio.CancelledError):
        await reader

    assert receiver.closed is True
    await sender.stop()


async def test_send_encodes_error_without_writing_or_closing() -> None:
    sender, receiver = create_pair()
    invalid_message = base_message()
    invalid_message["type"] = "PING"

    with pytest.raises(ControlProtocolError):
        await sender.send(invalid_message)

    assert sender.closed is False
    await sender.send(base_message())
    assert await receiver.receive() == base_message()

    await sender.stop()
    await receiver.stop()


async def test_send_serializes_complete_frames() -> None:
    sender, receiver = create_pair()
    first = base_message()
    second = base_message("REQUEST")

    await asyncio.gather(sender.send(first), sender.send(second))
    received = [await receiver.receive(), await receiver.receive()]

    assert received == [first, second]

    await sender.stop()
    await receiver.stop()


async def test_send_timeout_includes_waiting_for_the_write_lock() -> None:
    sender, receiver = create_pair(send_timeout=0.01)
    async with sender._send_lock:
        timed_send = asyncio.create_task(sender.send(base_message()))

        with pytest.raises(TimeoutError):
            await asyncio.wait_for(timed_send, timeout=0.2)

    assert sender.closed is True
    await receiver.stop()


async def test_caller_cancellation_leaves_owned_send_complete_and_channel_usable() -> None:
    sender, receiver = create_pair(send_timeout=1.0)
    message = large_message()
    caller = asyncio.create_task(sender.send(message))

    for _ in range(20):
        await asyncio.sleep(0)
        if sender._send_tasks:
            break
    assert sender._send_tasks

    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller

    assert sender.closed is False
    assert await receiver.receive() == message

    follow_up = base_message("REQUEST")
    follow_up["request_id"] = "fedcba9876543210fedcba9876543210"
    await sender.send(follow_up)
    assert await receiver.receive() == follow_up

    await sender.stop()
    await receiver.stop()


async def test_owned_send_tasks_are_bounded_at_thirty_two() -> None:
    sender, receiver = create_pair(send_timeout=1.0)
    message = large_message()
    callers = [asyncio.create_task(sender.send(message)) for _ in range(32)]
    await asyncio.sleep(0)

    assert len(sender._send_tasks) == 32
    with pytest.raises(ControlProtocolError, match="send queue is full"):
        await sender.send(base_message())

    assert sender.closed is False
    await sender.stop()
    await asyncio.gather(*callers, return_exceptions=True)
    assert not sender._send_tasks
    await receiver.stop()


async def test_close_wakes_receive_and_send_waiters() -> None:
    sender, receiver = create_pair()
    reader = asyncio.create_task(receiver.receive())
    blocked_sender = asyncio.create_task(sender.send(large_message()))
    await asyncio.sleep(0)

    receiver.close()
    sender.close()

    with pytest.raises(ControlChannelClosed):
        await reader
    with pytest.raises((ControlChannelClosed, asyncio.CancelledError)):
        await blocked_sender

    assert receiver.closed is True
    assert sender.closed is True


async def test_stop_cleans_owned_send_tasks_and_is_idempotent() -> None:
    sender, receiver = create_pair(send_timeout=1.0)
    blocked_sender = asyncio.create_task(sender.send(large_message()))
    await asyncio.sleep(0)
    assert sender._send_tasks

    await sender.stop()
    await sender.stop()

    assert sender.closed is True
    assert not sender._send_tasks
    with pytest.raises((ControlChannelClosed, asyncio.CancelledError)):
        await blocked_sender
    await receiver.stop()


async def test_receive_rejects_invalid_json_and_hides_payload_details() -> None:
    sender, receiver = create_pair()
    payload = b'{"secret": "canary"'
    sender._sock.send(struct.pack(">I", len(payload)) + payload)
    sender.close()

    with pytest.raises(ControlProtocolError, match="JSON") as error:
        await receiver.receive()

    assert receiver.closed is True
    assert "canary" not in str(error.value)
