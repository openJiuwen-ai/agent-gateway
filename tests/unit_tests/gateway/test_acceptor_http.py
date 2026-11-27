# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Bounded first-head classification before framework socket adoption."""

import asyncio
import socket
import threading

import pytest


def test_preserves_request_target_headers_and_finite_prefix():
    from agent_gateway.transport.acceptor_http import parse_request_head

    raw = b'POST /file-api/%75pload?user_id=alice HTTP/1.1\r\nHost: local\r\nContent-Length: 6\r\nX-User-Id: alice\r\n\r\nab'
    request = parse_request_head(raw)
    assert request.method == 'POST'
    assert request.target == b'/file-api/%75pload?user_id=alice'
    assert request.path == '/file-api/upload'
    assert request.query == b'user_id=alice'
    assert request.header_values('x-user-id') == ('alice',)
    assert request.protocol == 'http'
    assert request.prefix == raw


def test_classifies_websocket_without_consuming_or_validating_frames():
    from agent_gateway.transport.acceptor_http import parse_request_head

    raw = b'GET /ws?user_id=alice HTTP/1.1\r\nHost: local\r\nConnection: keep-alive, Upgrade\r\nUpgrade: websocket\r\n\r\n\x81\x80abcd'
    request = parse_request_head(raw)
    assert request.protocol == 'websocket'
    assert request.prefix == raw


@pytest.mark.parametrize('raw', [
    b'GET / HTTP/2.0\r\nHost:x\r\n\r\n',
    b'GET http://local/ws HTTP/1.1\r\nHost:x\r\n\r\n',
    b'GET /ws#hidden HTTP/1.1\r\nHost:x\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nX-User-Id:a\r\nX-User-Id:b\r\n\r\n',
    b'GET /?user_id=a&user%5fid=b HTTP/1.1\r\nHost:x\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nAuthorization:a\r\nAuthorization:b\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nX: first\r\n folded\r\n\r\n',
    b'POST / HTTP/1.1\r\nHost:x\r\nContent-Length:2\r\nTransfer-Encoding:chunked\r\n\r\n',
    b'POST / HTTP/1.1\r\nHost:x\r\nContent-Length:2\r\nContent-Length:2\r\n\r\n',
    b'GET /ws HTTP/1.1\r\nHost:x\r\nConnection:upgrade\r\nUpgrade:h2c\r\n\r\n',
    b'GET /ws HTTP/1.1\r\nHost:x\r\nUpgrade:websocket\r\n\r\n',
    b'POST /ws HTTP/1.1\r\nHost:x\r\nConnection:upgrade\r\nUpgrade:websocket\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\nX-Hidden:value\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nCookie:jiuwenswarm_auth=a; jiuwenswarm_auth=b\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nCookie:jiuwenswarm_auth=a; jiuwenswarm_auth =b\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nCookie:jiuwenswarm_auth=a; jiuwenswarm_auth\t=b\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nX-Token:a\r\nX-Token:b\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nX-Group-Id:a\r\nX-Group-Id:b\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nX-Bot-Id:a\r\nX-Bot-Id:b\r\n\r\n',
    b'GET / HTTP/1.1\r\nHost:x\r\nX-Gateway-Id:a\r\nX-Gateway-Id:b\r\n\r\n',
    b'GET /?group_id=a&group_id=b HTTP/1.1\r\nHost:x\r\n\r\n',
    b'GET /?bot_id=a&bot_id=b HTTP/1.1\r\nHost:x\r\n\r\n',
    b'GET /?gateway_id=a&gateway_id=b HTTP/1.1\r\nHost:x\r\n\r\n',
])
def test_rejects_ambiguous_or_unsupported_heads_without_echoing_input(raw):
    from agent_gateway.transport.acceptor_http import HeadRejected, parse_request_head

    with pytest.raises(HeadRejected) as error:
        parse_request_head(raw)
    assert error.value.status == 400
    assert raw.decode('latin1') not in str(error.value)


def test_exact_head_and_overread_boundaries_are_accepted():
    from agent_gateway.transport.acceptor_http import HEADER_LIMIT, OVERREAD_LIMIT, parse_request_head

    start = b'POST /upload HTTP/1.1\r\nHost:x\r\nContent-Length:16384\r\nX-Padding:'
    head = start + b'x' * (HEADER_LIMIT - len(start) - 4) + b'\r\n\r\n'
    raw = head + b'x' * OVERREAD_LIMIT
    assert len(head) == HEADER_LIMIT
    request = parse_request_head(raw)
    assert request.prefix == raw
    assert request.path == '/upload'


def test_separate_header_and_overread_limits():
    from agent_gateway.transport.acceptor_http import HeadRejected, parse_request_head

    with pytest.raises(HeadRejected) as error:
        parse_request_head(b'GET / HTTP/1.1\r\nHost:x\r\nX:' + b'a' * 32768 + b'\r\n\r\n')
    assert error.value.status == 431
    with pytest.raises(HeadRejected) as error:
        parse_request_head(b'GET / HTTP/1.1\r\nHost:x\r\n\r\n' + b'a' * 16385)
    assert error.value.status == 400


async def test_reads_until_first_head_keeps_suffix_and_does_not_read_later_body():
    from agent_gateway.transport.acceptor_http import read_request_head

    accepted, client = socket.socketpair()
    accepted.setblocking(False)
    client.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        await loop.sock_sendall(client, b'POST /upload HTTP/1.1\r\nHost:x\r\nContent-Length:4\r\n')
        reading = asyncio.create_task(read_request_head(accepted, timeout=1))
        await asyncio.sleep(0)
        await loop.sock_sendall(client, b'\r\nab')
        request = await reading
        assert request.prefix.endswith(b'\r\n\r\nab')
        await loop.sock_sendall(client, b'cd')
        assert await asyncio.wait_for(loop.sock_recv(accepted, 2), 1) == b'cd'
    finally:
        accepted.close()
        client.close()


async def test_read_timeout_disconnect_and_cancel_leave_socket_with_caller():
    from agent_gateway.transport.acceptor_http import HeadRejected, read_request_head

    accepted, client = socket.socketpair()
    accepted.setblocking(False)
    try:
        with pytest.raises(HeadRejected) as error:
            await read_request_head(accepted, timeout=.01)
        assert error.value.status == 408
        reading = asyncio.create_task(read_request_head(accepted, timeout=1))
        await asyncio.sleep(0)
        reading.cancel()
        with pytest.raises(asyncio.CancelledError):
            await reading
        assert accepted.fileno() >= 0
        client.close()
        with pytest.raises(HeadRejected) as error:
            await read_request_head(accepted, timeout=1)
        assert error.value.status == 400
    finally:
        accepted.close()
        client.close()


async def test_read_refuses_a_blocking_socket_without_stalling_the_loop():
    from agent_gateway.transport.acceptor_http import read_request_head

    accepted, client = socket.socketpair()
    # Until the guard exists, the read blocks the loop until this head arrives, so the
    # test fails on the missing error instead of hanging.
    late_head = threading.Timer(.3, client.sendall, args=(b'GET / HTTP/1.1\r\nHost:x\r\n\r\n',))
    late_head.start()
    try:
        with pytest.raises(ValueError, match='non-blocking'):
            await read_request_head(accepted, timeout=1)
    finally:
        late_head.cancel()
        accepted.close()
        client.close()
