# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Read one bounded HTTP head without becoming a body-forwarding transport."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import socket
from urllib.parse import parse_qsl, unquote

import h11

HEADER_LIMIT = 32768
OVERREAD_LIMIT = 16384
# Credential, identity and routing inputs are read again by the runtime with its own
# parsers; two values would let this precheck and the runtime disagree. The identity
# fields follow agent_gateway.common.request_identity.
_SINGLE_HEADERS = frozenset({
    b'host', b'authorization', b'x-token', b'x-user-id', b'x-auth-session',
    b'x-group-id', b'x-bot-id', b'x-gateway-id',
    b'content-length', b'transfer-encoding', b'upgrade',
    b'sec-websocket-key', b'sec-websocket-version',
})
_IDENTITY_QUERY = frozenset({
    'user_id', 'group_id', 'bot_id', 'gateway_id', 'auth_session', 'token', 'access_token',
})


class HeadRejected(ValueError):
    """A safe, fixed rejection before ownership transfer or application work."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f'connection head rejected ({status})')


@dataclass(frozen=True)
class RequestHead:
    method: str
    target: bytes = field(repr=False)
    version: str
    headers: tuple[tuple[bytes, bytes], ...] = field(repr=False)
    path: str
    query: bytes = field(repr=False)
    protocol: str
    prefix: bytes = field(repr=False)

    def header_values(self, name: str) -> tuple[str, ...]:
        key = name.lower().encode('ascii')
        return tuple(value.decode('latin1') for header, value in self.headers if header == key)


def parse_request_head(prefix: bytes) -> RequestHead:
    """Validate framing with h11; leave body and WebSocket checks to the runtime."""
    end = prefix.find(b'\r\n\r\n') + 4
    if end < 4:
        raise HeadRejected(431 if len(prefix) >= HEADER_LIMIT else 400)
    if end > HEADER_LIMIT:
        raise HeadRejected(431)
    if len(prefix) - end > OVERREAD_LIMIT:
        raise HeadRejected(400)
    lines = prefix[:end - 4].split(b'\r\n')
    if any(b'\r' in line or b'\n' in line for line in lines):
        raise HeadRejected(400)
    names = []
    for line in lines[1:]:
        if line.startswith((b' ', b'\t')):
            raise HeadRejected(400)
        names.append(line.partition(b':')[0].lower())
    if any(names.count(name) > 1 for name in _SINGLE_HEADERS):
        raise HeadRejected(400)
    if b'content-length' in names and b'transfer-encoding' in names:
        raise HeadRejected(400)
    try:
        parser = h11.Connection(h11.SERVER, max_incomplete_event_size=HEADER_LIMIT)
        parser.receive_data(prefix[:end])
        event = parser.next_event()
        if not isinstance(event, h11.Request):
            raise HeadRejected(400)
        target = event.target
        if not target.startswith(b'/') or b'#' in target:
            raise HeadRejected(400)
        raw_path, separator, query = target.partition(b'?')
        path = unquote(raw_path.decode('ascii'))
        version = event.http_version.decode('ascii')
        method = event.method.decode('ascii')
        if version not in ('1.0', '1.1'):
            raise HeadRejected(400)
        headers = tuple(event.headers)
        identity_names = [name for name, _value in parse_qsl(query.decode('ascii'), keep_blank_values=True) if name in _IDENTITY_QUERY]
        if len(identity_names) != len(set(identity_names)):
            raise HeadRejected(400)
        cookies = [value for name, value in headers if name == b'cookie']
        auth_cookies = sum(
            item.strip().partition(b'=')[0].strip() == b'jiuwenswarm_auth'
            for cookie in cookies for item in cookie.split(b';')
        )
        if auth_cookies > 1:
            raise HeadRejected(400)
        upgrades = [value.lower() for name, value in headers if name == b'upgrade']
        connection = {
            part.strip().lower()
            for name, value in headers if name == b'connection' for part in value.split(b',')
        }
        protocol = 'http'
        if upgrades or b'upgrade' in connection:
            if upgrades != [b'websocket'] or b'upgrade' not in connection or method != 'GET' or version != '1.1':
                raise HeadRejected(400)
            protocol = 'websocket'
        return RequestHead(method, target, version, headers, path, query if separator else b'', protocol, prefix)
    except (h11.RemoteProtocolError, UnicodeError, ValueError) as error:
        if isinstance(error, HeadRejected):
            raise
        raise HeadRejected(400) from None


async def read_request_head(sock: socket.socket, *, timeout: float = 5.0) -> RequestHead:
    """Borrow a non-blocking socket until the first head; the caller keeps ownership.

    A blocking socket would stall the event loop instead of honouring the timeout.
    """
    if sock.gettimeout() != 0:
        raise ValueError('read_request_head requires a non-blocking socket')
    prefix = bytearray()
    loop = asyncio.get_running_loop()
    try:
        async with asyncio.timeout(timeout):
            while True:
                chunk = await loop.sock_recv(sock, min(OVERREAD_LIMIT, HEADER_LIMIT - len(prefix) + OVERREAD_LIMIT))
                if not chunk:
                    raise HeadRejected(400)
                prefix.extend(chunk)
                if b'\r\n\r\n' in prefix:
                    return parse_request_head(bytes(prefix))
                if len(prefix) >= HEADER_LIMIT:
                    raise HeadRejected(431)
    except TimeoutError:
        raise HeadRejected(408) from None
