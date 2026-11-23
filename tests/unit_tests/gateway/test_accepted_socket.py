import asyncio
import socket

import pytest


async def connected_pair():
    listener = socket.socket()
    listener.bind(('127.0.0.1', 0))
    listener.listen()
    listener.setblocking(False)
    client = socket.socket()
    client.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        connecting = asyncio.create_task(loop.sock_connect(client, listener.getsockname()))
        accepted, _address = await loop.sock_accept(listener)
        await connecting
        return client, accepted
    finally:
        listener.close()


async def test_prefix_precedes_already_buffered_socket_bytes_and_protocol_switch():
    from agent_gateway.transport.accepted_socket import adopt_connection

    client, accepted = await connected_pair()
    loop = asyncio.get_running_loop()
    received = bytearray()
    complete = loop.create_future()

    class NextProtocol(asyncio.Protocol):
        def data_received(self, data):
            received.extend(data)
            if received == b'prefix-remainder' and not complete.done():
                complete.set_result(None)

    class InitialProtocol(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport

        def data_received(self, data):
            received.extend(data)
            self.transport.set_protocol(NextProtocol())

    transport = None
    try:
        await loop.sock_sendall(client, b'remainder')
        transport, _protocol = await adopt_connection(accepted, b'prefix-', InitialProtocol)
        await asyncio.wait_for(complete, 1)
        assert received == b'prefix-remainder'
        assert isinstance(transport.get_protocol(), NextProtocol)
    finally:
        if transport is not None:
            transport.close()
        accepted.close()
        client.close()


async def test_replay_keeps_protocol_requested_read_backpressure():
    from agent_gateway.transport.accepted_socket import adopt_connection

    client, accepted = await connected_pair()
    received = []

    class PausingProtocol(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport

        def data_received(self, data):
            received.append(data)
            self.transport.pause_reading()

    transport = None
    try:
        await asyncio.get_running_loop().sock_sendall(client, b'after')
        transport, protocol = await adopt_connection(accepted, b'before', PausingProtocol)
        await asyncio.sleep(0.01)
        assert received == [b'before']
        assert not transport.is_reading()
        protocol.transport.resume_reading()
        await asyncio.sleep(0.01)
        assert received == [b'before', b'after']
    finally:
        if transport is not None:
            transport.close()
        client.close()
        accepted.close()


async def test_replay_failure_closes_the_owned_connection():
    from agent_gateway.transport.accepted_socket import adopt_connection

    class BrokenProtocol(asyncio.Protocol):
        def data_received(self, data):
            raise ValueError('replay failed')

    client, accepted = await connected_pair()
    try:
        with pytest.raises(ValueError, match='replay failed'):
            await adopt_connection(accepted, b'prefix', BrokenProtocol)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        client.close()
        accepted.close()


async def test_eof_connection_lost_and_write_backpressure_reach_current_protocol():
    from agent_gateway.transport.accepted_socket import adopt_connection

    events = []
    closed = asyncio.Event()

    class Protocol(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport
            transport.set_write_buffer_limits(high=64, low=16)
            assert transport.get_write_buffer_limits() == (16, 64)
            assert transport.can_write_eof()
            assert transport.get_extra_info('peername') is not None

        def eof_received(self):
            events.append('eof')
            return False

        def connection_lost(self, error):
            events.append('lost')
            closed.set()

        def pause_writing(self):
            events.append('pause')

        def resume_writing(self):
            events.append('resume')

    client, accepted = await connected_pair()
    transport = None
    try:
        transport, protocol = await adopt_connection(accepted, b'', Protocol)
        transport.get_protocol().pause_writing()
        transport.get_protocol().resume_writing()
        client.shutdown(socket.SHUT_WR)
        await asyncio.wait_for(closed.wait(), 1)
        assert events == ['pause', 'resume', 'eof', 'lost']
    finally:
        if transport is not None:
            transport.close()
        client.close()


@pytest.mark.parametrize('failure', ['made', 'factory', 'prefix'])
async def test_initialization_failure_releases_owned_socket(failure):
    from agent_gateway.transport.accepted_socket import adopt_connection

    class Protocol(asyncio.Protocol):
        def connection_made(self, transport):
            if failure == 'made':
                raise ValueError('connection made failure')

    def factory():
        if failure == 'factory':
            raise ValueError('factory failure')
        return Protocol()

    client, accepted = await connected_pair()
    try:
        with pytest.raises(ValueError):
            await adopt_connection(accepted, b'x' * (48 * 1024 + 1) if failure == 'prefix' else b'', factory)
        assert await asyncio.wait_for(asyncio.get_running_loop().sock_recv(client, 1), 1) == b''
    finally:
        client.close()
        accepted.close()


async def test_real_write_buffer_pause_and_drain_callbacks_survive_adoption():
    from agent_gateway.transport.accepted_socket import adopt_connection

    paused = asyncio.Event()
    resumed = asyncio.Event()
    size = 2 * 1024 * 1024

    class Protocol(asyncio.Protocol):
        def connection_made(self, transport):
            self.transport = transport
            transport.set_write_buffer_limits(high=1024, low=512)

        def pause_writing(self):
            paused.set()

        def resume_writing(self):
            resumed.set()

    client, accepted = await connected_pair()
    accepted.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    transport = None
    try:
        transport, protocol = await adopt_connection(accepted, b'', Protocol)
        protocol.transport.write(b'x' * size)
        await asyncio.wait_for(paused.wait(), 1)
        received = 0
        loop = asyncio.get_running_loop()
        while received < size:
            chunk = await asyncio.wait_for(loop.sock_recv(client, 65536), 2)
            assert chunk
            received += len(chunk)
        await asyncio.wait_for(resumed.wait(), 1)
        assert protocol.transport.get_write_buffer_size() == 0
    finally:
        if transport is not None:
            transport.close()
        client.close()
