"""Authenticated native HTTP streaming and browser upload admission."""
from contextlib import asynccontextmanager
from types import SimpleNamespace

import anyio
import httpx
import pytest
from tests.unit_tests.extensions.test_adx_control_files import _business_authenticator, _file_channel, _files


@pytest.mark.asyncio
async def test_native_http_disconnect_closes_upstream_and_releases_activity(monkeypatch):
    from agent_gateway.channel_manager.web.container_file_http import _native_file_response
    router, control = _files(monkeypatch)
    await router.connect('')
    control.invoke.return_value = {'type': 'file', 'size': 6}
    response = await _native_file_response(
        router, request=SimpleNamespace(headers={}, method='GET'), user_id='alice',
        path='/home/agentos/a', session_id='',
    )
    async def send(_message):
        raise OSError('browser disconnected')
    async def receive():
        return {'type': 'http.disconnect'}
    with pytest.raises(Exception):
        await response({'type': 'http', 'asgi': {'spec_version': '2.4'}}, receive, send)
    runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
    assert runtime.task_count == 0
    await router.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize('upstream_type, expected_type', [
    ('application/octet-stream', 'text/plain'),
    ('application/pdf', 'application/pdf'),
])
async def test_native_head_uses_business_auth_and_releases_stream(monkeypatch, upstream_type, expected_type):
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app
    router, control = _files(monkeypatch, auth_client=_business_authenticator())
    await router.connect('')
    router.test_api.omit_download_length = True
    router.test_api.file_content_type = upstream_type
    control.invoke.return_value = {'type': 'file', 'size': 6}
    channel = _file_channel(router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_web_channel_app(channel)),
                                base_url='http://gateway') as client:
        params = {'path': '/home/agentos/a.txt', 'user_id': 'alice'}
        response = await client.head('/file-api/raw-file', params=params,
                                     headers={'Authorization': 'Bearer business-test-token'})
        assert response.status_code == 200 and response.content == b''
        assert response.headers['Content-Length'] == '6'
        assert response.headers['Content-Type'].split(';', 1)[0] == expected_type
        response = await client.head('/file-api/raw-file', params=params)
        assert response.status_code == 401
    assert len(router.test_api.transfers) == 1
    runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
    assert runtime.task_count == 0
    await router.shutdown()


@pytest.mark.parametrize('method', ['GET', 'HEAD'])
@pytest.mark.parametrize('on_wire', [False, True])
@pytest.mark.asyncio
async def test_native_unsatisfiable_range_keeps_status_and_content_range(monkeypatch, method, on_wire):
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app

    router, control = _files(monkeypatch, auth_client=_business_authenticator())
    await router.connect('')
    control.invoke.return_value = {'type': 'file', 'size': 12 if on_wire else 6}
    try:
        channel = _file_channel(router)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_web_channel_app(channel)),
                                     base_url='http://gateway') as client:
            response = await client.request(method, '/file-api/raw-file',
                params={'path': '/home/agentos/a', 'user_id': 'alice'},
                headers={'Authorization': 'Bearer business-test-token', 'Range': 'bytes=9-10'})
        assert response.status_code == 416
        assert response.headers['Content-Range'] == 'bytes */6'
        assert response.content == b''
        assert len(router.test_api.transfers) == int(on_wire)
        runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        assert runtime.task_count == 0
    finally:
        await router.shutdown()


@pytest.mark.parametrize('fail', [False, True])
@pytest.mark.asyncio
async def test_native_head_and_error_close_upstream_when_cancelled(fail):
    from agent_gateway.channel_manager.web.container_file_http import _native_file_response

    closes = []

    @asynccontextmanager
    async def open_file(**_kwargs):
        try:
            yield httpx.Response(200, content=b'data')
        finally:
            await anyio.sleep(0)
            closes.append(True)

    class CancelledRequest:
        headers = {}

        @property
        def method(self):
            scope.cancel()
            if fail:
                raise RuntimeError('response cancelled')
            return 'HEAD'

    with anyio.CancelScope() as scope:
        args = dict(request=CancelledRequest(), user_id='alice', path='/home/agentos/a', session_id='')
        client = SimpleNamespace(open_container_file=open_file)
        if fail:
            with pytest.raises(RuntimeError, match='response cancelled'):
                await _native_file_response(client, **args)
        else:
            assert (await _native_file_response(client, **args)).status_code == 200
    assert closes == [True]


@pytest.mark.parametrize('size', [5, 6])
@pytest.mark.parametrize('streaming', [True, False])
@pytest.mark.asyncio
async def test_gateway_checks_file_size_before_backend_activation(monkeypatch, size, streaming):
    from agent_gateway.channel_manager.web import container_file_http
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app
    monkeypatch.setattr(container_file_http, 'MAX_UPLOAD_FILE_SIZE', 5)
    router, control = _files(monkeypatch, auth_client=_business_authenticator())
    monkeypatch.setattr(router, 'native_file_streaming', streaming)
    await router.connect('')
    control.invoke.return_value = {'exit_code': 0}
    channel = _file_channel(router)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_web_channel_app(channel)),
                                    base_url='http://gateway', headers={'Authorization': 'Bearer business-test-token'}) as client:
            response = await client.post('/file-api/upload', data={'user_id': 'alice'},
                                         files={'file': ('file.txt', b'x' * size)})
        assert response.status_code == 200
        result = response.json()
        if size == 5:
            assert result['errors'] == [] and result['files'][0]['size_bytes'] == size
            assert len(router.test_api.creates) == len(router.test_api.transfers) == 1
        else:
            assert result['files'] == []
            assert result['errors'] == [{'filename': 'file.txt', 'error': 'upload exceeds 512 MiB'}]
            assert router.test_api.creates == router.test_api.transfers == []
            control.invoke.assert_not_awaited()
    finally:
        await router.shutdown()


@pytest.mark.asyncio
async def test_gateway_keeps_valid_files_in_a_mixed_upload_batch(monkeypatch):
    from agent_gateway.channel_manager.web import container_file_http
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app
    monkeypatch.setattr(container_file_http, 'MAX_UPLOAD_FILE_SIZE', 5)
    router, control = _files(monkeypatch, auth_client=_business_authenticator())
    await router.connect('')
    control.invoke.return_value = {'exit_code': 0}
    channel = _file_channel(router)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_web_channel_app(channel)),
                                    base_url='http://gateway', headers={'Authorization': 'Bearer business-test-token'}) as client:
            response = await client.post('/file-api/upload', data={'user_id': 'alice'},
                files=[('file', ('large.txt', b'123456')), ('file', ('ok.txt', b'12345'))])
        result = response.json()
        assert response.status_code == 200 and result['files'][0]['filename'] == 'ok.txt'
        assert result['errors'] == [{'filename': 'large.txt', 'error': 'upload exceeds 512 MiB'}]
        assert len(router.test_api.creates) == len(router.test_api.transfers) == 1
        assert router.test_api.transfers[0][1] == b'12345'
    finally:
        await router.shutdown()


@pytest.mark.asyncio
async def test_gateway_checks_multipart_size_without_content_length(monkeypatch):
    from agent_gateway.channel_manager.web import container_file_http
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app
    monkeypatch.setattr(container_file_http, 'MAX_UPLOAD_FILE_SIZE', 5)
    router, control = _files(monkeypatch, auth_client=_business_authenticator())
    await router.connect('')
    channel = _file_channel(router)
    body = (b'--boundary\r\nContent-Disposition: form-data; name="user_id"\r\n\r\nalice\r\n'
            b'--boundary\r\nContent-Disposition: form-data; name="file"; filename="large.txt"\r\n\r\n'
            b'123456\r\n--boundary--\r\n')
    async def chunks():
        for offset in range(0, len(body), 13):
            yield body[offset:offset + 13]
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_web_channel_app(channel)),
                                    base_url='http://gateway', headers={'Authorization': 'Bearer business-test-token'}) as client:
            response = await client.post('/file-api/upload', content=chunks(),
                headers={'Content-Type': 'multipart/form-data; boundary=boundary'})
        assert 'Content-Length' not in response.request.headers
        assert response.json()['errors'] == [{'filename': 'large.txt', 'error': 'upload exceeds 512 MiB'}]
        assert response.json()['files'] == []
        assert router.test_api.creates == router.test_api.transfers == []
        control.invoke.assert_not_awaited()
    finally:
        await router.shutdown()


@pytest.mark.asyncio
async def test_multipart_upload_forwards_and_rejects_invalid_mode(monkeypatch):
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app

    router, control = _files(monkeypatch)
    await router.connect('')
    control.invoke.return_value = {'exit_code': 0}
    channel = _file_channel(router)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_web_channel_app(channel)),
                                     base_url='http://gateway') as client:
            invalid = await client.post('/file-api/upload', data={'user_id': 'alice', 'mode': '999'},
                                        files={'file': ('a.txt', b'a')})
            assert invalid.json()['errors'] and not invalid.json()['files']
            assert not router.test_api.creates
            valid = await client.post('/file-api/upload', data={'user_id': 'alice', 'mode': '600'},
                                      files={'file': ('a.txt', b'a')})
        assert valid.json()['files'] and not valid.json()['errors']
        assert any(call.args[1] == 'cmd.run' and 'chmod -- 600 ' in call.args[2]['cmd']
                   for call in control.invoke.await_args_list)
    finally:
        await router.shutdown()
