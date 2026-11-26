"""Authenticated native HTTP streaming and browser upload admission."""
from contextlib import asynccontextmanager
from types import SimpleNamespace

import anyio
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI

from agent_gateway.channel_manager.web import container_file_http
from tests.unit_tests.extensions.test_adx_control_files import _business_authenticator, _file_channel, _files


pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture
async def browser(monkeypatch):
    router, control = _files(monkeypatch, auth_client=_business_authenticator())
    await router.connect('')
    try:
        app = FastAPI()
        container_file_http.attach_container_file_routes(app, _file_channel(router))
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://gateway',
                                     headers={'Authorization': 'Bearer business-test-token'}) as client:
            yield client, router, control
    finally:
        await router.shutdown()


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


@pytest.mark.parametrize('upstream_type, expected_type', [
    ('application/octet-stream', 'text/plain'), ('application/pdf', 'application/pdf'),
])
async def test_native_head_uses_business_auth_and_releases_stream(browser, upstream_type, expected_type):
    client, router, control = browser
    router.test_api.omit_download_length = True
    router.test_api.file_content_type = upstream_type
    control.invoke.return_value = {'type': 'file', 'size': 6}
    params = {'path': '/home/agentos/a.txt', 'user_id': 'alice'}
    response = await client.head('/file-api/raw-file', params=params)
    assert response.status_code == 200 and response.content == b''
    assert response.headers['Content-Length'] == '6'
    assert response.headers['Content-Type'].split(';', 1)[0] == expected_type
    del client.headers['Authorization']
    assert (await client.head('/file-api/raw-file', params=params)).status_code == 401
    assert len(router.test_api.transfers) == 1
    assert (await router._agent_manager.get_agent('alice', 'jiuwenswarm')).task_count == 0


@pytest.mark.parametrize('method', ['GET', 'HEAD'])
@pytest.mark.parametrize('on_wire', [False, True])
async def test_native_unsatisfiable_range_keeps_status_and_content_range(browser, method, on_wire):
    client, router, control = browser
    control.invoke.return_value = {'type': 'file', 'size': 12 if on_wire else 6}
    response = await client.request(method, '/file-api/raw-file', headers={'Range': 'bytes=9-10'},
                                   params={'path': '/home/agentos/a', 'user_id': 'alice'})
    assert response.status_code == 416 and response.content == b''
    assert response.headers['Content-Range'] == 'bytes */6'
    assert len(router.test_api.transfers) == int(on_wire)
    assert (await router._agent_manager.get_agent('alice', 'jiuwenswarm')).task_count == 0


@pytest.mark.parametrize('fail', [False, True])
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


async def test_native_head_preserves_response_when_upstream_close_fails():
    from agent_gateway.channel_manager.web.container_file_http import _native_file_response

    @asynccontextmanager
    async def open_file(**_kwargs):
        yield httpx.Response(416, headers={'Content-Range': 'bytes */6'})
        raise RuntimeError('upstream close failed')

    response = await _native_file_response(
        SimpleNamespace(open_container_file=open_file),
        request=SimpleNamespace(headers={}, method='HEAD'),
        user_id='alice', path='/home/agentos/a', session_id='',
    )
    assert response.status_code == 416
    assert response.headers['Content-Range'] == 'bytes */6'


@pytest.mark.parametrize('streaming', [True, False])
async def test_browser_upload_enforces_size_per_file(browser, monkeypatch, streaming):
    client, router, control = browser
    monkeypatch.setattr(container_file_http, 'MAX_UPLOAD_FILE_SIZE', 5)
    monkeypatch.setattr(router, 'native_file_streaming', streaming)
    control.invoke.return_value = {'exit_code': 0}
    large = ('file', ('large.txt', b'123456'))
    expected_error = [{'filename': 'large.txt', 'error': 'upload exceeds 512 MiB'}]
    for files in ([large], [large, ('file', ('ok.txt', b'12345'))]):
        response = await client.post('/file-api/upload', data={'user_id': 'alice'}, files=files)
        assert response.status_code == 200 and response.json()['errors'] == expected_error
        if len(files) == 1:
            assert response.json()['files'] == [] and not router.test_api.creates
            control.invoke.assert_not_awaited()
        else:
            assert response.json()['files'][0]['size_bytes'] == 5
            assert [data for _, data in router.test_api.transfers] == [b'12345']


async def test_chunked_multipart_rejects_oversize_before_activation(browser, monkeypatch):
    client, router, control = browser
    monkeypatch.setattr(container_file_http, 'MAX_UPLOAD_FILE_SIZE', 5)
    multipart = client.build_request('POST', '/file-api/upload', data={'user_id': 'alice'},
                                     files={'file': ('large.txt', b'123456')})
    async def chunks():
        async for chunk in multipart.stream:
            yield chunk

    response = await client.post('/file-api/upload', content=chunks(),
                                 headers={'Content-Type': multipart.headers['Content-Type']})
    assert 'Content-Length' not in response.request.headers
    assert response.json()['errors'] == [{'filename': 'large.txt', 'error': 'upload exceeds 512 MiB'}]
    assert not router.test_api.creates and not router.test_api.transfers
    control.invoke.assert_not_awaited()


async def test_multipart_upload_forwards_and_rejects_invalid_mode(browser):
    client, router, control = browser
    control.invoke.return_value = {'exit_code': 0}
    files = {'file': ('a.txt', b'a')}
    invalid = await client.post('/file-api/upload', data={'user_id': 'alice', 'mode': '999'}, files=files)
    assert invalid.json()['errors'] and not router.test_api.creates
    valid = await client.post('/file-api/upload', data={'user_id': 'alice', 'mode': '600'}, files=files)
    assert valid.json()['files'] and not valid.json()['errors']
    calls = [call.args[1:] for call in control.invoke.await_args_list]
    chmod = next(i for i, (action, args) in enumerate(calls) if action == 'cmd.run' and 'chmod -- 600 ' in args['cmd'])
    assert chmod < next(i for i, (action, _) in enumerate(calls) if action == 'file.rename')


async def test_mkdir_http_rejects_nul(browser):
    client, router, control = browser
    response = await client.post('/file-api/mkdir', params={'user_id': 'alice', 'path': '/home/agentos/bad\x00name'})
    assert response.status_code == 400 and response.json()['code'] == 'BAD_REQUEST'
    control.invoke.assert_not_awaited()
