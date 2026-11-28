"""Authenticated native HTTP streaming and browser upload admission."""
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import anyio
import httpx
import pytest
import pytest_asyncio
from fastapi import FastAPI
from starlette.requests import ClientDisconnect

from agent_gateway.channel_manager.web import container_file_http
from agent_gateway.routing import container_files
from tests.unit_tests.extensions.test_adx_control_files import _business_authenticator, _file_channel, _files


pytestmark = pytest.mark.asyncio
FILE = {'user_id': 'alice', 'path': '/home/agentos/a.txt'}


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


@pytest.mark.parametrize("status,payload,code", [
    (404, {"error": {"code": "INSTANCE_NOT_FOUND"}}, "instance_not_found"),
    (404, {"error": "missing", "error_code": "INSTANCE_NOT_FOUND"}, "instance_not_found"),
    (409, {}, "CONFLICT"),
    (409, {"error": {"code": "FILE_BUSY"}}, "FILE_BUSY"),
    (413, {"error_code": "FILE_TOO_LARGE"}, "FILE_TOO_LARGE"),
])
async def test_native_file_errors_reach_browser(browser, status, payload, code):
    client, router, control = browser
    router.test_api.transfer_error = status, payload
    control.invoke.return_value = {"type": "file", "size": 3}
    response = await client.get("/file-api/raw-file", params=FILE)
    assert (response.status_code, response.json()["code"]) == (status, code)
    response = await client.post("/file-api/upload", data={"user_id": "alice"}, files={"file": ("a.txt", b"abc")})
    assert response.status_code == 200
    assert response.json()["errors"][0]["code"] == code


async def test_native_http_disconnect_releases_activity(browser):
    _, router, control = browser
    control.invoke.return_value = {'type': 'file', 'size': 6}
    response = await container_file_http._native_file_response(
        router, request=SimpleNamespace(headers={}, method='GET'), session_id='', **FILE,
    )
    with pytest.raises(ClientDisconnect):
        await response({'type': 'http', 'asgi': {'spec_version': '2.4'}}, AsyncMock(),
                       AsyncMock(side_effect=OSError('browser disconnected')))
    assert (await router._agent_manager.get_agent('alice', 'jiuwenswarm')).task_count == 0


@pytest.mark.parametrize('upstream_type, expected_type', [
    ('application/octet-stream', 'text/plain'), ('application/pdf', 'application/pdf'),
])
async def test_native_head_uses_business_auth_and_releases_stream(browser, upstream_type, expected_type):
    client, router, control = browser
    router.test_api.file_content_type = upstream_type
    control.invoke.return_value = {'type': 'file', 'size': 6}
    response = await client.head('/file-api/raw-file', params=FILE)
    assert response.status_code == 200 and response.content == b''
    assert response.headers['Content-Length'] == '6'
    assert response.headers['Content-Type'].split(';', 1)[0] == expected_type
    del client.headers['Authorization']
    assert (await client.head('/file-api/raw-file', params=FILE)).status_code == 401
    assert len(router.test_api.transfers) == 1
    assert (await router._agent_manager.get_agent('alice', 'jiuwenswarm')).task_count == 0


@pytest.mark.parametrize('method', ['GET', 'HEAD'])
@pytest.mark.parametrize('on_wire', [False, True])
async def test_native_unsatisfiable_range_keeps_status_and_content_range(browser, method, on_wire):
    client, router, control = browser
    control.invoke.return_value = {'type': 'file', 'size': 12 if on_wire else 6}
    response = await client.request(method, '/file-api/raw-file', headers={'Range': 'bytes=9-10'},
                                   params=FILE)
    assert response.status_code == 416 and response.content == b''
    assert response.headers['Content-Range'] == 'bytes */6'
    assert len(router.test_api.transfers) == int(on_wire)
    assert (await router._agent_manager.get_agent('alice', 'jiuwenswarm')).task_count == 0


@pytest.mark.parametrize('failure', ['cancel_head', 'cancel_error', 'upstream_close'])
async def test_native_response_finalizes_transfer(failure):
    closed = []

    @asynccontextmanager
    async def open_file(**_kwargs):
        try:
            if failure == 'upstream_close':
                yield httpx.Response(416, headers={'Content-Range': 'bytes */6'})
            else:
                yield httpx.Response(200, content=b'data')
        finally:
            await anyio.sleep(0)
            closed.append(True)
            if failure == 'upstream_close':
                raise RuntimeError('upstream close failed')

    class Request:
        headers = {}

        @property
        def method(self):
            scope.cancel()
            if failure == 'cancel_error':
                raise RuntimeError('response cancelled')
            return 'HEAD'

    with anyio.CancelScope() as scope:
        response = container_file_http._native_file_response(
            SimpleNamespace(open_container_file=open_file), request=Request(), session_id='', **FILE,
        )
        if failure == 'cancel_error':
            with pytest.raises(RuntimeError, match='response cancelled'):
                await response
        elif failure == 'upstream_close':
            result = await response
            assert result.status_code == 416 and result.headers['Content-Range'] == 'bytes */6'
        else:
            assert (await response).status_code == 200
    assert closed == [True]


@pytest.mark.parametrize('streaming', [True, False])
async def test_browser_upload_enforces_size_per_file(browser, monkeypatch, streaming):
    client, router, control = browser
    monkeypatch.setattr(container_files, 'MAX_UPLOAD_FILE_SIZE', 5)
    monkeypatch.setattr(router, 'native_file_streaming', streaming)
    control.invoke.return_value = {'exit_code': 0}
    large = ('file', ('large.txt', b'123456'))
    expected_error = [{'filename': 'large.txt', 'error': 'upload exceeds 5 bytes'}]
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
    monkeypatch.setattr(container_files, 'MAX_UPLOAD_FILE_SIZE', 5)
    multipart = client.build_request('POST', '/file-api/upload', data={'user_id': 'alice'},
                                     files={'file': ('large.txt', b'123456')})
    response = await client.post('/file-api/upload', content=aiter(multipart.stream),
                                 headers={'Content-Type': multipart.headers['Content-Type']})
    assert 'Content-Length' not in response.request.headers
    assert response.json()['errors'] == [{'filename': 'large.txt', 'error': 'upload exceeds 5 bytes'}]
    assert not router.test_api.creates and not router.test_api.transfers
    control.invoke.assert_not_awaited()


@pytest.mark.parametrize('result', ['missing_method', {}, None, {'size': -1}, {'size': '3'}, {'size': True}])
async def test_native_upload_reports_client_errors_per_file(browser, monkeypatch, result):
    client, router, _ = browser
    missing_method = result == 'missing_method'
    monkeypatch.setattr(router, 'upload_container_file_stream', None if missing_method else AsyncMock(return_value=result))
    expected = 'does not implement upload_container_file_stream' if missing_method else 'non-negative integer size'
    response = await client.post('/file-api/upload', data={'user_id': 'alice'}, files={'file': ('a.txt', b'abc')})
    body = response.json()
    assert response.status_code == 200 and body['files'] == []
    error, = body['errors']
    assert (error['filename'], error['code']) == ('a.txt', 'INTERNAL_ERROR') and expected in error['error']
    assert not router.test_api.transfers


async def test_multipart_upload_forwards_and_rejects_invalid_mode(browser, monkeypatch):
    client, router, control = browser
    control.invoke.return_value = {'exit_code': 0}
    files = {'file': ('empty.txt', b'')}
    with monkeypatch.context() as patch:
        stream, buffered = AsyncMock(), AsyncMock()
        patch.setattr(router, 'upload_container_file_stream', stream)
        patch.setattr(router, 'upload_container_file', buffered)
        for native in (True, False):
            patch.setattr(router, 'native_file_streaming', native)
            invalid = await client.post('/file-api/upload', data={'user_id': 'alice', 'mode': '999'}, files=files)
            assert invalid.json()['errors'][0]['code'] == 'BAD_REQUEST'
        stream.assert_not_awaited()
        buffered.assert_not_awaited()
    valid = await client.post('/file-api/upload', data={'user_id': 'alice', 'mode': '600'}, files=files)
    assert valid.json()['files'][0]['size_bytes'] == 0 and not valid.json()['errors']
    calls = [call.args[1:] for call in control.invoke.await_args_list]
    chmod = next(i for i, (action, args) in enumerate(calls) if action == 'cmd.run' and 'chmod -- 600 ' in args['cmd'])
    assert chmod < next(i for i, (action, _) in enumerate(calls) if action == 'file.rename')


async def test_mkdir_http_rejects_nul(browser):
    client, router, control = browser
    response = await client.post('/file-api/mkdir', params={'user_id': 'alice', 'path': '/home/agentos/bad\x00name'})
    assert response.status_code == 400 and response.json()['code'] == 'BAD_REQUEST'
    control.invoke.assert_not_awaited()
