"""Environment-scoped file contracts through the ADX adapter."""
import asyncio
import shlex
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
import pytest_asyncio

from agent_gateway import telemetry
from agent_gateway.extensions.adx.control_client import AdxControlClient, AdxControlError
from agent_gateway.routing.container_files import AgentOSFileTransferError
from agent_gateway.extensions.auth.credential_authenticator import AuthResult
from tests.unit_tests.extensions.test_adx_router import _config, _router
import anyio
from fastapi import FastAPI
from starlette.requests import ClientDisconnect
from agent_gateway.channel_manager.web import container_file_http
from agent_gateway.routing import container_files


pytestmark = pytest.mark.asyncio


def _files(monkeypatch, *, auth_client=None):
    router, _clients = _router(_config(monkeypatch), auth_client=auth_client)
    router._control.invoke = AsyncMock()
    api = router.test_api
    original = api.handle
    api.transfer_error = None
    api.transfers = []
    api.file_data = b'abcdef'
    async def handle(request):
        if not request.url.path.endswith(('/upload', '/download')):
            return original(request)
        assert request.headers['authorization'] == 'Bearer platform-secret'
        if api.transfer_error is not None:
            status, payload = api.transfer_error
            return httpx.Response(status, json=payload)
        if request.url.path.endswith('/upload'):
            content = await request.aread()
            api.transfers.append((request, content))
            return httpx.Response(200, json={'size': len(content)})
        api.transfers.append((request, None))
        content = api.file_data
        if 'range' in request.headers:
            start, end = map(int, request.headers['range'][6:].split('-'))
            if start >= len(content):
                return httpx.Response(416, content=b'unsatisfiable', headers={
                    'Content-Range': f'bytes */{len(content)}',
                })
            return httpx.Response(206, content=content[start:end + 1], headers={
                'Content-Range': f'bytes {start}-{end}/{len(content)}',
            })
        response = httpx.Response(200, content=content)
        if content_type := getattr(api, 'file_content_type', None):
            response.headers['Content-Type'] = content_type
            del response.headers['Content-Length']
        return response
    router._control.http._transport = httpx.MockTransport(handle)
    return router, router._control


@pytest_asyncio.fixture
async def file_router(monkeypatch):
    router, control = _files(monkeypatch)
    await router.connect('')
    try:
        yield router, control
    finally:
        await router.shutdown()


def _alice_instance(router):
    creates = [body for body in router.test_api.creates
               if body['mounts'][0]['host_path'].endswith('/alice')]
    assert len(creates) == 1
    body = creates[0]
    return body['namespace'] + '-' + body['name']


def _file_channel(router):
    from agent_gateway.channel_manager.base import RobotMessageRouter
    from agent_gateway.channel_manager.web.web_connect import WebChannel, WebChannelConfig

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter(), agent_client=router)
    router.set_channel_manager(SimpleNamespace(
        get_channel=lambda kind: channel if kind.value == 'web' else None,
        subscribe_channel_events=lambda callback: None,
    ))
    return channel


def _business_authenticator():
    async def authenticate(context):
        if context.headers.get('authorization') != 'Bearer business-test-token':
            return AuthResult(False, error='Invalid business credential')
        return AuthResult(True, user_id='iam-alice', extensions={'username': 'alice'})

    return SimpleNamespace(authenticate=AsyncMock(side_effect=authenticate))


@pytest.mark.parametrize('connected', [True, False])
async def test_gateway_health_check_is_mocked_without_activation(monkeypatch, connected):
    from agent_gateway.health_check import (
        HEALTH_CHECK_OK,
        GatewayHealthCheckService,
        HealthCheckConfig,
    )

    router, control = _files(monkeypatch)
    router._factory = lambda: pytest.fail('Health check must not open a user connection')
    messages = SimpleNamespace(publish_robot_messages=AsyncMock())
    service = GatewayHealthCheckService(
        router,
        HealthCheckConfig(interval_seconds=60, timeout_seconds=1, relay_channel_id='web'),
        message_handler=messages,
    )
    try:
        if connected:
            await router.connect('')
        await service._tick()
        assert service.last_tick_ok is connected
        if connected:
            message = messages.publish_robot_messages.call_args.args[0]
            assert HEALTH_CHECK_OK in message.payload['health_check']
            assert 'mock' in message.payload['health_check']
        else:
            messages.publish_robot_messages.assert_not_awaited()
        assert router.test_api.requests == []
        control.invoke.assert_not_awaited()
    finally:
        await router.shutdown()


@pytest.mark.parametrize('root', ['/home/agentos', '/custom/workspace'])
async def test_file_upload_is_atomic_and_scoped_to_activated_user(file_router, monkeypatch, root):
    from agent_gateway.routing import container_files

    router, control = file_router
    monkeypatch.setattr(container_files, 'AGENT_FILE_PATH_ROOT', root)
    target = "existing/new dir/it's a file.txt"
    existing = f'{root}/existing'
    payload = b'hello' * 17000

    def invoke(env, action, args):
        assert env == _alice_instance(router)
        if action == 'file.exists':
            return {'exists': root == '/home/agentos' and args['path'] == existing}
        return {'exit_code': 0}

    control.invoke.side_effect = invoke
    result = await router.upload_container_file(user_id='alice', path=target, content=payload)
    (request, uploaded), = router.test_api.transfers
    assert uploaded == payload and result['size'] == len(payload)
    assert request.url.path.endswith('/upload')
    actions = [call.args[1:] for call in control.invoke.await_args_list]
    assert [action for action, _ in actions if action != 'file.exists'] == ['cmd.run', 'file.rename', 'file.remove']
    ownership = shlex.split(actions[-3][1]['cmd'])
    assert ownership[:4] == ['chown', '-h', f'--reference={root}', '--']
    assert f'{existing}/new dir' in ownership[4:]
    assert (existing in ownership[4:]) == (root != '/home/agentos')
    assert actions[-2][1]['new_path'] == f'{root}/{target}'
    with pytest.raises(AgentOSFileTransferError):
        await router.download_container_file(user_id='bob', path=f'{root}/{target}', instance_id=_alice_instance(router))


async def test_http_download_keeps_environment_and_releases_activity(monkeypatch):
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app

    router, control = _files(monkeypatch)
    await router.connect('')
    def invoke(env, action, args):
        assert env == _alice_instance(router)
        if action == 'file.stat':
            return {'type': 'file', 'size': 6}
        return {'data': (b'abc' if args['offset'] == 0 else b'def').hex(), 'bytes_read': 3}
    control.invoke.side_effect = invoke
    channel = _file_channel(router)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_web_channel_app(channel)),
                                    base_url='http://gateway', headers={'Authorization': 'Bearer business-test-token'}) as client:
            response = await client.get('/file-api/raw-file', params={'path': '/home/agentos/a', 'user_id': 'alice'})
            assert response.status_code == 200 and response.content == b'abcdef'
        assert len(router.test_api.creates) == 1
        runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        assert runtime.task_count == 0
    finally:
        await router.shutdown()


async def test_http_file_routes_require_business_auth_and_reject_other_users(monkeypatch):
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app
    auth_client = _business_authenticator()
    router, control = _files(monkeypatch, auth_client=auth_client)
    await router.connect('')
    control.invoke.side_effect = lambda env, action, args: (
        {'entries': [{'name': 'a.txt', 'path': '/home/agentos/a.txt', 'type': 'file', 'size': 3}]}
        if action == 'file.list' else
        {'type': 'file', 'size': 3} if action == 'file.stat' else
        {'data': b'abc'.hex(), 'bytes_read': 3, 'eof': True})
    channel = _file_channel(router)
    router.test_api.file_data = b'abc'
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_web_channel_app(channel)), base_url='http://gateway', headers={'Authorization': 'Bearer business-test-token'}) as client:
        response = await client.get('/file-api/list-files', params={'dir': '/home/agentos', 'user_id': 'alice'})
        assert response.status_code == 200 and 'a.txt' in response.text
        response = await client.get('/file-api/raw-file', params={'path': '/home/agentos/a.txt', 'user_id': 'alice'})
        assert response.status_code == 200 and response.content == b'abc'
        response = await client.get('/file-api/list-files', params={'dir': '/home/agentos', 'user_id': 'bob'})
        assert response.status_code == 403 and response.json()['code'] == 'USER_MISMATCH'
        response = await client.get('/file-api/list-files', params={'dir': '/home/agentos', 'user_id': 'alice'},
            headers={'X-User-Id': 'bob'})
        assert response.status_code == 403
        response = await client.get('/file-api/list-files', params={'dir': '/home/agentos', 'user_id': 'alice'},
            headers={'Authorization': 'Bearer unrelated-platform-token'})
        assert response.status_code == 401
        response = await client.get('/file-api/list-files', params={'dir': '/home/agentos'})
        assert response.status_code == 200
    assert all(call.args[0] == _alice_instance(router) for call in control.invoke.await_args_list)
    assert len(router.test_api.transfers) == 1
    assert auth_client.authenticate.await_count == 6
    await router.shutdown()


@pytest.mark.parametrize('route, params', [
    ('list-files', {'dir': '/home/agentos'}),
    ('raw-file', {'path': '/home/agentos/probe.txt'}),
])
@pytest.mark.parametrize('failure, status, code', [
    ('agent_type', 404, 'instance_not_found'),
    ('environment', 502, 'SERVICE_UNAVAILABLE'),
])
async def test_http_file_routes_map_adx_failures(monkeypatch, route, params, failure, status, code):
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app

    router, control = _files(monkeypatch)
    await router.connect('')
    params = {**params, 'user_id': 'alice'}
    if failure == 'agent_type':
        params['agent_type'] = 'unsupported'
    else:
        control.create_sandbox = AsyncMock(side_effect=AdxControlError('ADX Environment unavailable'))
    channel = _file_channel(router)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=build_web_channel_app(channel)),
                                    base_url='http://gateway', headers={'Authorization': 'Bearer business-test-token'}) as client:
            response = await client.get('/file-api/' + route, params=params)
        assert response.status_code == status
        assert response.json()['code'] == code
        control.invoke.assert_not_awaited()
    finally:
        await router.shutdown()


async def test_recursive_listing_preserves_unlimited_depth_contract(monkeypatch):
    router, control = _files(monkeypatch)
    await router.connect('')
    boundary = '/home/agentos/' + '/'.join(['d'] * 64)
    def invoke(env, action, args):
        assert action == 'file.list' and args['depth'] == 64
        if args['path'] == '/home/agentos':
            return {'entries': [{'path': boundary, 'type': 'dir'}]}
        assert args['path'] == boundary
        return {'entries': [{'path': boundary + '/deep.txt', 'type': 'file'}]}
    control.invoke.side_effect = invoke
    result = await router.list_container_files(user_id='alice', dir_path='/home/agentos', recursive=True, max_depth=0)
    assert control.invoke.await_count == 2 and result[-1]['path'].endswith('/deep.txt')
    await router.shutdown()


async def test_file_routes_follow_original_auth_disabled_behavior(monkeypatch):
    from agent_gateway.channel_manager.web.web_channel_app import build_web_channel_app

    router, control = _files(monkeypatch)
    await router.connect('')
    control.invoke.return_value = {'entries': []}
    channel = _file_channel(router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=build_web_channel_app(channel)), base_url='http://gateway',
    ) as client:
        response = await client.get('/file-api/list-files', params={'dir': '/home/agentos', 'user_id': 'alice'})
    assert response.status_code == 200
    control.invoke.assert_awaited_once()
    await router.shutdown()


async def test_recursive_mkdir_uses_native_adx_action_and_default_mode(file_router):
    router, control = file_router
    control.invoke.side_effect = lambda env, action, args: (
        {'created': True, 'error': None} if action == 'file.mkdir' else {'exit_code': 0}
    )
    assert await router.mkdir_container_dir(
        user_id='alice', path='/home/agentos/nested/dir', recursive=True,
    ) == {'success': True, 'path': '/home/agentos/nested/dir', 'created': True}
    control.invoke.assert_awaited_once_with(_alice_instance(router), 'file.mkdir', {'path': '/home/agentos/nested/dir'})
    assert await router.mkdir_container_dir(
        user_id='alice', path='/home/agentos/other', mode='700', recursive=True,
    ) == {'success': True, 'path': '/home/agentos/other', 'created': True}
    assert control.invoke.await_args_list[-2].args[1:] == ('file.mkdir', {'path': '/home/agentos/other'})
    assert control.invoke.await_args_list[-1].args[1] == 'cmd.run'
    assert 'chmod -- 700 /home/agentos/other' in control.invoke.await_args_list[-1].args[2]['cmd']
    assert control.invoke.await_count == 3


async def test_native_download_propagates_trace_and_holds_activity(file_router, monkeypatch):
    router, control = file_router
    control.invoke.return_value = {'type': 'file', 'size': 6}
    monkeypatch.setattr(telemetry, '_runtime', None)
    monkeypatch.delenv('OTEL_ENABLED', raising=False)
    telemetry.configure_gateway_telemetry({'telemetry': {'enabled': True, 'exporter': 'none'}})
    operation = telemetry.start_request_trace('request', 'file.download', channel='web', connection_id='connection')
    try:
        with telemetry.bind_request_trace(operation):
            async with router.open_container_file(user_id='alice', path='/home/agentos/a') as response:
                runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
                assert runtime.task_count == 1 and await response.aread() == b'abcdef'
        assert (await router._agent_manager.get_agent('alice', 'jiuwenswarm')).task_count == 0
        (request, _), = router.test_api.transfers
        assert request.headers['x-trace-id'] == str(UUID(hex=operation.trace_id))
        assert request.headers['traceparent'].split('-')[1] == operation.trace_id
    finally:
        operation.finish('success')
        await telemetry.shutdown_gateway_telemetry()


async def test_native_range_download_preserves_chunk_contract(file_router):
    router, control = file_router
    control.invoke.return_value = {'type': 'file', 'size': 6}
    result = await router.download_container_file(user_id='alice', path='/home/agentos/a', offset=2, limit=3)
    assert result.data == b'cde' and result.offset == 2 and result.size == 6 and not result.eof
    assert router.test_api.transfers[0][0].headers['range'] == 'bytes=2-4'
    result = await router.download_container_file(user_id='alice', path='/home/agentos/a', offset=6)
    assert result.eof and result.data == b'' and len(router.test_api.transfers) == 1


async def test_native_upload_rejects_traversal_before_creating_environment(file_router):
    router, control = file_router
    with pytest.raises(AgentOSFileTransferError):
        await router.upload_container_file(user_id='alice', path='../escape', content=b'bad')
    assert not router.test_api.creates and not router.test_api.transfers


@pytest.mark.parametrize('failure,exception', [
    ('disconnect', RuntimeError), ('oversize', AgentOSFileTransferError),
    ('chown', AgentOSFileTransferError), ('chmod', AgentOSFileTransferError),
])
async def test_failed_upload_discards_temporary_and_releases_activity(file_router, monkeypatch, failure, exception):
    from agent_gateway.extensions.adx import file_client

    router, control = file_router
    monkeypatch.setattr(file_client.container_files, 'MAX_UPLOAD_FILE_SIZE', 5)
    transmitted = bytearray()

    async def chunks():
        yield b'12'
        yield b'345'
        if failure == 'disconnect':
            raise RuntimeError('client disconnected')
        if failure == 'oversize':
            yield b'6'

    @asynccontextmanager
    async def transfer(*args, content):
        async for chunk in content:
            transmitted.extend(chunk)
        yield httpx.Response(200, json={'size': len(transmitted)})

    def invoke(env, action, args):
        return {'exit_code': int(action == 'cmd.run' and args['cmd'].startswith(failure))}

    monkeypatch.setattr(file_client, 'file_request', transfer)
    control.invoke.side_effect = invoke
    with pytest.raises(exception) as caught:
        await router.upload_container_file_stream(user_id='alice', path='a', chunks=chunks(), mode='600')
    if failure == 'oversize':
        assert (caught.value.code, str(caught.value)) == ('file_too_large', 'upload exceeds 5 bytes')
    assert transmitted == b'12345'
    actions = [call.args[1] for call in control.invoke.await_args_list]
    assert 'file.rename' not in actions and actions[-1] == 'file.remove'
    assert (await router._agent_manager.get_agent('alice', 'jiuwenswarm')).task_count == 0


@pytest.mark.parametrize('status, payload, expected', [
    (400, {'error': 'path not found: /home/agentos/a'}, 'file_not_found'),
    (404, {'error': {'code': 'NOT_FOUND', 'message': 'instance not found'}}, 'instance_not_found'),
    (404, {'error': 'not found'}, 'INTERNAL_ERROR'),
    (404, {'error': 'missing', 'error_code': 'FILE_NOT_FOUND'}, 'file_not_found'),
])
async def test_invoke_distinguishes_structured_instance_and_file_errors(monkeypatch, status, payload, expected):
    control = AdxControlClient(_config(monkeypatch), transport=httpx.MockTransport(
        lambda request: httpx.Response(status, json=payload)))
    try:
        with pytest.raises(AdxControlError) as caught:
            await control.invoke('test-alice', 'file.stat', {'path': '/home/agentos/a'})
        assert caught.value.code == expected
    finally:
        await control.close()


@pytest.mark.parametrize('recursive', [False, True])
@pytest.mark.parametrize('path,mode', [
    ('relative', None), ('/outside/root', None), ('/home/agentos/../escape', None),
    ('/home/agentos/bad\x00name', None), ('/home/agentos/a', '999'),
])
async def test_mkdir_rejects_invalid_input_before_activation(file_router, recursive, path, mode):
    router, control = file_router
    with pytest.raises(AgentOSFileTransferError) as error:
        await router.mkdir_container_dir(user_id='alice', path=path, mode=mode, recursive=recursive)
    assert error.value.code == 'BAD_REQUEST'
    assert not router.test_api.creates
    control.invoke.assert_not_awaited()


async def test_nonrecursive_mkdir_preserves_filesystem_contract(file_router, monkeypatch, tmp_path):
    import subprocess
    from agent_gateway.routing import container_files

    monkeypatch.setattr(container_files, 'AGENT_FILE_PATH_ROOT', str(tmp_path))
    router, control = file_router
    async def invoke(environment, action, args):
        assert action == 'cmd.run'
        value = await asyncio.to_thread(subprocess.run, args['cmd'], shell=True,
                                       capture_output=True, text=True)
        return {'exit_code': value.returncode, 'stdout': value.stdout, 'stderr': value.stderr}
    control.invoke.side_effect = invoke
    missing = tmp_path / 'missing/deep'
    with pytest.raises(AgentOSFileTransferError) as error:
        await router.mkdir_container_dir(user_id='alice', path=str(missing))
    assert error.value.code == 'BAD_REQUEST'
    assert not missing.parent.exists()

    target = tmp_path / "quoted '; echo unsafe #"
    result = await router.mkdir_container_dir(user_id='alice', path=str(target))
    assert result == {'success': True, 'path': str(target), 'created': True}
    assert target.is_dir()
    result = await router.mkdir_container_dir(user_id='alice', path=str(target))
    assert result['created'] is False

    regular = tmp_path / 'regular'
    regular.write_text('retained')
    with pytest.raises(AgentOSFileTransferError) as error:
        await router.mkdir_container_dir(user_id='alice', path=str(regular))
    assert error.value.code == 'BAD_REQUEST'
    assert regular.read_text() == 'retained'


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
