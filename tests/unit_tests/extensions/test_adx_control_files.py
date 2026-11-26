"""Environment-scoped file contracts through the ADX adapter."""
import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
import pytest_asyncio

from agent_gateway import telemetry
from agent_gateway.extensions.adx.control_client import AdxControlClient, AdxControlError
from agent_gateway.extensions.adx.router_client import AdxRouterClient, AdxRoutingError
from agent_gateway.extensions.agentos.agentos_router.router_client import AgentOSFileTransferError
from agent_gateway.extensions.auth.credential_authenticator import AuthResult
from tests.unit_tests.extensions.test_adx_router import _config, FakeClient, _request, _router


def _files(monkeypatch, *, auth_client=None):
    router, _clients = _router(_config(monkeypatch), auth_client=auth_client)
    router._control.invoke = AsyncMock()
    api = router.test_api
    original = api.handle
    api.transfers = []
    api.file_data = b'abcdef'
    async def handle(request):
        if request.url.path.endswith('/upload'):
            assert request.headers['authorization'] == 'Bearer platform-secret'
            content = await request.aread()
            api.transfers.append((request, content))
            return httpx.Response(200, json={'size': len(content)})
        if request.url.path.endswith('/download'):
            assert request.headers['authorization'] == 'Bearer platform-secret'
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
            if getattr(api, 'omit_download_length', False):
                del response.headers['Content-Length']
            return response
        return original(request)
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


async def test_composed_adx_client_mounts_file_routes(monkeypatch):
    from fastapi import FastAPI
    from agent_gateway.channel_manager.web.container_file_http import attach_container_file_routes
    from agent_gateway.extensions.agentos.agentos_router.router_client import AgentOSRouterClient
    from agent_gateway.southbound.orchestration.managed_router import ManagedRouter

    router, control = _files(monkeypatch, auth_client=_business_authenticator())
    assert not isinstance(router, AgentOSRouterClient)
    assert isinstance(router._router, ManagedRouter)
    app = FastAPI()
    attach_container_file_routes(app, _file_channel(router))
    await router.connect('')
    try:
        control.invoke.return_value = {'entries': []}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://gateway',
                                     headers={'Authorization': 'Bearer business-test-token'}) as client:
            response = await client.get('/file-api/list-files', params={'dir': '/home/agentos', 'user_id': 'alice'})
            assert response.status_code == 200
        runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        assert runtime.task_count == 0
    finally:
        await router.shutdown()


@pytest.mark.parametrize('connected', [True, False])
@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_file_upload_is_atomic_and_scoped_to_activated_user(monkeypatch):
    router, control = _files(monkeypatch)
    await router.connect('')
    payload = b'hello' * 17000
    actions = []
    def invoke(env, action, args):
        assert env == _alice_instance(router)
        actions.append((action, args))
        return {'bytes_written': len(bytes.fromhex(args['data']))} if action == 'file.write_chunk' else {'exit_code': 0}
    control.invoke.side_effect = invoke
    result = await router.upload_container_file(user_id='alice', path='probe.txt', content=payload)
    assert len(router.test_api.transfers) == 1
    request, uploaded = router.test_api.transfers[0]
    assert uploaded == payload
    assert request.url.path.endswith('/upload')
    assert not any(action == 'file.write_chunk' for action, _ in actions)
    assert actions[-2][0] == 'file.rename' and actions[-2][1]['new_path'] == '/home/agentos/probe.txt'
    assert result['size'] == len(payload)
    with pytest.raises(AgentOSFileTransferError):
        await router.download_container_file(user_id='bob', path='/home/agentos/probe.txt', instance_id=_alice_instance(router))
    await router.shutdown()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
@pytest.mark.asyncio
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


@pytest.mark.parametrize('payload', [
    {'error': {'code': 'INSTANCE_NOT_FOUND'}},
    {'error': 'instance missing', 'error_code': 'INSTANCE_NOT_FOUND'},
])
@pytest.mark.asyncio
async def test_native_stream_maps_instance_not_found_for_both_response_shapes(payload):
    from agent_gateway.extensions.adx.file_transport import _check_response

    with pytest.raises(AdxControlError) as caught:
        await _check_response(httpx.Response(404, json=payload))
    assert caught.value.code == 'instance_not_found'


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
@pytest.mark.parametrize('root', ['/home/agentos', '/custom/workspace'])
async def test_uploaded_file_and_new_parents_inherit_workspace_owner(file_router, monkeypatch, root):
    import shlex
    from agent_gateway.routing import container_files

    monkeypatch.setattr(container_files, 'AGENT_FILE_PATH_ROOT', root)
    router, control = file_router
    actions = []
    def invoke(env, action, args):
        actions.append((action, args))
        if action == 'file.exists':
            assert args['path'].startswith(root + '/')
            return {'exists': root == '/home/agentos' and args['path'] == f'{root}/existing'}
        return {'exit_code': 0}
    control.invoke.side_effect = invoke
    await router.upload_container_file(
        user_id='alice', path="existing/new dir/it's a file.txt", content=b'owned')
    commands = [args['cmd'] for action, args in actions if action == 'cmd.run']
    assert len(commands) == 1
    words = shlex.split(commands[0])
    assert words[:4] == ['chown', '-h', f'--reference={root}', '--']
    assert f'{root}/existing/new dir' in words[4:]
    assert (f'{root}/existing' in words[4:]) == (root != '/home/agentos')
    assert actions[-3][0] == 'cmd.run' and actions[-2][0] == 'file.rename'


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_native_range_download_preserves_chunk_contract(file_router):
    router, control = file_router
    control.invoke.return_value = {'type': 'file', 'size': 6}
    result = await router.download_container_file(user_id='alice', path='/home/agentos/a', offset=2, limit=3)
    assert result.data == b'cde' and result.offset == 2 and result.size == 6 and not result.eof
    assert router.test_api.transfers[0][0].headers['range'] == 'bytes=2-4'
    result = await router.download_container_file(user_id='alice', path='/home/agentos/a', offset=6)
    assert result.eof and result.data == b'' and len(router.test_api.transfers) == 1


@pytest.mark.asyncio
async def test_interrupted_upload_never_publishes_target_and_releases_runtime(file_router):
    router, control = file_router
    control.invoke.return_value = {'exists': True, 'exit_code': 0}
    async def chunks():
        yield b'partial'
        raise RuntimeError('client disconnected')
    with pytest.raises(RuntimeError, match='client disconnected'):
        await router.upload_container_file_stream(user_id='alice', path='a', chunks=chunks())
    assert not any(call.args[1] == 'file.rename' for call in control.invoke.await_args_list)
    assert control.invoke.await_args_list[-1].args[1] == 'file.remove'
    runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
    assert runtime.task_count == 0


@pytest.mark.asyncio
async def test_native_upload_rejects_traversal_before_creating_environment(file_router):
    router, control = file_router
    with pytest.raises(AgentOSFileTransferError):
        await router.upload_container_file(user_id='alice', path='../escape', content=b'bad')
    assert not router.test_api.creates and not router.test_api.transfers


@pytest.mark.asyncio
async def test_native_upload_retains_stream_limit_for_unknown_size(file_router, monkeypatch):
    from agent_gateway.extensions.adx import file_client
    monkeypatch.setattr(file_client, 'MAX_UPLOAD_FILE_SIZE', 5)
    router, control = file_router
    control.invoke.return_value = {'exit_code': 0}
    transmitted = b''
    async def chunks():
        for chunk in (b'12', b'345', b'6'):
            yield chunk
    @asynccontextmanager
    async def transfer(*args, content):
        nonlocal transmitted
        async for chunk in content:
            transmitted += chunk
        yield SimpleNamespace(aread=AsyncMock(return_value=b''), json=lambda: {'size': len(transmitted)})
    monkeypatch.setattr(file_client, 'file_request', transfer)
    with pytest.raises(AgentOSFileTransferError) as caught:
        await router.upload_container_file_stream(user_id='alice', path='large', chunks=chunks())
    assert caught.value.code == 'file_too_large' and transmitted == b'12345'
    assert not any(call.args[1] == 'file.rename' for call in control.invoke.await_args_list)
    assert any(call.args[1] == 'file.remove' for call in control.invoke.await_args_list)


@pytest.mark.parametrize('status, payload, expected', [
    (400, {'error': 'path not found: /home/agentos/a'}, 'file_not_found'),
    (404, {'error': {'code': 'NOT_FOUND', 'message': 'instance not found'}}, 'instance_not_found'),
    (404, {'error': 'not found'}, 'INTERNAL_ERROR'),
    (404, {'error': 'missing', 'error_code': 'FILE_NOT_FOUND'}, 'file_not_found'),
])
@pytest.mark.asyncio
async def test_invoke_distinguishes_structured_instance_and_file_errors(monkeypatch, status, payload, expected):
    control = AdxControlClient(_config(monkeypatch), transport=httpx.MockTransport(
        lambda request: httpx.Response(status, json=payload)))
    try:
        with pytest.raises(AdxControlError) as caught:
            await control.invoke('test-alice', 'file.stat', {'path': '/home/agentos/a'})
        assert caught.value.code == expected
    finally:
        await control.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('command', ['chown', 'chmod'])
async def test_metadata_failure_discards_pending_upload(file_router, command):
    router, control = file_router
    def invoke(env, action, args):
        failed = action == 'cmd.run' and args['cmd'].startswith(command)
        return {'exit_code': int(failed)}

    control.invoke.side_effect = invoke
    with pytest.raises(AgentOSFileTransferError):
        await router.upload_container_file(user_id='alice', path='a', content=b'a', mode='600')
    actions = [call.args[1] for call in control.invoke.await_args_list]
    assert 'file.rename' not in actions and actions[-1] == 'file.remove'


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
