# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Real Gateway parent and Workers with an isolated loopback ADX stand-in."""

import asyncio
from contextlib import AsyncExitStack
import json

from aiohttp import web
import httpx
import pytest
import yaml
from websockets.asyncio.client import connect

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.instance_routes import InstanceProviderSpec
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.processes.capabilities import management_preflight
from tests.integration.gateway.test_gateway_cron_cli import environment, read_until, start_gateway, stop_gateway
from tests.unit_tests.gateway.instance_fakes import EnvironmentAPI, InstanceFrontend

pytestmark = pytest.mark.skipif(not management_preflight().supported, reason='requires fd passing')


async def test_adx_cli_starts_lazy_workers_and_recovers_idle_instances(tmp_path):
    async with AsyncExitStack() as stack:
        ingress = await InstanceFrontend().start()
        stack.push_async_callback(ingress.stop)
        api = EnvironmentAPI()
        api.on_create = ingress.adopt

        async def serve(request):
            if request.headers.get('Upgrade', '').lower() == 'websocket':
                # Proxy only within this test stand-in so ADX control and data
                # can share one configured origin like the real deployment.
                ws = web.WebSocketResponse()
                async with connect(f'ws://{ingress.netloc}{request.raw_path}',
                                   additional_headers={'Authorization': request.headers['Authorization']}, proxy=None) as peer:
                    await ws.prepare(request)

                    async def upstream():
                        async for frame in ws:
                            if frame.type == web.WSMsgType.TEXT:
                                await peer.send(frame.data)

                    async def downstream():
                        async for frame in peer:
                            await ws.send_str(frame)

                    tasks = [asyncio.create_task(upstream()), asyncio.create_task(downstream())]
                    try:
                        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    finally:
                        for task in tasks:
                            task.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                return ws
            upstream_response = await api.handle(httpx.Request(
                request.method, 'http://localhost' + request.raw_path, headers=request.headers,
                content=await request.read(),
            ))
            return web.Response(status=upstream_response.status_code, body=upstream_response.content,
                                content_type='application/json')

        app = web.Application()
        app.router.add_route('*', '/{path:.*}', serve)
        runner = web.AppRunner(app)
        await runner.setup()
        stack.push_async_callback(runner.cleanup)
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        endpoint = f'http://127.0.0.1:{runner.addresses[0][1]}'
        root, config_dir = tmp_path / 'managed', tmp_path / 'config'
        config_dir.mkdir()
        for name in ('alice', 'bob'):
            (root / name).mkdir(parents=True)
        spec = InstanceProviderSpec(
            LocalIdentityPolicy(domain='adx-trial'), WorkerManifest((0, 1), config_version=9),
            ('alice', 'bob'), endpoint, 'test', str(root),
        )
        spec_file = tmp_path / 'instance-provider.json'
        spec_file.write_text(spec.to_json())
        (config_dir / 'config-gateway.yaml').write_text(yaml.safe_dump({'channels': {name: {'enabled': False} for name in ('feishu', 'xiaoyi')}, 'gateway': {
            'agent_client': {'type': 'adx_router', 'agent_namespace': 'test'},
            'adx': {'ingress_url': endpoint, 'allow_loopback_http': True, 'tenant': 'appliance',
                    'token_env': 'TEST_ADX_KEY', 'connect_timeout_seconds': 2},
            'agentos': {'workspace_root': str(root), 'sandbox_idle_timeout_seconds': 0.6,
                        'sandbox_idle_check_interval_seconds': 0.1},
        }}))
        proc = await start_gateway([
            '--mode', 'multiprocess', '--workers', '2', '--host', '127.0.0.1', '--port', '0',
            '--instance-provider-file', str(spec_file),
        ], environment(tmp_path, AGENT_GATEWAY_CONFIG_DIR=str(config_dir), TEST_ADX_KEY='platform-secret',
                       JIUWENSWARM_RUNTIME_PROFILE=''))
        try:
            ready, _ = await read_until(proc, r'multiprocess_ready workers=2 web_port=(\d+) protocol_port=(\d+) '
                                              r'worker_pids=(\d+),(\d+)')
            web_port, protocol_port, *worker_pids = map(int, ready.groups())
            assert not api.requests

            async def chat(user, *, turn=1):
                async with connect(f'ws://127.0.0.1:{web_port}/ws?user_id={user}', proxy=None) as ws:
                    async with asyncio.timeout(12):
                        assert json.loads(await ws.recv())['event'] == 'connection.ack'
                        await ws.send(json.dumps({'type': 'req', 'id': f'chat-{user}-{turn}', 'method': 'chat.send',
                            'is_stream': True, 'params': {'content': 'hello', 'session_id': f'session-{user}-{turn}'}}))
                        while True:
                            frame = json.loads(await ws.recv())
                            if frame.get('event') == 'chat.final':
                                return frame['payload']['content']

            assert await asyncio.gather(chat('alice'), chat('bob')) == ['alice', 'bob']
            assert len(api.creates) == 2
            assert {(user, row['user_id']) for user, row in ingress.requests if row['method'] == 'chat.send'} == {
                ('alice', 'alice'), ('bob', 'bob'),
            }
            async with asyncio.timeout(8):
                while len(api.deletes) < 2:
                    await asyncio.sleep(0.05)
            assert not api.environments and len({instance for instance, _ in api.deletes}) == 2
            assert await chat('alice', turn=2) == 'alice'
            assert len(api.creates) == 3 and len(api.environments) == 1
            assert next(iter(api.environments)) not in {instance for instance, _ in api.deletes}
            await stop_gateway(proc, worker_pids, (web_port, protocol_port))
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.communicate(), 12)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
