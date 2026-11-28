# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Real WSS contract tests with a transparent Ingress double and E2A server."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
import json
import sys
import ssl
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from tests.unit_tests.extensions.test_adx_router import EnvironmentAPI, _config
from websockets.exceptions import ConnectionClosed, InvalidHandshake
from websockets.legacy.client import connect
from websockets.legacy.server import serve

from agent_gateway.extensions.adx.control_client import AdxControlClient
from agent_gateway.extensions.adx.router_client import AdxRouterClient
from e2a.models import E2AEnvelope
from e2a.wire_codec import (
    encode_agent_chunk_for_wire,
    encode_agent_response_for_wire,
)
from e2a.agent_models import AgentResponse, AgentResponseChunk
from unittest.mock import AsyncMock
from agent_gateway.extensions.adx import control_client
from agent_gateway.extensions.adx.control_client import AdxControlError
from agent_gateway.extensions.agentos.agentos_router.agent_manager import AgentCreateFailed
from tests.unit_tests.extensions.test_adx_router import _request, _router
import os
from unittest.mock import Mock
import yaml
from agent_gateway.extensions.adx.network import agent_policy, http_policy, tool_policy, workspace_model_origins
import ipaddress
from agent_gateway.extensions.adx.network import managed_tool_policy


@pytest.fixture(scope="session")
def network_addresses():
    return SimpleNamespace(
        ingress='10.88.4.1',
        model='192.168.4.40',
        public='1.1.1.1',
        public_cluster_node='8.8.8.8',
        loopback='127.0.0.1',
        other_private='10.88.4.20',
        metadata='169.254.169.254',
        shared='100.64.1.1',
    )


def _certificate(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder().subject_name(subject).issuer_name(subject)
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1)).not_valid_after(now + timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp_path / "ca.pem", tmp_path / "key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption(),
    ))
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_path, key_path)
    return context, str(cert_path)


@asynccontextmanager
async def _network(tmp_path, monkeypatch, *, restart_policy='default'):
    monkeypatch.setitem(sys.modules, 'adx_environment', None)
    monkeypatch.setattr(
        'agent_gateway.extensions.agentos.agentos_router.router_client._WS_CONNECT_READY_TIMEOUT_SECONDS', 5,
    )
    tls, ca_path = _certificate(tmp_path)
    monkeypatch.setenv("SSL_CERT_FILE", ca_path)
    requests, handshakes, targets = [], [], []
    expected_key = "platform-secret"

    async def backend(ws, path):
        targets.append(parse_qs(urlsplit(path).query)["target"][0])
        assert "Authorization" not in ws.request_headers
        await ws.send(json.dumps({
            "type": "event", "event": "connection.ack",
            "payload": {"status": "ready", "readiness": "AGENT_READY"},
        }))

        async def reply(request):
            requests.append(request)
            rid = request["request_id"]
            action = request["params"].get("action")
            if request["is_stream"]:
                for index in range(3):
                    chunk = AgentResponseChunk(rid, request["channel"], {"index": index}, index == 2)
                    await ws.send(json.dumps(encode_agent_chunk_for_wire(chunk, response_id=rid, sequence=index)))
                    if action == "drop" and index == 0:
                        await ws.close(code=1011, reason="test route withdrawn")
                        return
                    await asyncio.sleep(0)
            else:
                await ws.send(json.dumps(encode_agent_response_for_wire(
                    AgentResponse(rid, request["channel"], payload={"user": request["user_id"]}),
                    response_id=rid,
                )))

        running = set()
        try:
            async for raw in ws:
                task = asyncio.create_task(reply(json.loads(raw)))
                running.add(task)
        except ConnectionClosed:
            pass
        finally:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)

    async with serve(backend, "127.0.0.1", 0) as agent_server:
        agent_port = agent_server.sockets[0].getsockname()[1]

        async def auth(path, headers):
            handshakes.append((path, dict(headers)))
            if not urlsplit(path).path.endswith("/18092") or headers.get("Authorization") != f"Bearer {expected_key}":
                return 401, [], b"unauthorized"
            return None

        async def ingress(downstream, path):
            target = urlsplit(path).path.split("/")[1]
            async with connect(f"ws://127.0.0.1:{agent_port}/?target={target}") as upstream:
                async def copy(source, destination):
                    try:
                        async for frame in source:
                            await destination.send(frame)
                    except ConnectionClosed:
                        return

                pipes = [asyncio.create_task(copy(downstream, upstream)), asyncio.create_task(copy(upstream, downstream))]
                try:
                    await asyncio.wait(pipes, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for pipe in pipes:
                        pipe.cancel()
                    await asyncio.gather(*pipes, return_exceptions=True)

        async with serve(ingress, "127.0.0.1", 0, ssl=tls, process_request=auth) as proxy:
            port = proxy.sockets[0].getsockname()[1]
            monkeypatch.setenv("ADX_TEST_PLATFORM", expected_key)
            config = _config(monkeypatch, ingress_url=f"https://localhost:{port}")
            if restart_policy != 'default':
                config = replace(config, restart_policy=restart_policy)
            api = EnvironmentAPI(config.tenant)
            control = AdxControlClient(config, transport=httpx.MockTransport(api.handle))
            router = AdxRouterClient(config, control_client=control)
            router.test_api = api
            await router.connect("")
            try:
                yield router, requests, handshakes, targets
            finally:
                await router.disconnect()


def _request(user="alice", rid="request", action=""):
    return E2AEnvelope(request_id=rid, user_id=user, channel="web", method="chat.send", params={"action": action})


@pytest.mark.asyncio
async def test_wss_proxy_isolates_users_reuses_connections_and_multiplexes_streams(tmp_path, monkeypatch):
    async with _network(tmp_path, monkeypatch) as (router, requests, handshakes, targets):
        responses = await asyncio.gather(*[
            router.send_request(_request(user, f"unary-{index}"))
            for index, user in enumerate(["alice", "bob", "alice", "alice"])
        ])
        assert [response.payload["user"] for response in responses] == ["alice", "bob", "alice", "alice"]

        async def stream(user):
            return [chunk async for chunk in router.send_request_stream(_request(user, "same-stream-id"))]

        streams = await asyncio.gather(stream("alice"), stream("bob"))
        for chunks in streams:
            assert [chunk.payload["index"] for chunk in chunks] == [0, 1, 2]
            assert chunks[-1].is_complete
        assert len(handshakes) == 2 and len(set(targets)) == 2
        assert all(headers["authorization"] == "Bearer platform-secret" for _, headers in handshakes)
        assert len(requests) == 6


@pytest.mark.asyncio
@pytest.mark.parametrize('restart_policy', ['default', None])
async def test_real_stream_disconnect_is_not_replayed_and_next_call_reauthenticates(tmp_path, monkeypatch, restart_policy):
    async with _network(tmp_path, monkeypatch, restart_policy=restart_policy) as (router, requests, handshakes, targets):
        stream = router.send_request_stream(_request(rid="lost", action="drop"))
        assert (await anext(stream)).payload["index"] == 0
        failure = await asyncio.wait_for(anext(stream), timeout=3)
        assert failure.is_complete and failure.payload.get("error")
        await stream.aclose()
        assert [request["request_id"] for request in requests] == ["lost"]
        current = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        assert (current is None) == (restart_policy is None)
        assert (await router.send_request(_request(rid="next"))).ok
        assert len(router.test_api.creates) == (2 if restart_policy is None else 1)
        assert len(handshakes) == 2
        assert (targets[0] != targets[1]) == (restart_policy is None)
        if restart_policy is None:
            assert router.test_api.creates[0]['mounts'] == router.test_api.creates[1]['mounts']
        assert all(headers["authorization"] == "Bearer platform-secret" for _, headers in handshakes)


@pytest.mark.parametrize("failure", ["untrusted_ca", "invalid_key"])
@pytest.mark.asyncio
async def test_tls_or_ingress_auth_failure_never_sends_business_requests(tmp_path, monkeypatch, failure):
    async with _network(tmp_path, monkeypatch) as (router, requests, _handshakes, targets):
        if failure == "untrusted_ca":
            monkeypatch.delenv("SSL_CERT_FILE")
            monkeypatch.setattr(
                'agent_gateway.extensions.agentos.agentos_router.router_client._WS_CONNECT_READY_TIMEOUT_SECONDS', 0.1,
            )
        config = router._config if failure == "untrusted_ca" else replace(router._config, token="wrong-key")
        api = EnvironmentAPI(config.tenant)
        rejected = AdxRouterClient(config, control_client=AdxControlClient(
            router._config, transport=httpx.MockTransport(api.handle)))
        await rejected.connect("")
        try:
            if failure == "invalid_key":
                with pytest.raises(InvalidHandshake):
                    await rejected.send_request(_request())
            else:
                assert not (await rejected.send_request(_request())).ok
            assert not rejected._ws_clients
            assert not requests and not targets
        finally:
            await rejected.disconnect()

@pytest.fixture(autouse=True)
def gateway_without_adx_sdk(monkeypatch):
    monkeypatch.setitem(sys.modules, 'adx_environment', None)


@pytest.fixture
def workspace_without_models(monkeypatch):
    monkeypatch.setattr('agent_gateway.extensions.adx.network.workspace_model_origins', lambda _: [])


def network_launch(**network):
    return {'process': {'commands': [['true']]}, 'network': network}


@pytest.mark.usefixtures('workspace_without_models')
@pytest.mark.parametrize('network', [{}, {'ingress_url': 'invalid'}])
def test_builtin_network_requires_ingress_url(monkeypatch, network):
    with pytest.raises(ValueError, match=r'builtin_launch\.network\.ingress_url'):
        _config(monkeypatch, builtin_launch=network_launch(**network))


@asynccontextmanager
async def live_control(monkeypatch, handle):
    config = _config(monkeypatch, builtin_launch=network_launch(ingress_url='http://node-ingress.test:8080'))
    client = AdxControlClient(config, transport=httpx.MockTransport(handle))
    try:
        yield client
    finally:
        await client.close()


@pytest.mark.usefixtures('workspace_without_models')
@pytest.mark.asyncio
async def test_model_policy_refresh_reconciles_and_deduplicates(monkeypatch):
    origins = ['https://one.example/v1']
    monkeypatch.setattr('agent_gateway.extensions.adx.network.workspace_model_origins', lambda _: origins)
    requests = []

    def handle(request):
        operation = request.headers['x-request-id']
        assert operation.startswith('network-') and request.headers['x-adx-request-id'] == operation
        requests.append((operation, request.content))
        if len(requests) == 2:
            raise httpx.ReadTimeout('unknown outcome', request=request)
        return httpx.Response(200, json={})

    async with live_control(monkeypatch, handle) as client:
        async def update():
            await client.ensure_builtin_network('adx-alice', '/alice')

        await asyncio.gather(update(), update())
        assert len(requests) == 1
        origins[:] = ['https://two.example/v1']
        with pytest.raises(AdxControlError):
            await update()
        origins[:] = ['https://one.example/v1']
        await update()
        initial, timed_out, replayed, latest = requests
        assert timed_out == replayed
        assert initial[1] == latest[1] != replayed[1]
        assert len({initial[0], timed_out[0], latest[0]}) == 3


@pytest.mark.usefixtures('workspace_without_models')
@pytest.mark.asyncio
async def test_eviction_on_cache_limit_and_missing_environment(monkeypatch):
    monkeypatch.setattr(control_client, '_MAX_APPLIED_NETWORK_POLICIES', 2)
    applied = []

    def handle(request):
        if request.method == 'GET':
            return httpx.Response(404)
        applied.append(request.url.path.split('/')[-2])
        return httpx.Response(200, json={})

    async with live_control(monkeypatch, handle) as client:
        for instance in ('alice', 'bob', 'alice', 'carol', 'bob'):
            await client.ensure_builtin_network(instance, '/workspace')
        assert applied == ['alice', 'bob', 'carol', 'bob']
        assert list(client._network_applied) == ['carol', 'bob']
        with pytest.raises(control_client.AdxEnvironmentNotFoundError):
            await client.get_agent_info('bob')
        assert list(client._network_applied) == ['carol']


@pytest.mark.usefixtures('workspace_without_models')
@pytest.mark.asyncio
@pytest.mark.parametrize('failure', ['backend', 'release', 'cancel'])
async def test_policy_failure_releases_business_activity(monkeypatch, failure):
    launch = network_launch(ingress_url='http://node-ingress.test:8080')
    router, _ = _router(_config(monkeypatch, builtin_launch=launch))
    updating, releasing, finish, released = (asyncio.Event() for _ in range(4))

    async def update(*_args):
        updating.set()
        if failure == 'cancel':
            await asyncio.Future()
        raise AdxControlError('policy rejected')

    async def release(*_args, **_kwargs):
        releasing.set()
        if failure == 'release':
            raise RuntimeError('release failed')
        if failure == 'cancel':
            await finish.wait()
        released.set()

    monkeypatch.setattr(router._control, 'ensure_builtin_network', update)
    release_call = AsyncMock(side_effect=release)
    monkeypatch.setattr(router._agent_manager, 'release', release_call)
    await router.connect('')
    try:
        task = asyncio.create_task(router._resolve_agent(_request(), acquire=True))
        if failure == 'cancel':
            await asyncio.wait_for(updating.wait(), 2)
            task.cancel()
            await asyncio.wait_for(releasing.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            finish.set()
            await asyncio.wait_for(released.wait(), 2)
        else:
            with pytest.raises(AgentCreateFailed, match='AGENT_NETWORK_UPDATE_FAILED: policy rejected'):
                await task
        runtime = await router._agent_manager.get_agent('alice', 'jiuwenswarm')
        release_call.assert_awaited_once_with(runtime.key, expected_agent_id=runtime.info.agent_id)
    finally:
        finish.set()
        await router.shutdown()


@pytest.mark.usefixtures('workspace_without_models')
@pytest.mark.asyncio
async def test_delete_waits_for_update_and_clears_cached_generation(monkeypatch):
    updating, finish = asyncio.Event(), asyncio.Event()
    methods = []

    async def handle(request):
        methods.append(request.method)
        if methods == ['PUT']:
            updating.set()
            await finish.wait()
        return httpx.Response(200, json={})

    async with live_control(monkeypatch, handle) as client:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(client.ensure_builtin_network('alice', '/alice'))
            await asyncio.wait_for(updating.wait(), 2)
            tasks.create_task(client.delete_sandbox('alice'))
            await asyncio.sleep(0)
            assert methods == ['PUT']
            finish.set()
        assert 'alice' not in client._environment_locks
        await client.ensure_builtin_network('alice', '/alice')
        assert methods == ['PUT', 'DELETE', 'PUT']


def test_http_policy_encodes_peer_ports_defaults_and_dns_without_mutating_configuration(network_addresses):
    configuration = {
        'traffic': {'ingress_default_action': 'deny', 'egress_default_action': 'deny',
                    'rules': [{'protocol': 'tcp', 'cidr': f'{network_addresses.model}/32',
                               'port_range': 8443, 'environment_port_range': {'first': 19000}},
                              {'action': 'deny', 'direction': 'ingress', 'protocol': 'udp',
                               'port_range': {'first': 10000, 'last': 10010}, 'priority': 200},
                              {'domain': '*.model.example', 'protocol': 'tcp', 'port_range': 443}]},
        'dns': {'default_action': 'deny', 'rules': [{'pattern': '*.model.example', 'action': 'allow'},
                                                  {'pattern': 'blocked.example'}]},
    }
    original = json.dumps(configuration, sort_keys=True)
    assert http_policy(configuration) == {
        'schemaVersion': 2,
        'traffic': {'ingressDefaultAction': 'deny', 'egressDefaultAction': 'deny', 'mode': 'stateful',
                    'rules': [{'action': 'allow', 'direction': 'egress', 'protocol': 'tcp', 'priority': 100,
                               'peer': {'cidr': f'{network_addresses.model}/32', 'portRange': {'first': 8443, 'last': 8443}},
                               'environmentPortRange': {'first': 19000, 'last': 19000}},
                              {'action': 'deny', 'direction': 'ingress', 'protocol': 'udp', 'priority': 200,
                               'peer': {'portRange': {'first': 10000, 'last': 10010}}},
                              {'action': 'allow', 'direction': 'egress', 'protocol': 'tcp', 'priority': 100,
                               'peer': {'domain': '*.model.example', 'portRange': {'first': 443, 'last': 443}}}]},
        'dns': {'defaultAction': 'deny', 'rules': [{'pattern': '*.model.example', 'action': 'allow'},
                                                {'pattern': 'blocked.example', 'action': 'deny'}]},
    }
    assert json.dumps(configuration, sort_keys=True) == original


def test_http_policy_does_not_silently_discard_an_unknown_rule_field(network_addresses):
    configuration = tool_policy([], profile='N0')
    configuration['traffic']['rules'] = [{'action': 'allow', 'peer_cidr': f'{network_addresses.model}/32'}]
    with pytest.raises(TypeError, match='peer_cidr'):
        http_policy(configuration)


def test_workspace_model_policy_refreshes_with_user_configuration(tmp_path, monkeypatch, network_addresses):
    monkeypatch.setenv('API_BASE', 'https://gateway-only.example/v1')
    config = tmp_path / '.jiuwenswarm/config'
    config.mkdir(parents=True)
    yaml_file, env_file = config / 'config.yaml', config / '.env'
    yaml_file.write_text('models: {}\n')
    env_file.write_text('API_BASE=\n')
    ingress = {'ingress_url': f'http://{network_addresses.ingress}:8080'}
    isolated = agent_policy(ingress, [], [], profile='N0')
    rule, = isolated['traffic']['rules']
    assert rule['cidr'] == f'{network_addresses.ingress}/32' and not isolated['dns']['rules']
    read_yaml = Mock(wraps=yaml.safe_load)
    monkeypatch.setattr(yaml, 'safe_load', read_yaml)
    assert workspace_model_origins(tmp_path) == []

    env_file.write_text('API_BASE=https://tenant.example/v1\nMODEL_KEY=secret-value\n')
    for _ in range(2):
        assert workspace_model_origins(tmp_path) == ['https://tenant.example/v1']
    assert read_yaml.call_count == 2
    yaml_file.write_text('models:\n  primary:\n    api_base: ${API_BASE}\n    api_key: ${MODEL_KEY}\n')
    assert workspace_model_origins(tmp_path) == ['https://tenant.example/v1']
    assert read_yaml.call_count == 3
    env_file.write_text('API_BASE=https://model.example/v1\nMODEL_KEY=secret-value\n')
    origins = workspace_model_origins(tmp_path)
    assert origins == ['https://model.example/v1']
    wire = http_policy(agent_policy(ingress, origins, []))
    assert wire['dns']['rules'] == [{'action': 'allow', 'pattern': 'model.example'}]
    assert 'secret-value' not in json.dumps(wire) and os.environ['API_BASE'] == 'https://gateway-only.example/v1'


def test_gateway_traffic_alias_survives_node_changes_without_a_dns_grant(network_addresses):
    policy = agent_policy({'ingress_url': f'http://{network_addresses.ingress}:8080',
                           'ingress_policy_peer': 'agentos-ingress.node.invalid'}, [], [])
    rule = policy['traffic']['rules'][0]
    assert rule['domain'] == 'agentos-ingress.node.invalid'
    assert 'cidr' not in rule
    assert rule['port_range'] == {'first': 8080, 'last': 8080}
    assert policy['dns']['rules'] == []


def test_managed_legacy_denials_are_retained_and_unknown_policy_needs_migration():
    from agent_gateway.extensions.adx.network import managed_tool_policy
    blocked = managed_tool_policy({'block_network': True})
    assert blocked['traffic']['egress_default_action'] == 'deny'
    blacklist = managed_tool_policy({'dns_blacklist': ['blocked.example']})
    assert blacklist['dns']['rules'] == [{'action': 'deny', 'pattern': 'blocked.example'}]
    for old in ({'network_mode': 'host'}, {'traffic': {'egress_default_action': 'deny'}}):
        with pytest.raises(ValueError):
            managed_tool_policy(old)


def decision(policy, target, port=443):
    """Evaluate IPv4 rules using the runtime's higher-priority-first contract."""
    address = ipaddress.IPv4Address(target)
    traffic = policy['traffic']
    for rule in sorted(traffic['rules'], key=lambda item: item['priority'], reverse=True):
        if rule.get('direction') not in ('egress', 'both') or rule.get('domain'):
            continue
        if rule.get('cidr') and address not in ipaddress.IPv4Network(rule['cidr']):
            continue
        selected = rule.get('port_range')
        if selected and not selected['first'] <= port <= selected['last']:
            continue
        return rule['action']
    return traffic['egress_default_action']


def test_default_n2_allows_public_but_denies_internal_and_metadata(network_addresses):
    policy = tool_policy([], blocked_cidrs=[f'{network_addresses.public_cluster_node}/32'])
    assert decision(policy, network_addresses.public_cluster_node) == 'deny'
    assert decision(policy, network_addresses.public) == 'allow'
    for target in (network_addresses.loopback, network_addresses.other_private, network_addresses.model, network_addresses.metadata, network_addresses.shared):
        assert decision(policy, target) == 'deny'
    assert policy['traffic']['ingress_default_action'] == 'deny'
    assert policy['dns']['default_action'] == 'allow'


@pytest.mark.parametrize('profile,public,granted', [('N0', 'deny', 'deny'), ('N1', 'deny', 'allow'), ('N2', 'allow', 'allow')])
def test_tool_profile_and_exact_legacy_origin(profile, public, granted, network_addresses):
    policy = tool_policy([f'http://{network_addresses.model}:18144'], profile=profile)
    assert decision(policy, network_addresses.public) == public
    assert decision(policy, network_addresses.model, 18144) == granted
    assert decision(policy, network_addresses.model, 6379) == 'deny'
    if profile == 'N0':
        assert not policy['traffic']['rules']
        assert policy['dns'] == {'default_action': 'deny', 'rules': []}


@pytest.mark.parametrize('profile', ['N0', 'N1', 'N2'])
def test_agent_keeps_ingress_and_limits_private_model_grants(profile, network_addresses):
    ingress = {'ingress_url': f'http://{network_addresses.ingress}:8080'}
    model = f'http://{network_addresses.model}:18144'
    if profile != 'N0':
        with pytest.raises(ValueError, match='explicit allowed origin'):
            agent_policy(ingress, [model], [], profile=profile)
    policy = agent_policy(ingress, [model + '/v1'], [model], profile=profile)
    assert decision(policy, network_addresses.ingress, 8080) == 'allow'
    assert decision(policy, network_addresses.model, 18144) == ('deny' if profile == 'N0' else 'allow')
    assert decision(policy, network_addresses.model, 6379) == 'deny'


@pytest.mark.parametrize('profile', ['N3', '', None, True, 2])
def test_unknown_profile_is_not_silently_downgraded(profile):
    with pytest.raises(ValueError, match='profile'):
        tool_policy([], profile=profile)


def test_native_n2_roundtrips_through_managed_tool_overlay():
    policy = tool_policy([])
    assert managed_tool_policy(policy) == policy
