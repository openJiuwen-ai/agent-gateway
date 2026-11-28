"""C-mode remote ports require endpoint-bound private leases."""

from types import SimpleNamespace

import pytest

from agent_gateway.auth.credential_access import CredentialAccessError
from agent_gateway.auth.remote_credential_access import RemoteCredentialAccess
from agent_gateway.processes.control_service import ControlServiceIdentity


class Client:
    identity = ControlServiceIdentity('group', 0, 1, 101, 100)

    def __init__(self):
        self.response = {'status': 'ok', 'credential': {
            'api_base': 'https://test.invalid', 'api_key': 'canary', 'credential_ref': 'a' * 32},
            'lease': {'group_id': 'group', 'slot_id': 0, 'generation': 1,
                      'server_id': 'as', 'credential_ref': 'a' * 32, 'ref_revision': 1}}

    async def call(self, operation, arguments, *, timeout):
        return self.response


class Runtime:
    server_id = 'as'

    def issue(self, ref, revision, server_id):
        return SimpleNamespace(credential_ref=ref, ref_revision=revision, server_id=server_id)


async def test_explicit_runtime_requires_bound_lease_and_keeps_it_off_public_params():
    client = Client()
    port = RemoteCredentialAccess(client, runtime=Runtime())
    credential = await port.request_model_auth('auth-session', 'login-model')
    assert credential.lease.ref_revision == 1
    assert set(credential.to_params()) == {'api_base', 'api_key', 'credential_ref'}
    client.response['lease']['generation'] = 2
    with pytest.raises(CredentialAccessError) as failure:
        await port.request_model_auth('auth-session', 'login-model')
    assert failure.value.reason == 'protocol'


async def test_c_runtime_rejects_b_response_and_boolean_revision():
    client = Client()
    port = RemoteCredentialAccess(client, runtime=Runtime())
    client.response['lease']['ref_revision'] = True
    with pytest.raises(CredentialAccessError):
        await port.request_model_auth('auth-session', 'login-model')
    del client.response['lease']
    with pytest.raises(CredentialAccessError):
        await port.request_model_auth('auth-session', 'login-model')


async def test_c_refresh_keeps_operation_and_explicit_target():
    client = Client()
    port = RemoteCredentialAccess(client, runtime=Runtime())
    client.response = {'status': 'ok', 'operation_id': 'b' * 32, 'server_id': 'as',
                       'ref_revision': 2, 'update': {'credential_ref': 'a' * 32, 'revoked': True}}
    refresh = await port.credential_refresh_for_ref('a' * 32, 'as')
    assert refresh.operation_id == 'b' * 32
    assert refresh.ref_revision == 2
    assert refresh.update.revoked is True
    with pytest.raises(CredentialAccessError):
        await port.credential_refresh_for_ref('a' * 32, 'other')
    client.response['update']['revoked'] = 'true'
    with pytest.raises(CredentialAccessError) as failure:
        await port.credential_refresh_for_ref('a' * 32, 'as')
    assert failure.value.reason == 'protocol'
