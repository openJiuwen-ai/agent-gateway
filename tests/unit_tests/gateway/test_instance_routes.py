from __future__ import annotations

import json

import pytest

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.instance_routes import InstanceProviderSpec
from agent_gateway.common.worker_affinity import WorkerManifest


def spec(**changes):
    values = {
        'identity_policy': LocalIdentityPolicy(domain='instances'),
        'manifest': WorkerManifest((0, 1), config_version=37),
        'admitted_users': ('alice', 'bob'),
        'frontend_endpoint': 'https://manager.example',
        'namespace': 'gateway-test',
        'workspace_root': '/managed/agentos',
    }
    values.update(changes)
    return InstanceProviderSpec(**values)


def test_spec_validates_and_round_trips_bounded_private_configuration() -> None:
    value = spec(cpu=2000, memory=8192, agent_port=18092, registry_endpoint='http://registry.example')

    encoded = value.to_json()
    decoded = InstanceProviderSpec.from_json(encoded)

    assert decoded == value
    assert len(encoded.encode('utf-8')) <= 16 * 1024


def test_spec_rejects_unsupported_or_secret_and_unmanaged_paths() -> None:
    for changes in (
        {'identity_policy': 'local-v1'},
        {'manifest': {'slots': [0]}},
        {'admitted_users': ['alice', 'bob']},
        {'admitted_users': ()},
        {'admitted_users': ('alice',) * 33},
        {'admitted_users': ('alice', 'alice')},
        {'admitted_users': (' alice',)},
        {'admitted_users': ('.',)},
        {'admitted_users': ('..',)},
        {'admitted_users': ('a/b', 'a_b')},
        {'frontend_endpoint': 'http://user:secret@manager.example'},
        {'frontend_endpoint': 'https://manager.example/path'},
        {'frontend_endpoint': 'https://manager.example?token=secret'},
        {'frontend_endpoint': 'ftp://manager.example'},
        {'namespace': 'bad namespace'},
        {'workspace_root': 'relative/root'},
        {'registry_endpoint': 'ftp://registry.example'},
        {'cpu': True},
        {'cpu': 0},
        {'cpu': 65536},
        {'memory': 0},
        {'memory': 1_048_577},
        {'agent_port': 0},
        {'agent_port': 65536},
    ):
        with pytest.raises(ValueError):
            spec(**changes)


def test_spec_json_rejects_unknown_missing_duplicate_and_non_standard_values() -> None:
    encoded = spec().to_json()
    value = json.loads(encoded)

    with pytest.raises(ValueError):
        InstanceProviderSpec.from_json(encoded.replace('}', ',"extra":1}'))
    with pytest.raises(ValueError):
        InstanceProviderSpec.from_json(encoded.replace('"schema_version":1,', ''))
    with pytest.raises(ValueError):
        InstanceProviderSpec.from_json(
            encoded.replace('"namespace":"gateway-test"', '"namespace":"one","namespace":"two"')
        )
    with pytest.raises(ValueError):
        InstanceProviderSpec.from_json(encoded.replace('"schema_version":1', '"schema_version":true'))
    with pytest.raises(ValueError):
        InstanceProviderSpec.from_json(encoded.replace('"cpu":null', '"cpu":NaN'))
