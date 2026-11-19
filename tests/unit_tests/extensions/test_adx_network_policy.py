"""Managed runsc policies select business destinations without copying secrets."""
import json
import os
import sys
from unittest.mock import Mock

import pytest
import yaml

from agent_gateway.extensions.adx.network import agent_policy, http_policy, tool_policy, workspace_model_origins


@pytest.fixture(autouse=True)
def gateway_without_adx_sdk(monkeypatch):
    monkeypatch.setitem(sys.modules, 'adx_environment', None)


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
