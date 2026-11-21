"""Managed runsc policies select business destinations without copying secrets."""
import json
import sys

import pytest

from agent_gateway.extensions.adx.network import agent_policy, http_policy, tool_policy, workspace_model_origins


@pytest.fixture(autouse=True)
def gateway_without_adx_sdk(monkeypatch):
    monkeypatch.setitem(sys.modules, 'adx_environment', None)


def test_http_policy_encodes_peer_ports_defaults_and_dns_without_mutating_configuration():
    configuration = {
        'traffic': {'ingress_default_action': 'deny', 'egress_default_action': 'deny',
                    'rules': [{'protocol': 'tcp', 'cidr': '192.168.77.10/32',
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
                               'peer': {'cidr': '192.168.77.10/32', 'portRange': {'first': 8443, 'last': 8443}},
                               'environmentPortRange': {'first': 19000, 'last': 19000}},
                              {'action': 'deny', 'direction': 'ingress', 'protocol': 'udp', 'priority': 200,
                               'peer': {'portRange': {'first': 10000, 'last': 10010}}},
                              {'action': 'allow', 'direction': 'egress', 'protocol': 'tcp', 'priority': 100,
                               'peer': {'domain': '*.model.example', 'portRange': {'first': 443, 'last': 443}}}]},
        'dns': {'defaultAction': 'deny', 'rules': [{'pattern': '*.model.example', 'action': 'allow'},
                                                {'pattern': 'blocked.example', 'action': 'deny'}]},
    }
    assert json.dumps(configuration, sort_keys=True) == original


def test_http_policy_does_not_silently_discard_an_unknown_rule_field():
    configuration = tool_policy([], profile='N0')
    configuration['traffic']['rules'] = [{'action': 'allow', 'peer_cidr': '192.168.77.10/32'}]
    with pytest.raises(TypeError, match='peer_cidr'):
        http_policy(configuration)


def test_model_origins_come_from_retained_config_and_environment(tmp_path):
    config = tmp_path / '.jiuwenswarm/config'
    config.mkdir(parents=True)
    (config / '.env').write_text('MODEL_ENDPOINT=https://model.example/v1\nMODEL_KEY=secret-value\n')
    (config / 'config.yaml').write_text('models:\n  primary:\n    api_base: ${MODEL_ENDPOINT}\n    api_key: ${MODEL_KEY}\n')
    origins = workspace_model_origins(tmp_path)
    assert origins == ['https://model.example/v1']
    wire = http_policy(agent_policy({'ingress_url': 'http://10.88.4.1:8080'}, origins, []))
    assert 'secret-value' not in json.dumps(wire)
    assert wire['traffic']['egressDefaultAction'] == 'allow'
    assert wire['dns']['rules'] == [{'action': 'allow', 'pattern': 'model.example'}]


def test_model_origins_refresh_after_yaml_or_dotenv_changes(tmp_path, monkeypatch):
    import yaml
    from unittest.mock import Mock

    read_yaml = Mock(wraps=yaml.safe_load)
    monkeypatch.setattr(yaml, 'safe_load', read_yaml)
    config = tmp_path / '.jiuwenswarm/config'
    config.mkdir(parents=True)
    yaml_file, env_file = config / 'config.yaml', config / '.env'
    yaml_file.write_text('models: {}\n')
    env_file.write_text('API_BASE=https://first.example/v1\n')
    assert workspace_model_origins(tmp_path) == ['https://first.example/v1']
    assert workspace_model_origins(tmp_path) == ['https://first.example/v1']
    assert read_yaml.call_count == 1
    env_file.write_text('API_BASE=https://other.example/v1\n')
    assert workspace_model_origins(tmp_path) == ['https://other.example/v1']
    yaml_file.write_text('models: {defaults: [{base_url: "https://new.example/v1"}]}\n')
    assert workspace_model_origins(tmp_path) == ['https://new.example/v1']
    assert read_yaml.call_count == 3


def test_gateway_traffic_alias_survives_node_changes_without_a_dns_grant():
    policy = agent_policy({'ingress_url': 'http://10.88.4.1:8080',
                           'ingress_policy_peer': 'agentos-ingress.node.invalid'}, [], [])
    rule = policy['traffic']['rules'][0]
    assert rule['domain'] == 'agentos-ingress.node.invalid'
    assert 'cidr' not in rule
    assert rule['port_range'] == {'first': 8080, 'last': 8080}
    assert policy['dns']['rules'] == []


def test_n0_tools_have_no_gateway_or_dns_grant():
    policy = http_policy(tool_policy([], profile='N0'))
    assert policy['traffic']['rules'] == []
    assert policy['dns'] == {'defaultAction': 'deny', 'rules': []}


def test_private_models_require_an_explicit_deployment_origin():
    with pytest.raises(ValueError, match='explicit allowed origin'):
        agent_policy({'ingress_url': 'http://10.88.4.1:8080'}, ['http://192.168.77.10:18000/v1'], [])
    policy = agent_policy({'ingress_url': 'http://10.88.4.1:8080'}, ['http://192.168.77.10:18000/v1'], ['http://192.168.77.10:18000'])
    assert policy['traffic']['rules'][-1]['priority'] == 300


def test_legacy_environment_model_fallback_is_scoped_to_workspace(tmp_path, monkeypatch):
    config = tmp_path / '.jiuwenswarm/config'
    config.mkdir(parents=True)
    (config / 'config.yaml').write_text('sandbox: {}\n')
    (config / '.env').write_text('API_BASE=https://tenant.example/v1\n')
    monkeypatch.setenv('API_BASE', 'https://gateway.example/v1')
    assert workspace_model_origins(tmp_path) == ['https://tenant.example/v1']
    import os
    assert os.environ['API_BASE'] == 'https://gateway.example/v1'


def test_unconfigured_model_grants_no_model_network_access(tmp_path, monkeypatch):
    config = tmp_path / '.jiuwenswarm/config'
    config.mkdir(parents=True)
    (config / 'config.yaml').write_text('models: {}\n')
    (config / '.env').write_text('API_BASE=\n')
    monkeypatch.setenv('API_BASE', 'https://gateway-only.example/v1')
    origins = workspace_model_origins(tmp_path)
    assert origins == []
    policy = agent_policy({'ingress_url': 'http://10.88.4.1:8080'}, origins, [], profile='N0')
    assert len(policy['traffic']['rules']) == 1
    assert policy['traffic']['rules'][0]['cidr'] == '10.88.4.1/32'
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
