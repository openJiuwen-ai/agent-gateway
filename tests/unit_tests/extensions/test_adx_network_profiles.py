"""Network presets preserve control paths without opening internal services."""
import ipaddress

import pytest

from agent_gateway.extensions.adx.network import agent_policy, managed_tool_policy, tool_policy


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
