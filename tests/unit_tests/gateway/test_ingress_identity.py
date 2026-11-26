# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""The local-v1 rules that keep the Acceptor and the Worker on one business user."""

from dataclasses import FrozenInstanceError
import json
import traceback
from urllib.parse import quote

import pytest

from agent_gateway.common.ingress_identity import (
    IngressIdentity,
    IngressIdentityError,
    LocalIdentityPolicy,
    resolve_local_identity,
)
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest

ENTRIES = ('web', 'http', 'tui', 'acp')
SECRET = 's3cret-va1ue-Zq9'
# Spelled with escapes: a composed or decomposed literal must not depend on how this file was saved.
COMPOSED = '\u00e9'  # U+00E9 LATIN SMALL LETTER E WITH ACUTE
DECOMPOSED = 'e\u0301'  # the same user typed as e + U+0301
CJK = '\u5f20\u4e09'
ANONYMOUS = IngressIdentity('local-v1', 'local_compat', 'local', 'anonymous', None)


def resolve(query=b'', headers=(), *, entry='web'):
    return resolve_local_identity(query, headers, entry=entry, policy=LocalIdentityPolicy())


def refused(query=b'', headers=(), *, entry='web'):
    with pytest.raises(IngressIdentityError) as error:
        resolve(query, headers, entry=entry)
    return error.value


def declared(user, *, entry='web'):
    return resolve(f'user_id={quote(user)}'.encode('ascii'), entry=entry)


def test_query_user_is_a_named_identity_under_the_selected_policy():
    identity = resolve_local_identity(
        b'user_id=alice', (), entry='web', policy=LocalIdentityPolicy(domain='team'),
    )
    assert identity == IngressIdentity(
        contract_id='local-v1', mode='local_compat', domain='team',
        kind='named', business_user_id='alice',
    )


@pytest.mark.parametrize(('query', 'user'), [
    (b'user_id=%E5%BC%A0%E4%B8%89', CJK),
    (b'user_id=a+b', 'a b'),
    (b'user_id=a%2Bb', 'a+b'),
    (b'user_id=a%26b', 'a&b'),
    (b'user_id=%20alice%20', 'alice'),
])
def test_query_user_is_percent_decoded_utf8(query, user):
    assert resolve(query).business_user_id == user


@pytest.mark.parametrize('query', [
    b'', b'token=t&auth_session=s&session_id=x', b'user_id=&token=t', b'user_id=%20&token=t',
])
def test_query_without_a_user_is_anonymous(query):
    assert resolve(query) == ANONYMOUS


def test_credentials_and_session_parameters_are_not_part_of_the_identity():
    identity = resolve(b'user_id=alice&token=old&auth_session=s&session_id=x')
    assert identity == IngressIdentity('local-v1', 'local_compat', 'local', 'named', 'alice')


def test_query_field_name_is_case_sensitive():
    assert resolve(b'User_ID=alice') == ANONYMOUS


@pytest.mark.parametrize('name', [b'X-User-Id', b'x-user-id', b'X-USER-ID'])
@pytest.mark.parametrize('entry', ENTRIES)
def test_header_user_is_named_for_every_entry(name, entry):
    assert resolve(headers=[(name, b'alice')], entry=entry).business_user_id == 'alice'


def test_other_headers_are_not_part_of_the_identity():
    identity = resolve(headers=[
        (b'Authorization', b'Bearer t'), (b'X-Group-Id', b'g'), (b'Cookie', b'auth_session=s'),
    ])
    assert identity == ANONYMOUS


@pytest.mark.parametrize('value', [b'alice', b'alice ', b'  alice', b'\talice\t', b' \talice \t'])
def test_optional_whitespace_around_the_header_user_is_not_part_of_it(value):
    assert resolve(headers=[(b'X-User-Id', value)]).business_user_id == 'alice'


@pytest.mark.parametrize(('first', 'second'), [(b'alice', b'alice'), (b'alice', b'bob')])
def test_repeated_header_is_refused_even_when_both_values_match(first, second):
    assert refused(headers=[(b'X-User-Id', first), (b'x-user-id', second)]).status == 400


def test_conflicting_query_and_header_users_are_refused():
    assert refused(b'user_id=bob', [(b'X-User-Id', b'alice')]).status == 400


def test_blank_query_user_falls_back_to_the_header():
    assert resolve(b'user_id=&token=t', [(b'X-User-Id', b'alice')]).business_user_id == 'alice'


def test_blank_header_user_falls_back_to_the_query():
    assert resolve(b'user_id=alice', [(b'X-User-Id', b'')]).business_user_id == 'alice'


@pytest.mark.parametrize('entry', ENTRIES)
def test_one_user_declared_in_query_or_in_header_has_one_key_and_one_slot(entry):
    selector = FixedUserHash(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())
    identities = [
        resolve(b'user_id=alice', entry=entry),
        resolve(headers=[(b'X-User-Id', b'alice')], entry=entry),
        resolve(b'user_id=alice', [(b'X-User-Id', b'alice')], entry=entry),
    ]
    assert len({identity.user_key_bytes() for identity in identities}) == 1
    assert len({selector.select(identity) for identity in identities}) == 1


@pytest.mark.parametrize('raw', [
    CJK.encode(), f'caf{COMPOSED}'.encode(), f'caf{COMPOSED}'.encode('latin-1'),
    f'alice{COMPOSED}'.encode(), b'\xc2\xb7',
])
def test_non_ascii_header_user_is_refused_instead_of_being_reinterpreted(raw):
    assert refused(headers=[(b'X-User-Id', raw)]).status == 400


@pytest.mark.parametrize('user', [f'caf{COMPOSED}', CJK])
@pytest.mark.parametrize('entry', ('web', 'http'))
def test_non_ascii_user_is_declared_through_the_percent_encoded_query(user, entry):
    assert declared(user, entry=entry).business_user_id == user


@pytest.mark.parametrize('query', [
    b'user_id=alice&user_id=alice', b'user_id=alice&user_id=bob', b'user_id=&user_id=alice',
])
def test_repeated_query_user_is_refused(query):
    assert refused(query).status == 400


@pytest.mark.parametrize('query', [
    b'user_id=%zz', b'user_id=100%', b'user_id=%c3', b'user_id=%ff', b'user_id=%ed%a0%80',
    b'user_id=alice%00', b'user_id=alice%0a', b'user_id=%09alice', b'user_id=a%0db', b'user_id=a%7fb',
])
def test_malformed_or_control_query_user_is_refused(query):
    assert refused(query).status == 400


def test_raw_non_ascii_query_bytes_are_refused():
    assert refused(f'user_id={CJK}'.encode()).status == 400


@pytest.mark.parametrize('user', ['a' * 256, '\u5f20' * 85, 'a' * 254 + COMPOSED])
def test_user_up_to_256_utf8_bytes_is_accepted(user):
    assert declared(user).business_user_id == user


@pytest.mark.parametrize('user', ['a' * 257, '\u5f20' * 86, 'a' * 255 + COMPOSED])
def test_user_over_256_utf8_bytes_is_refused(user):
    assert refused(f'user_id={quote(user)}'.encode('ascii')).status == 400


def test_user_is_stored_in_nfc_form():
    assert resolve(b'user_id=e%CC%81').business_user_id == COMPOSED
    assert declared(DECOMPOSED).business_user_id == COMPOSED


def test_nfc_and_decomposed_users_share_one_key_and_one_slot():
    selector = FixedUserHash(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())
    composed = resolve(b'user_id=%C3%A9')
    decomposed = resolve(b'user_id=e%CC%81')
    assert composed.user_key_bytes() == decomposed.user_key_bytes()
    assert selector.select(composed) == selector.select(decomposed)


def test_length_limit_applies_to_the_nfc_form():
    # 100 decomposed users take 300 bytes and 200 bytes once composed.
    identity = resolve(b'user_id=' + b'e%CC%81' * 100)
    assert identity.business_user_id == COMPOSED * 100


@pytest.mark.parametrize('user', ['alice', 'user_1-x', 'Alice_09'])
@pytest.mark.parametrize('entry', ('tui', 'acp'))
def test_tui_and_acp_accept_ascii_word_users(user, entry):
    assert declared(user, entry=entry).business_user_id == user


@pytest.mark.parametrize('user', [CJK, 'a.b', 'a b', 'a@b', 'a+b', COMPOSED])
@pytest.mark.parametrize('entry', ('tui', 'acp'))
def test_tui_and_acp_refuse_users_outside_the_ascii_word_rule(user, entry):
    assert refused(f'user_id={quote(user)}'.encode('ascii'), entry=entry).status == 400


@pytest.mark.parametrize('user', [CJK, 'a.b', 'a b', 'a@b', 'a+b', COMPOSED])
@pytest.mark.parametrize('entry', ('web', 'http'))
def test_web_and_http_accept_unicode_users(user, entry):
    assert declared(user, entry=entry).business_user_id == user


def test_anonymous_connections_share_one_bucket():
    # Deliberate: nothing stable tells two anonymous clients apart, so they must
    # meet on one slot to reach the same sessions.
    selector = FixedUserHash(WorkerManifest((0, 1, 2, 3)), LocalIdentityPolicy())
    anonymous = [resolve(entry=entry) for entry in ENTRIES] + [
        resolve(b'token=t&auth_session=s&session_id=x'),
        resolve(headers=[(b'Authorization', b'Bearer t'), (b'X-Session-Id', b's')]),
    ]
    assert len({identity.user_key_bytes() for identity in anonymous}) == 1
    assert len({selector.select(identity) for identity in anonymous}) == 1


@pytest.mark.parametrize('entry', ['ssh', 'im', ''])
def test_unsupported_entry_is_a_caller_error(entry):
    with pytest.raises(ValueError, match='unsupported ingress entry'):
        resolve(entry=entry)


@pytest.mark.parametrize('policy', [None, {}, 'local-v1'])
def test_missing_policy_is_a_caller_error(policy):
    with pytest.raises(ValueError, match='explicit local identity policy'):
        resolve_local_identity(b'user_id=alice', (), entry='web', policy=policy)


@pytest.mark.parametrize('kwargs', [
    {'contract_id': 'local-v2'}, {'domain': ''}, {'domain': 'local '}, {'domain': 'a/b'},
    {'domain': COMPOSED}, {'domain': '-a'}, {'domain': 'a' * 65}, {'domain': 1},
])
def test_policy_rejects_other_contracts_and_unbounded_domains(kwargs):
    with pytest.raises(ValueError):
        LocalIdentityPolicy(**kwargs)


@pytest.mark.parametrize('domain', ['a', 'team-1.prod_2', 'a' * 64])
def test_policy_accepts_bounded_ascii_domains(domain):
    assert LocalIdentityPolicy(domain=domain).domain == domain


def test_anonymous_identity_cannot_have_a_business_user():
    for user in ('alice', ''):
        with pytest.raises(ValueError, match='anonymous identity cannot have a business user'):
            IngressIdentity('local-v1', 'local_compat', 'local', 'anonymous', user)


@pytest.mark.parametrize('user', [None, '', 1, ' alice', 'alice ', DECOMPOSED])
def test_named_identity_requires_a_canonical_user(user):
    with pytest.raises(ValueError, match='canonical business user'):
        IngressIdentity('local-v1', 'local_compat', 'local', 'named', user)


@pytest.mark.parametrize('user', ['ali\nce', 'a' * 257, CJK * 86])
def test_named_identity_refuses_a_malformed_user(user):
    with pytest.raises(IngressIdentityError):
        IngressIdentity('local-v1', 'local_compat', 'local', 'named', user)


@pytest.mark.parametrize(('field', 'value'), [
    ('kind', 'service'), ('mode', 'iam'), ('contract_id', 'local-v2'), ('domain', 'a/b'),
])
def test_identity_rejects_unsupported_kinds_modes_and_namespaces(field, value):
    kwargs = {
        'contract_id': 'local-v1', 'mode': 'local_compat', 'domain': 'local',
        'kind': 'named', 'business_user_id': 'alice',
    }
    kwargs[field] = value
    with pytest.raises(ValueError):
        IngressIdentity(**kwargs)


def test_identity_is_frozen():
    identity = resolve(b'user_id=alice')
    with pytest.raises(FrozenInstanceError):
        identity.business_user_id = 'bob'


@pytest.mark.parametrize(('query', 'headers'), [
    (f'user_id={SECRET}&user_id={SECRET}'.encode('ascii'), ()),
    (f'user_id=%ff{SECRET}'.encode('ascii'), ()),
    (f'user_id={SECRET}%zz'.encode('ascii'), ()),
    (f'user_id={SECRET}%00'.encode('ascii'), ()),
    (f'user_id={SECRET}'.encode('ascii'), [(b'X-User-Id', b'bob')]),
    (b'', [(b'X-User-Id', SECRET.encode()), (b'X-User-Id', b'bob')]),
])
def test_refusal_does_not_echo_any_client_value(query, headers):
    error = refused(query, headers)
    rendered = ''.join(traceback.format_exception(error))
    assert error.status == 400
    assert 'invalid ingress identity' in rendered
    assert SECRET not in rendered
    assert 'bob' not in rendered


def test_identity_repr_does_not_expose_the_business_user():
    identity = declared(SECRET)
    assert 'named' in repr(identity)
    assert SECRET not in repr(identity)


def test_user_key_bytes_are_a_versioned_utf8_json_list():
    assert declared('alice').user_key_bytes() == b'["gateway-user",1,"local_compat","local","named","alice"]'
    assert ANONYMOUS.user_key_bytes() == b'["gateway-user",1,"local_compat","local","anonymous",""]'
    assert declared(CJK).user_key_bytes() == (
        f'["gateway-user",1,"local_compat","local","named","{CJK}"]'.encode()
    )


@pytest.mark.parametrize('user', ['a","x', 'a\\b', 'a","x","y'])
def test_user_key_bytes_cannot_forge_another_list_element(user):
    key = declared(user).user_key_bytes()
    assert json.loads(key) == ['gateway-user', 1, 'local_compat', 'local', 'named', user]
