# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Pure local business identity rules, without authentication or worker selection."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
import json
import re
from typing import Literal
import unicodedata
from urllib.parse import unquote_to_bytes


_LOCAL_CONTRACT = 'local-v1'
_MAX_USER_BYTES = 256
_DOMAIN_PATTERN = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,63}')
_PROTOCOL_USER_PATTERN = re.compile(r'[A-Za-z0-9_-]+')
_BAD_PERCENT = re.compile(rb'%(?![0-9A-Fa-f]{2})')
_ENTRIES = frozenset({'web', 'http', 'tui', 'acp'})

IngressEntry = Literal['web', 'http', 'tui', 'acp']


class IngressIdentityError(ValueError):
    """Reject an explicit identity without retaining client values in the error."""

    status = 400

    def __init__(self) -> None:
        super().__init__('invalid ingress identity')


@dataclass(frozen=True)
class LocalIdentityPolicy:
    """Non-secret versioned rules for an explicitly enabled local runtime."""

    contract_id: str = _LOCAL_CONTRACT
    domain: str = 'local'

    def __post_init__(self) -> None:
        if self.contract_id != _LOCAL_CONTRACT:
            raise ValueError('unsupported local identity contract')
        if not isinstance(self.domain, str) or _DOMAIN_PATTERN.fullmatch(self.domain) is None:
            raise ValueError('identity domain must be a bounded ASCII namespace')


def _normalize_user(value: str) -> str | None:
    if any(unicodedata.category(character) == 'Cc' for character in value):
        raise IngressIdentityError()
    normalized = value.strip()
    if not normalized:
        return None
    try:
        encoded = normalized.encode('utf-8')
    except UnicodeError:
        raise IngressIdentityError() from None
    if len(encoded) > _MAX_USER_BYTES:
        raise IngressIdentityError()
    return normalized


@dataclass(frozen=True)
class IngressIdentity:
    """A local user declaration; this object never asserts verified IAM identity."""

    contract_id: str
    mode: str
    domain: str
    kind: Literal['named', 'anonymous']
    business_user_id: str | None = field(repr=False)

    def __post_init__(self) -> None:
        LocalIdentityPolicy(contract_id=self.contract_id, domain=self.domain)
        if self.mode != 'local_compat':
            raise ValueError('unsupported ingress identity mode')
        if self.kind == 'anonymous':
            if self.business_user_id is not None:
                raise ValueError('anonymous identity cannot have a business user')
        elif self.kind == 'named':
            if (
                not isinstance(self.business_user_id, str)
                or not self.business_user_id
                or _normalize_user(self.business_user_id) != self.business_user_id
            ):
                raise ValueError('named identity requires a canonical business user')
        else:
            raise ValueError('unsupported ingress identity kind')

    def user_key_bytes(self) -> bytes:
        """Return versioned UTF-8 key bytes; do not hash credentials or resources."""
        return json.dumps(
            ['gateway-user', 1, self.mode, self.domain, self.kind, self.business_user_id or ''],
            ensure_ascii=False, separators=(',', ':'),
        ).encode('utf-8')


def _decode_query_field(value: bytes) -> str:
    if _BAD_PERCENT.search(value) is not None:
        raise IngressIdentityError()
    try:
        return unquote_to_bytes(value.replace(b'+', b' ')).decode('utf-8', errors='strict')
    except UnicodeError:
        raise IngressIdentityError() from None


def _query_user(query: bytes) -> str | None:
    values: list[str] = []
    for part in query.split(b'&'):
        name, _separator, value = part.partition(b'=')
        if _decode_query_field(name) == 'user_id':
            if values:
                raise IngressIdentityError()
            values.append(_decode_query_field(value))
    return _normalize_user(values[0]) if values else None


def resolve_local_identity(
    query: bytes,
    headers: Sequence[tuple[bytes, bytes]],
    *,
    entry: IngressEntry,
    policy: LocalIdentityPolicy,
) -> IngressIdentity:
    """Resolve raw query and header pairs with identical rules in both processes.

    Args:
        query: Raw ASCII URL query, without the leading question mark.
        headers: Raw header pairs, preserving duplicates; values decode as Latin-1.
        entry: Business entry kind; TUI and ACP retain their ASCII user rules.
        policy: Explicit local contract and identity namespace.

    Returns:
        A canonical named user or an anonymous identity with no business user ID.

    Raises:
        IngressIdentityError: Identity fields are malformed, duplicated or conflicting.
        ValueError: The caller did not select a supported entry or explicit policy.
    """
    if not isinstance(policy, LocalIdentityPolicy):
        raise ValueError('an explicit local identity policy is required')
    if entry not in _ENTRIES:
        raise ValueError('unsupported ingress entry')
    if not isinstance(query, bytes) or not query.isascii():
        raise IngressIdentityError()
    query_user = _query_user(query)
    header_values = [value for name, value in headers if name.lower() == b'x-user-id']
    if len(header_values) > 1:
        raise IngressIdentityError()
    header_user = _normalize_user(header_values[0].decode('latin1')) if header_values else None
    if query_user is not None and header_user is not None and query_user != header_user:
        raise IngressIdentityError()
    user = query_user or header_user
    if user is not None and entry in {'tui', 'acp'} and _PROTOCOL_USER_PATTERN.fullmatch(user) is None:
        raise IngressIdentityError()
    return IngressIdentity(
        contract_id=policy.contract_id, mode='local_compat', domain=policy.domain,
        kind='named' if user is not None else 'anonymous', business_user_id=user,
    )
