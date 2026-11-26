# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Pure local business identity rules, without authentication or worker selection.

``local-v1`` is one rule for the raw query and header bytes of a handshake or a
request. Both processes apply it to the same bytes, so the slot an Acceptor
chooses and the business user a Worker serves cannot disagree. It only reads what
the client declares; it never authenticates anyone.

The rules:

* ``user_id`` may be declared once in the URL query, percent-encoded as UTF-8,
  and once in ``X-User-Id``. When both are present they must name the same user.
* ``X-User-Id`` carries ASCII only, without its optional whitespace. HTTP stacks
  decode other bytes differently: websockets reads them as ASCII with surrogate
  escapes and aiohttp as UTF-8 with surrogate escapes. A non-ASCII user can only
  be declared in the query, where Web and HTTP accept it.
* A user is stripped, normalized to NFC, has no control characters and takes at
  most 256 UTF-8 bytes. TUI and ACP accept ``[A-Za-z0-9_-]+`` only.
* A malformed, repeated or conflicting declaration is refused with HTTP 400. It
  is never ignored or repaired: a dropped declaration would move the connection
  to another user or to the anonymous bucket without anyone noticing.

A connection that declares no user joins one anonymous bucket. Nothing tells two
anonymous clients apart across reconnects, or between the WebSocket and the HTTP
requests of one client. A key made up per connection would therefore scatter one
client over slots whose state is not shared and cut it off from its own sessions.
The price is that anonymous traffic does not spread, and it may be much of the
traffic: the personal Web client declares no user until its page URL has carried
``user_id`` once. A deployment that needs this traffic spread must have its
clients declare a user.

No entry uses this contract yet. Once a runtime adopts it, it replaces the
per-entry readers, which changes behavior on purpose:

* Web ``/ws`` reads the query before the header and accepts any text
  (``WebChannel._resolve_connection_user_id``).
* TUI and ACP read the header before the query and skip a value outside
  ``[A-Za-z0-9_-]+`` (``ProtocolRuntime._extract_ws_user_id``).
* ``e2a.identity.normalize_routing_identity`` keeps the first non-empty value of
  a field, with no length limit. It still normalizes routing metadata; a consumer
  of this contract must take the business user of a connection from the resolved
  identity instead of reading the handshake again.

Refusing an ambiguous declaration follows the AgentOS IAM handshake, which
already refuses repeated or mismatched ``user_id`` declarations.
"""

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
    normalized = unicodedata.normalize('NFC', value)
    if any(unicodedata.category(character) == 'Cc' for character in normalized):
        raise IngressIdentityError()
    normalized = normalized.strip()
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
        headers: Raw header pairs, preserving duplicates; ``X-User-Id`` is ASCII
            once its optional whitespace is removed.
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
    header_user = None
    if header_values:
        raw_header_user = header_values[0].strip(b' \t')
        if not raw_header_user.isascii():
            raise IngressIdentityError()
        header_user = _normalize_user(raw_header_user.decode('ascii'))
    if query_user is not None and header_user is not None and query_user != header_user:
        raise IngressIdentityError()
    user = query_user or header_user
    if user is not None and entry in {'tui', 'acp'} and _PROTOCOL_USER_PATTERN.fullmatch(user) is None:
        raise IngressIdentityError()
    return IngressIdentity(
        contract_id=policy.contract_id, mode='local_compat', domain=policy.domain,
        kind='named' if user is not None else 'anonymous', business_user_id=user,
    )
