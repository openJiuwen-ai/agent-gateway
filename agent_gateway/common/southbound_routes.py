# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Bounded non-secret local route snapshots; no discovery or network side effects."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import unicodedata
from urllib.parse import urlsplit

from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest


MAX_ROUTE_DIRECTORY_BYTES = 16 * 1024
_MAX_TARGETS = 32
_MAX_INTEGER = 2**63 - 1
_TARGET_FIELDS = frozenset({
    'target_id', 'physical_id', 'generation', 'user_id', 'agent_type', 'owner_slot', 'ws_url',
})
_DIRECTORY_FIELDS = frozenset({'schema_version', 'revision', 'identity_policy', 'manifest', 'targets'})


def _bounded_label(value: str) -> bool:
    if type(value) is not str or not value or any(
        character.isspace() or unicodedata.category(character) == 'Cc' for character in value
    ):
        return False
    try:
        return len(value.encode('utf-8')) <= 256
    except UnicodeError:
        return False


def _positive_integer(value: int) -> bool:
    return type(value) is int and 1 <= value <= _MAX_INTEGER


def _identity(user_id: str, policy: LocalIdentityPolicy) -> IngressIdentity:
    return IngressIdentity(policy.contract_id, 'local_compat', policy.domain, 'named', user_id)


def _url_key(value: str) -> tuple[str, int, str]:
    """Reject secrets and normalize only obvious aliases, not remote AS identity."""
    try:
        if (
            type(value) is not str or not value or len(value.encode('utf-8')) > 2048
            or any(character.isspace() or unicodedata.category(character) == 'Cc' for character in value)
            or '?' in value or '#' in value
        ):
            raise ValueError
        parsed = urlsplit(value)
        if (
            parsed.scheme != 'ws' or parsed.hostname not in {'127.0.0.1', '::1'}
            or parsed.username is not None or parsed.password is not None
            or parsed.port is None or not 1 <= parsed.port <= 65535
        ):
            raise ValueError
        return parsed.hostname, parsed.port, parsed.path or '/'
    except (ValueError, UnicodeError):
        raise ValueError('target requires a bounded non-secret loopback ws URL with an explicit port') from None


@dataclass(frozen=True)
class SouthboundTarget:
    """One named user and one trusted physical AS held by a single Worker."""

    target_id: str
    physical_id: str
    generation: int
    user_id: str = field(repr=False)
    agent_type: str
    owner_slot: int
    ws_url: str = field(repr=False)

    def __post_init__(self) -> None:
        if not _bounded_label(self.target_id) or not _bounded_label(self.physical_id):
            raise ValueError('target and physical IDs must be bounded non-whitespace labels')
        if not _positive_integer(self.generation):
            raise ValueError('physical generation must be a bounded positive integer')
        if type(self.owner_slot) is not int or not 0 <= self.owner_slot <= _MAX_INTEGER:
            raise ValueError('owner slot must be a bounded non-negative integer')
        if type(self.agent_type) is not str or self.agent_type != 'jiuwenswarm':
            raise ValueError('static routes only support builtin jiuwenswarm')
        try:
            if type(self.user_id) is not str:
                raise ValueError
            _identity(self.user_id, LocalIdentityPolicy())
        except ValueError:
            raise ValueError('target requires a canonical named local user') from None
        _url_key(self.ws_url)


def _exact_object(value: object, keys: frozenset[str]) -> dict:
    if type(value) is not dict or value.keys() != keys:
        raise ValueError('invalid static route object fields')
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    value: dict = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate static route JSON key')
        value[key] = item
    return value


@dataclass(frozen=True)
class StaticRouteDirectory:
    """Immutable opt-in fixture; revisions never salt the existing user hash."""

    revision: int
    identity_policy: LocalIdentityPolicy
    manifest: WorkerManifest
    targets: tuple[SouthboundTarget, ...]
    schema_version: int = 1

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError('unsupported static route schema')
        if not _positive_integer(self.revision):
            raise ValueError('static route revision must be a bounded positive integer')
        if type(self.identity_policy) is not LocalIdentityPolicy:
            raise ValueError('static routes require an explicit LocalIdentityPolicy')
        if type(self.manifest) is not WorkerManifest:
            raise ValueError('static routes require an explicit WorkerManifest')
        if (
            self.manifest.config_version > _MAX_INTEGER
            or any(slot > _MAX_INTEGER for slot in self.manifest.slots)
        ):
            raise ValueError('static route manifest integers exceed the supported bounds')
        if (
            type(self.targets) is not tuple or not 1 <= len(self.targets) <= _MAX_TARGETS
            or any(type(item) is not SouthboundTarget for item in self.targets)
        ):
            raise ValueError('static routes require one to 32 explicit targets')
        affinity = FixedUserHash(self.manifest, self.identity_policy)
        seen: dict[str, set] = {name: set() for name in ('target_id', 'physical_id', 'user_id', 'ws_url')}
        for item in self.targets:
            if affinity.select(_identity(item.user_id, self.identity_policy)) != item.owner_slot:
                raise ValueError('static route owner does not match fixed user affinity')
            for name in seen:
                key = _url_key(item.ws_url) if name == 'ws_url' else getattr(item, name)
                if key in seen[name]:
                    raise ValueError('static route ownership must be unique')
                seen[name].add(key)
        self.to_json()

    def to_json(self) -> str:
        """Serialize a complete snapshot without credentials, capped in UTF-8 bytes."""
        value = {
            'schema_version': self.schema_version,
            'revision': self.revision,
            'identity_policy': {
                'contract_id': self.identity_policy.contract_id, 'domain': self.identity_policy.domain,
            },
            'manifest': {
                'slots': list(self.manifest.slots), 'config_version': self.manifest.config_version,
                'algorithm': self.manifest.algorithm,
            },
            'targets': [
                {
                    'target_id': item.target_id, 'physical_id': item.physical_id,
                    'generation': item.generation, 'user_id': item.user_id,
                    'agent_type': item.agent_type, 'owner_slot': item.owner_slot, 'ws_url': item.ws_url,
                }
                for item in self.targets
            ],
        }
        try:
            encoded = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
            if len(encoded.encode('utf-8')) > MAX_ROUTE_DIRECTORY_BYTES:
                raise ValueError
            return encoded
        except (ValueError, UnicodeError):
            raise ValueError('static route JSON exceeds the supported UTF-8 budget') from None

    @classmethod
    def from_json(cls, encoded: str) -> StaticRouteDirectory:
        """Reject unknown, missing, duplicate or coerced fields before construction."""
        try:
            if type(encoded) is not str or len(encoded.encode('utf-8')) > MAX_ROUTE_DIRECTORY_BYTES:
                raise ValueError
            value = _exact_object(json.loads(encoded, object_pairs_hook=_unique_object), _DIRECTORY_FIELDS)
            policy = _exact_object(value['identity_policy'], frozenset({'contract_id', 'domain'}))
            manifest = _exact_object(value['manifest'], frozenset({'slots', 'config_version', 'algorithm'}))
            if type(manifest['slots']) is not list or type(value['targets']) is not list:
                raise ValueError
            if not 1 <= len(value['targets']) <= _MAX_TARGETS:
                raise ValueError
            targets = []
            for item in value['targets']:
                item = _exact_object(item, _TARGET_FIELDS)
                targets.append(SouthboundTarget(
                    target_id=item['target_id'], physical_id=item['physical_id'], generation=item['generation'],
                    user_id=item['user_id'], agent_type=item['agent_type'], owner_slot=item['owner_slot'],
                    ws_url=item['ws_url'],
                ))
            return cls(
                schema_version=value['schema_version'], revision=value['revision'],
                identity_policy=LocalIdentityPolicy(contract_id=policy['contract_id'], domain=policy['domain']),
                manifest=WorkerManifest(
                    tuple(manifest['slots']), config_version=manifest['config_version'], algorithm=manifest['algorithm'],
                ),
                targets=tuple(targets),
            )
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise ValueError('invalid static southbound route JSON') from None

    def targets_for_slot(self, slot_id: int) -> tuple[SouthboundTarget, ...]:
        """Return this slot's targets; keep the full manifest in the directory."""
        if type(slot_id) is not int or slot_id not in self.manifest.slots:
            raise ValueError('slot is not in the static route manifest')
        return tuple(item for item in self.targets if item.owner_slot == slot_id)

    def resolve(
        self, identity: IngressIdentity, *, slot_id: int, agent_type: str = 'jiuwenswarm',
    ) -> SouthboundTarget:
        """Resolve an already-bound named identity without a default AS fallback."""
        if type(identity) is not IngressIdentity or identity.kind != 'named':
            raise ValueError('static routing requires an explicit named identity')
        if type(agent_type) is not str or agent_type != 'jiuwenswarm':
            raise ValueError('static routing only supports builtin jiuwenswarm')
        if FixedUserHash(self.manifest, self.identity_policy).select(identity) != slot_id:
            raise ValueError('identity is not owned by this Worker')
        for item in self.targets_for_slot(slot_id):
            if item.user_id == identity.business_user_id:
                return item
        raise ValueError('no static southbound target for the bound identity')
