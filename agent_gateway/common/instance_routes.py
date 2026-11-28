# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Bounded non-secret configuration for one trusted dynamic-instance leaf."""

from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import re
import unicodedata
from urllib.parse import urlsplit

from agent_gateway.common.ingress_identity import IngressIdentity, LocalIdentityPolicy
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest


MAX_INSTANCE_SPEC_BYTES = 16 * 1024
_MAX_ADMITTED_USERS = 32
_MAX_INTEGER = 2**63 - 1
_MAX_CPU = 65535
_MAX_MEMORY_MIB = 1024 * 1024
_WORKSPACE_NAME_RE = re.compile(r'[^A-Za-z0-9._-]+')
_MAX_PORT = 65535
_SPEC_FIELDS = frozenset({
    'schema_version', 'identity_policy', 'manifest', 'admitted_users',
    'frontend_endpoint', 'namespace', 'workspace_root', 'registry_endpoint',
    'cpu', 'memory', 'agent_port',
})


def _bounded_label(value: object) -> bool:
    return (
        type(value) is str and 0 < len(value) <= 256
        and not any(
            character.isspace() or unicodedata.category(character) == 'Cc' for character in value
        )
    )


def _bounded_integer(value: object, maximum: int) -> bool:
    return type(value) is int and 1 <= value <= maximum


def _authority_url(value: object) -> str:
    try:
        if (
            type(value) is not str or not value or len(value.encode('utf-8')) > 2048
            or any(character.isspace() or unicodedata.category(character) == 'Cc' for character in value)
        ):
            raise ValueError
        parsed = urlsplit(value)
        if (
            parsed.scheme not in ('http', 'https') or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.path not in ('', '/') or parsed.query or parsed.fragment
        ):
            raise ValueError
        if parsed.port is not None and not 1 <= parsed.port <= _MAX_PORT:
            raise ValueError
    except (ValueError, UnicodeError):
        raise ValueError('instance authority must be a bounded HTTP(S) URL without a path') from None
    return value


def _identity(user_id: object, policy: LocalIdentityPolicy) -> IngressIdentity:
    return IngressIdentity(policy.contract_id, 'local_compat', policy.domain, 'named', user_id)


def _exact_object(value: object, keys: frozenset[str]) -> dict:
    if type(value) is not dict or value.keys() != keys:
        raise ValueError('invalid instance provider spec fields')
    return value


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    value: dict = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('duplicate instance provider spec JSON key')
        value[key] = item
    return value


def _reject_json_constant(value: str) -> None:
    raise ValueError(value)


@dataclass(frozen=True)
class InstanceProviderSpec:
    """Private, non-secret configuration for one trusted dynamic-instance leaf."""

    identity_policy: LocalIdentityPolicy
    manifest: WorkerManifest
    admitted_users: tuple[str, ...]
    frontend_endpoint: str
    namespace: str
    workspace_root: str
    registry_endpoint: str = ''
    cpu: int | None = None
    memory: int | None = None
    agent_port: int | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError('unsupported instance provider schema')
        if type(self.identity_policy) is not LocalIdentityPolicy:
            raise ValueError('instance routes require an explicit LocalIdentityPolicy')
        if type(self.manifest) is not WorkerManifest:
            raise ValueError('instance routes require an explicit WorkerManifest')
        if (
            self.manifest.config_version > _MAX_INTEGER
            or any(slot > _MAX_INTEGER for slot in self.manifest.slots)
        ):
            raise ValueError('instance route manifest integers exceed the supported bounds')
        if (
            type(self.admitted_users) is not tuple
            or not 1 <= len(self.admitted_users) <= _MAX_ADMITTED_USERS
            or any(type(user) is not str for user in self.admitted_users)
        ):
            raise ValueError('instance routes require one to 32 admitted users')
        affinity = FixedUserHash(self.manifest, self.identity_policy)
        seen: set[str] = set()
        workspace_names: set[str] = set()
        for user in self.admitted_users:
            try:
                _identity(user, self.identity_policy)
            except ValueError:
                raise ValueError('admitted instance users must be canonical local users') from None
            if user in seen:
                raise ValueError('duplicate admitted instance user')
            seen.add(user)
            affinity.select(_identity(user, self.identity_policy))
            workspace_name = _WORKSPACE_NAME_RE.sub('_', user) or 'default'
            if workspace_name in {'.', '..'}:
                raise ValueError('admitted instance user cannot map to a workspace boundary')
            if workspace_name in workspace_names:
                raise ValueError('admitted instance users cannot share a managed workspace')
            workspace_names.add(workspace_name)
        if not _bounded_label(self.namespace):
            raise ValueError('instance namespace must be a bounded non-whitespace label')
        _authority_url(self.frontend_endpoint)
        if self.registry_endpoint != '':
            _authority_url(self.registry_endpoint)
        if (
            type(self.workspace_root) is not str
            or not self.workspace_root
            or len(self.workspace_root.encode('utf-8')) > 4096
            or '\x00' in self.workspace_root
            or not Path(self.workspace_root).is_absolute()
        ):
            raise ValueError('workspace root must be an absolute managed path')
        if self.cpu is not None and not _bounded_integer(self.cpu, _MAX_CPU):
            raise ValueError('agent CPU must be a bounded positive integer')
        if self.memory is not None and not _bounded_integer(self.memory, _MAX_MEMORY_MIB):
            raise ValueError('agent memory must be a bounded positive integer')
        if self.agent_port is not None and not _bounded_integer(self.agent_port, _MAX_PORT):
            raise ValueError('agent port must be a bounded positive integer')
        self.to_json()

    def to_json(self) -> str:
        """Serialize the complete private configuration without secrets."""
        value = {
            'schema_version': self.schema_version,
            'identity_policy': {
                'contract_id': self.identity_policy.contract_id,
                'domain': self.identity_policy.domain,
            },
            'manifest': {
                'slots': list(self.manifest.slots),
                'config_version': self.manifest.config_version,
                'algorithm': self.manifest.algorithm,
            },
            'admitted_users': list(self.admitted_users),
            'frontend_endpoint': self.frontend_endpoint,
            'namespace': self.namespace,
            'workspace_root': self.workspace_root,
            'registry_endpoint': self.registry_endpoint,
            'cpu': self.cpu,
            'memory': self.memory,
            'agent_port': self.agent_port,
        }
        try:
            encoded = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)
            if len(encoded.encode('utf-8')) > MAX_INSTANCE_SPEC_BYTES:
                raise ValueError
            return encoded
        except (ValueError, UnicodeError):
            raise ValueError('instance provider JSON exceeds the supported UTF-8 budget') from None

    @classmethod
    def from_json(cls, encoded: str) -> InstanceProviderSpec:
        """Reject unknown, missing, duplicate or coerced fields before construction."""
        try:
            if type(encoded) is not str or len(encoded.encode('utf-8')) > MAX_INSTANCE_SPEC_BYTES:
                raise ValueError
            value = _exact_object(
                json.loads(
                    encoded,
                    object_pairs_hook=_unique_object,
                    parse_constant=_reject_json_constant,
                ),
                _SPEC_FIELDS,
            )
            policy = _exact_object(value['identity_policy'], frozenset({'contract_id', 'domain'}))
            manifest = _exact_object(
                value['manifest'], frozenset({'slots', 'config_version', 'algorithm'})
            )
            if type(value['admitted_users']) is not list or type(manifest['slots']) is not list:
                raise ValueError
            return cls(
                identity_policy=LocalIdentityPolicy(
                    contract_id=policy['contract_id'], domain=policy['domain']
                ),
                manifest=WorkerManifest(
                    tuple(manifest['slots']),
                    config_version=manifest['config_version'],
                    algorithm=manifest['algorithm'],
                ),
                admitted_users=tuple(value['admitted_users']),
                frontend_endpoint=value['frontend_endpoint'],
                namespace=value['namespace'],
                workspace_root=value['workspace_root'],
                registry_endpoint=value['registry_endpoint'],
                cpu=value['cpu'],
                memory=value['memory'],
                agent_port=value['agent_port'],
                schema_version=value['schema_version'],
            )
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise ValueError('invalid instance provider spec JSON') from None
