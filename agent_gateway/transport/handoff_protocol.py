# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Bounded private connection offers and acknowledgements, not business RPC."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import re
import struct

VERSION = 1
FRAME_LIMIT = 65536
METADATA_LIMIT = 4096
PREFIX_LIMIT = 49152
_COMMON = frozenset({'version', 'type', 'group_id', 'slot_id', 'generation', 'handoff_id'})
_NAME = re.compile(r'[A-Za-z][A-Za-z0-9_.-]{0,63}')


class HandoffProtocolError(ValueError):
    """Invalid private frame; errors deliberately omit untrusted payloads."""


@dataclass(frozen=True)
class HandoffIdentity:
    group_id: str
    slot_id: int
    generation: int


@dataclass(frozen=True)
class HandoffOffer:
    identity: HandoffIdentity
    handoff_id: str
    runtime_id: str
    prefix: bytes = field(repr=False)


@dataclass(frozen=True)
class HandoffAck:
    identity: HandoffIdentity
    handoff_id: str
    status: str
    error_code: str | None = None


def sequence_for(identity: HandoffIdentity, handoff_id: str) -> int:
    """Use an increasing per-slot sequence, bounded without an unbounded seen-ID set."""
    if not isinstance(identity, HandoffIdentity):
        raise HandoffProtocolError('invalid target identity')
    if not isinstance(identity.group_id, str) or not 1 <= len(identity.group_id) <= 256:
        raise HandoffProtocolError('invalid group identity')
    if type(identity.slot_id) is not int or not 0 <= identity.slot_id < 2**31:
        raise HandoffProtocolError('invalid slot identity')
    if type(identity.generation) is not int or not 1 <= identity.generation < 2**63:
        raise HandoffProtocolError('invalid generation identity')
    if not isinstance(handoff_id, str) or len(handoff_id) > 32:
        raise HandoffProtocolError('invalid handoff identity')
    slot, separator, sequence = handoff_id.partition(':')
    if (
        separator != ':' or slot != str(identity.slot_id) or not sequence.isascii()
        or not sequence.isdecimal() or sequence.startswith('0') or len(sequence) > 19
    ):
        raise HandoffProtocolError('invalid handoff identity')
    value = int(sequence)
    if value >= 2**63:
        raise HandoffProtocolError('invalid handoff sequence')
    return value


def _metadata(message: HandoffOffer | HandoffAck) -> dict:
    if not isinstance(message, (HandoffOffer, HandoffAck)):
        raise HandoffProtocolError('invalid frame type')
    sequence_for(message.identity, message.handoff_id)
    identity = message.identity
    result = {
        'version': VERSION, 'type': 'OFFER' if isinstance(message, HandoffOffer) else 'ACK',
        'group_id': identity.group_id, 'slot_id': identity.slot_id,
        'generation': identity.generation, 'handoff_id': message.handoff_id,
    }
    if isinstance(message, HandoffOffer):
        if not isinstance(message.runtime_id, str) or not _NAME.fullmatch(message.runtime_id):
            raise HandoffProtocolError('invalid runtime identifier')
        if not isinstance(message.prefix, bytes) or len(message.prefix) > PREFIX_LIMIT:
            raise HandoffProtocolError('invalid or oversized connection prefix')
        result.update(runtime_id=message.runtime_id, prefix_length=len(message.prefix))
    else:
        if not isinstance(message.status, str) or message.status not in {'ADOPTED', 'REJECTED'}:
            raise HandoffProtocolError('invalid acknowledgement status')
        if message.status == 'ADOPTED' and message.error_code is not None:
            raise HandoffProtocolError('adopted acknowledgement cannot contain an error')
        if message.status == 'REJECTED' and (
            not isinstance(message.error_code, str) or not _NAME.fullmatch(message.error_code)
        ):
            raise HandoffProtocolError('rejected acknowledgement requires an error code')
        result.update(status=message.status, error_code=message.error_code)
    return result


def encode_frame(message: HandoffOffer | HandoffAck) -> bytes:
    """Encode length, metadata length, UTF-8 JSON, and the unchanged binary prefix."""
    metadata = _metadata(message)
    encoded = json.dumps(metadata, ensure_ascii=True, separators=(',', ':')).encode('utf-8')
    if len(encoded) > METADATA_LIMIT:
        raise HandoffProtocolError('metadata exceeds frame limit')
    prefix = message.prefix if isinstance(message, HandoffOffer) else b''
    payload = struct.pack('>I', len(encoded)) + encoded + prefix
    if len(payload) + 4 > FRAME_LIMIT:
        raise HandoffProtocolError('connection frame exceeds limit')
    return struct.pack('>I', len(payload)) + payload


def _unique_object(pairs: list[tuple[str, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise HandoffProtocolError('duplicate metadata field')
        result[key] = value
    return result


def decode_frame(frame: bytes) -> HandoffOffer | HandoffAck:
    """Strictly decode a complete frame without retaining or displaying raw secrets."""
    if not isinstance(frame, bytes) or not 8 <= len(frame) <= FRAME_LIMIT:
        raise HandoffProtocolError('invalid frame size')
    if int.from_bytes(frame[:4], 'big') != len(frame) - 4:
        raise HandoffProtocolError('frame length mismatch')
    metadata_length = int.from_bytes(frame[4:8], 'big')
    if not 1 <= metadata_length <= METADATA_LIMIT or metadata_length > len(frame) - 8:
        raise HandoffProtocolError('invalid metadata length')
    try:
        metadata = json.loads(frame[8:8 + metadata_length], object_pairs_hook=_unique_object)
    except (ValueError, UnicodeError, RecursionError):
        raise HandoffProtocolError('invalid frame metadata') from None
    if not isinstance(metadata, dict):
        raise HandoffProtocolError('metadata must be an object')
    kind = metadata.get('type')
    if not isinstance(kind, str) or kind not in {'OFFER', 'ACK'}:
        raise HandoffProtocolError('unknown frame type')
    fields = {'runtime_id', 'prefix_length'} if kind == 'OFFER' else {'status', 'error_code'}
    if metadata.keys() != _COMMON | fields:
        raise HandoffProtocolError('unexpected metadata fields')
    if type(metadata['version']) is not int or metadata['version'] != VERSION:
        raise HandoffProtocolError('unsupported frame version')
    identity = HandoffIdentity(metadata['group_id'], metadata['slot_id'], metadata['generation'])
    prefix = frame[8 + metadata_length:]
    if kind == 'OFFER':
        length = metadata['prefix_length']
        if type(length) is not int or length != len(prefix):
            raise HandoffProtocolError('prefix length mismatch')
        message = HandoffOffer(identity, metadata['handoff_id'], metadata['runtime_id'], prefix)
    else:
        if prefix:
            raise HandoffProtocolError('acknowledgement contains trailing data')
        message = HandoffAck(identity, metadata['handoff_id'], metadata['status'], metadata['error_code'])
    _metadata(message)
    return message
