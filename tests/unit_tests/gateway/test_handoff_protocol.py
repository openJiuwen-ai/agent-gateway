# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Bounded connection offers; no business data in management frames or repr."""

import json
import struct
from dataclasses import replace

import pytest

from agent_gateway.transport.handoff_protocol import (
    HandoffAck,
    HandoffIdentity,
    HandoffOffer,
    HandoffProtocolError,
    decode_frame,
    encode_frame,
)


def offer(prefix: bytes = b'GET /ws HTTP/1.1\r\n\r\nfirst-frame') -> HandoffOffer:
    return HandoffOffer(HandoffIdentity('test-group', 0, 1), '0:1', 'web', prefix)


def raw_frame(metadata: dict, prefix: bytes = b'') -> bytes:
    header = json.dumps(metadata, separators=(',', ':')).encode()
    payload = struct.pack('>I', len(header)) + header + prefix
    return struct.pack('>I', len(payload)) + payload


def test_offer_roundtrip_preserves_binary_prefix() -> None:
    message = offer(bytes(range(256)))
    assert decode_frame(encode_frame(message)) == message


@pytest.mark.parametrize('status,error', [('ADOPTED', None), ('REJECTED', 'UNKNOWN_RUNTIME')])
def test_ack_roundtrip(status: str, error: str | None) -> None:
    message = HandoffAck(offer().identity, '0:1', status, error)
    assert decode_frame(encode_frame(message)) == message


def test_maximum_prefix_is_bounded_and_binary() -> None:
    assert decode_frame(encode_frame(offer(b'x' * 49152))).prefix == b'x' * 49152
    with pytest.raises(HandoffProtocolError):
        encode_frame(offer(b'x' * 49153))


def test_secret_prefix_is_not_in_repr_or_rejection() -> None:
    secret = b'Bearer secret-do-not-print'
    assert secret.decode() not in repr(offer(secret))
    with pytest.raises(HandoffProtocolError) as error:
        decode_frame(secret)
    assert secret.decode() not in str(error.value)


@pytest.mark.parametrize('field,value', [
    ('handoff_id', '0:0'), ('handoff_id', '1:1'), ('handoff_id', '0:01'),
    ('runtime_id', ''), ('runtime_id', '../web'), ('runtime_id', 'x' * 65),
    ('prefix', 'not-bytes'),
])
def test_invalid_offer_fields(field: str, value: object) -> None:
    with pytest.raises(HandoffProtocolError):
        encode_frame(replace(offer(), **{field: value}))


@pytest.mark.parametrize('identity', [
    HandoffIdentity('', 0, 1), HandoffIdentity('x' * 257, 0, 1),
    HandoffIdentity('group', True, 1), HandoffIdentity('group', -1, 1),
    HandoffIdentity('group', 0, 0), HandoffIdentity('group', 0, True),
])
def test_invalid_identity(identity: HandoffIdentity) -> None:
    with pytest.raises(HandoffProtocolError):
        encode_frame(replace(offer(), identity=identity))


@pytest.mark.parametrize('status,error', [
    ('ADOPTED', 'ERROR'), ('REJECTED', None), ('REJECTED', 'contains secret!'), ('READY', None),
])
def test_invalid_ack_semantics(status: str, error: str | None) -> None:
    with pytest.raises(HandoffProtocolError):
        encode_frame(HandoffAck(offer().identity, '0:1', status, error))


@pytest.mark.parametrize('change', [
    {'version': 2}, {'version': True}, {'unknown': 1}, {'type': 'STOP'},
    {'prefix_length': True}, {'prefix_length': 100}, {'slot_id': True},
])
def test_wire_metadata_is_strict(change: dict) -> None:
    metadata = {
        'version': 1, 'type': 'OFFER', 'group_id': 'test-group', 'slot_id': 0,
        'generation': 1, 'handoff_id': '0:1', 'runtime_id': 'web', 'prefix_length': 0,
    }
    metadata.update(change)
    with pytest.raises(HandoffProtocolError):
        decode_frame(raw_frame(metadata))


def test_duplicate_json_key_rejected() -> None:
    header = b'{"version":1,"version":1}'
    payload = struct.pack('>I', len(header)) + header
    with pytest.raises(HandoffProtocolError):
        decode_frame(struct.pack('>I', len(payload)) + payload)


@pytest.mark.parametrize('frame', [b'', b'\0' * 8, b'\xff' * 8, b'\0\0\0\x04\0\0\x20\0'])
def test_truncated_or_overlimit_frame_rejected(frame: bytes) -> None:
    with pytest.raises(HandoffProtocolError):
        decode_frame(frame)


@pytest.mark.parametrize('field,value', [
    ('type', []), ('type', {}), ('status', []), ('status', {}),
    ('error_code', []), ('handoff_id', []), ('group_id', []), ('runtime_id', []),
])
def test_malformed_container_fields_are_protocol_errors(field: str, value: object) -> None:
    if field in {'status', 'error_code'}:
        message = HandoffAck(offer().identity, '0:1', 'REJECTED', 'ERROR')
    else:
        message = offer(b'')
    frame = encode_frame(message)
    size = int.from_bytes(frame[4:8], 'big')
    metadata = json.loads(frame[8:8 + size])
    metadata[field] = value
    with pytest.raises(HandoffProtocolError):
        decode_frame(raw_frame(metadata))
