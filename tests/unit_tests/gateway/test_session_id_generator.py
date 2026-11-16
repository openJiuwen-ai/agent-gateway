"""Tests for shared gateway-owned session ID generation."""

import re

from agent_gateway.common.protocol_ids import is_valid_session_id
from agent_gateway.common.session_ids import make_session_id


def test_make_session_id_matches_protocol_contract():
    session_id = make_session_id()

    assert is_valid_session_id(session_id)
    assert re.fullmatch(r"^sess_[0-9a-f]+_[0-9a-f]{6}$", session_id)
