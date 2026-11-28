# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A static Worker refuses a frame before any handler keeps state for it."""

import json
from types import SimpleNamespace

import pytest
from e2a.gateway_normalize import e2a_from_agent_fields
from e2a.message import ReqMethod

from agent_gateway.application.worker_data import is_data_frame
from agent_gateway.application.worker_routed_client import SessionRouteError, WorkerRoutedClient
from agent_gateway.application.worker_routed_ingress import RoutedFrameGate
from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.southbound_routes import SouthboundTarget, StaticRouteDirectory
from agent_gateway.common.worker_affinity import WorkerManifest


def target(user: str, index: int) -> SouthboundTarget:
    return SouthboundTarget(
        target_id=f"target-{index}", physical_id=f"physical-{index}", generation=1, user_id=user,
        agent_type="jiuwenswarm", owner_slot=0, ws_url=f"ws://127.0.0.1:{2000 + index}/agent",
    )


def directory(*targets: SouthboundTarget) -> StaticRouteDirectory:
    return StaticRouteDirectory(revision=1, identity_policy=LocalIdentityPolicy(), manifest=WorkerManifest((0,)),
                                targets=targets)


def frame(request_id="r1", method="chat.send", **params):
    return {"type": "req", "id": request_id, "method": method, "params": params}


def wire(value) -> str:
    return json.dumps(value)


@pytest.fixture
def gate():
    client = WorkerRoutedClient(directory(target("alice", 0), target("bob", 1)), slot_id=0)
    return RoutedFrameGate(client), client


def test_gate_requires_a_routed_client_and_a_frame_policy():
    with pytest.raises(ValueError, match="routed Worker client"):
        RoutedFrameGate(SimpleNamespace(user_routing=False))
    with pytest.raises(ValueError, match="frame policy"):
        RoutedFrameGate(SimpleNamespace(user_routing=True), base=None)
    with pytest.raises(ValueError, match="frame policy"):
        RoutedFrameGate(SimpleNamespace(user_routing=True), base="nope")


def test_frame_is_admitted_for_the_user_of_its_connection(gate):
    routed, client = gate
    assert routed(wire(frame()), user_id="alice")
    assert routed(wire(frame()), user_id="bob")
    # No target, an empty name or a user of another Worker slot: no route.
    assert not routed(wire(frame()), user_id=None)
    assert not routed(wire(frame()), user_id="")
    assert not routed(wire(frame()), user_id="carol")


def test_admission_records_the_owner_of_the_request_id(gate):
    routed, client = gate
    assert routed(wire(frame("web-1")), user_id="alice")
    assert client.request_owner("web-1") == "alice"
    assert routed(wire(frame("tui-1")), user_id="bob")
    assert client.request_owner("tui-1") == "bob"
    assert client.request_owner("never-admitted") is None


def test_frame_must_agree_with_the_user_of_its_connection(gate):
    routed, _client = gate
    assert routed(wire(frame(user_id="alice")), user_id="alice")
    assert routed(wire(frame(user_id="")), user_id="alice")
    # The connection, never a frame field, names the user.
    assert not routed(wire(frame(user_id="bob")), user_id="alice")
    assert not routed(wire(frame(user_id=7)), user_id="alice")


def test_only_the_builtin_agent_and_data_methods_are_admitted(gate):
    routed, _client = gate
    assert routed(wire(frame(agent_type="jiuwenswarm")), user_id="alice")
    assert routed(wire(frame(agent_type=None)), user_id="alice")
    assert not routed(wire(frame(agent_type="other")), user_id="alice")
    value = frame()
    value["channel_context"] = {"agent_type": "other"}
    assert not routed(wire(value), user_id="alice")
    for unsupported in ("config.set", "session.create", "models.list", "cron.job.list"):
        assert not routed(wire(frame(method=unsupported)), user_id="alice")
    assert not routed(wire(frame(credential_ref="ref")), user_id="alice")


@pytest.mark.parametrize("request_id", [None, "", " padded ", 7, True, ["x"]])
def test_request_id_must_be_an_exact_bounded_string(gate, request_id):
    routed, _client = gate
    assert not routed(wire(frame(request_id)), user_id="alice")


def test_malformed_frames_are_refused(gate):
    routed, _client = gate
    for raw in ("", "not json", "[]", "null", "42", '{"type":"req","id":"a","method":"chat.send","params":7}',
                '{"type":"res","id":"a"}', '{"type":"req","id":"a","method":"chat.send","params":[1]}'):
        assert not routed(raw, user_id="alice")


def test_attachments_are_refused(gate):
    routed, _client = gate
    for key in ("files", "media_items", "attachments", "documents"):
        assert not routed(wire(frame(**{key: ["/etc/passwd"]})), user_id="alice")
    # An empty list carries nothing to import.
    assert routed(wire(frame(files=[])), user_id="alice")
    # Other methods are not the attachment carrier of this trial.
    assert routed(wire(frame(method="history.get", files=["x"])), user_id="alice")


def test_session_of_another_target_is_refused(gate):
    routed, client = gate
    alice, bob = client.owned_targets
    client.session_routes.claim(alice, "alice-session")
    assert routed(wire(frame(session_id="alice-session")), user_id="alice")
    assert not routed(wire(frame(session_id="alice-session")), user_id="bob")
    assert not routed(wire(frame(group_id="alice-session")), user_id="bob")
    assert routed(wire(frame(session_id="never-seen")), user_id="bob")
    assert not routed(wire(frame(session_id=5)), user_id="alice")


def test_request_id_in_use_on_another_target_is_refused(gate):
    routed, client = gate
    alice, bob = client.owned_targets
    # One active request on Alice's target protects its id on every target.
    key = client._push_guard.begin(alice, e2a_from_agent_fields(
        request_id="shared", channel_id="web", req_method=ReqMethod.CHAT_SEND, params={}, is_stream=True,
        timestamp=0, user_id="alice",
    ))
    client._push_guard.mark_dispatched(key)
    try:
        assert not routed(wire(frame("shared")), user_id="bob")
        assert routed(wire(frame("fresh")), user_id="bob")
    finally:
        client._push_guard.finish(key)


def test_acp_session_belongs_to_the_first_user_only(gate):
    routed, client = gate
    initialize = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    assert routed(wire(initialize), acp=True, user_id="alice", path="/acp")
    prompt = {"jsonrpc": "2.0", "id": 2, "method": "session/prompt", "params": {"sessionId": "acp_one"}}
    assert routed(wire(prompt), acp=True, user_id="alice", path="/acp")
    assert not routed(wire(prompt), acp=True, user_id="bob", path="/acp")
    # An ACP frame on a non-ACP path is not an ACP frame.
    assert not routed(wire(prompt), acp=False, user_id="bob", path="/tui")
    with pytest.raises(SessionRouteError):
        client.session_routes.claim_acp_session(client.owned_targets[1], "acp_one")


def test_an_acp_session_of_one_user_is_closed_on_every_socket_of_another(gate):
    routed, client = gate
    prompt = {"jsonrpc": "2.0", "id": 2, "method": "session/prompt", "params": {"sessionId": "acp_bob"}}
    assert routed(wire(prompt), acp=True, user_id="bob", path="/acp")
    # Bob's own TUI socket may still name it; Alice's sockets may not, on any
    # channel, and nothing of her refused frame is kept.
    assert routed(wire(frame("b-1", session_id="acp_bob")), user_id="bob", path="/tui")
    assert not routed(wire(frame("a-1", session_id="acp_bob")), user_id="alice", path="/tui")
    assert not routed(wire(frame("a-2", session_id="acp_bob")), user_id="alice")
    assert client.request_owner("a-1") is None and client.request_owner("a-2") is None
    assert client.session_routes.owner("acp_bob") is None


def test_connection_bound_gates_read_their_socket_per_frame(gate):
    routed, _client = gate
    web = routed.for_user("alice")
    assert web(wire(frame()))
    protocol = SimpleNamespace(_gateway_user_id="bob", path="/acp?x=1")
    tui = routed.for_protocol(protocol)
    assert tui(wire(frame()), acp=True)
    # The user is read when the frame arrives, as the runtime does.
    protocol._gateway_user_id = "carol"
    assert not tui(wire(frame()), acp=True)
    protocol._gateway_user_id = "alice"
    assert tui(wire(frame()), acp=True)
    assert routed.client is gate[1]


def test_worker_frame_policy_still_applies():
    assert is_data_frame(wire(frame()), accounts=False)
    assert not is_data_frame(wire(frame(source="huawei-maas-login")), accounts=False)
    assert is_data_frame(wire(frame(source="huawei-maas-login")), accounts=True)
