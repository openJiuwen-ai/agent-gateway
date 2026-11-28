# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A static target may speak only for the requests and sessions it proved.

Every target of a Worker shares the downstream consumers, which index their
state by request and session id. These tests use the guard seam directly so
each selector can be varied on its own; the socket-level behaviour is covered
by ``test_worker_routed_client.py``.
"""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

from e2a.constants import (
    E2A_RESPONSE_KIND_ACP_OUTPUT_REQUEST, E2A_WIRE_LEGACY_AGENT_CHUNK_KEY, E2A_WIRE_SERVER_PUSH_KEY,
)
from e2a.agent_models import AgentResponseChunk
from e2a.gateway_normalize import e2a_from_agent_fields
from e2a.message import ReqMethod
from e2a.models import E2AResponse
from e2a.wire_codec import encode_agent_chunk_for_wire
import pytest

from agent_gateway.application.worker_routed_push import RoutedPushGuard, RoutedRequestBusy
from agent_gateway.application.worker_session_routes import SessionRouteError, SessionRouteIndex
from agent_gateway.common.session_message import (
    SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY,
    SESSION_MESSAGE_ORIGIN,
    SESSION_MESSAGE_OWNER_SCOPE_METADATA_KEY,
)
from tests.unit_tests.gateway.routed_agent_server import legacy_push_wire, push_wire

pytestmark = pytest.mark.asyncio

ALICE = SimpleNamespace(target_id="target-0", generation=1, user_id="alice", physical_id="physical-0")
BOB = SimpleNamespace(target_id="target-1", generation=1, user_id="bob", physical_id="physical-1")


def envelope(user, rid, *, method="chat.send", session_id=None, channel="web", is_stream=False, params=None):
    return e2a_from_agent_fields(
        request_id=rid, channel_id=channel, session_id=session_id, req_method=method, params=params or {},
        is_stream=is_stream, timestamp=0, user_id=user,
    )


def guard(**options) -> RoutedPushGuard:
    value = RoutedPushGuard(session_routes=SessionRouteIndex(), **options)
    value.session_routes.claim(ALICE, "sa")
    value.session_routes.claim(BOB, "sb")
    return value


def dispatched(value: RoutedPushGuard, target, env) -> None:
    key = value.begin(target, env)
    value.mark_dispatched(key)
    return key


async def test_request_push_is_bound_to_the_target_that_dispatched_it():
    value = guard()
    key = dispatched(value, ALICE, envelope("alice", "a", session_id="sa"))

    admitted = value.admit(ALICE, push_wire("a", "sa", event="chat.delta", content="hi"))

    # The owner comes from the connection, never from a wire field.
    assert admitted["user_id"] == "alice" and admitted["session_id"] == "sa"
    assert admitted.target is ALICE and admitted.request_key == key
    assert value.admit(BOB, push_wire("a", "sa", event="chat.delta")) is None
    assert value.owner_of(BOB, "a") is None


@pytest.mark.parametrize("wire", [
    push_wire("a", "sa", event="chat.delta"),
    push_wire("a", "sa", event="chat.delta", user="bob"),
    push_wire("b", "sa", event="chat.delta"),
    push_wire("b", "sb", event="chat.delta", user="alice"),
    push_wire("b", "sb", event="chat.delta", user="mallory"),
    push_wire("unknown", "sb", event="chat.delta"),
    push_wire("a", "sa", event="context.usage"),
    push_wire("unknown", "sa", event="context.usage"),
    push_wire("b", "sb", event="cron.response"),
    push_wire("b", "sb", event="jiuwenmate.delivery.send"),
    push_wire("b", "sb", event="heartbeat.relay", heartbeat={"status": "ok"}),
])
async def test_foreign_or_unsupported_push_never_reaches_shared_consumers(wire):
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb"))

    assert value.admit(BOB, wire) is None


async def test_request_push_may_omit_the_channel_only_for_host_completion():
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb"))

    completion = push_wire("b", "sb", event="chat.request_completed", channel="")
    assert value.admit(BOB, completion)["user_id"] == "bob"
    # A request-bound frame without a session inherits the request's route.
    inherited = value.admit(BOB, push_wire("b", "", event="chat.delta"))
    assert inherited["user_id"] == "bob" and inherited["session_id"] == "sb"
    # Any other channel is another user's consumer.
    assert value.admit(BOB, push_wire("b", "sb", event="chat.delta", channel="acp")) is None
    assert value.admit(BOB, push_wire("b", "sb", event="chat.delta", channel="")) is None


async def test_payload_ids_cannot_disagree_with_the_wire_route():
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb"))

    # The deprecated legacy shape keeps payload fields the typed codec drops.
    assert value.admit(BOB, legacy_push_wire("b", "sb", event="chat.delta", payload_fields={"request_id": "a"})) is None
    assert value.admit(BOB, legacy_push_wire("b", "sb", event="chat.delta", payload_fields={"request_id": "b"})) is not None
    assert value.admit(BOB, legacy_push_wire("b", "sb", event="chat.delta", payload_fields={"session_id": "sa"})) is None
    assert value.admit(BOB, legacy_push_wire("b", "sb", event="chat.delta", payload_fields={"session_id": "sb"})) is not None


@pytest.mark.parametrize("event", ["chat.delta", "context.usage", "trace.updated", "cron.response"])
async def test_no_frame_may_name_another_users_request(event):
    value = guard()
    dispatched(value, ALICE, envelope("alice", "a-rid", session_id="sa"))

    # Bob's AgentServer may only speak about Bob's sessions, never about the
    # request id Alice's consumers keep state for.
    assert value.admit(BOB, push_wire("a-rid", "sb", event=event)) is None
    assert value.admit_cron(BOB, push_wire("a-rid", "sb", event=event)) is None
    assert value.admit(BOB, push_wire("child", "sb", event="chat.delta",
                                      payload_fields={"turn_request_id": "a-rid"})) is None
    assert value.admit(BOB, push_wire("b-own", "sb", event="chat.delta",
                                      payload_fields={"request_id": "a-rid"})) is None
    assert value.admit(BOB, push_wire("", "sb", event="trace.updated")) is not None


@pytest.mark.parametrize("event", [
    "trace.updated", "trajectory.diagnosis.progress", "trajectory.diagnosis.status", "context.compression_state",
    "context.usage", "plan.mode_exited", "session.message.updated",
])
async def test_session_scoped_push_needs_the_session_on_the_sending_target(event):
    value = guard()

    admitted = value.admit(BOB, push_wire("", "sb", event=event, trace_id="t", revision=1))

    assert admitted["user_id"] == "bob" and admitted["session_id"] == "sb"
    assert admitted.request_key is None
    # Alice confirmed "sa"; Bob's AgentServer never did, and nobody owns "other".
    assert value.admit(BOB, push_wire("", "sa", event=event)) is None
    assert value.admit(BOB, push_wire("", "other", event=event)) is None
    assert value.admit(BOB, push_wire("", "", event=event)) is None


@pytest.mark.parametrize("event", ["trace.updated", "trajectory.diagnosis.progress", "trajectory.diagnosis.status"])
async def test_web_session_events_are_not_delivered_to_other_channels(event):
    value = guard()

    assert value.admit(BOB, push_wire("", "sb", event=event, channel="acp")) is None
    assert value.admit(BOB, push_wire("", "sb", event=event, channel="tui")) is None
    assert value.admit(BOB, push_wire("", "sb", event=event, channel="unknown")) is None


async def test_session_push_cannot_disagree_with_a_request_it_names():
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb"))

    assert value.admit(BOB, push_wire("b", "sb", event="context.usage", user="bob")) is not None
    assert value.admit(BOB, push_wire("b", "sa", event="context.usage")) is None


async def test_background_turn_is_admitted_only_for_an_owned_session():
    value = guard()
    def background(sid, origin=SESSION_MESSAGE_ORIGIN, **extra):
        return push_wire("background-b", sid, event="chat.processing_status", turn_request_id="background-b",
                         message_origin=origin, is_processing=True, **extra)

    admitted = value.admit(BOB, background("sb"))

    assert admitted["user_id"] == "bob" and admitted["session_id"] == "sb"
    assert value.admit(BOB, background("sa")) is None
    assert value.admit(BOB, background("unknown")) is None
    # Without the AgentServer's explicit marker an unknown id has no route.
    assert value.admit(BOB, background("sb", origin="client")) is None
    assert value.admit(BOB, background("sb", channel="")) is None
    assert value.admit(BOB, background("sb", channel="acp")) is not None


async def test_turn_scoped_frame_follows_the_request_that_owns_the_turn():
    value = guard()
    key = dispatched(value, BOB, envelope("bob", "turn", session_id="sb"))

    frame = push_wire("child", "sb", event="chat.delta", turn_request_id="turn")

    admitted = value.admit(BOB, frame)

    assert admitted["user_id"] == "bob" and admitted.request_key == key
    assert value.admit(ALICE, frame) is None
    assert value.admit(BOB, push_wire("child", "sa", event="chat.delta", turn_request_id="turn")) is None


@pytest.mark.parametrize("field, other", [
    ("fan_out_targets", [{"channel_id": "web", "session_id": "sa"}]),
    ("fan_out_targets", [{"intent": "all"}]),
    (SESSION_MESSAGE_OWNER_SCOPE_METADATA_KEY, "alice"),
    (SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY, True),
    ("acp_original_session_id", "sa"),
])
async def test_push_metadata_cannot_override_downstream_routing(field, other):
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb"))

    bad = push_wire("b", "sb", event="chat.delta", metadata={field: other})
    good = push_wire("b", "sb", event="chat.delta", metadata={
        SESSION_MESSAGE_OWNER_SCOPE_METADATA_KEY: "bob",
        SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY: False,
    })

    assert value.admit(BOB, bad) is None
    assert value.admit(BOB, good)["user_id"] == "bob"


@pytest.mark.parametrize("payload", [
    {"event_type": "chat.final", "source": "proactive_notification", "content": "global"},
    {"event_type": "chat.final", "cron": {"run_id": "other"}, "content": "global"},
])
async def test_routed_push_never_activates_legacy_broadcast_modes(payload):
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb"))

    assert value.admit(BOB, push_wire("b", "sb", **payload)) is None


def permission(sid: str, rpc_id="tool-b") -> dict:
    return {"jsonrpc": "2.0", "id": rpc_id, "method": "session/request_permission", "params": {"sessionId": sid}}


async def test_acp_approval_with_its_own_wire_id_needs_one_unambiguous_live_prompt():
    value = guard()
    prompt = envelope("alice", "prompt-1", session_id="acp-s", channel="acp", is_stream=True)
    value.session_routes.claim(ALICE, "acp-s")
    key = dispatched(value, ALICE, prompt)
    approval = push_wire("acp_out_independent", "acp-s", event="acp.output_request", channel="acp",
                         jsonrpc=permission("acp-s"))

    admitted = value.admit(ALICE, approval)

    assert admitted["user_id"] == "alice" and admitted.request_key == key

    # Two live prompts leave the association ambiguous.
    dispatched(value, ALICE, replace(prompt, request_id="prompt-2"))
    assert value.admit(ALICE, approval) is None

    # A prompt that was cancelled may still own the approval until its host
    # completion is known; it must not be re-bound to the next prompt.
    value = guard()
    value.session_routes.claim(ALICE, "acp-s")
    old = dispatched(value, ALICE, prompt)
    value.finish(old)
    dispatched(value, ALICE, replace(prompt, request_id="new"))
    assert value.admit(ALICE, approval) is None
    value.mark_host_completed(old)
    assert value.admit(ALICE, approval).request_key.request_id == "new"


async def test_acp_approval_binds_only_to_an_acp_prompt_on_the_sending_target():
    value = guard()
    value.session_routes.claim(ALICE, "acp-s")
    dispatched(value, ALICE, envelope("alice", "prompt-1", session_id="acp-s", channel="acp", is_stream=True))
    approval = push_wire("acp_out_independent", "acp-s", event="acp.output_request", jsonrpc=permission("acp-s"))

    assert value.admit(ALICE, {**approval, "channel": "acp"}) is not None
    assert value.admit(ALICE, approval) is None
    assert value.admit(BOB, {**approval, "channel": "acp", "session_id": "sb",
                             "jsonrpc": permission("sb")}) is None


@pytest.mark.parametrize("rpc", [
    {"jsonrpc": "2.0", "id": "", "method": "session/request_permission", "params": {"sessionId": "sb"}},
    {"jsonrpc": "2.0", "id": "x", "params": {"sessionId": "sb"}},
    {"jsonrpc": "2.0", "id": "x", "method": "session/request_permission", "params": {"sessionId": "sa"}},
    {"jsonrpc": "2.0", "id": {"nested": True}, "method": "session/request_permission"},
    "not-a-request",
])
async def test_malformed_acp_approval_is_dropped(rpc):
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb", channel="acp"))

    wire = push_wire("b", "sb", event="acp.output_request", channel="acp", jsonrpc=rpc)
    assert value.admit(BOB, wire) is None


async def test_real_typed_acp_output_request_is_admitted_with_original_jsonrpc():
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb", channel="acp"))
    body = permission("sb", "typed-permission")
    wire = E2AResponse(
        request_id="b", response_id="typed", channel="acp", session_id="sb",
        response_kind=E2A_RESPONSE_KIND_ACP_OUTPUT_REQUEST, is_final=False, body=body,
        metadata={E2A_WIRE_SERVER_PUSH_KEY: True},
    ).to_dict()

    admitted = value.admit(BOB, wire)

    assert admitted["body"] == body and admitted["user_id"] == "bob"


async def test_typed_frame_cannot_smuggle_routing_selectors_in_either_metadata():
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb"))
    chunk = AgentResponseChunk(
        request_id="b", channel_id="web", is_complete=False,
        payload={"event_type": "chat.delta", "content": "bad"},
        metadata={E2A_WIRE_SERVER_PUSH_KEY: True, "fan_out_targets": [{"intent": "mention", "mention_all": True}]},
    )
    typed = encode_agent_chunk_for_wire(chunk, response_id="typed", sequence=0)
    typed["session_id"] = "sb"
    legacy = push_wire("b", "sb", event="chat.delta", metadata={"fan_out_targets": [{"intent": "all"}]})
    nested = E2AResponse(
        request_id="b", response_id="typed", channel="web", session_id="sb", response_kind="e2a.chunk",
        is_final=False, metadata={E2A_WIRE_SERVER_PUSH_KEY: True, E2A_WIRE_LEGACY_AGENT_CHUNK_KEY: legacy},
    ).to_dict()

    assert value.admit(BOB, typed) is None
    assert value.admit(BOB, legacy) is None
    # A legacy blob nested in typed metadata cannot carry selectors either:
    # the codec falls back to it and exposes the same metadata.
    assert value.admit(BOB, nested) is None
    plain = E2AResponse(
        request_id="b", response_id="typed", channel="web", session_id="sb", response_kind="e2a.chunk",
        is_final=False, body={"event_type": "chat.delta", "content": "ok"},
        metadata={E2A_WIRE_SERVER_PUSH_KEY: True},
    ).to_dict()
    assert value.admit(BOB, plain)["user_id"] == "bob"


@pytest.mark.parametrize("metadata", [[], [["fan_out_targets", [{"intent": "all"}]]], "x", 5, True])
async def test_wire_metadata_must_be_an_object(metadata):
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb"))
    wire = push_wire("b", "sb", event="chat.delta")
    wire["metadata"] = metadata

    assert value.admit(BOB, wire) is None


@pytest.mark.parametrize("field, value_", [("request_id", {"nested": 1}), ("user_id", 5), ("channel", 7)])
async def test_malformed_routing_fields_are_dropped(field, value_):
    value = guard()
    dispatched(value, BOB, envelope("bob", "b", session_id="sb"))
    wire = push_wire("b", "sb", event="chat.delta")
    wire[field] = value_

    assert value.admit(BOB, wire) is None


@pytest.mark.parametrize("wire", ["text", 5, None, {}, {"payload": "x"}])
async def test_unparseable_frames_are_dropped(wire):
    value = guard()
    assert value.admit(BOB, wire) is None


async def test_request_ids_are_unique_across_every_target():
    value = guard()
    dispatched(value, ALICE, envelope("alice", "shared-id", session_id="sa"))

    with pytest.raises(ValueError, match="unused request ID"):
        value.begin(BOB, envelope("bob", "shared-id", session_id="sb"))
    with pytest.raises(ValueError, match="requires a request ID"):
        value.begin(BOB, envelope("bob", "  "))


async def test_a_session_confirmed_for_one_target_is_refused_for_another():
    value = guard()

    with pytest.raises(SessionRouteError, match="another static target"):
        value.begin(BOB, envelope("bob", "b", session_id="sa"))
    assert value.admit(BOB, push_wire("b", "sa", event="chat.delta")) is None
    assert value.begin(BOB, envelope("bob", "b", session_id="sb")) is not None


async def test_generation_change_invalidates_the_confirmed_route():
    value = guard()
    restarted = SimpleNamespace(**{**vars(ALICE), "generation": 2})

    with pytest.raises(SessionRouteError, match="another instance generation"):
        value.begin(restarted, envelope("alice", "a", session_id="sa"))
    assert value.session_routes.owner("sa") == ("target-0", 1)
    # Only the restarted target's own answer re-establishes the route.
    value.observe_response(restarted, envelope("alice", "meta", method="session.get_metadata", session_id="sa"),
                           SimpleNamespace(ok=True, payload={"result": {"session_id": "sa"}}))
    assert value.session_routes.owner("sa") == ("target-0", 2)
    assert value.begin(restarted, envelope("alice", "a", session_id="sa")) is not None


async def test_an_undispatched_request_leaves_no_protected_id():
    value = guard()
    key = value.begin(BOB, envelope("bob", "never-sent", session_id="sb"))

    value.finish(key)

    assert value.owner_of(BOB, "never-sent") is None
    assert value.admit(BOB, push_wire("never-sent", "sb", event="chat.delta")) is None
    assert value.begin(BOB, envelope("bob", "never-sent", session_id="sb")) is not None


async def test_cancelled_or_completed_requests_keep_their_scoped_push():
    value = guard()
    cancelled = dispatched(value, ALICE, envelope("alice", "cancelled", session_id="sa", is_stream=True))
    value.finish(cancelled)
    completed = dispatched(value, ALICE, envelope("alice", "done", session_id="sa"))
    value.mark_stream_completed(completed)
    value.finish(completed)

    for rid in ("cancelled", "done"):
        admitted = value.admit(ALICE, push_wire(rid, "sa", event="chat.request_completed"))
        assert admitted["user_id"] == "alice" and admitted["session_id"] == "sa"
    assert value.counts() == (0, 2, 0)


async def test_tombstones_expire_and_free_their_ids():
    now = 10.0
    value = RoutedPushGuard(session_routes=SessionRouteIndex(), clock=lambda: now)
    key = dispatched(value, ALICE, envelope("alice", "old-id", session_id="sa"))
    value.finish(key)

    with pytest.raises(ValueError, match="unused request ID"):
        value.begin(ALICE, envelope("alice", "old-id", session_id="sa"))
    now += 299.0
    assert value.counts() == (0, 1, 0)
    now += 1.0
    assert value.counts() == (0, 0, 0)
    assert value.owner_of(ALICE, "old-id") is None
    assert value.admit(ALICE, push_wire("old-id", "sa", event="chat.delta")) is None
    assert value.begin(ALICE, envelope("alice", "old-id", session_id="sa")) is not None
    value.clear()


async def test_capacity_is_bounded_and_reported():
    value = guard(max_requests=2, max_recent=3)
    first = dispatched(value, ALICE, envelope("alice", "one", session_id="sa"))
    dispatched(value, ALICE, envelope("alice", "two", session_id="sa"))
    with pytest.raises(RoutedRequestBusy, match="active request capacity"):
        value.begin(ALICE, envelope("alice", "three", session_id="sa"))
    value.finish(first)
    # One active and one protected id leave room for exactly one more request.
    assert value.counts() == (1, 1, 1)
    third = dispatched(value, ALICE, envelope("alice", "three", session_id="sa"))
    value.finish(third)
    with pytest.raises(RoutedRequestBusy, match="recent request capacity"):
        value.begin(ALICE, envelope("alice", "four", session_id="sa"))
    assert value.counts() == (1, 2, 2)
    value.clear()
    # The rejection count is a diagnostic and survives a clear.
    assert value.counts()[:2] == (0, 0)


async def test_ingress_check_refuses_used_ids_before_any_state_is_registered():
    value = guard()
    live = value.begin(BOB, envelope("bob", "live", session_id="sb"))
    value.mark_dispatched(live)
    retired = value.begin(BOB, envelope("bob", "retired", session_id="sb"))
    value.mark_dispatched(retired)
    value.finish(retired)

    # Another user's frame cannot take over either id, and neither can a retry.
    for target in (ALICE, BOB):
        with pytest.raises(ValueError, match="unused request ID"):
            value.check_request(target, "live")
        with pytest.raises(ValueError, match="unused request ID"):
            value.check_request(target, "retired")
    assert value.check_request(BOB, "fresh").request_id == "fresh"


@pytest.mark.parametrize("options", [
    {"max_requests": 0}, {"max_requests": True}, {"max_recent": 1}, {"max_recent": 2.0},
])
async def test_capacity_options_are_validated(options):
    with pytest.raises(ValueError, match="capacity"):
        guard(**options)


async def test_only_successful_session_answers_claim_a_route():
    value = guard()
    routes = value.session_routes

    value.observe_response(ALICE, envelope("alice", "create", method="session.create"),
                           SimpleNamespace(ok=True, payload={"result": {"session_id": "created"}}))
    value.observe_response(ALICE, envelope("alice", "fork", method="session.fork", session_id="sa"),
                           SimpleNamespace(ok=True, payload={"session_id": "forked"}))
    value.observe_response(BOB, envelope("bob", "failed", method="session.create"),
                           SimpleNamespace(ok=False, payload={"result": {"session_id": "nope"}}))
    value.observe_response(BOB, envelope("bob", "empty", method="session.create"),
                           SimpleNamespace(ok=True, payload={"result": {}}))
    value.observe_response(BOB, envelope("bob", "other", method="session.get_metadata", session_id="sb"),
                           SimpleNamespace(ok=True, payload={"result": {"session_id": "someone-elses"}}))

    assert routes.owner("created") == ("target-0", 1) and routes.owner("forked") == ("target-0", 1)
    assert routes.owner("nope") is None and routes.owner("someone-elses") is None
    with pytest.raises(SessionRouteError, match="another static target"):
        value.observe_response(BOB, envelope("bob", "steal", method="session.create"),
                               SimpleNamespace(ok=True, payload={"result": {"session_id": "created"}}))


async def test_session_answers_are_read_in_the_shape_the_agentserver_returns():
    # The AgentServer names a session at the top level of its payload, which
    # is where the single-process handlers read it too.
    value = guard()
    routes = value.session_routes

    value.observe_response(ALICE, envelope("alice", "create", method="session.create"),
                           SimpleNamespace(ok=True, payload={"session_id": "created-flat"}))
    assert routes.owner("created-flat") == ("target-0", 1)
    # A failed create never claims, whatever it names.
    value.observe_response(BOB, envelope("bob", "failed", method="session.create"),
                           SimpleNamespace(ok=False, payload={"session_id": "not-created"}))
    assert routes.owner("not-created") is None


async def test_cron_answers_are_admitted_only_through_their_own_path():
    value = guard()
    dispatched(value, ALICE, envelope("alice", "a-rid", session_id="sa"))
    retired = dispatched(value, BOB, envelope("bob", "b", session_id="sb"))
    value.finish(retired)

    def cron(rid, sid, user=None, **metadata):
        return push_wire(rid, sid, event="cron.response", channel="", user=user, command_id="job-1",
                         metadata=metadata)

    # The shared consumers never see a Cron answer through the ordinary path.
    assert value.admit(BOB, cron("b", "sb")) is None
    assert value.admit_cron(BOB, push_wire("b", "sb", event="chat.delta")) is None
    # Another user's request, an unconfirmed session and a disagreeing wire
    # user leave no authoritative route from Bob's AgentServer.
    assert value.admit_cron(BOB, cron("a-rid", "sa")) is None
    assert value.admit_cron(BOB, cron("", "unknown")) is None
    assert value.admit_cron(BOB, cron("b", "sb", user="alice")) is None
    assert value.admit_cron(BOB, cron("b", "sb", _jiuwenswarm_cron_owner_user_id="alice")) is None
    assert value.admit_cron(ALICE, cron("", "sb")) is None

    # The chat request that asked for the tool call names the route.
    admitted = value.admit_cron(BOB, cron("b", "sb", _jiuwenswarm_cron_owner_user_id="bob"))

    assert admitted["user_id"] == "bob" and admitted["session_id"] == "sb"
    assert admitted["metadata"]["_jiuwenswarm_cron_owner_user_id"] == "bob"
    assert admitted.request_key == retired
    # A missing wire owner is completed from the connection, never inferred.
    assert value.admit_cron(BOB, cron("b", "sb"))["metadata"]["_jiuwenswarm_cron_owner_user_id"] == "bob"
    # A session this target confirmed is enough once no request is named.
    session_only = value.admit_cron(BOB, cron("", "sb"))
    assert session_only["user_id"] == "bob" and session_only.request_key is None
    unknown = value.admit_cron(BOB, cron("never-sent", "sb"))
    assert unknown["user_id"] == "bob" and unknown.request_key is None


async def test_retired_ids_schedule_their_own_expiry():
    value = RoutedPushGuard(session_routes=SessionRouteIndex())
    value.session_routes.claim(ALICE, "sa")
    try:
        assert value._expiry_handle is None
        value.finish(dispatched(value, ALICE, envelope("alice", "done", session_id="sa")))
        assert value._expiry_handle is not None and not value._expiry_handle.when() == 0
    finally:
        value.clear()
    assert value._expiry_handle is None
