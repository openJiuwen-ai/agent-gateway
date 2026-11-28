# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Every routing rule of the instance push guard, checked without a transport.

The guard is the only thing between a user's own AgentServer and consumers
that several generations share, so each rule is pinned on its own.
"""

from __future__ import annotations

import json
from typing import Any

from e2a.constants import E2A_WIRE_SERVER_PUSH_KEY
from e2a.message import EventType
from e2a.models import E2AEnvelope
import pytest

from agent_gateway.application.worker_instance_push import (
    InstancePushGuard, MAX_ACTIVE_REQUESTS, MAX_PROTECTED_REQUESTS, MAX_SESSIONS, PROTECTION_SECONDS,
)
from agent_gateway.auth.login_credentials import CREDENTIAL_REFRESH_EVENT
from agent_gateway.common.session_message import (
    SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY,
    SESSION_MESSAGE_ORIGIN,
    SESSION_MESSAGE_OWNER_SCOPE_METADATA_KEY,
)
from agent_gateway.extensions.video_duplex.backend.tasks.bridge import EVENT
from agent_gateway.southbound.orchestration.instance_provider import InstanceTarget, OwnedRoute

ALICE = OwnedRoute('owner-alice', 'alice', 'jiuwenswarm', 0)
BOB = OwnedRoute('owner-bob', 'bob', 'jiuwenswarm', 0)


def target(user: str = 'alice', generation: str = 'g1') -> InstanceTarget:
    route = ALICE if user == 'alice' else BOB
    return InstanceTarget(
        route.target_id, f'private:sandbox-{generation}', generation, user, 'jiuwenswarm', 0,
        f'ws://ingress/{generation}/18092',
    )


def envelope(rid: str, *, user: str = 'alice', channel: str = 'web', sid: str | None = None) -> E2AEnvelope:
    return E2AEnvelope(
        request_id=rid, user_id=user, channel=channel, method='chat.send', session_id=sid, params={},
    )


def dispatched(guard: InstancePushGuard, to: InstanceTarget, rid: str, **options: Any) -> str:
    """A request the Worker handed to this generation."""
    options.setdefault('user', to.user_id)
    key = guard.begin(to, envelope(rid, **options))
    guard.dispatched(key)
    return key


def frame(
    rid: str = '', sid: str = '', *, channel: str = 'web', event: str = 'chat.delta',
    payload: dict[str, Any] | None = None, metadata: dict[str, Any] | None = None, **wire: Any,
) -> dict[str, Any]:
    """A server push in the legacy chunk shape the AgentServer sends."""
    frame: dict[str, Any] = {
        'request_id': rid, 'channel_id': channel, 'session_id': sid,
        'metadata': {E2A_WIRE_SERVER_PUSH_KEY: True, **(metadata or {})},
        'payload': {'event_type': event, **(payload or {})}, 'is_complete': False, **wire,
    }
    # Nothing reaches the guard that could not have been on the wire.
    json.dumps(frame)
    return frame


def routed(wire: dict[str, Any], user: str, sid: str) -> dict[str, Any]:
    return {**wire, 'user_id': user, 'session_id': sid}


# --------------------------------------------------------------------------- requests


def test_request_frames_take_the_route_of_the_request_the_worker_dispatched() -> None:
    guard = InstancePushGuard()
    dispatched(guard, target(), 'live', sid='s')
    wire = frame('live', 's')
    assert guard.admit(target(), wire) == routed(wire, 'alice', 's')
    # The Worker's record supplies what the frame leaves empty ...
    assert guard.admit(target(), frame('live')) == routed(frame('live'), 'alice', 's')
    # ... including the channel a host completion does not carry.
    completion = frame('live', 's', channel='', event='chat.request_completed')
    assert guard.admit(target(), completion) == routed(completion, 'alice', 's')
    # ... and overrides the route written on the wire.
    misaddressed = frame('live', 's', user_id='alice')
    assert guard.admit(target(), misaddressed) == routed(misaddressed, 'alice', 's')


def test_nothing_is_admitted_before_the_request_was_dispatched() -> None:
    guard = InstancePushGuard()
    # Reserved by ingress admission; the transport has not seen it yet.
    guard.begin(target(), envelope('live', sid='s'))
    assert guard.admit(target(), frame('live', 's')) is None
    dispatched(guard, target(), 'later', sid='s')
    assert guard.admit(target(), frame('later', 's')) is not None
    # The first request never left: it still lends nothing.
    assert guard.admit(target(), frame('live', 's')) is None


def test_a_finished_request_keeps_its_route_until_its_completion_window_passes() -> None:
    now = [100.0]
    guard = InstancePushGuard(clock=lambda: now[0])
    dispatched(guard, target(), 'live', sid='s')
    guard.finish('live')
    assert guard.counts == (0, 1, 1)
    # A completion push may follow the response.
    assert guard.admit(target(), frame('live', 's')) is not None
    now[0] += PROTECTION_SECONDS + 1
    assert guard.admit(target(), frame('live', 's')) is None
    # The session stays with its generation for the life of the Worker.
    assert guard.counts == (0, 0, 1)
    assert guard.admit(target(), frame('', 's', event='context.usage')) is not None


@pytest.mark.parametrize('wire', [
    frame('live', 'other'),                                       # another session
    frame('live', 's', channel='acp'),                            # another channel
    frame('live', 's', channel=''),                               # no channel, and no host completion
    frame('live', 's', channel='acp', event='chat.request_completed'),
    frame('unknown', 'unknown'),                                  # no request and no session
    frame('unknown', 's'),                                        # a held session lends nothing to a chat frame
    frame('unknown', 's', payload={'turn_request_id': 'nobody'}),  # a turn nothing dispatched
    frame('live', 's', payload={'request_id': 'other'}),          # the payload names another request
    frame('live', 's', user_id='bob'),                            # the wire user contradicts the owner
    frame('live', 's', user_id=['alice']),
    frame('live', 'other', payload={'session_id': 's'}),          # two sessions in one frame
    frame('live', 's', metadata={'fan_out_targets': ['bob']}),
    frame('live', 's', metadata={SESSION_MESSAGE_OWNER_SCOPE_METADATA_KEY: 'bob'}),
    frame('live', 's', metadata={SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY: True}),
    frame('live', 's', metadata={SESSION_MESSAGE_ANONYMOUS_OWNER_METADATA_KEY: 'bob'}),
    frame('live', 's', metadata={'acp_original_session_id': 'foreign'}),
    frame('live', 's', metadata={'acp_original_session_id': 7}),
    {**frame('live', 's'), 'metadata': 'not a mapping'},
    {'request_id': 'live', 'nothing_a_consumer_parses': True},
    'not a frame',
    None,
    7,
])
def test_frames_a_dispatched_request_did_not_earn_are_dropped(wire: Any) -> None:
    guard = InstancePushGuard()
    dispatched(guard, target(), 'live', sid='s')
    assert guard.admit(target(), wire) is None
    # A refused frame leaves no association behind.
    assert guard.counts == (1, 0, 1)


def test_a_frame_naming_two_sessions_is_dropped_whichever_the_target_meant() -> None:
    guard = InstancePushGuard()
    guard.pin(target(), 'a')
    guard.pin(target(), 'b')
    # Both sessions are this generation's own, so either alone is routed:
    # only the disagreement between the wire and the payload can be refused.
    assert guard.admit(target(), frame('', 'a', event='context.usage')) is not None
    assert guard.admit(target(), frame('', 'b', event='context.usage')) is not None
    assert guard.admit(
        target(), frame('', 'a', event='context.usage', payload={'session_id': 'b'}),
    ) is None
    assert guard.admit(
        target(), frame('', '', event='context.usage', payload={'session_id': 'b'}),
    ) is not None
    # A request's frame may not borrow a session this generation also holds.
    dispatched(guard, target(), 'live', sid='a')
    assert guard.admit(target(), frame('live', 'b')) is None
    assert guard.admit(target(), frame('live', 'b', payload={'session_id': 'b'})) is None
    assert guard.admit(target(), frame('live', 'a', payload={'session_id': 'a'})) is not None


def test_a_request_of_one_generation_lends_nothing_to_another() -> None:
    guard = InstancePushGuard()
    dispatched(guard, target(), 'live', sid='s')
    # The same user's replacement generation owns neither the request nor the session.
    assert guard.admit(target(generation='g2'), frame('live', 's')) is None
    assert guard.admit(target(generation='g2'), frame('', 's', event='context.usage')) is None
    # Another user's generation.
    dispatched(guard, target('bob', 'b1'), 'live-bob', sid='s-bob')
    assert guard.admit(target('bob', 'b1'), frame('live', 's')) is None
    assert guard.admit(target('bob', 'b1'), frame('live-bob', 's', turn_request_id='live')) is None
    assert guard.admit(target('bob', 'b1'), frame('x', 's-bob', turn_request_id='live')) is None
    assert guard.admit(target('bob', 'b1'), frame('', 's', event='context.usage')) is None
    # Its own request is routed as its own.
    own = frame('live-bob', 's-bob')
    assert guard.admit(target('bob', 'b1'), own) == routed(own, 'bob', 's-bob')


def test_turn_scoped_frames_belong_to_the_request_that_owns_the_turn() -> None:
    guard = InstancePushGuard()
    dispatched(guard, target(), 'live', sid='s')
    # An interaction inside the turn carries its own wire id.
    inside = frame('tool-1', 's', payload={'turn_request_id': 'live'})
    assert guard.admit(target(), inside) == routed(inside, 'alice', 's')
    # Once seen in the turn, that id is answered without repeating the turn ...
    assert guard.admit(target(), frame('tool-1', 's')) == routed(frame('tool-1', 's'), 'alice', 's')
    # ... only on this generation, and it is not free for a new request meanwhile.
    assert guard.admit(target(generation='g2'), frame('tool-1', 's')) is None
    for route in (ALICE, BOB):
        with pytest.raises(ValueError, match='unused request ID'):
            guard.reserve_request(route, 'tool-1')
    # An id that names another request of the same generation is ambiguous.
    dispatched(guard, target(), 'other', sid='s')
    assert guard.admit(target(), frame('other', 's', payload={'turn_request_id': 'live'})) is None


def test_request_ids_are_unique_across_routes_while_live_or_protected() -> None:
    guard = InstancePushGuard()
    dispatched(guard, target(), 'live', sid='s')
    for route in (ALICE, BOB):
        with pytest.raises(ValueError, match='unused request ID'):
            guard.reserve_request(route, 'live')
        with pytest.raises(ValueError, match='request ID'):
            guard.reserve_request(route, '  ')
        with pytest.raises(ValueError, match='request ID'):
            guard.reserve_request(route, None)
    guard.finish('live')
    with pytest.raises(ValueError, match='unused request ID'):
        guard.reserve_request(BOB, 'live')


def test_a_session_belongs_to_one_route_and_to_its_first_generation() -> None:
    guard = InstancePushGuard()
    guard.claim_session(ALICE, 's')
    with pytest.raises(ValueError, match='another instance route'):
        guard.claim_session(BOB, 's')
    with pytest.raises(ValueError, match='another instance route'):
        guard.pin(target('bob', 'b1'), 's')
    guard.pin(target(), 's')
    guard.pin(target(), 's')
    with pytest.raises(ValueError, match='another instance generation'):
        guard.pin(target(generation='g2'), 's')
    # An instance may not take, by naming it in a response, a session another
    # route reserved before any generation served it.
    guard.claim_session(ALICE, 'reserved')
    guard.claim_session(BOB, 'bobs')
    with pytest.raises(ValueError, match='another instance route'):
        guard.pin(target('bob', 'b1'), 'reserved')
    assert guard.admit(target('bob', 'b1'), frame('', 'reserved', event='context.usage')) is None
    assert guard.admit(target('bob', 'b1'), frame('bobs-req', 'bobs')) is None
    # The reserving route is the one that serves it.
    guard.pin(target(), 'reserved')
    assert guard.admit(target(), frame('', 'reserved', event='context.usage')) is not None


# --------------------------------------------------------------------------- what consumers do with a frame


@pytest.mark.parametrize('wire', [
    # The web channel sends these two to every connected client.
    frame('live', 's', event=EventType.CHAT_FINAL.value, payload={'source': 'proactive_notification'}),
    frame('live', 's', event=EventType.CHAT_FINAL.value,
          payload={'cron': {'run_id': 'r1', 'user_id': 'bob'}, 'user_id': 'bob'}),
    # The message handler broadcasts these without looking at the route.
    frame('live', 's', event='models.updated'),
    frame('live', 's', event='gateway.push_terminated', payload={'reason': 'agentserver gone'}),
    # ... or acts on them across users.
    frame('live', 's', event='jiuwenmate.group.inspect', payload={'chat_id': 'c', 'user_id': 'bob'}),
    frame('live', 's', event='jiuwenmate.thinking_notice', payload={'text': 'x'}),
    frame('live', 's', event='jiuwenmate.delivery.send', payload={'file': {'path': 'p'}}),
    frame('live', 's', event=CREDENTIAL_REFRESH_EVENT, payload={'credential_ref': 'bob'}),
    frame('live', 's', event=EVENT, payload={'command': 'stop'}),
    frame('live', 's', event='cron.response', payload={'result': {}}),
    frame('live', 's', event='keepalive'),
    # Case and spacing do not turn a global event into a chat event.
    frame('live', 's', event=' Models.Updated '),
    frame('live', 's', event='GATEWAY.PUSH_TERMINATED'),
])
def test_frames_the_consumers_broadcast_or_act_on_globally_never_pass(wire: Any) -> None:
    guard = InstancePushGuard()
    dispatched(guard, target(), 'live', sid='s')
    dispatched(guard, target(), 'live-acp', channel='acp', sid='s-acp')
    # The sending generation holds a request and a session on both channels:
    # only the rule on the event itself stands between the frame and the consumers.
    for rid, channel, sid in (('live', 'web', 's'), ('live-acp', 'acp', 's-acp'),
                              ('live', 'web', ''), ('live-acp', 'acp', '')):
        candidate = {**wire, 'request_id': rid, 'channel_id': channel, 'session_id': sid}
        candidate['payload'] = {**wire['payload'], 'session_id': sid}
        assert guard.admit(target(), candidate) is None
        # ... with only a held session and no request at all.
        assert guard.admit(target(), {**candidate, 'request_id': ''}) is None
    assert guard.counts == (2, 0, 2)


def test_session_events_need_a_session_this_generation_served() -> None:
    guard = InstancePushGuard()
    usage = frame('', 's', event='context.usage')
    assert guard.admit(target(), usage) is None
    # Reserved by ingress admission; no generation has served it yet.
    guard.claim_session(ALICE, 's')
    assert guard.admit(target(), usage) is None
    guard.pin(target(), 's')
    assert guard.admit(target(), usage) == routed(usage, 'alice', 's')
    assert guard.admit(target(generation='g2'), usage) is None
    assert guard.admit(target('bob', 'b1'), usage) is None
    assert guard.admit(target(), frame('', '', event='context.usage')) is None
    assert guard.admit(target(), frame('', 's', event='context.usage', channel='cron')) is None
    # The three events the web client alone handles are web frames only.
    for event in ('trace.updated', 'trajectory.diagnosis.progress', 'trajectory.diagnosis.status'):
        assert guard.admit(target(), frame('', 's', event=event, channel='tui')) is None
        assert guard.admit(target(), frame('', 's', event=event, channel='acp')) is None
        assert guard.admit(target(), frame('', 's', event=event))['session_id'] == 's'
    for event in ('context.compression_state', 'context.usage', 'plan.mode_exited', 'session.message.updated'):
        for channel in ('web', 'tui', 'acp'):
            assert guard.admit(target(), frame('', 's', event=event, channel=channel))['session_id'] == 's'
    # Naming a request of another session is not a session event of this one.
    dispatched(guard, target(), 'live', sid='t')
    assert guard.admit(target(), frame('live', 's', event='context.usage')) is None
    # Naming the request's own session keeps it scoped to that request.
    assert guard.admit(target(), frame('live', 't', event='context.usage'))['session_id'] == 't'


def test_background_turns_need_their_marker_and_a_held_session() -> None:
    def background(
        turn: str, *, sid: str = 's', channel: str = 'web', marker: bool = True,
    ) -> dict[str, Any]:
        # A fresh turn per frame: an admitted one would vouch for the next.
        payload = {'turn_request_id': turn}
        if marker:
            payload['message_origin'] = SESSION_MESSAGE_ORIGIN
        return frame('', sid, channel=channel, payload=payload)

    guard = InstancePushGuard()
    # A turn the Worker never dispatched lends nothing by itself, marked or not.
    assert guard.admit(target(), background('bg-1')) is None
    guard.pin(target(), 's')
    assert guard.admit(target(), background('bg-2', marker=False)) is None
    marked = background('bg-3')
    assert guard.admit(target(), marked) == routed(marked, 'alice', 's')
    # The marker lends a route to no other session ...
    assert guard.admit(target(), background('bg-4', sid='other')) is None
    # ... on no channel the consumers route ...
    assert guard.admit(target(), background('bg-5', channel='cron')) is None
    # ... and to no other generation, of this user or another.
    assert guard.admit(target('bob', 'b1'), background('bg-6')) is None
    assert guard.admit(target(generation='g2'), background('bg-7')) is None
    # A frame of the admitted turn needs no marker once the turn is known.
    later = frame('', 's', payload={'turn_request_id': 'bg-3'})
    assert guard.admit(target(), later) == routed(later, 'alice', 's')
    # The marker does not make a global event a turn frame.
    assert guard.admit(target(), frame(
        '', 's', event='models.updated', payload={
            'turn_request_id': 'bg-8', 'message_origin': SESSION_MESSAGE_ORIGIN,
        },
    )) is None


def test_acp_interactions_are_bound_to_one_session_of_one_generation() -> None:
    guard = InstancePushGuard()
    dispatched(guard, target(), 'acp-req', channel='acp', sid='s')
    dispatched(guard, target(), 'acp-req-2', channel='acp', sid='t')

    def output(rpc: dict[str, Any], *, rid: str = 'acp-req', sid: str = 's', channel: str = 'acp') -> dict[str, Any]:
        return frame(rid, sid, channel=channel, event='acp.output_request', payload={'jsonrpc': rpc})

    first = output({'id': 'rpc-1', 'method': 'acp/request'})
    assert guard.admit(target(), first) == routed(first, 'alice', 's')
    # The same interaction id cannot address another session of this generation.
    assert guard.admit(target(), output({'id': 'rpc-1', 'method': 'acp/request'}, rid='acp-req-2', sid='t')) is None
    # Shapes the consumers cannot route.
    for rpc in (
        {'id': 'rpc-3', 'method': 'acp/request', 'params': {'sessionId': 't'}},
        {'id': '', 'method': 'acp/request'},
        {'id': {'nested': 1}, 'method': 'acp/request'},
        {'id': True, 'method': 'acp/request'},
        {'id': 'rpc-4', 'method': 5},
        {'id': 'rpc-5'},
    ):
        assert guard.admit(target(), output(rpc)) is None
    assert guard.admit(target(), output({'id': 'rpc-6', 'method': 'acp/request'}, channel='web')) is None
    assert guard.admit(
        target(), frame('acp-req', 's', channel='acp', event='acp.output_request', payload={'jsonrpc': 'x'}),
    ) is None
    # An interaction id the Worker owns no request for.
    orphan = output({'id': 'rpc-7', 'method': 'acp/request'}, rid='nobody')
    assert guard.admit(target(), orphan) is None
    # An integer id is an id too.
    assert guard.admit(target(), output({'id': 7, 'method': 'acp/request'}))['session_id'] == 's'


def test_the_acp_session_alias_must_be_a_session_this_generation_holds() -> None:
    guard = InstancePushGuard()
    dispatched(guard, target(), 'live', channel='acp', sid='s')
    alias = {'acp_original_session_id': 'raw'}
    assert guard.admit(target(), frame('live', 's', channel='acp', metadata=alias)) is None
    guard.pin(target(), 'raw')
    assert guard.admit(target(), frame('live', 's', channel='acp', metadata=alias)) is not None
    # Held by another generation: the alias would re-address the frame there.
    assert guard.admit(target(generation='g2'), frame('live', 's', channel='acp', metadata=alias)) is None


# --------------------------------------------------------------------------- bounds


def test_capacity_fails_closed() -> None:
    guard = InstancePushGuard()
    for index in range(MAX_SESSIONS):
        guard.claim_session(ALICE, f's{index}')
    with pytest.raises(RuntimeError, match='capacity'):
        guard.claim_session(BOB, 'one-too-many')
    guard = InstancePushGuard()
    for index in range(MAX_ACTIVE_REQUESTS):
        guard.begin(target(), envelope(f'r{index}'))
    with pytest.raises(RuntimeError, match='capacity'):
        guard.begin(target(), envelope('one-too-many'))


def test_an_association_that_cannot_be_kept_is_not_made() -> None:
    guard = InstancePushGuard()
    for index in range(MAX_PROTECTED_REQUESTS - 2):
        guard.finish(dispatched(guard, target(), f'r{index}'))
    dispatched(guard, target(), 'live', sid='s')
    assert guard.counts == (1, MAX_PROTECTED_REQUESTS - 2, 1)
    first = frame('tool-1', 's', payload={'turn_request_id': 'live'})
    assert guard.admit(target(), first) is not None
    assert guard.counts[1] == MAX_PROTECTED_REQUESTS - 1
    second = frame('tool-2', 's', payload={'turn_request_id': 'live'})
    assert guard.admit(target(), second) is not None
    assert guard.counts[1] == MAX_PROTECTED_REQUESTS
    # No room to remember the interaction: the frame is dropped whole.
    third = frame('tool-3', 's', payload={'turn_request_id': 'live'})
    assert guard.admit(target(), third) is None
    assert guard.admit(target(), frame('tool-3', 's')) is None
    assert guard.counts[1] == MAX_PROTECTED_REQUESTS
    # The request's own completion still gets a slot: the oldest association goes.
    guard.finish('live')
    assert guard.counts == (0, MAX_PROTECTED_REQUESTS, 1)
    assert guard.admit(target(), frame('live', 's')) is not None


def test_clear_forgets_every_association() -> None:
    guard = InstancePushGuard()
    dispatched(guard, target(), 'live', channel='acp', sid='s')
    guard.admit(target(), frame('live', 's', channel='acp', event='acp.output_request',
                                payload={'jsonrpc': {'id': 'rpc-1', 'method': 'm'}}))
    guard.admit(target(), frame('tool-1', 's', payload={'turn_request_id': 'live'}))
    guard.clear()
    assert guard.counts == (0, 0, 0)
    assert guard.admit(target(), frame('live', 's')) is None
    assert guard.admit(target(), frame('tool-1', 's')) is None
    assert guard.admit(target(), frame('', 's', event='context.usage')) is None
    # A new Worker generation may take the same ids.
    dispatched(guard, target(), 'live', sid='s')
    assert guard.admit(target(), frame('live', 's')) is not None
