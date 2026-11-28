# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A routed Worker reaches each user's own AgentServer and nothing else."""

import asyncio
from contextlib import AsyncExitStack
from dataclasses import replace
import socket

import pytest
from e2a.gateway_normalize import e2a_from_agent_fields
from e2a.message import ReqMethod

from agent_gateway.application.worker_routed_client import SessionRouteError, WorkerRoutedClient
from agent_gateway.common.ingress_identity import LocalIdentityPolicy, resolve_local_identity
from agent_gateway.common.southbound_routes import SouthboundTarget, StaticRouteDirectory
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest
from tests.unit_tests.gateway.routed_agent_server import RoutedAgentServer, push_wire

pytestmark = pytest.mark.asyncio


def target(user: str, index: int, url: str = "ws://127.0.0.1:1/agent") -> SouthboundTarget:
    return SouthboundTarget(
        target_id=f"target-{index}", physical_id=f"physical-{index}", generation=1, user_id=user,
        agent_type="jiuwenswarm", owner_slot=0, ws_url=url,
    )


def directory(*targets: SouthboundTarget) -> StaticRouteDirectory:
    return StaticRouteDirectory(revision=1, identity_policy=LocalIdentityPolicy(), manifest=WorkerManifest((0,)),
                                targets=targets)


def envelope(user, rid, *, method="chat.send", session_id=None, channel="web", is_stream=False, params=None):
    return e2a_from_agent_fields(
        request_id=rid, channel_id=channel, session_id=session_id, req_method=method, params=params or {},
        is_stream=is_stream, timestamp=0, user_id=user,
    )


def closed_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


async def make_client(stack: AsyncExitStack, *, users=("alice", "bob"), sessions=((), ()), hold=((), ())):
    agents = []
    for index, name in enumerate(users):
        agent = await RoutedAgentServer(name).start()
        stack.push_async_callback(agent.stop)
        agent.sessions |= set(sessions[index])
        agent.hold |= set(hold[index])
        agents.append(agent)
    route = directory(*(target(user, index, agent.url) for index, (user, agent) in enumerate(zip(users, agents))))
    client = WorkerRoutedClient(route, slot_id=0)
    stack.push_async_callback(client.disconnect)
    received: list = []
    pushes = asyncio.Event()

    async def consume(wire) -> None:
        received.append(wire)
        pushes.set()

    client.set_server_push_handler(consume)
    await client.activate()
    return client, agents, route, received


async def test_an_acp_session_id_of_another_user_is_neither_probed_nor_claimed():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack)
        bob = client.owned_targets[1]
        client.session_routes.claim_acp_session(bob, "acp-bob")

        with pytest.raises(SessionRouteError, match="ACP session is owned by another static target"):
            await client.send_request(envelope("alice", "steal", session_id="acp-bob"))

        # No target was asked anything, and the id was not claimed as a new
        # session of Alice's target.
        assert [agent.requests for agent in agents] == [[], []]
        assert client.session_routes.owner("acp-bob") is None
        # Bob's own request naming it is served by his target as before.
        assert (await client.send_request(envelope("bob", "b-1", session_id="acp-bob"))).ok
        assert [agent.has_request("steal") for agent in agents] == [False, False]
        assert agents[1].has_request("b-1") and not agents[0].has_request("b-1")


async def test_unary_and_stream_reach_the_bound_users_own_target():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack)

        response = await client.send_request(envelope("alice", "a-unary", method="history.get", session_id="sa"))

        assert response.ok and response.payload["echo"] == "alice"
        # The first request for an unknown session probes every target under
        # that target's own user before the request itself is sent.
        # The first request for an unknown session probes every target under
        # that target's own user before the request itself is sent.
        assert agents[0].methods() == ["session.get_metadata", "history.get"]
        assert {item["user_id"] for item in agents[0].requests} == {"alice"}
        assert agents[1].methods() == ["session.get_metadata"]
        chunks = [chunk async for chunk in client.send_request_stream(
            envelope("bob", "b-stream", session_id="sb", is_stream=True),
        )]
        assert [chunk.payload["content"] for chunk in chunks] == ["bob-delta", "bob-final"]
        assert agents[1].methods() == ["session.get_metadata", "session.get_metadata", "chat.send"]
        assert [item["request_id"] for item in agents[1].requests if item["method"] != "session.get_metadata"] == [
            "b-stream",
        ]
        # Bob's new session was searched on Alice's AgentServer too.
        assert agents[0].methods() == ["session.get_metadata", "history.get", "session.get_metadata"]
        assert {item["user_id"] for item in agents[0].requests} == {"alice"}
        assert {item["user_id"] for item in agents[1].requests} == {"bob"}
        assert client.server_ready and client.agent_ready and client.can_reconnect


@pytest.mark.parametrize("user", [None, "", "unknown", "default", "alice ", 5])
async def test_missing_or_unknown_users_have_no_default_target(user):
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack)

        with pytest.raises(ValueError, match="no owned static southbound target"):
            await client.send_request(envelope(user, "anonymous"))
        assert agents[0].requests == [] and agents[1].requests == []


async def test_non_builtin_agent_type_is_refused_before_dispatch():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack)

        with pytest.raises(ValueError, match="no owned static southbound target"):
            await client.send_request(envelope("alice", "agent-type", params={"agent_type": "other"}))
        assert agents[0].requests == [] and agents[1].requests == []


async def test_activation_fails_closed_when_one_owned_target_is_unreachable():
    async with AsyncExitStack() as stack:
        agent = await RoutedAgentServer("alice").start()
        stack.push_async_callback(agent.stop)
        route = directory(target("alice", 0, agent.url), target("bob", 1, f"ws://127.0.0.1:{closed_port()}/agent"))
        client = WorkerRoutedClient(route, slot_id=0)
        stack.push_async_callback(client.disconnect)
        client.set_server_push_handler(None)

        with pytest.raises(RuntimeError, match="unreachable at activation: target-1"):
            await client.activate()

        assert not client.server_ready
        with pytest.raises(RuntimeError, match="not active"):
            await client.send_request(envelope("alice", "after-failure"))
        await asyncio.sleep(0.05)
        assert agent._connections == set()


async def collect(received: list, count: int, timeout: float = 3.0) -> None:
    """Wait for admitted pushes, then let any wrong one arrive before asserting."""
    async with asyncio.timeout(timeout):
        while len(received) < count:
            await asyncio.sleep(0.01)
    await asyncio.sleep(0.1)


async def test_push_is_admitted_only_for_the_target_that_proved_the_route():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack)
        await client.send_request(envelope("alice", "a-chat", session_id="sa"))
        await client.send_request(envelope("bob", "b-chat", session_id="sb"))

        # Bob's AgentServer cannot speak for Alice's request or session, and an
        # id nobody requested has no authoritative route at all.
        await agents[1].push(push_wire("a-chat", "sa"))
        await agents[1].push(push_wire("unknown", "sb"))
        await agents[1].push(push_wire("unknown", "sa", event="context.usage"))
        await agents[1].push(push_wire("b-chat", "sb", user="alice"))
        await agents[1].push(push_wire("b-chat", "sb", user="mallory"))
        await agents[1].push(push_wire("b-chat", "sa"))
        await agents[1].push(push_wire("b-chat", "sb"))

        await collect(_received, 1)
        assert len(_received) == 1
        assert _received[0]["user_id"] == "bob" and _received[0]["session_id"] == "sb"
        assert _received[0].target is client.owned_targets[1]


async def test_session_scoped_push_needs_the_session_on_the_sending_target():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack)
        await client.send_request(envelope("alice", "a-chat", session_id="sa"))
        await client.send_request(envelope("bob", "b-chat", session_id="sb"))

        # Alice confirmed "sa"; Bob's AgentServer never did.
        await agents[1].push(push_wire("", "sa", event="context.usage", total_tokens=3))
        await agents[0].push(push_wire("", "sb", event="context.usage", total_tokens=3))
        await agents[0].push(push_wire("", "never-seen", event="plan.mode_exited", mode="agent.work.normal"))
        await agents[0].push(push_wire("", "sa", event="context.usage", total_tokens=12))

        await collect(_received, 1)
        assert len(_received) == 1
        assert _received[0]["user_id"] == "alice" and _received[0]["session_id"] == "sa"
        assert client.session_routes.owner("sa") == ("target-0", 1)
        assert client.session_routes.owner("sb") == ("target-1", 1)


async def test_a_session_held_by_another_target_is_refused_and_never_probed_twice():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack, sessions=(("shared",), ()))
        # The request names a session only Alice's AgentServer holds.

        with pytest.raises(SessionRouteError, match="another static target"):
            await client.send_request(envelope("bob", "b-shared", session_id="shared"))

        assert agents[1].methods() == ["session.get_metadata"]
        assert agents[0].methods() == ["session.get_metadata"]
        assert client.session_routes.owner("shared") is None
        assert (await client.send_request(envelope("alice", "a-shared", session_id="shared"))).ok
        assert client.session_routes.owner("shared") == ("target-0", 1)
        with pytest.raises(SessionRouteError, match="another static target"):
            client.validate_bound_session("bob", "shared")


async def test_new_session_is_claimed_for_its_user_after_every_target_answered():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack)

        assert (await client.send_request(envelope("bob", "b-new", session_id="fresh"))).ok

        assert agents[1].methods() == ["session.get_metadata", "chat.send"]
        assert agents[0].methods() == ["session.get_metadata"]
        assert client.session_routes.owner("fresh") == ("target-1", 1)
        # The confirmed route is reused without another probe.
        assert (await client.send_request(envelope("bob", "b-again", session_id="fresh"))).ok
        assert agents[1].methods() == ["session.get_metadata", "chat.send", "chat.send"]
        # Alice cannot take the id Bob's AgentServer now holds.
        with pytest.raises(SessionRouteError, match="another static target"):
            await client.send_request(envelope("alice", "a-take", session_id="fresh"))


async def test_request_ids_are_unique_across_every_target_of_the_worker():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack, hold=(("shared-id",), ()))
        stream = asyncio.create_task(_drain(client.send_request_stream(
            envelope("alice", "shared-id", session_id="sa", is_stream=True),
        )))
        await agents[0].wait_request("shared-id")
        try:
            with pytest.raises(ValueError, match="unused request ID"):
                await client.send_request(envelope("bob", "shared-id", session_id="sb"))
        finally:
            stream.cancel()
            await asyncio.gather(stream, return_exceptions=True)
        # A dispatched id stays protected while a late frame can still arrive.
        with pytest.raises(ValueError, match="unused request ID"):
            await client.send_request(envelope("alice", "shared-id", session_id="sa"))
        assert client.request_counts()[0] == 0


async def _drain(aiter):
    return [chunk async for chunk in aiter]


async def test_cancelled_stream_keeps_its_scoped_push_and_frees_active_capacity():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack, hold=(("held",), ()))
        stream = asyncio.create_task(_drain(client.send_request_stream(
            envelope("alice", "held", session_id="sa", is_stream=True),
        )))
        await agents[0].wait_request("held")
        stream.cancel()
        await asyncio.gather(stream, return_exceptions=True)
        assert client.request_counts()[0] == 0

        # The AgentServer may still finish the cancelled turn. It reaches only
        # the user who owned that request.
        await agents[0].push(push_wire("held", "sa"))
        await collect(_received, 1)
        assert len(_received) == 1
        assert _received[0]["user_id"] == "alice" and _received[0]["session_id"] == "sa"


async def test_dropped_target_is_restored_by_the_next_envelope_addressed_to_it():
    async with AsyncExitStack() as stack:
        client, agents, route, _received = await make_client(stack)
        # Both sessions are already confirmed, so no request below needs a probe.
        client.session_routes.claim(route.targets[0], "sa")
        client.session_routes.claim(route.targets[1], "sb")

        await agents[0].drop()
        # The receiver observes the closed socket asynchronously.
        async with asyncio.timeout(5):
            while client.target_client(route.targets[0].target_id).agent_ready:
                await asyncio.sleep(0.01)
        assert not client.agent_ready and client.server_ready
        assert (await client.send_request(envelope("bob", "b-after-drop", session_id="sb"))).ok
        assert not client.agent_ready
        assert (await client.send_request(envelope("alice", "a-after-drop", session_id="sa"))).ok

        # Both routes were confirmed, so the restored sockets saw no probe.
        assert agents[1].methods() == ["chat.send"] and agents[1].has_request("b-after-drop")
        assert agents[0].methods() == ["chat.send"] and agents[0].has_request("a-after-drop")
        assert client.agent_ready


async def test_disconnect_is_bounded_idempotent_and_final():
    async with AsyncExitStack() as stack:
        client, agents, _route, _received = await make_client(stack)
        loop = asyncio.get_running_loop()

        await client.disconnect(deadline=loop.time() + 30)
        await client.disconnect(deadline=loop.time() + 1)

        assert not client.server_ready and not client.can_reconnect
        with pytest.raises(RuntimeError, match="not active"):
            await client.send_request(envelope("alice", "stopped"))
        with pytest.raises(RuntimeError, match="stopped"):
            await client.activate()
        assert all(agent._connections == set() for agent in agents)
        assert client.session_routes.owner("sa") is None


async def test_shared_push_consumer_may_follow_activation_but_never_the_stop():
    async with AsyncExitStack() as stack:
        agent = await RoutedAgentServer("alice").start()
        stack.push_async_callback(agent.stop)
        client = WorkerRoutedClient(directory(target("alice", 0, agent.url)), slot_id=0)
        stack.push_async_callback(client.disconnect)
        with pytest.raises(ValueError, match="handler must be callable"):
            client.set_server_push_handler("nope")

        async def consumer(_wire):  # The Worker registers its handlers after connecting.
            return None

        client.set_server_push_handler(None)
        await client.activate()
        # The Worker connects its targets before the shared handlers exist.
        client.set_server_push_handler(consumer)
        assert client._push_handler is consumer
        with pytest.raises(ValueError, match="activate"):
            await client.connect(agent.url)
        await client.disconnect()
        with pytest.raises(RuntimeError, match="stopped"):
            client.set_server_push_handler(consumer)
        assert agent.requests == []


async def test_slot_without_targets_has_no_route_and_nothing_to_connect():
    async with AsyncExitStack() as stack:
        agent = await RoutedAgentServer("alice").start()
        stack.push_async_callback(agent.stop)
        owner = target("alice", 0, agent.url)
        alice_slot = FixedUserHash(WorkerManifest((0, 1)), LocalIdentityPolicy()).select(
            resolve_local_identity(b"user_id=alice", (), entry="web", policy=LocalIdentityPolicy()),
        )
        route = StaticRouteDirectory(
            revision=1, identity_policy=LocalIdentityPolicy(), manifest=WorkerManifest((0, 1)),
            targets=(replace(owner, owner_slot=alice_slot),),
        )
        client = WorkerRoutedClient(route, slot_id=1 - alice_slot)
        stack.push_async_callback(client.disconnect)
        client.set_server_push_handler(None)
        await client.activate()

        assert client.owned_targets == () and client.server_ready and not client.agent_ready
        with pytest.raises(ValueError, match="no owned static southbound target"):
            await client.send_request(envelope("alice", "empty"))
        assert agent.requests == []
