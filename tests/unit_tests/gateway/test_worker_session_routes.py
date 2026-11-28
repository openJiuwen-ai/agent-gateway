# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""One session id has at most one confirmed target inside a Worker."""

from types import SimpleNamespace

import pytest

from agent_gateway.application.worker_session_routes import SessionRouteError, SessionRouteIndex

ALICE = SimpleNamespace(target_id="target-0", generation=1, user_id="alice")
RESTARTED = SimpleNamespace(target_id="target-0", generation=2, user_id="alice")
BOB = SimpleNamespace(target_id="target-1", generation=1, user_id="bob")


def test_an_unknown_session_has_no_owner_and_is_not_claimed_by_a_check():
    routes = SessionRouteIndex()

    assert routes.owner("s") is None and routes.owner(None) is None and routes.owner("") is None
    assert routes.check(ALICE, "s") is False
    assert routes.check(ALICE, None) is False and routes.check(ALICE, "  ") is False
    assert len(routes) == 0


def test_only_a_claim_binds_and_only_the_same_target_may_reuse_it():
    routes = SessionRouteIndex()
    routes.claim(ALICE, " s ")

    assert len(routes) == 1 and routes.owner("s") == ("target-0", 1)
    assert routes.owner(" s ") == ("target-0", 1)
    assert routes.check(ALICE, "s") is True
    with pytest.raises(SessionRouteError, match="another static target"):
        routes.check(BOB, "s")
    with pytest.raises(SessionRouteError, match="another static target"):
        routes.claim(BOB, "s")
    # The confirmed owner stays unchanged after the refusal.
    assert routes.owner("s") == ("target-0", 1)
    routes.claim(ALICE, "s")
    assert len(routes) == 1


def test_a_restart_generation_does_not_inherit_the_confirmed_route():
    routes = SessionRouteIndex()
    routes.claim(ALICE, "s")

    with pytest.raises(SessionRouteError, match="another instance generation"):
        routes.check(RESTARTED, "s")
    assert routes.owner("s") == ("target-0", 1)
    # Only the restarted target's own confirmation moves the route forward.
    routes.claim(RESTARTED, "s")
    assert routes.check(RESTARTED, "s") is True
    assert routes.owner("s") == ("target-0", 2)


def test_empty_ids_are_ignored_and_capacities_are_validated():
    routes = SessionRouteIndex()
    routes.claim(ALICE, None)
    routes.claim(ALICE, "   ")
    routes.claim(ALICE, 5)
    routes.claim_acp_session(ALICE, "")
    assert len(routes) == 0 and routes.acp_owner("x") is None

    with pytest.raises(ValueError, match="capacity"):
        SessionRouteIndex(max_sessions=0)
    with pytest.raises(ValueError, match="capacity"):
        SessionRouteIndex(max_acp_sessions=True)
    with pytest.raises(ValueError, match="capacity"):
        SessionRouteIndex(max_acp_sessions=0)


def test_least_recently_used_routes_are_evicted_and_bounded():
    routes = SessionRouteIndex(max_sessions=2)
    routes.claim(ALICE, "one")
    routes.claim(ALICE, "two")
    assert routes.check(ALICE, "one") is True  # refreshed
    routes.claim(BOB, "three")

    assert len(routes) == 2
    assert routes.owner("two") is None
    assert routes.owner("one") == ("target-0", 1) and routes.owner("three") == ("target-1", 1)
    # An evicted id is simply unknown again; nothing is inferred for it.
    assert routes.check(BOB, "two") is False
    routes.claim(BOB, "two")
    assert routes.owner("two") == ("target-1", 1)
    # The oldest route is evicted next; the index never grows past its bound.
    assert routes.owner("one") is None and len(routes) == 2


def test_acp_external_ids_are_bound_to_one_target_per_worker():
    routes = SessionRouteIndex(max_acp_sessions=1)
    routes.claim_acp_session(ALICE, " acp-1 ")
    routes.claim_acp_session(BOB, "acp-2")

    assert routes.acp_owner("acp-1") is None  # evicted by the bound
    assert routes.acp_owner("acp-2") == "target-1"
    assert routes.check_acp_session(BOB, "acp-2") is True
    with pytest.raises(SessionRouteError, match="ACP session is owned by another static target"):
        routes.check_acp_session(ALICE, "acp-2")
    with pytest.raises(SessionRouteError, match="ACP session is owned by another static target"):
        routes.claim_acp_session(ALICE, "acp-2")
    assert routes.check_acp_session(ALICE, "acp-3") is False
    assert routes.check_acp_session(ALICE, None) is False


def test_one_id_belongs_to_one_target_under_either_of_its_names():
    routes = SessionRouteIndex()
    routes.claim_acp_session(BOB, "acp-bob")

    # A frame of any channel that names Bob's ACP session is refused for
    # Alice, before anything probes or claims the id for her target.
    with pytest.raises(SessionRouteError, match="ACP session is owned by another static target"):
        routes.check(ALICE, "acp-bob")
    with pytest.raises(SessionRouteError, match="ACP session is owned by another static target"):
        routes.claim(ALICE, "acp-bob")
    assert routes.owner("acp-bob") is None
    assert routes.check(BOB, "acp-bob") is False  # no confirmed AgentServer route yet

    # The other way round: a confirmed session id is not a free client-visible
    # id. Taking it would lock its owner out of her own session.
    routes.claim(ALICE, "session-alice")
    with pytest.raises(SessionRouteError, match="another static target"):
        routes.claim_acp_session(BOB, "session-alice")
    assert routes.acp_owner("session-alice") is None
    assert routes.check(ALICE, "session-alice") is True


def test_acp_alias_requires_a_confirmed_route_for_the_same_target():
    routes = SessionRouteIndex()
    routes.claim(ALICE, "internal")

    with pytest.raises(SessionRouteError, match="no confirmed route"):
        routes.remember_acp_alias(BOB.target_id, "external", "internal")
    with pytest.raises(SessionRouteError, match="no confirmed route"):
        routes.remember_acp_alias(ALICE.target_id, "external", "unconfirmed")
    with pytest.raises(SessionRouteError, match="both session ids"):
        routes.remember_acp_alias(ALICE.target_id, "", "internal")
    with pytest.raises(SessionRouteError, match="both session ids"):
        routes.remember_acp_alias(ALICE.target_id, "external", None)
    assert routes.acp_alias(ALICE.target_id, "external") is None

    routes.remember_acp_alias(ALICE.target_id, "external", "internal")

    assert routes.acp_alias(ALICE.target_id, " external ") == "internal"
    assert routes.acp_alias(BOB.target_id, "external") is None
    assert routes.acp_alias(ALICE.target_id, "other") is None


def test_acp_aliases_are_bounded_and_clear_resets_every_table():
    routes = SessionRouteIndex(max_acp_sessions=1)
    routes.claim(ALICE, "one")
    routes.claim(BOB, "two")
    routes.remember_acp_alias(ALICE.target_id, "e1", "one")
    routes.remember_acp_alias(BOB.target_id, "e2", "two")
    routes.claim_acp_session(ALICE, "a1")

    assert routes.acp_alias(ALICE.target_id, "e1") is None
    assert routes.acp_alias(BOB.target_id, "e2") == "two"

    routes.clear()

    assert len(routes) == 0
    assert routes.acp_alias(BOB.target_id, "e2") is None
    assert routes.acp_owner("a1") is None
    assert routes.owner("one") is None and routes.owner("two") is None
