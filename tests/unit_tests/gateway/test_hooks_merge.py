# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from agent_gateway.common.hooks_config import (
    AGENT_HOOK_EVENTS,
    GATEWAY_HOOK_EVENTS,
    HookEvent,
    merge_hook_summaries,
)


def _summary(name: str, total: int = 1) -> dict:
    return {"name": name, "total_hooks": total, "matchers": []}


def test_merge_hook_summaries_uses_owner_sets_and_preserves_order() -> None:
    gateway_summary = [
        *[_summary(name, 2) for name in GATEWAY_HOOK_EVENTS],
        _summary("PreToolUse", 99),
    ]
    agent_summary = [
        *[_summary(name, 3) for name in AGENT_HOOK_EVENTS],
        _summary("SessionStart", 99),
    ]

    payload = merge_hook_summaries(
        gateway_summary=gateway_summary,
        agent_summary=agent_summary,
        gateway_disable_all_hooks=False,
        agent_disable_all_hooks=False,
    )

    events = payload["events"]
    assert len(events) == len(GATEWAY_HOOK_EVENTS) + len(AGENT_HOOK_EVENTS)
    assert {item["name"] for item in events} == GATEWAY_HOOK_EVENTS | AGENT_HOOK_EVENTS
    assert all(
        item["owner"] == ("gateway" if item["name"] in GATEWAY_HOOK_EVENTS else "agent")
        for item in events
    )
    assert next(item for item in events if item["name"] == "PreToolUse")["total_hooks"] == 3
    assert next(item for item in events if item["name"] == "SessionStart")["total_hooks"] == 2


def test_merge_hook_summaries_exposes_each_side_disable_flag() -> None:
    payload = merge_hook_summaries(
        gateway_summary=[],
        agent_summary=[],
        gateway_disable_all_hooks=True,
        agent_disable_all_hooks=False,
        degraded=True,
    )

    assert payload["gateway_disable_all_hooks"] is True
    assert payload["agent_disable_all_hooks"] is False
    assert payload["disable_all_hooks"] is True
    assert payload["degraded"] is True
    assert payload["events"] == []


def test_hook_event_owner_sets_partition_all_events() -> None:
    expected = {event.value for event in HookEvent}
    assert GATEWAY_HOOK_EVENTS | AGENT_HOOK_EVENTS == expected
    assert GATEWAY_HOOK_EVENTS.isdisjoint(AGENT_HOOK_EVENTS)
