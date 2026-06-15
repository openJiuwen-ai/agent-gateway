from types import SimpleNamespace

import pytest

from openjiuwen.core.single_agent.rail.base import ToolCallInputs

from jiuwenswarm.agents.harness.common.rails.stream_event_rail import (
    JiuSwarmStreamEventRail,
)
from jiuwenswarm.symphony.agent import AgenticToolResult


class _StreamSession:
    def __init__(self):
        self.chunks = []

    async def write_stream(self, chunk):
        self.chunks.append(chunk)


def _ctx(
    session,
    tool_name: str,
    tool_call_id: str = "call-1",
    tool_result=None,
):
    tool_call = SimpleNamespace(id=tool_call_id, name=tool_name, arguments={})
    force_finish_requests = []
    return SimpleNamespace(
        session=session,
        inputs=ToolCallInputs(
            tool_call=tool_call,
            tool_name=tool_name,
            tool_args={},
            tool_result=tool_result if tool_result is not None else {"success": True},
        ),
        extra={},
        exception=None,
        request_force_finish=force_finish_requests.append,
        force_finish_requests=force_finish_requests,
    )


@pytest.mark.asyncio
async def test_stream_event_rail_does_not_enable_symphony_status_events_for_plan_tool():
    rail = JiuSwarmStreamEventRail()
    session = _StreamSession()
    ctx = _ctx(session, "symphony_compose_score", tool_call_id="parent-call")

    await rail.before_tool_call(ctx)

    status_events = [
        chunk
        for chunk in session.chunks
        if chunk.type == "chat.symphony_status"
    ]
    assert status_events == []

    await rail.after_tool_call(ctx)
    assert not any(chunk.type == "chat.symphony_status" for chunk in session.chunks)


@pytest.mark.asyncio
async def test_stream_event_rail_force_finishes_symphony_compose_score_result():
    rail = JiuSwarmStreamEventRail()
    session = _StreamSession()
    result = {
        "success": True,
        "direct_display": True,
        "display_format": "markdown",
        "content": "## Symphony plan\n\n```mermaid\nflowchart LR\n  A --> B\n```",
        "mermaid": "flowchart LR\n  A --> B",
        "score_status": {"success": True, "exists": True, "stale": False},
    }
    ctx = _ctx(session, "symphony_compose_score", tool_result=result)

    await rail.before_tool_call(ctx)
    await rail.after_tool_call(ctx)

    tool_results = []
    for chunk in session.chunks:
        tool_result = chunk.payload.get("tool_result")
        if (
            chunk.type == "tool_result"
            and tool_result is not None
            and tool_result.get("tool_name") == "symphony_compose_score"
        ):
            tool_results.append(tool_result)
    assert tool_results[0]["raw_output"] == result
    assert tool_results[0]["score_status"] == result["score_status"]
    assert tool_results[0]["direct_display"] is True
    direct_messages = [chunk for chunk in session.chunks if chunk.type == "chat.final"]
    assert direct_messages == []
    assert ctx.force_finish_requests == [
        {"output": result["content"], "result_type": "answer"}
    ]


@pytest.mark.asyncio
async def test_stream_event_rail_continues_after_symphony_skill_gap_result():
    rail = JiuSwarmStreamEventRail()
    session = _StreamSession()
    result = {
        "success": True,
        "direct_display": True,
        "display_format": "markdown",
        "content": "## Symphony plan\n\nNo suitable skill found.",
        "continue_after_display": True,
        "followup_action": "external_skill_discovery",
    }
    ctx = _ctx(session, "symphony_compose_score", tool_result=result)

    await rail.before_tool_call(ctx)
    await rail.after_tool_call(ctx)

    tool_results = [
        chunk.payload.get("tool_result")
        for chunk in session.chunks
        if chunk.type == "tool_result"
    ]
    assert tool_results[0]["continue_after_display"] is True
    assert tool_results[0]["followup_action"] == "external_skill_discovery"
    assert not any(chunk.type == "chat.final" for chunk in session.chunks)
    assert ctx.force_finish_requests == []


@pytest.mark.asyncio
async def test_stream_event_rail_uses_agentic_tool_detailed_output_as_raw_output():
    rail = JiuSwarmStreamEventRail()
    session = _StreamSession()
    detailed_output = {
        "success": True,
        "result": "# Skill Branch Explore",
        "skill_tree": {
            "query": "skill_branch_explore: OfficeDocs",
            "steps": [{"order": 0, "node_id": "OfficeDocs"}],
            "candidates": [],
        },
    }
    result = AgenticToolResult(
        {"success": True, "result": "# Skill Branch Explore"},
        detailed_output=detailed_output,
    )
    ctx = _ctx(session, "skill_branch_explore", tool_result=result)

    await rail.before_tool_call(ctx)
    await rail.after_tool_call(ctx)

    tool_results = [
        chunk.payload.get("tool_result")
        for chunk in session.chunks
        if chunk.type == "tool_result"
    ]
    assert tool_results[0]["raw_output"] == detailed_output
    assert "skill_tree" not in tool_results[0]["result"]


@pytest.mark.asyncio
async def test_stream_event_rail_does_not_enable_symphony_status_events_for_other_tools():
    rail = JiuSwarmStreamEventRail()
    session = _StreamSession()
    ctx = _ctx(session, "todo_list")

    await rail.before_tool_call(ctx)
    await rail.after_tool_call(ctx)

    assert not any(chunk.type == "chat.symphony_status" for chunk in session.chunks)
    assert ctx.force_finish_requests == []
