# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Per-dispatch query throttling must not outlive the dispatch itself."""

from __future__ import annotations

import dataclasses
from types import SimpleNamespace

import pytest
from a2a.types import Task, TaskStatus, TaskState

from agent_gateway.enterprise.capabilities.a2a_manager.outbound.dispatcher import (
    A2AOutboundDispatcher,
)
from agent_gateway.enterprise.capabilities.a2a_manager.outbound.models import (
    A2AOutboundDispatch,
    A2AOutboundDispatchMode,
    A2AOutboundDispatchStatus,
)


def _dispatch(status=A2AOutboundDispatchStatus.WORKING) -> A2AOutboundDispatch:
    return A2AOutboundDispatch(
        dispatch_id="dispatch-1",
        agent_id="agent-1",
        agent_revision=1,
        mode=A2AOutboundDispatchMode.ASYNC,
        status=status,
        request_message_id="msg-1",
        source_session_id="session-1",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
        remote_task_id="remote-1",
        remote_context_id="ctx-1",
    )


class _Repository:
    manager_owned = False

    def __init__(self, dispatch: A2AOutboundDispatch) -> None:
        self.dispatch = dispatch

    async def get_dispatch(self, dispatch_id: str):
        return self.dispatch if dispatch_id == self.dispatch.dispatch_id else None

    async def get_agent(self, agent_id: str):
        return SimpleNamespace(
            agent_id=agent_id,
            agent_card={},
            credential_ref=None,
            connect_timeout_seconds=1.0,
            selected_interface=None,
            network_policy=None,
        )

    async def transition_dispatch(self, dispatch_id: str, status, **changes):
        allowed = {
            key: value
            for key, value in changes.items()
            if key in {field.name for field in dataclasses.fields(self.dispatch)}
        }
        self.dispatch = dataclasses.replace(self.dispatch, status=status, **allowed)
        return self.dispatch


class _Client:
    def __init__(self, state) -> None:
        self._state = state
        self.closed = False

    async def get_task(self, request, context=None):
        return Task(
            id="remote-1",
            context_id="ctx-1",
            status=TaskStatus(state=self._state),
        )

    async def close(self) -> None:
        self.closed = True


def _dispatcher(state, *, now=None):
    repository = _Repository(_dispatch())
    client = _Client(state)

    async def _builder(agent, credential):
        return client

    dispatcher = A2AOutboundDispatcher(
        repository,
        client_builder=_builder,
        monotonic=(lambda: 100.0) if now is None else now,
    )
    return dispatcher, repository, client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state,terminal",
    [
        (TaskState.TASK_STATE_COMPLETED, True),
        (TaskState.TASK_STATE_FAILED, True),
        (TaskState.TASK_STATE_CANCELED, True),
        (TaskState.TASK_STATE_WORKING, False),
    ],
)
async def test_query_throttle_entry_is_dropped_once_the_dispatch_ends(state, terminal):
    dispatcher, _, _ = _dispatcher(state)
    await dispatcher.query_dispatch("dispatch-1", source_session_id="session-1")
    if terminal:
        assert "dispatch-1" not in dispatcher._last_query_monotonic
    else:
        assert dispatcher._last_query_monotonic["dispatch-1"] == 100.0


@pytest.mark.asyncio
async def test_query_throttle_still_limits_repeated_polls_while_running():
    dispatcher, _, client = _dispatcher(TaskState.TASK_STATE_WORKING)
    await dispatcher.query_dispatch("dispatch-1", source_session_id="session-1")
    result = await dispatcher.query_dispatch("dispatch-1", source_session_id="session-1")
    assert result["status"] == A2AOutboundDispatchStatus.WORKING.value
    assert result["retry_after_seconds"] == 1.0
    assert client.closed is True
