# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A direct Worker's local cache is insufficient for a Git write."""
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from agent_gateway.channel_manager.web.git_ws_handler import GitDiffWebSocketHandler
from agent_gateway.channel_manager.web.web_connect import WebChannelConfig
from agent_gateway.routing import e2a_proxy


def git_channel(*, busy=False, user="alice"):
    return SimpleNamespace(
        agent_client=object(), channel_id="web", connection_user_id=lambda _: user,
        is_session_busy=lambda _: busy, send_response=AsyncMock(),
    )


def patch_git_proxy(monkeypatch, payload, *, ok=True):
    fetch = AsyncMock(return_value=(ok, payload))
    proxy = AsyncMock()
    monkeypatch.setattr(e2a_proxy, "fetch_agent_unary", fetch)
    monkeypatch.setattr(e2a_proxy, "proxy_unary_request", proxy)
    return fetch, proxy


@pytest.mark.parametrize('runtime, code', [
    ({'is_processing': True}, 'SESSION_BUSY'),
    ({'recovery_pending': True}, 'SERVICE_UNAVAILABLE'),
    ({'recovery_pending': True, 'is_processing': False}, 'SERVICE_UNAVAILABLE'),
    (None, 'SERVICE_UNAVAILABLE'),
    ({}, 'SERVICE_UNAVAILABLE'),
    ({'is_processing': 'false'}, 'SERVICE_UNAVAILABLE'),
    ({'is_processing': False}, None),
])
async def test_git_write_checks_raw_as_snapshot_before_forwarding(monkeypatch, runtime, code):
    channel = git_channel()
    registry = SimpleNamespace(mark_dirty=Mock())
    payload = {'user_id': 'alice', 'is_processing': False, 'runtime': runtime}
    fetch, proxy = patch_git_proxy(monkeypatch, payload)
    handler = GitDiffWebSocketHandler(channel, registry, require_activity_snapshot=True, app_id="desk")
    params = {'session_id': 'real-session', 'project_id': 'project'}
    await handler._proxy_turn_mutation(
        object(), 'git-request', params, req_method=Mock(value='project.git.discard_turn_changes'),
        busy_error='busy',
    )

    assert fetch.await_count == 1
    options = fetch.await_args.kwargs
    assert options['params'] == {'session_id': 'real-session', 'enable_writeback': False}
    assert options['user_id'] == 'alice' and options['metadata'] == {'app_id': 'desk'}
    assert options['req_method'].value == 'session.get_metadata'
    if code is None:
        proxy.assert_awaited_once()
        assert proxy.await_args.kwargs['params'] == params
        channel.send_response.assert_not_awaited()
    else:
        proxy.assert_not_awaited()
        assert channel.send_response.await_args.kwargs['code'] == code
        registry.mark_dirty.assert_not_called()


@pytest.mark.parametrize('snapshot, code', [
    ({'user_id': 'bob', 'runtime': {'is_processing': False}}, 'SESSION_IDENTITY_REQUIRED'),
    ({'user_id': None, 'runtime': {'is_processing': False}}, 'SESSION_IDENTITY_REQUIRED'),
    ({'error': 'AgentServer request timed out', 'code': 'AGENT_SERVER_TIMEOUT'}, 'AGENT_SERVER_TIMEOUT'),
])
async def test_git_write_refuses_a_foreign_owner_or_an_unknown_state(monkeypatch, snapshot, code):
    channel = git_channel()
    fetch, proxy = patch_git_proxy(monkeypatch, snapshot, ok='error' not in snapshot)
    handler = GitDiffWebSocketHandler(channel, SimpleNamespace(), require_activity_snapshot=True)
    await handler._proxy_turn_mutation(
        object(), 'git-request', {'session_id': 'real-session', 'project_id': 'project'},
        req_method=Mock(value='project.git.redo_turn_changes'), busy_error='busy',
    )
    proxy.assert_not_awaited()
    assert channel.send_response.await_args.kwargs['code'] == code
    assert fetch.await_count == 1


async def test_git_write_with_a_cache_only_gateway_does_not_consult_the_agentserver(monkeypatch):
    # The single-process Gateway keeps its previous behavior: the local cache decides.
    channel = git_channel()
    fetch, proxy = patch_git_proxy(monkeypatch, {})
    handler = GitDiffWebSocketHandler(channel, SimpleNamespace())
    await handler._proxy_turn_mutation(
        object(), 'git-request', {'session_id': 'session', 'project_id': 'project'},
        req_method=Mock(value='project.git.redo_turn_changes'), busy_error='busy',
    )
    proxy.assert_awaited_once()
    fetch.assert_not_awaited()
    channel.send_response.assert_not_awaited()
    busy = git_channel(busy=True)
    await GitDiffWebSocketHandler(busy, SimpleNamespace())._proxy_turn_mutation(
        object(), 'git-request', {'session_id': 'session', 'project_id': 'project'},
        req_method=Mock(value='project.git.redo_turn_changes'), busy_error='busy',
    )
    assert busy.send_response.await_args.kwargs['code'] == "SESSION_BUSY"


@pytest.mark.parametrize('session_id', [None, '', ' spaced ', '../escape', 1, 'x' * 300])
async def test_snapshot_gateway_requires_a_well_formed_session_before_a_write(monkeypatch, session_id):
    channel = git_channel()
    fetch, proxy = patch_git_proxy(monkeypatch, {})
    handler = GitDiffWebSocketHandler(channel, SimpleNamespace(), require_activity_snapshot=True)
    await handler._proxy_turn_mutation(
        object(), 'git-request', {'session_id': session_id, 'project_id': 'project'},
        req_method=Mock(value='project.git.redo_turn_changes'), busy_error='busy',
    )
    proxy.assert_not_awaited()
    fetch.assert_not_awaited()
    assert channel.send_response.await_args.kwargs['code'] == 'BAD_REQUEST'
    error = channel.send_response.await_args.kwargs['error']
    assert error  # value-safe: the caller's value is never echoed
    assert not session_id or str(session_id).strip() not in error


def test_activity_snapshot_is_opt_in():
    assert WebChannelConfig().require_git_activity_snapshot is False
    assert WebChannelConfig(require_git_activity_snapshot=True).require_git_activity_snapshot is True
    assert GitDiffWebSocketHandler(git_channel(), SimpleNamespace())._require_activity_snapshot is False
