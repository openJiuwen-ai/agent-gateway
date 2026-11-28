# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Compatibility operations for the public account-free local Web profile."""

from collections.abc import Awaitable, Callable
from typing import Any

from e2a.message import ReqMethod

from agent_gateway.application.personal_settings import allows_settings_origin
from agent_gateway.channel_manager.web.designer_handlers import PERSONAL_DESIGNER_METHODS
from agent_gateway.common.protocol_ids import InvalidProtocolId, validate_session_id
from agent_gateway.routing.e2a_proxy import fetch_agent_unary

# Removing and restoring a project is one request to the AgentServer, which
# owns the project, its Sessions and their jobs. It must say it can do that.
PROJECT_LIFECYCLE_CAPABILITY = 'personal_project_lifecycle_v1'
PROJECT_MUTATIONS = frozenset({'project.remove', 'project.restore'})
_PROJECT_METHODS = frozenset({'project.create', 'project.rename', 'project.pin'}) | PROJECT_MUTATIONS
_HEARTBEAT_METHODS = frozenset({
    'heartbeat.job.get', 'heartbeat.job.preview', 'heartbeat.job.create', 'heartbeat.job.update',
    'heartbeat.job.toggle', 'heartbeat.job.run_now', 'heartbeat.job.cancel', 'heartbeat.job.delete',
})
# A Git write goes to the AgentServer that owns the worktree. It must say it
# takes these from the personal panel, and the request is marked as such.
PROJECT_GIT_CAPABILITY = 'personal_project_git_v1'
PROJECT_GIT_MARKER = '_personal_project_git_v1'
PROJECT_GIT_OPERATIONS = frozenset({
    'project.git.probe', 'project.git.init', 'project.git.create_branch',
    'project.git.switch_branch', 'project.git.commit', 'project.git.push',
})
# The personal panel never asks for these. A request that does is not the
# page's, and history rewrite and forced or deleted remote branches are not
# offered here.
_RESTRICTED_GIT_FLAGS = {
    'project.git.commit': ('amend', 'no_verify'),
    'project.git.push': ('force', 'delete'),
}
PERSONAL_WEB_OPERATIONS = _PROJECT_METHODS | _HEARTBEAT_METHODS | PROJECT_GIT_OPERATIONS | PERSONAL_DESIGNER_METHODS


def protect_personal_web_operation(
    channel: Any, method: str, handler: Callable[..., Awaitable[None]],
) -> Callable[..., Awaitable[None]]:
    """Reuse the existing AS proxy handlers after checking the local caller."""

    async def guarded(ws, req_id, params, session_id, user_id=None):
        if not allows_settings_origin(getattr(ws, 'request_headers', None)):
            await channel.send_response(
                ws, req_id, ok=False, error='This operation requires an allowed Web Origin', code='FORBIDDEN',
            )
            return
        if not isinstance(params, dict):
            await channel.send_response(ws, req_id, ok=False, error='params must be an object', code='BAD_REQUEST')
            return
        for flag in _RESTRICTED_GIT_FLAGS.get(method, ()):
            if flag not in params or params[flag] is False:
                continue
            code = 'UNSUPPORTED_REQUEST' if params[flag] is True else 'BAD_REQUEST'
            await channel.send_response(ws, req_id, ok=False, error=f'{flag} is not supported', code=code)
            return
        if method in PROJECT_MUTATIONS:
            project_id = params.get('project_id')
            if (not isinstance(project_id, str) or not project_id.strip()
                    or len(project_id) > 256 or not project_id.isprintable()):
                await channel.send_response(
                    ws, req_id, ok=False, error='project_id must be a bounded string', code='BAD_REQUEST',
                )
                return
            params = {'project_id': project_id.strip()}
        if method in _HEARTBEAT_METHODS:
            # The regular Web dispatcher supplies a synthetic session ID if
            # there isn't one in the request. Never use it as an access scope.
            try:
                session_id = validate_session_id(params.get('session_id'))
            except InvalidProtocolId as exc:
                await channel.send_response(ws, req_id, ok=False, error=str(exc), code='BAD_REQUEST')
                return
            ok, metadata = await fetch_agent_unary(
                agent_client=channel.agent_client, req_method=ReqMethod.SESSION_GET_METADATA,
                params={'session_id': session_id, 'enable_writeback': False},
                session_id=session_id, user_id=user_id, channel_id='web',
                label='heartbeat session ownership', timeout_seconds=5.0,
            )
            if not ok:
                await channel.send_response(ws, req_id, ok=False, error=metadata['error'], code=metadata['code'])
                return
            if not isinstance(metadata, dict) or not metadata:
                await channel.send_response(
                    ws, req_id, ok=False, error='Session ownership is unavailable', code='SERVICE_UNAVAILABLE',
                )
                return
            owner = metadata.get('user_id')
            if ('' if owner is None else owner) != (user_id or ''):
                await channel.send_response(
                    ws, req_id, ok=False, error='Session belongs to another user', code='SESSION_IDENTITY_REQUIRED',
                )
                return
            # Older proxy callers may fall back to params.user_id; only use
            # the connection identity, including for anonymous sessions.
            params = {key: value for key, value in params.items() if key != 'user_id'}
        await handler(ws, req_id, params, session_id, user_id=user_id)

    return guarded
