# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Bind controlled Cron handlers to the original connection business identity."""

from __future__ import annotations

import inspect
import json
from typing import Any

from agent_gateway.application.worker_cron import requests_real_cron_project

CRON_PANEL_METHODS = frozenset({
    'cron.job.list', 'cron.job.meta', 'cron.job.get', 'cron.job.create',
    'cron.job.update', 'cron.job.delete', 'cron.job.toggle',
    'cron.job.preview', 'cron.job.run_now',
})
_CRON_MUTATION_METHODS = frozenset({'cron.job.create', 'cron.job.update'})


def bind_owned_cron_handlers(web_channel: Any, protocol_runtime: Any) -> None:
    """Keep legacy handlers, removing their anonymous payload-identity fallback.

    The controlled personal path has no group or bot identity. The business user
    of the connection is the only owner a Cron request may act for: an anonymous
    connection cannot claim a payload user, and a non-default project is refused
    here, before a handler can ask the AgentServer to bind one. This guard is
    applied only when sharded Cron is explicitly assembled, never to single.
    """
    def bind(handler: Any, *, entry: str, method: str) -> Any:
        takes_user = 'user_id' in inspect.signature(handler).parameters

        async def invoke(ws, req_id, params, session_id, user_id=None):
            del user_id
            if entry == 'web':
                owner = web_channel.connection_user_id(ws)
            else:
                owner = getattr(ws, '_gateway_user_id', None)
            owner = str(owner or '').strip()

            async def reject(error: str, code: str) -> None:
                if entry == 'web':
                    await web_channel.send_response(ws, req_id, ok=False, error=error, code=code)
                    return
                send_response = getattr(protocol_runtime, 'send_response', None)
                if callable(send_response):
                    await send_response(ws, req_id, ok=False, error=error, code=code)
                    return
                await ws.send(json.dumps({
                    'type': 'res', 'id': req_id, 'ok': False,
                    'error': error, 'code': code,
                }))

            if not owner or owner == 'default':
                await reject('Cron requires a named connection user', 'CRON_IDENTITY_REQUIRED')
                return
            if params is not None and not isinstance(params, dict):
                await reject('params must be object', 'BAD_REQUEST')
                return
            owned_params = dict(params or {})
            owned_params.update(user_id=owner, group_id='', bot_id='')
            if method in _CRON_MUTATION_METHODS:
                # A private credential-selection parameter is never accepted
                # from a client, and no connection session may put it back:
                # login models are not enabled for sharded Cron.
                owned_params.pop('_auth_session', None)
                target = owned_params
                if method == 'cron.job.update':
                    patch = owned_params.get('patch')
                    if isinstance(patch, dict):
                        # The handler keeps its own patch-shape validation.
                        owned_params['patch'] = target = dict(patch)
                        target.pop('_auth_session', None)
                if requests_real_cron_project(target):
                    await reject(
                        'Cron jobs for a non-default project are not enabled',
                        'CRON_PROJECT_DISABLED',
                    )
                    return
            try:
                if takes_user:
                    await handler(ws, req_id, owned_params, session_id, user_id=owner)
                else:
                    await handler(ws, req_id, owned_params, session_id)
            except KeyError:
                await reject('job not found', 'NOT_FOUND')
            except ValueError:
                # Validation text is not echoed: it may quote an upstream error.
                await reject('Cron request is not supported or invalid', 'BAD_REQUEST')
            except RuntimeError:
                await reject('Cron is unavailable', 'CRON_UNAVAILABLE')

        return invoke

    for method in CRON_PANEL_METHODS:
        handler = web_channel._method_handlers.get(method)
        if handler is not None:
            web_channel.register_method(method, bind(handler, entry='web', method=method))
    for route in protocol_runtime.config.routes.values():
        tui_channel = getattr(route, 'ws_channel', None)
        if tui_channel is not None:
            tui_channel.config.cron_owner_filter = True

            async def register_cron_peer(ws, *, channel=tui_channel):
                from agent_gateway.routing.keys import AgentRef, RoutingKey

                owner = str(getattr(ws, '_gateway_user_id', None) or '').strip()
                if not owner or owner == 'default':
                    return
                await channel.register_ws(ws, RoutingKey(
                    user_id=owner, channel_id='tui', app_id='default',
                    agent_ref=AgentRef(mode='agent', id='default'), session_id='',
                ))

            tui_channel.on_connect(register_cron_peer)
        for method in CRON_PANEL_METHODS:
            handler = route.local_handlers.get(method)
            if handler is not None:
                route.local_handlers[method] = bind(handler, entry='tui', method=method)
