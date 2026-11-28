# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Settings RPC boundary for the public local, account-free Worker profile."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
import inspect
import logging
from typing import Any
from urllib.parse import urlsplit

from e2a.constants import E2A_AUTH_SESSION_PARAM_KEY, E2A_MODEL_AUTH_PARAM_KEY

from agent_gateway.common.security.ws_origin import get_allowed_origin_hosts, get_header_value


PERSONAL_SETTINGS_METHODS = frozenset({
    'config.get', 'config.set', 'config.save_all', 'config.validate_model',
    'models.list', 'models.get', 'models.upsert', 'models.delete', 'models.refresh_status',
    'model_groups.upsert', 'model_groups.delete', 'session.selection.set',
    'vendors.list', 'vendors.fetch_models',
    'locale.set_conf', 'permissions.tools.update', 'permissions.tools.delete',
})

_LOOPBACK_HOSTS = frozenset({'127.0.0.1', '::1', 'localhost'})
_SLOT_FIELDS = ('api_base', 'api_key', 'model', 'provider', 'enabled')
_MEDIA_FIELDS = (*_SLOT_FIELDS, 'endpoint_profile', 'vendor_key', 'plan', 'context_window_tokens')
_GENERATION_FIELDS = (*_SLOT_FIELDS, 'protocol', 'context_window_tokens')
_YAML_FIELDS = frozenset({
    'skill_evolution', 'skill_retrieval_enabled', 'skill_retrieval_index_enabled', 'ttse_enabled',
    'multi_model_collaboration_enabled',
})
_CONFIG_FIELDS = _YAML_FIELDS | {
    'embed_api_base', 'embed_api_key', 'embed_model',
    'free_search_ddg_enabled', 'free_search_bing_enabled',
    'jina_api_key', 'bocha_api_key', 'perplexity_api_key', 'serper_api_key',
    *(f'{slot}_{name}' for slot in ('vision', 'audio', 'video') for name in _MEDIA_FIELDS),
    *(f'{slot}_{name}' for slot in ('visual_gen', 'video_gen') for name in _GENERATION_FIELDS),
    *(f'music_gen_{name}' for name in (*_SLOT_FIELDS, 'protocol')),
}
_PANEL_SWITCH_FIELDS = frozenset({'setup_guide_enabled', 'symphony_enabled'})
_PERMISSION_PROFILES = frozenset({'default', 'automatic', 'full_access'})
_EMPTY_PARAMS = frozenset({'config.get', 'models.list', 'models.refresh_status', 'vendors.list'})
_PRIVATE_FIELDS = frozenset({
    E2A_MODEL_AUTH_PARAM_KEY, E2A_AUTH_SESSION_PARAM_KEY, 'credential_ref', '_gateway_login_catalog',
    'auth_session', 'login_model_settings',
})


def is_local_settings_target(url: str) -> bool:
    try:
        return urlsplit(url).hostname in _LOOPBACK_HOSTS
    except ValueError:
        return False


def settings_transport_logger() -> logging.Logger:
    """The websockets logger of a link that carries settings.

    websockets logs whole frames at DEBUG, settings readbacks included. This
    logger stays at INFO whatever the root level is, so those frames are never
    formatted.
    """
    logger = logging.getLogger('gateway.personal_settings.transport')
    logger.setLevel(logging.INFO)
    return logger


def allows_settings_origin(headers: Any) -> bool:
    """Apply the settings-only Origin gate even when the general WS gate is off."""
    get_list = getattr(headers, 'getlist', None) or getattr(headers, 'get_all', None)
    if get_list and len(get_list('Origin') or ()) != 1:
        return False
    origin = get_header_value(headers, 'Origin')
    if not origin:
        return False
    try:
        parsed = urlsplit(origin)
        port = parsed.port
    except ValueError:
        return False
    return (
        parsed.scheme in {'http', 'https'}
        and parsed.username is None and parsed.password is None
        and not parsed.path and not parsed.query and not parsed.fragment
        and port != 0
        and parsed.hostname is not None
        and parsed.hostname in (_LOOPBACK_HOSTS | get_allowed_origin_hosts())
    )


def _contains_account_fields(params: dict[str, Any]) -> bool:
    pending = [params]
    while pending:
        value = pending.pop()
        if isinstance(value, dict):
            if _PRIVATE_FIELDS.intersection(value) or any(
                isinstance(value.get(key), str) and value[key].strip().lower() in (
                    'huawei-maas-login', 'openaiaccount', 'openai_account_oauth',
                )
                for key in ('source', 'model_source', 'type', 'model_provider', 'client_provider', 'auth_mode')
            ):
                return True
            pending.extend(value.values())
        elif isinstance(value, list):
            pending.extend(value)
    return False


def validate_settings_params(method: str, params: dict[str, Any]) -> None:
    """Reject unsupported requests as a unit before running a handler."""
    if not isinstance(params, dict):
        raise ValueError('Settings parameters must be an object')
    if _contains_account_fields(params):
        raise ValueError('Account settings are unavailable in this Worker')
    if method in _EMPTY_PARAMS and params:
        raise ValueError('This settings method takes no parameters')
    if method == 'locale.set_conf':
        language = params.get('preferred_language')
        if set(params) != {'preferred_language'} or not isinstance(language, str) or (
            language.strip().lower() not in ('zh', 'en')
        ):
            raise ValueError('preferred_language must be zh or en')
        return
    if method in ('permissions.tools.update', 'permissions.tools.delete'):
        updating = method == 'permissions.tools.update'
        tool_fields = params.keys() & {'tool', 'name'}
        if len(tool_fields) != 1 or set(params) != tool_fields | ({'level'} if updating else set()):
            raise ValueError('Expected one tool and the fields for this permission operation')
        tool = params[next(iter(tool_fields))]
        if not isinstance(tool, str) or not tool.strip() or len(tool) > 256 or any(ord(c) < 32 for c in tool):
            raise ValueError('Invalid tool name')
        if updating:
            level = params['level']
            if isinstance(level, dict) and set(level) == {'*'}:
                level = level['*']
            if not isinstance(level, str) or level not in ('allow', 'ask', 'deny'):
                raise ValueError('Invalid tool permission level')
        return
    if method == 'config.set':
        if not params or not params.keys() <= (_PANEL_SWITCH_FIELDS | {'permissions_profile'}):
            raise ValueError('This Worker cannot change one or more of the requested panel settings')
        for field, value in params.items():
            if field == 'permissions_profile':
                if not isinstance(value, str) or value not in _PERMISSION_PROFILES:
                    raise ValueError('Invalid permissions profile')
            elif not isinstance(value, bool) and (
                not isinstance(value, str) or value not in ('true', 'false')
            ):
                raise ValueError('Panel switches require a boolean value')
        return
    if method != 'config.save_all':
        return
    config = params.get('config')
    if set(params) != {'config'} or not isinstance(config, dict) or not config:
        raise ValueError('config.save_all requires a non-empty config object')
    if not config.keys() <= _CONFIG_FIELDS:
        raise ValueError('This Worker cannot save one or more of the requested settings')
    for field, value in config.items():
        if value is not None and not isinstance(value, (str, bool, int)):
            raise ValueError('Settings values must be scalar')
        # The existing AS dotenv writer wraps values in double quotes without escaping.
        if field not in _YAML_FIELDS and isinstance(value, str) and any(c in value for c in '\r\n\0"\\'):
            raise ValueError('This setting contains unsupported characters')


def protect_settings_handler(
    channel: Any, method: str, handler: Callable[..., Awaitable[None]],
) -> Callable[..., Awaitable[None]]:
    """Keep settings requests out of the generic data-plane forwarding path."""
    accepts_user_id = 'user_id' in inspect.signature(handler).parameters

    async def settings_handler(ws, req_id, params, session_id, user_id=None):
        if not allows_settings_origin(getattr(ws, 'request_headers', None)):
            await channel.send_response(
                ws, req_id, ok=False, error='Settings require an allowed Web Origin', code='FORBIDDEN',
            )
            return
        try:
            validate_settings_params(method, params)
        except ValueError as exc:
            await channel.send_response(ws, req_id, ok=False, error=str(exc), code='BAD_REQUEST')
            return
        if accepts_user_id:
            await handler(ws, req_id, params, session_id, user_id=user_id)
        else:
            await handler(ws, req_id, params, session_id)

    return settings_handler
