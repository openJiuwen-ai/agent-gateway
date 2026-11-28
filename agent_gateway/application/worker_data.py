# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Assemble the existing data handlers for the internal single-AS Worker test host.

With an explicit slot this host also owns one Cron shard. The shard is prepared
here but only started when the parent sends ``CRON_ACTIVATE``.
"""

from __future__ import annotations

from contextlib import AsyncExitStack, asynccontextmanager
from functools import partial
import json
import logging

from fastapi import FastAPI, Request, WebSocket
from starlette.responses import PlainTextResponse

from agent_gateway.application.inbound import (
    MessagePreprocessor, _inject_session_work_mode, _normalize_gateway_message,
)
from agent_gateway.auth.passthrough import MODEL_AUTH_PARAM_KEY
from agent_gateway.common.ingress_identity import LocalIdentityPolicy, resolve_local_identity
from agent_gateway.transport.protocol_connection_runtime import ProtocolConnectionRuntime
from agent_gateway.transport.web_protocol_runtime import WebProtocolRuntime

logger = logging.getLogger("gateway")
_DATA_METHODS = frozenset({
    "chat.send", "chat.interrupt", "chat.resume", "connection.status", "history.get",
})
_ACP_METHODS = frozenset({"initialize", "session/new", "session/prompt", "session/cancel"})


# Requests the account-free personal Worker answers on its Web listener with the
# regular read and Session handlers. Settings, and every write that needs a
# gate of its own, are added by personal_web_methods().
PERSONAL_WEB_WORKER_METHODS = frozenset({
    "commands.list",
    "heartbeat.job.list",
    "heartbeat.job.meta",
    "locale.get_conf",
    "mcp.list",
    "path.get",
    "permissions.tools.get",
    "personal_context.runtime.status",
    "project.get_sessions",
    "project.git.diff_status",
    "project.git.status",
    "project.git.turn_diff",
    "project.git.turn_diff_list",
    "project.info",
    "project.list",
    "project.pinned_sessions",
    "session.archive",
    "session.archived.list",
    "session.assets.list",
    "session.delete",
    "session.unarchive",
    "skills.list",
})


class _DisabledAccounts:
    async def remember(self, session_id, credential_ref, model_name):
        raise RuntimeError("account bindings are not enabled in the Worker")

    async def lookup(self, session_id):
        return None

    async def model_auth_for_binding(self, binding):
        return None

    async def request_model_auth(self, auth_session, model_name):
        return None

    async def credential_update_for_ref(self, credential_ref):
        return None

    async def credential_ref_for_login_session(self, auth_session):
        return None

    async def model_auth_for_ref(self, credential_ref):
        return None


def personal_web_methods() -> frozenset[str]:
    """What the personal Worker's Web listener admits beyond the data plane."""
    from agent_gateway.application.personal_settings import PERSONAL_SETTINGS_METHODS

    return PERSONAL_WEB_WORKER_METHODS | PERSONAL_SETTINGS_METHODS


def is_data_frame(
    raw: str | bytes, *, acp: bool = False, accounts: bool = False, cron: bool = False,
    tui: bool = False, web: bool = False, personal: bool = False,
) -> bool:
    """Keep unadapted login, configuration and Cron out of this Worker host.

    ``cron=True`` admits only the owned Cron panel methods; a frame that still
    carries a credential selector is refused like every other Worker frame.
    Native TUI compatibility is limited to the account-free personal profile.
    """
    from agent_gateway.application.worker_cron_ingress import CRON_PANEL_METHODS

    try:
        frame = json.loads(raw)
    except (ValueError, RecursionError):
        return False
    if not isinstance(frame, dict):
        return False
    method = frame.get("method")
    methods = _ACP_METHODS if acp and frame.get("jsonrpc") == "2.0" else _DATA_METHODS
    if cron and not acp:
        methods = methods | CRON_PANEL_METHODS
    if personal and not acp:
        if tui:
            from agent_gateway.application.personal_tui import PERSONAL_TUI_METHODS

            methods = methods | PERSONAL_TUI_METHODS
        if web:
            methods = methods | personal_web_methods()
    if not isinstance(method, str) or method not in methods:
        return False
    params = frame.get("params")
    if not isinstance(params, dict):
        return True
    return not (
        any(key in params for key in (MODEL_AUTH_PARAM_KEY, "credential_ref", "_jiuwen_auth_session"))
        or (not accounts and any(params.get(key) == "huawei-maas-login" for key in ("source", "model_source")))
    )


def _build_web_app(channel, *, frame_allowed=is_data_frame):
    from agent_gateway.channel_manager.web import web_channel_app
    from agent_gateway.common.ingress_identity import IngressIdentityError

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, redirect_slashes=False)
    policy = LocalIdentityPolicy()

    @app.middleware("http")
    async def bind_http_identity(request: Request, call_next):
        try:
            identity = resolve_local_identity(request.scope["query_string"], request.scope["headers"],
                                              entry="http", policy=policy)
        except IngressIdentityError:
            return PlainTextResponse("invalid user identity", status_code=400)
        request.state.agentos_user_id = identity.business_user_id
        return await call_next(request)

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.websocket("/ws")
    async def web(websocket: WebSocket) -> None:
        try:
            identity = resolve_local_identity(websocket.scope["query_string"], websocket.scope["headers"],
                                              entry="web", policy=policy)
        except IngressIdentityError:
            await websocket.close(code=1008)
            return
        await web_channel_app._serve_channel_websocket(
            channel, websocket, local_user_id=identity.business_user_id or "",
            frame_allowed=frame_allowed,
        )

    return app


async def _reject_config(*args, **kwargs) -> None:
    raise RuntimeError("configuration changes are not enabled in the Worker")


async def _no_refresh(_message) -> None:
    pass


def _wire_cron_command_runtime(
    *, control_client, message_handler, cron_runtime, manifest, slot_id,
):
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands
    from agent_gateway.cron.ownership import CronOwnership

    cron_commands = WorkerCronCommands(
        client=control_client,
        message_handler=message_handler,
        ownership=CronOwnership(manifest, LocalIdentityPolicy()),
        slot_id=slot_id,
        # A delivered command is refused until the parent activated this slot.
        enabled=lambda: cron_runtime._activated.is_set(),
    )
    message_handler.set_cron_command_runtime(cron_commands)
    message_handler.set_cron_tool_push_enabled(True)
    return cron_commands


@asynccontextmanager
async def data_worker(
    agent_server_url: str, *, control_client=None, on_credentials=None, accounts: bool = True,
    cron_slot: int | None = None, cron_manifest=None,
):
    """Yield ``(runtimes, web_runtime, cron)``.

    ``cron`` is ``None``, or ``(runtime, commands)`` for the owned Cron shard.
    ``accounts`` must be false when the Control client exists only for Cron.
    """
    from agent_gateway.common.worker_affinity import WorkerManifest

    cron_enabled = cron_slot is not None or cron_manifest is not None
    if cron_enabled and (type(cron_manifest) is not WorkerManifest or type(cron_slot) is not int
                         or cron_slot not in cron_manifest.slots):
        raise ValueError("sharded Cron requires an explicit Worker manifest and slot")
    if cron_enabled and control_client is None:
        raise ValueError("Worker Cron requires its process Control client")
    from agent_gateway.application.preparation import callback_framework
    from agent_gateway.application.tui_acp import assemble_tui_acp
    from agent_gateway.application.web import assemble_web
    from agent_gateway.channel_manager.channel_manager import ChannelManager
    from agent_gateway.extensions.registry import ExtensionRegistry
    from agent_gateway.message_handler.message_handler import MessageHandler
    from agent_gateway.routing.third_agent import get_unsupported_third_agent
    from agent_gateway.southbound.orchestration.client import assemble_client

    async with AsyncExitStack() as cleanup:
        from agent_gateway.application.personal_settings import (
            PERSONAL_SETTINGS_METHODS, is_local_settings_target, settings_transport_logger,
        )
        from agent_gateway.application.personal_tui import enable_personal_tui

        registry = ExtensionRegistry.create_instance(
            callback_framework=None, callback_framework_factory=callback_framework, config={}, logger=logger,
        )
        cleanup.callback(ExtensionRegistry.reset_instance)
        personal = not (control_client is not None and accounts)
        personal_settings = personal and is_local_settings_target(agent_server_url)
        wire_options = {}
        if personal_settings:
            wire_options = {
                "wire_logger": settings_transport_logger(), "private_methods": PERSONAL_SETTINGS_METHODS,
            }
        client = assemble_client(None, mode="websocket", enterprise=False, uri=agent_server_url, **wire_options)
        cleanup.push_async_callback(client.disconnect)
        await client.connect(agent_server_url)
        credentials = None
        if control_client is not None and accounts:
            from agent_gateway.auth.worker_credential_services import WorkerCredentialServices

            credentials = WorkerCredentialServices.from_client(
                control_client, server_id=agent_server_url, agent_client=client,
            )
            cleanup.push_async_callback(credentials.stop)
            if on_credentials is not None:
                on_credentials(credentials)
        accounts = credentials.bindings if credentials is not None else _DisabledAccounts()
        # The account-free profile is the personal Worker: it alone offers the
        # native TUI compatibility requests, and only to a loopback AgentServer.
        personal_tui = personal and enable_personal_tui(agent_server_url)
        handler = MessageHandler(client, session_login_bindings=accounts)
        if credentials is not None:
            credentials.attach_handler(handler)
        else:
            handler.set_credential_access(accounts)
        manager = ChannelManager(handler, config={})
        handler.set_channel_manager(manager)
        cleanup.push_async_callback(handler.stop_forwarding)
        await handler.start_forwarding()
        cleanup.push_async_callback(manager.stop_dispatch)
        await manager.start_dispatch()
        preprocessor = MessagePreprocessor(
            _no_refresh, partial(_normalize_gateway_message, apply_model_auth=False), _inject_session_work_mode,
        )
        if credentials is not None:
            preprocessor = preprocessor.with_credential_access(credentials.credentials)
        cron = None
        if cron_enabled:
            from agent_gateway.application.worker_cron import build_worker_cron

            cron_runtime = await build_worker_cron(
                manifest=cron_manifest, slot_id=cron_slot, agent_client=client,
                message_handler=handler,
            )
            cron_commands = _wire_cron_command_runtime(
                control_client=control_client, message_handler=handler, cron_runtime=cron_runtime,
                manifest=cron_manifest, slot_id=cron_slot,
            )
            cron = (cron_runtime, cron_commands)
            # Unwound after the northbound runtimes below and before the
            # handlers, credentials and southbound connection: a stop must
            # still reach the AgentServer to confirm every active run ended.
            cleanup.push_async_callback(cron_runtime.stop)
            cleanup.push_async_callback(cron_commands.stop)

        async with AsyncExitStack() as northbound:
            web, _ = assemble_web(
                agent_client=client, message_handler=handler, channel_manager=manager,
                extension_registry=registry, on_config_saved=_reject_config, heartbeat_service=None,
                cron_controller=cron_runtime.controller if cron is not None else None,
                heartbeat_controller=None, updater_service=None,
                host="127.0.0.1", port=0, path="/ws", ws_permessage_deflate=False,
                preprocessor=preprocessor, log_stage=lambda _stage: None,
                personal_settings=personal_settings,
            )
            northbound.push_async_callback(web.stop)
            tui = await assemble_tui_acp(
                agent_client=client, message_handler=handler, channel_manager=manager,
                third_agent=get_unsupported_third_agent(), on_config_saved=_reject_config,
                cron_controller=cron_runtime.controller if cron is not None else None,
                heartbeat_controller=None, preprocessor=preprocessor,
                log_stage=lambda _stage: None, personal_tui=personal_tui,
                web_session_busy=web.is_session_busy,
            )
            northbound.push_async_callback(tui.stop)
            if cron is not None:
                from agent_gateway.application.worker_cron_ingress import bind_owned_cron_handlers

                bind_owned_cron_handlers(web, tui.protocol_runtime)
            await tui.tui_channel.start()
            frame_allowed = partial(
                is_data_frame, accounts=credentials is not None, cron=cron is not None, personal=personal,
            )
            web_app = _build_web_app(web, frame_allowed=partial(frame_allowed, web=True))
            web_runtime = WebProtocolRuntime(
                web_app, ws_per_message_deflate=False, single_request_http=True,
            )
            northbound.push_async_callback(web_runtime.stop)
            await web_runtime.start()
            protocol = ProtocolConnectionRuntime(tui.protocol_runtime, frame_allowed=frame_allowed)
            northbound.push_async_callback(protocol.stop)
            await protocol.start()
            runtimes = {
                "web-http": web_runtime, "web-ws": _WebSocketAdopter(web_runtime), "protocol": protocol,
            }
            yield runtimes, web_runtime, cron


class _WebSocketAdopter:
    def __init__(self, runtime: WebProtocolRuntime) -> None:
        self._runtime = runtime

    async def adopt(self, sock, prefix: bytes) -> None:
        await self._runtime.adopt(sock, prefix, protocol="websocket")
