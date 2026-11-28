# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Public settings remain a Web-only, strictly bounded RPC surface."""

import json
import logging
from types import SimpleNamespace

import pytest
from e2a.models import E2AEnvelope
from starlette.datastructures import Headers

from agent_gateway.application.personal_settings import (
    PERSONAL_SETTINGS_METHODS, allows_settings_origin, is_local_settings_target, settings_transport_logger,
    validate_settings_params,
)
from agent_gateway.application.personal_web_operations import PERSONAL_WEB_OPERATIONS
from agent_gateway.application.web import _PersonalSettingsRegistration
from agent_gateway.application.worker_data import (
    _DATA_METHODS, PERSONAL_WEB_WORKER_METHODS, is_data_frame, personal_web_methods,
)
from agent_gateway.channel_manager.web import app_web_handlers
from agent_gateway.channel_manager.web.app_web_handlers import WebHandlersBindParams, _register_web_handlers
from agent_gateway.routing.agent_client import WebSocketAgentServerClient
from agent_gateway.routing.agent_request_timeout import AgentRequestTimeoutError
from tests.unit_tests.gateway.test_agent_client_reconnect_headers import transport


class Channel:
    def __init__(self):
        self.channel_id = "web"
        self.methods: dict[str, object] = {}
        self.local_only: set[str] = set()
        self.responses: list[dict] = []
        self.connect_handler = None
        self.disconnect_handlers: list = []

    def register_method(self, name, handler, *, local_only=False):
        self.methods[name] = handler
        if local_only:
            self.local_only.add(name)

    def registered_methods(self):
        return self.methods.keys()

    def on_connect(self, handler):
        self.connect_handler = handler

    def on_disconnect(self, handler):
        self.disconnect_handlers.append(handler)

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
        self.responses.append({"id": req_id, "ok": ok, "payload": payload, "error": error, "code": code})

    async def routing_keys_for_ws(self, ws):
        return []

    @staticmethod
    def connection_user_id(ws):
        return getattr(ws, "_web_connection_user_id", None)

    def a4p_pending_routes(self, routes, *, ws, session_id):
        return set()


def register(channel, **kwargs):
    _register_web_handlers(WebHandlersBindParams(
        channel=_PersonalSettingsRegistration(channel), **kwargs,
    ))
    return channel


def test_settings_target_is_a_loopback_agentserver():
    for url in ("ws://127.0.0.1:9000", "ws://[::1]:9000", "ws://localhost:9000", "wss://localhost:9000"):
        assert is_local_settings_target(url), url
    for url in ("ws://example.test:9000", "ws://localhost.example:9000", "ws://127.0.0.1.example:9000",
                "ws://192.168.1.2:9000", "", "not a url", "http://[::1"):
        assert not is_local_settings_target(url), url


@pytest.mark.parametrize("failure", ["timeout", "disconnect", "restart-required"])
async def test_save_preserves_single_attempt_and_agentserver_outcome(monkeypatch, failure):
    monkeypatch.setattr("agent_gateway.enterprise.is_enterprise", lambda: False)
    requests = []

    class Client:
        async def send_request(self, request):
            requests.append(request)
            if failure == "timeout":
                raise AgentRequestTimeoutError()
            if failure == "disconnect":
                raise ConnectionError("connection lost")
            return SimpleNamespace(ok=True, payload={"updated": ["embed_model"], "applied_without_restart": False})

    channel = register(Channel(), agent_client=Client())
    ws = SimpleNamespace(request_headers=Headers({"Origin": "http://localhost:5173"}))
    payload = {"config": {"embed_model": "fixture-model"}}
    await channel.methods["config.save_all"](ws, "save", payload, "session", "user")
    assert [request.params for request in requests] == [payload]
    assert requests[0].user_id == "user"
    result = channel.responses[-1]
    if failure == "restart-required":
        assert result["ok"] is True
        assert result["payload"] == {"updated": ["embed_model"], "applied_without_restart": False}
    else:
        assert result["ok"] is False
        assert result["code"] == ("AGENT_SERVER_TIMEOUT" if failure == "timeout" else "SERVICE_UNAVAILABLE")
    assert len(channel.responses) == 1


def test_registration_guard_only_touches_settings_methods():
    async def handler(ws, req_id, params, session_id, user_id=None):
        return (params, user_id)

    # Outside the personal Worker nothing changes: the same handlers, the same routing.
    plain = Channel()
    _register_web_handlers(WebHandlersBindParams(channel=plain, agent_client=None))
    guarded = Channel()
    proxy = _PersonalSettingsRegistration(guarded)
    proxy.register_method("session.custom", handler)
    proxy.register_method("session.custom.local", handler, local_only=True)
    register(guarded, agent_client=None)

    assert plain.methods.keys() == guarded.methods.keys() - {"session.custom", "session.custom.local"}
    # Other methods keep their handler and routing; only the personal methods become local-only.
    assert guarded.methods["session.custom"] is handler
    assert guarded.methods["session.custom.local"] is handler
    assert guarded.local_only == (
        plain.local_only | PERSONAL_SETTINGS_METHODS | PERSONAL_WEB_OPERATIONS | {'session.custom.local'}
    )
    assert plain.local_only.isdisjoint(PERSONAL_SETTINGS_METHODS | PERSONAL_WEB_OPERATIONS)
    # Every settings method has a handler to guard, and only those are wrapped.
    assert PERSONAL_SETTINGS_METHODS <= guarded.methods.keys()
    for method, handler in guarded.methods.items():
        wrapped = getattr(handler, "__name__", "") == "settings_handler"
        assert wrapped == (method in PERSONAL_SETTINGS_METHODS), method
    assert {"path.set", "cron.job.create", "project.create", "skills.install"}.isdisjoint(PERSONAL_SETTINGS_METHODS)
    # The proxy is a registration-only view: attributes belong to the channel.
    proxy.marker = "channel"
    assert guarded.marker == "channel" and proxy.registered_methods is not None


async def test_settings_request_is_forwarded_once_with_connection_identity(monkeypatch):
    monkeypatch.setattr("agent_gateway.enterprise.is_enterprise", lambda: False)
    requests = []

    class Client:
        async def send_request(self, request):
            requests.append(request)
            return SimpleNamespace(ok=True, payload={"preferred_language": "zh"})

    channel = register(Channel(), agent_client=Client())
    await channel.methods["config.get"](
        SimpleNamespace(request_headers=Headers({"Origin": "http://127.0.0.1:5173"})), "get", {}, "session", "owner",
    )
    assert [request.method for request in requests] == ["config.get"]
    assert requests[0].params == {} and requests[0].user_id == "owner"
    assert channel.responses == [
        {"id": "get", "ok": True, "payload": {"preferred_language": "zh"}, "error": None, "code": None},
    ]
    channel.responses.clear()
    requests.clear()
    for headers, params, code in (
        (Headers(), {}, "FORBIDDEN"),
        (Headers({"Origin": "https://untrusted.example"}), {}, "FORBIDDEN"),
        (Headers({"Origin": "null"}), {}, "FORBIDDEN"),
        (Headers({"Origin": "http://127.0.0.1:5173"}), {"key": "model_api_key"}, "BAD_REQUEST"),
        (Headers({"Origin": "http://127.0.0.1:5173"}), {"_gateway_login_catalog": {}}, "BAD_REQUEST"),
        (Headers({"Origin": "http://127.0.0.1:5173"}), "not an object", "BAD_REQUEST"),
    ):
        await channel.methods["config.get"](
            SimpleNamespace(request_headers=headers), "denied", params, "session", "owner",
        )
        assert channel.responses[-1]["ok"] is False and channel.responses[-1]["code"] == code, code
    # A refused request never reaches the AgentServer, and the socket stays usable.
    assert requests == []
    assert len(channel.responses) == 6


def test_settings_transport_logger_keeps_frames_out_of_debug_logs(monkeypatch):
    monkeypatch.setattr(logging.getLogger(), "level", logging.DEBUG)
    logger = settings_transport_logger()
    assert logger.name == "gateway.personal_settings.transport"
    assert not logger.isEnabledFor(logging.DEBUG) and logger.isEnabledFor(logging.INFO)


async def test_settings_debug_logging_omits_rpc_body(monkeypatch, caplog):
    calls, sockets = transport(monkeypatch)
    client = WebSocketAgentServerClient(
        wire_logger=settings_transport_logger(), private_methods=PERSONAL_SETTINGS_METHODS,
    )
    secret = "synthetic-personal-settings-log-marker"
    try:
        await client.connect("ws://127.0.0.1:9000")
        assert calls[0][1]["logger"] is client._wire_logger
        with caplog.at_level(logging.DEBUG, logger="agent_gateway.southbound.data.websocket"):
            await client.send_request(E2AEnvelope(
                request_id="save", channel="web", method="config.save_all",
                params={"config": {"embed_api_key": secret}},
            ), timeout=1)
            await client.send_request(E2AEnvelope(
                request_id="plain", channel="web", method="project.list", params={"marker": secret},
            ), timeout=1)
        sent = [json.dumps(frame) for frame in sockets[0].sent]
        assert [secret in frame for frame in sent] == [True, True]
        bodies = [record.getMessage() for record in caplog.records if "发送请求" in record.getMessage()]
        assert sum("project.list" in body for body in bodies) == 2
        # The private request reaches the socket, but neither of its bodies is logged.
        assert not any(secret in body for body in bodies if "config.save_all" in body)
        assert sum(secret in body for body in bodies) == 2
    finally:
        await client.disconnect()
    plain_calls, _ = transport(monkeypatch)
    plain = WebSocketAgentServerClient()
    try:
        await plain.connect("ws://127.0.0.1:9000")
    finally:
        await plain.disconnect()
    # Without the option the single-process client connects exactly as before.
    assert "logger" not in plain_calls[0][1] and not plain._private_methods


def test_client_assembly_passes_the_log_options_only_when_requested(monkeypatch):
    """Without the options the single process builds its client exactly as on main."""
    from agent_gateway.southbound.orchestration import client as southbound

    calls = []

    def build(uri, **options):
        calls.append((uri, options))
        return object()

    monkeypatch.setattr(southbound, "build_static_client", build)
    for options in ({}, {"wire_logger": settings_transport_logger(),
                         "private_methods": PERSONAL_SETTINGS_METHODS}):
        southbound.assemble_client(None, mode="websocket", enterprise=False, uri="ws://127.0.0.1:9000", **options)
    assert calls[0] == ("ws://127.0.0.1:9000", {"shared_directory_layout": True}), calls[0]
    # Requested: both travel to the builder that owns the connection.
    assert calls[1] == ("ws://127.0.0.1:9000", {
        "shared_directory_layout": True, "wire_logger": calls[1][1]["wire_logger"],
        "private_methods": PERSONAL_SETTINGS_METHODS,
    })
    assert calls[1][1]["wire_logger"].name == "gateway.personal_settings.transport"


def test_settings_requires_single_http_origin(monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS", "desk.example,none")
    assert allows_settings_origin(Headers({"Origin": "https://desk.example:443"}))
    assert allows_settings_origin(Headers({"Origin": "http://[::1]:5173"}))
    assert allows_settings_origin(Headers({"Origin": "http://127.0.0.1"}))
    assert not allows_settings_origin(Headers({}))
    assert not allows_settings_origin(None)
    assert not allows_settings_origin(Headers({"Origin": "https://localhost.example"}))
    assert not allows_settings_origin(Headers({"Origin": "http://user:pw@localhost:5173"}))
    assert not allows_settings_origin(Headers({"Origin": "http://localhost:5173/path"}))
    assert not allows_settings_origin(Headers({"Origin": "http://localhost:0"}))
    assert not allows_settings_origin(Headers(raw=[(b"origin", b"http://localhost"), (b"origin", b"http://localhost")]))


@pytest.mark.parametrize("method, params", [
    ("config.save_all", {"config": {"embed_model": ["wrong type"]}}),
    ("config.save_all", {"config": {"embed_api_key": "fixture\nsecond line"}}),
    ("config.save_all", {"config": {"embed_api_key": 'fixture"quote'}}),
    ("config.save_all", {"config": {"agents": {}}}),
    ("config.save_all", {"config": {}}),
    ("config.save_all", {"config": {"embed_model": "m"}, "other": 1}),
    ("config.save_all", "not an object"),
    ("models.upsert", {"model": {"model_client_config": {"client_provider": "OpenAIAccount"}}}),
    ("models.list", {"_gateway_login_catalog": {}}),
    ("models.get", {"nested": [{"credential_ref": "ref"}]}),
    ("config.get", {"key": "model_api_key"}),
    ("config.set", {}),
    ("config.set", {"external_cli_agent_codex_enabled": True}),
    ("config.set", {"setup_guide_enabled": "maybe"}),
    ("config.set", {"permissions_profile": "owner"}),
    ("config.validate_model", {"model": {"source": "huawei-maas-login"}}),
    ("locale.set_conf", {"preferred_language": "other"}),
    ("locale.set_conf", {}),
    ("locale.set_conf", {"preferred_language": "en", "user_id": "another"}),
    ("locale.set_conf", {"preferred_language": None}),
    ("permissions.tools.update", {"tool": "read_file", "level": {"*": "allow", "path": "deny"}}),
    ("permissions.tools.update", {"tool": "read_file", "level": "ask", "session_id": "s"}),
    ("permissions.tools.update", {"tool": "a", "name": "b", "level": "allow"}),
    ("permissions.tools.update", {"tool": "read_file"}),
    ("permissions.tools.update", {"tool": "read_file", "level": "always"}),
    ("permissions.tools.update", {"tool": "read_file", "level": True}),
    ("permissions.tools.update", {"tool": "read\x00file", "level": "allow"}),
    ("permissions.tools.update", {"tool": "", "level": "allow"}),
    ("permissions.tools.update", {"tool": "x" * 257, "level": "allow"}),
    ("permissions.tools.delete", {"tool": "read_file", "user_id": "another"}),
    ("permissions.tools.delete", {"tool": "read_file", "level": "allow"}),
    ("permissions.tools.delete", {}),
    ("permissions.tools.delete", {"tool": "a", "name": "b"}),
])
def test_settings_rejects_unsupported_write_and_private_payloads(method, params):
    with pytest.raises(ValueError):
        validate_settings_params(method, params)


@pytest.mark.parametrize("method, params", [
    ("locale.set_conf", {"preferred_language": "en"}),
    ("locale.set_conf", {"preferred_language": " ZH "}),
    ("permissions.tools.update", {"tool": "read_file", "level": "deny"}),
    ("permissions.tools.update", {"name": "read_file", "level": {"*": "ask"}}),
    ("permissions.tools.delete", {"name": "read_file"}),
    ("config.get", {}),
    ("models.list", {}),
    ("config.set", {"setup_guide_enabled": True}),
    ("config.set", {"symphony_enabled": "false", "permissions_profile": "automatic"}),
    ("config.save_all", {"config": {"free_search_ddg_enabled": "true", "skill_evolution": "a\nb"}}),
    ("config.save_all", {"config": {"vision_api_key": "k", "music_gen_protocol": "p", "ttse_enabled": None}}),
    ("models.upsert", {"model": {"model": "m", "name": "display"}}),
    ("vendors.fetch_models", {"vendor": "openai"}),
    ("locale.set_conf", {"preferred_language": "en"}),
    ("permissions.tools.update", {"tool": "write_file", "level": "deny"}),
    ("permissions.tools.delete", {"tool": "write_file"}),
])
def test_settings_accept_what_the_old_page_sends(method, params):
    validate_settings_params(method, params)


def test_personal_web_listener_admits_only_its_methods():
    admitted = personal_web_methods()
    assert admitted == PERSONAL_WEB_WORKER_METHODS | PERSONAL_SETTINGS_METHODS | PERSONAL_WEB_OPERATIONS
    assert admitted.isdisjoint(_DATA_METHODS)

    def frame(method, **params):
        return json.dumps({"type": "req", "id": "1", "method": method, "params": params})

    for method in sorted(admitted):
        assert is_data_frame(frame(method), web=True, personal=True, compat=True), method
        assert is_data_frame(frame(method, session_id="s"), web=True, personal=True, compat=True), method
    for method in ("cron.job.list", "project.delete", "skills.install",
                   "path.set", "designer.graph.list", "session.create", "models.replace_all", "models.validate",
                   "permissions.tools.set", "openai_account.auth.status", "external_cli.install", "webui.dispatch"):
        assert not is_data_frame(frame(method), web=True, personal=True, compat=True), method
    # The additions belong to the personal Worker's Web listener alone.
    assert not any(is_data_frame(frame(method), web=True) for method in admitted)
    assert not any(is_data_frame(frame(method), personal=True) for method in admitted)
    assert not any(is_data_frame(frame(method), personal=True, compat=True) for method in admitted)
    assert not any(is_data_frame(frame(method), tui=True, personal=True) for method in PERSONAL_SETTINGS_METHODS - {
        "config.get", "models.list"})
    assert not any(is_data_frame(frame(method), tui=True, personal=True) for method in PERSONAL_WEB_OPERATIONS)
    assert not any(is_data_frame(frame(method), web=True, personal=True, compat=True, acp=True) for method in admitted)
    assert is_data_frame(frame("history.get", session_id="s"), web=True, personal=True)
    assert is_data_frame(frame("config.get"), tui=True, personal=True, compat=True)
    # The public gate still drops requests that carry private authority.
    for key in ("_model_auth", "credential_ref", "_jiuwen_auth_session"):
        assert not is_data_frame(frame("config.get", **{key: "x"}), web=True, personal=True, compat=True)
    assert not is_data_frame(frame("config.get", source="huawei-maas-login"), web=True, personal=True, compat=True)
    for raw in ("not json", "[]", "null", '{"type": "req", "method": "config.get",',
                json.dumps({"type": "req", "method": 1})):
        assert not is_data_frame(raw, web=True, personal=True)
    # A non-object params value has always passed the adapter; the settings
    # guard is what answers it, as BAD_REQUEST.
    assert is_data_frame(json.dumps({"type": "req", "method": "config.get", "params": []}), web=True,
                         personal=True, compat=True)
    with pytest.raises(ValueError):
        validate_settings_params("config.get", [])


@pytest.mark.parametrize("method, params, payload", [
    ("locale.get_conf", {}, {"preferred_language": "en"}),
    ("locale.set_conf", {"preferred_language": "en"}, {"preferred_language": "en"}),
    ("permissions.tools.get", {}, {"tools": {"read_file": "ask"}}),
    ("permissions.tools.update", {"tool": "read_file", "level": "deny"}, {"read_file": "deny"}),
    ("permissions.tools.update", {"name": "read_file", "level": {"*": "ask"}}, {}),
    ("permissions.tools.delete", {"tool": "read_file"}, {}),
])
async def test_personal_settings_route_panel_operations_to_as(monkeypatch, method, params, payload):
    monkeypatch.setattr("agent_gateway.enterprise.is_enterprise", lambda: False)
    requests = []

    class Client:
        async def send_request(self, request):
            requests.append(request)
            return SimpleNamespace(ok=True, payload=payload)

    channel = register(Channel(), agent_client=Client(), personal_settings=True)
    assert {"permissions.tools.set", "permissions.tools.update_user"} <= channel.methods.keys()
    await channel.methods[method](
        SimpleNamespace(request_headers=Headers({"Origin": "http://localhost:5173"})), "panel", params, "session", "user",
    )
    assert [(request.method, request.params or {}) for request in requests] == [(method, params)]
    assert requests[0].user_id == "user"
    assert channel.responses[-1]["payload"] == payload


async def test_personal_settings_never_read_or_write_the_gateway_config(monkeypatch):
    monkeypatch.setattr("agent_gateway.enterprise.is_enterprise", lambda: False)
    touched = []

    def read_config():
        touched.append("read")
        return {"preferred_language": "zh"}

    monkeypatch.setattr(app_web_handlers, "get_config", read_config)
    monkeypatch.setattr(app_web_handlers, "update_preferred_language_in_config",
                        lambda language: touched.append(language))
    ws = SimpleNamespace(request_headers=Headers({"Origin": "http://127.0.0.1:5173"}))
    # Without a reachable AgentServer the personal Worker answers unavailable...
    channel = register(Channel(), agent_client=None, personal_settings=True)
    await channel.methods["locale.get_conf"](ws, "get", {}, "session", "user")
    await channel.methods["locale.set_conf"](ws, "set", {"preferred_language": "en"}, "session", "user")
    assert touched == []
    assert [response["code"] for response in channel.responses] == ["SERVICE_UNAVAILABLE"] * 2
    # ...where the single process keeps its local fallback, as before.
    plain = Channel()
    _register_web_handlers(WebHandlersBindParams(channel=plain, agent_client=None))
    await plain.methods["locale.get_conf"](ws, "get", {}, "session", "user")
    await plain.methods["locale.set_conf"](ws, "set", {"preferred_language": "en"}, "session", "user")
    assert touched == ["read", "en"]
    assert [response["ok"] for response in plain.responses] == [True, True]


def test_the_personal_worker_registers_the_agentserver_push_handler_itself():
    """MessageHandler does not register its server-push handler in ``__init__``.

    The single process gets it from ``set_outbound_pipeline``, which the Worker
    assembly never calls. A Worker that does not register it drops every
    AgentServer push as it arrives.
    """
    import inspect

    from agent_gateway.application import worker_data
    from agent_gateway.message_handler.message_handler import MessageHandler

    module = inspect.getsource(MessageHandler)
    registration = module.index("self._register_agent_server_push_handler()")
    enclosing = module[module.rfind("\n    def ", 0, registration):]
    assert enclosing.startswith("\n    def set_outbound_pipeline("), "moved: drop the Worker's own call"
    assert "handler._register_agent_server_push_handler()" in inspect.getsource(worker_data.data_worker)
