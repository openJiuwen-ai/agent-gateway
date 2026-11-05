# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import asyncio
from types import SimpleNamespace

import pytest
from openjiuwen_protocol.e2a.agent_models import AgentResponse
from openjiuwen_protocol.message import ReqMethod

from agent_gateway.channel_manager.tui import tui_connect as tui_connect_module
from agent_gateway.channel_manager.tui.tui_connect import (
    CliHandlersBindParams,
    CliRouteBindParams,
    build_cli_route_binding,
    register_cli_handlers,
)
from agent_gateway.common.config_panel import tui_models_handlers
from agent_gateway.heartbeat import HeartbeatServiceUnavailableError
from agent_gateway.routing.agent_client import WebSocketAgentServerClient


class FakeGatewayServer:
    """Fake GatewayServer for testing CLI handler registration."""

    def __init__(self):
        self.local_handlers: dict[str, dict] = {}
        self.responses = []
        self.session_owners = {}
        self.channel_id = "tui"

    def register_local_handler(self, path, method, handler):
        self.local_handlers.setdefault(path, {})[method] = handler

    def bind_session_owner(self, channel_id, session_id, ws):
        self.session_owners[(channel_id, session_id)] = ws

    def is_session_bound_to_client(self, channel_id, session_id, ws):
        return self.session_owners.get((channel_id, session_id)) is ws

    def get_active_session_ids(self, _channel_id, *, exclude_ws=None):
        del exclude_ws
        return set()

    async def send_response(self, ws, req_id, *, ok, payload=None, error=None, code=None):
        del ws
        self.responses.append(
            {
                "id": req_id,
                "ok": ok,
                "payload": payload or {},
                "error": error,
                "code": code,
            }
        )


class FakeAgentClient:
    def __init__(self, payload=None):
        self.requests = []
        self.payload = payload if payload is not None else {"ok": True}

    async def send_request(self, request):
        self.requests.append(request)
        return SimpleNamespace(ok=True, payload=self.payload)


class FakeMessageHandler:
    def __init__(self):
        self.cancelled = []
        self.scheduled = []
        self.scheduled_delays = []
        self.reconnected = []
        self.disconnected_websockets = []

    async def unregister_ws_subscriptions(self, channel_id, ws_id):
        self.disconnected_websockets.append((channel_id, ws_id))
        return 1

    async def cancel_agent_sessions_on_disconnect(
        self, session_keys, *, stale_request_keys=None, user_id=None
    ):
        del user_id
        self.cancelled.append((session_keys, stale_request_keys or []))
        return True

    async def schedule_cancel_agent_sessions_on_disconnect(
        self,
        session_keys,
        *,
        stale_request_keys=None,
        delay_seconds=60.0,
        user_id=None,
        mode=None,
    ):
        del user_id, mode
        self.scheduled.append((session_keys, stale_request_keys or []))
        self.scheduled_delays.append(delay_seconds)

    def cancel_scheduled_disconnect_cancel(self, channel_id, session_id):
        self.reconnected.append((channel_id, session_id))
        return True


class BlockingScheduledDisconnectMessageHandler(FakeMessageHandler):
    def __init__(self):
        super().__init__()
        self.schedule_started = asyncio.Event()
        self.release_schedule = asyncio.Event()

    async def schedule_cancel_agent_sessions_on_disconnect(
        self,
        session_keys,
        *,
        stale_request_keys=None,
        delay_seconds=60.0,
        user_id=None,
        mode=None,
    ):
        self.schedule_started.set()
        await self.release_schedule.wait()
        await super().schedule_cancel_agent_sessions_on_disconnect(
            session_keys,
            stale_request_keys=stale_request_keys,
            delay_seconds=delay_seconds,
            user_id=user_id,
            mode=mode,
        )


class FailedOnceScheduledDisconnectMessageHandler(FakeMessageHandler):
    def __init__(self):
        super().__init__()
        self.schedule_attempts = 0

    async def schedule_cancel_agent_sessions_on_disconnect(
        self,
        session_keys,
        *,
        stale_request_keys=None,
        delay_seconds=60.0,
        user_id=None,
        mode=None,
    ):
        self.schedule_attempts += 1
        if self.schedule_attempts == 1:
            raise RuntimeError("cleanup scheduling failed")
        await super().schedule_cancel_agent_sessions_on_disconnect(
            session_keys,
            stale_request_keys=stale_request_keys,
            delay_seconds=delay_seconds,
            user_id=user_id,
            mode=mode,
        )


def _register(server, **kwargs):
    register_cli_handlers(CliHandlersBindParams(channel=server, path="/tui", **kwargs))


def _offline_local_client() -> WebSocketAgentServerClient:
    return WebSocketAgentServerClient()


def _assert_control_method_unavailable(server: FakeGatewayServer) -> None:
    response = server.responses[-1]
    assert response["ok"] is False
    assert response["code"] == "SERVICE_UNAVAILABLE"


@pytest.mark.asyncio
async def test_heartbeat_cli_reports_unavailable_and_missing_job_codes():
    server = FakeGatewayServer()
    _register(server, heartbeat_controller=None)
    await server.local_handlers["/tui"]["heartbeat.job.list"](
        object(), "list-unavailable", {}, "session-current"
    )
    assert server.responses[-1]["code"] == "SERVICE_UNAVAILABLE"

    class Controller:
        async def get_job(self, *_args, **_kwargs):
            raise KeyError("missing")

        async def get_meta(self, **_kwargs):
            raise HeartbeatServiceUnavailableError("agentserver offline")

    server = FakeGatewayServer()
    _register(server, heartbeat_controller=Controller())
    await server.local_handlers["/tui"]["heartbeat.job.get"](
        object(), "get-missing", {"id": "missing"}, "session-current"
    )
    assert server.responses[-1]["code"] == "NOT_FOUND"
    await server.local_handlers["/tui"]["heartbeat.job.meta"](
        object(), "meta-unavailable", {}, "session-current"
    )
    assert server.responses[-1]["code"] == "SERVICE_UNAVAILABLE"


@pytest.mark.asyncio
async def test_register_cli_handlers_registers_local_methods():
    server = FakeGatewayServer()
    _register(server, agent_client=None, message_handler=None, on_config_saved=None)

    handlers = server.local_handlers["/tui"]
    for method in (
        "config.get",
        "config.validate_model",
        "session.list",
        "chat.send",
        "chat.resume",
        "history.get",
        "tui.disconnect",
        "harmonyos.dev_init",
        "harmonyos.dev_init_cancel",
        "harmonyos.project_init",
        "memory.list",
        "memory.edit",
        "memory.status",
        "memory.toggle",
        "memory.open",
    ):
        assert method in handlers

    await handlers["chat.send"](object(), "req-1", {}, "sess-1")
    assert server.responses == [
        {
            "id": "req-1",
            "ok": True,
            "payload": {"accepted": True, "session_id": "sess-1"},
            "error": None,
            "code": None,
        }
    ]


@pytest.mark.asyncio
async def test_memory_list_forwards_to_agentserver_with_routing_user():
    server = FakeGatewayServer()
    client = FakeAgentClient({"files": [], "mode": "code"})
    _register(server, agent_client=client)

    await server.local_handlers["/tui"]["memory.list"](
        object(), "req-memory", {"mode": "code"}, "sess-memory", "user-a"
    )

    assert len(client.requests) == 1
    request = client.requests[0]
    assert request.method == ReqMethod.MEMORY_LIST.value
    assert request.user_id == "user-a"
    assert request.params == {"mode": "code"}
    assert request.session_id == "sess-memory"
    assert server.responses[-1]["ok"] is True
    assert server.responses[-1]["payload"] == {"files": [], "mode": "code"}


def _install_hanging_dev_init_send(monkeypatch, *, record=None):
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def fake_send_agent_request_with_timeout(
        _client, env, *, label, timeout_seconds=None
    ):
        del timeout_seconds
        if record is not None:
            record.append((env, label))
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    monkeypatch.setattr(
        tui_connect_module,
        "send_agent_request_with_timeout",
        fake_send_agent_request_with_timeout,
    )
    return started, cancelled


def _install_echo_dev_init_send(monkeypatch, *, record=None):
    async def fake_send_agent_request_with_timeout(
        _client, env, *, label, timeout_seconds=None
    ):
        del label, timeout_seconds
        if record is not None:
            record.append(env)
        return AgentResponse(
            request_id=env.request_id,
            channel_id="tui",
            ok=True,
            payload={"ok": True},
        )

    monkeypatch.setattr(
        tui_connect_module,
        "send_agent_request_with_timeout",
        fake_send_agent_request_with_timeout,
    )


@pytest.mark.asyncio
async def test_harmonyos_dev_init_cancel_waits_for_background_cleanup(monkeypatch):
    server = FakeGatewayServer()
    received_envs = []
    started, cancelled = _install_hanging_dev_init_send(monkeypatch, record=received_envs)
    _register(server, agent_client=object())
    ws = type("FakeWs", (), {})()
    handlers = server.local_handlers["/tui"]

    await handlers["harmonyos.dev_init"](
        ws,
        "req-init",
        {"operationId": "dev-init-op-1", "skipDevecocliUpdate": True},
        "sess-1",
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    assert len(received_envs) == 1
    env, label = received_envs[0]
    assert env.method == ReqMethod.HARMONYOS_DEV_INIT.value
    assert env.params == {"skipDevecocliUpdate": True}
    assert label == "tui harmonyos.dev_init"
    assert all(response["id"] != "req-init" for response in server.responses)

    await asyncio.wait_for(
        handlers["harmonyos.dev_init_cancel"](
            ws, "req-cancel", {"operationId": "dev-init-op-1"}, "sess-1"
        ),
        timeout=1,
    )

    assert cancelled.is_set()
    assert server.responses[-2] == {
        "id": "req-init",
        "ok": False,
        "payload": {},
        "error": "HarmonyOS Dev Init operation was cancelled",
        "code": "CANCELLED",
    }
    assert server.responses[-1] == {
        "id": "req-cancel",
        "ok": True,
        "payload": {
            "operationId": "dev-init-op-1",
            "cancelRequested": True,
            "cancelled": True,
        },
        "error": None,
        "code": None,
    }


@pytest.mark.asyncio
async def test_harmonyos_dev_init_is_cancelled_when_websocket_disconnects(monkeypatch):
    server = FakeGatewayServer()
    started, cancelled = _install_hanging_dev_init_send(monkeypatch)
    binding = build_cli_route_binding(CliRouteBindParams(path="/tui", agent_client=object()))
    binding.install(server)
    ws = type("FakeWs", (), {})()

    await server.local_handlers["/tui"]["harmonyos.dev_init"](
        ws,
        "req-disconnect-init",
        {"operationId": "dev-init-disconnect-op"},
        "sess-disconnect",
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.wait_for(binding.disconnect_handler(ws, [], []), timeout=1)

    assert cancelled.is_set()
    assert server.responses[-1]["id"] == "req-disconnect-init"
    assert server.responses[-1]["code"] == "CANCELLED"


@pytest.mark.asyncio
async def test_harmonyos_dev_init_rejects_untrackable_websocket(monkeypatch):
    server = FakeGatewayServer()
    sent = []
    _install_echo_dev_init_send(monkeypatch, record=sent)
    _register(server, agent_client=object())

    class SlottedWs:
        __slots__ = ()

    await server.local_handlers["/tui"]["harmonyos.dev_init"](
        SlottedWs(),
        "req-untrackable",
        {"operationId": "dev-init-untrackable-op"},
        "sess-untrackable",
    )

    assert sent == []
    assert server.responses[-1] == {
        "id": "req-untrackable",
        "ok": False,
        "payload": {},
        "error": "websocket does not support HarmonyOS Dev Init task tracking",
        "code": "INTERNAL_ERROR",
    }


def _install_failing_dev_init_send(monkeypatch):
    async def fake_send_agent_request_with_timeout(
        _client, env, *, label, timeout_seconds=None
    ):
        del env, label, timeout_seconds
        raise RuntimeError("connection closed")

    monkeypatch.setattr(
        tui_connect_module,
        "send_agent_request_with_timeout",
        fake_send_agent_request_with_timeout,
    )


@pytest.mark.asyncio
async def test_harmonyos_dev_init_legacy_client_falls_back_to_local(monkeypatch):
    server = FakeGatewayServer()
    _install_failing_dev_init_send(monkeypatch)
    monkeypatch.setattr(
        "agent_gateway.routing.e2a_proxy.is_legacy_shared_directory_client",
        lambda client: True,
    )
    local_calls = []

    async def fake_local_init(params):
        local_calls.append(dict(params))
        return {"ok": True, "source": "local-fallback"}

    monkeypatch.setattr(
        "agent_gateway.channel_manager.tui.harmonyos.harmonyos_dev.run_harmonyos_dev_init",
        fake_local_init,
    )
    _register(server, agent_client=object())
    ws = type("FakeWs", (), {})()

    await server.local_handlers["/tui"]["harmonyos.dev_init"](
        ws,
        "req-fallback",
        {"operationId": "dev-init-fallback-op", "skipDevecocliUpdate": True},
        "sess-1",
    )
    await asyncio.sleep(0.05)

    assert local_calls == [{"skipDevecocliUpdate": True}]
    assert server.responses[-1] == {
        "id": "req-fallback",
        "ok": True,
        "payload": {"ok": True, "source": "local-fallback"},
        "error": None,
        "code": None,
    }


@pytest.mark.asyncio
async def test_harmonyos_dev_init_remote_client_error_without_fallback(monkeypatch):
    server = FakeGatewayServer()
    _install_failing_dev_init_send(monkeypatch)
    monkeypatch.setattr(
        "agent_gateway.routing.e2a_proxy.is_legacy_shared_directory_client",
        lambda client: False,
    )
    local_calls = []

    async def fake_local_init(params):
        local_calls.append(dict(params))
        return {"ok": True}

    monkeypatch.setattr(
        "agent_gateway.channel_manager.tui.harmonyos.harmonyos_dev.run_harmonyos_dev_init",
        fake_local_init,
    )
    _register(server, agent_client=object())
    ws = type("FakeWs", (), {})()

    await server.local_handlers["/tui"]["harmonyos.dev_init"](
        ws,
        "req-remote-error",
        {"operationId": "dev-init-remote-op"},
        "sess-1",
    )
    await asyncio.sleep(0.05)

    assert local_calls == []
    assert server.responses[-1]["id"] == "req-remote-error"
    assert server.responses[-1]["ok"] is False
    assert server.responses[-1]["code"] == "INTERNAL_ERROR"
    assert "connection closed" in server.responses[-1]["error"]


@pytest.mark.asyncio
async def test_harmonyos_dev_init_business_failure_does_not_rerun_locally(monkeypatch):
    server = FakeGatewayServer()

    async def business_failure_send(_client, env, *, label, timeout_seconds=None):
        del label, timeout_seconds
        return AgentResponse(
            request_id=env.request_id,
            channel_id="tui",
            ok=False,
            payload={"error": "harmonyos project invalid", "code": "BAD_REQUEST"},
        )

    monkeypatch.setattr(tui_connect_module, "send_agent_request_with_timeout", business_failure_send)
    monkeypatch.setattr(
        "agent_gateway.routing.e2a_proxy.is_legacy_shared_directory_client",
        lambda client: True,
    )
    local_calls = []

    async def fake_local_init(params):
        local_calls.append(dict(params))
        return {"ok": True}

    monkeypatch.setattr(
        "agent_gateway.channel_manager.tui.harmonyos.harmonyos_dev.run_harmonyos_dev_init",
        fake_local_init,
    )
    _register(server, agent_client=object())
    ws = type("FakeWs", (), {})()

    await server.local_handlers["/tui"]["harmonyos.dev_init"](
        ws,
        "req-business-failure",
        {"operationId": "dev-init-biz-op"},
        "sess-1",
    )
    await asyncio.sleep(0.05)

    assert local_calls == []
    assert server.responses[-1]["id"] == "req-business-failure"
    assert server.responses[-1]["ok"] is False
    assert "harmonyos project invalid" in server.responses[-1]["error"]


@pytest.mark.asyncio
async def test_tui_disconnect_handler_schedules_session_cleanup():
    server = FakeGatewayServer()
    handler = FakeMessageHandler()
    _register(server, agent_client=None, message_handler=handler)
    ws = object()
    server.bind_session_owner("tui", "sess-exit", ws)

    await server.local_handlers["/tui"]["tui.disconnect"](
        ws, "req-exit", {"reason": "user_exit"}, "sess-exit"
    )

    assert handler.cancelled == []
    assert handler.scheduled == [( [("tui", "sess-exit")], [])]
    assert handler.scheduled_delays == [1.0]
    assert server.responses[-1] == {
        "id": "req-exit",
        "ok": True,
        "payload": {"accepted": True, "session_id": "sess-exit"},
        "error": None,
        "code": None,
    }


@pytest.mark.asyncio
async def test_tui_disconnect_ack_waits_until_cleanup_is_scheduled():
    server = FakeGatewayServer()
    handler = BlockingScheduledDisconnectMessageHandler()
    _register(server, message_handler=handler)
    ws = type("FakeWs", (), {})()
    server.bind_session_owner("tui", "sess-exit-order", ws)
    disconnect = asyncio.create_task(
        server.local_handlers["/tui"]["tui.disconnect"](
            ws, "req-exit-order", {"reason": "user_exit"}, "sess-exit-order"
        )
    )

    await asyncio.wait_for(handler.schedule_started.wait(), timeout=1)
    assert server.responses == []
    handler.release_schedule.set()
    await disconnect

    assert handler.scheduled == [([("tui", "sess-exit-order")], [])]
    assert server.responses[-1]["id"] == "req-exit-order"


@pytest.mark.asyncio
async def test_tui_disconnect_handler_does_not_cancel_session_owned_by_another_ws():
    server = FakeGatewayServer()
    handler = FakeMessageHandler()
    owner_ws = object()
    exiting_ws = object()
    server.bind_session_owner("tui", "sess-shared", owner_ws)
    _register(server, message_handler=handler)

    await server.local_handlers["/tui"]["tui.disconnect"](
        exiting_ws, "req-exit-other", {"reason": "user_exit"}, "sess-shared"
    )

    assert handler.cancelled == []
    assert handler.scheduled == []
    assert server.responses[-1] == {
        "id": "req-exit-other",
        "ok": True,
        "payload": {"accepted": True, "session_id": "sess-shared"},
        "error": None,
        "code": None,
    }


def test_build_cli_route_binding_creates_route_and_install_hook():
    binding = build_cli_route_binding(CliRouteBindParams(path="/tui"))
    server = FakeGatewayServer()

    assert binding.path == "/tui"
    assert binding.channel_id == "tui"
    assert "chat.send" in binding.forward_methods
    assert "history.get" in binding.forward_methods
    assert "team.mq.publish" in binding.forward_methods
    assert "team.mq.publish" in binding.forward_no_local_handler_methods
    assert binding.install is not None

    binding.install(server)
    handlers = server.local_handlers["/tui"]
    for method in ("config.get", "config.validate_model", "session.list", "chat.send"):
        assert method in handlers


@pytest.mark.asyncio
async def test_tui_route_disconnect_schedules_cancel_for_transport_close():
    handler = FakeMessageHandler()
    binding = build_cli_route_binding(CliRouteBindParams(path="/tui", message_handler=handler))

    await binding.disconnect_handler(
        object(), [("tui", "sess-drop")], [("tui", "req-drop")]
    )

    assert handler.scheduled == [([("tui", "sess-drop")], [("tui", "req-drop")])]
    assert handler.cancelled == []


@pytest.mark.asyncio
async def test_tui_route_disconnect_unregisters_physical_subscriptions():
    handler = FakeMessageHandler()
    binding = build_cli_route_binding(CliRouteBindParams(path="/tui", message_handler=handler))
    ws = type("FakeWs", (), {"_jiuwen_ws_id": "tui-ws-dead"})()

    await binding.disconnect_handler(ws, [("tui", "sess-drop")], [])

    assert handler.disconnected_websockets == [("tui", "tui-ws-dead")]


@pytest.mark.asyncio
async def test_tui_route_disconnect_skips_scheduled_cancel_after_explicit_exit():
    handler = FakeMessageHandler()
    binding = build_cli_route_binding(CliRouteBindParams(path="/tui", message_handler=handler))
    ws = type("FakeWs", (), {})()
    ws._jiuwenswarm_tui_user_exit = True

    await binding.disconnect_handler(ws, [("tui", "sess-exit")], [])

    assert handler.scheduled == []


@pytest.mark.asyncio
async def test_tui_route_disconnect_retries_when_cleanup_scheduling_is_cancelled():
    server = FakeGatewayServer()
    handler = BlockingScheduledDisconnectMessageHandler()
    binding = build_cli_route_binding(CliRouteBindParams(path="/tui", message_handler=handler))
    binding.install(server)
    ws = type("FakeWs", (), {})()
    server.bind_session_owner("tui", "sess-exit-race", ws)

    explicit_exit = asyncio.create_task(
        server.local_handlers["/tui"]["tui.disconnect"](
            ws, "req-exit-race", {"reason": "user_exit"}, "sess-exit-race"
        )
    )
    await handler.schedule_started.wait()
    explicit_exit.cancel()
    await asyncio.gather(explicit_exit, return_exceptions=True)
    handler.release_schedule.set()

    await binding.disconnect_handler(ws, [("tui", "sess-exit-race")], [])

    assert handler.scheduled == [([("tui", "sess-exit-race")], [])]


@pytest.mark.asyncio
async def test_tui_route_disconnect_retries_when_cleanup_scheduling_fails():
    server = FakeGatewayServer()
    handler = FailedOnceScheduledDisconnectMessageHandler()
    binding = build_cli_route_binding(CliRouteBindParams(path="/tui", message_handler=handler))
    binding.install(server)
    ws = type("FakeWs", (), {})()
    server.bind_session_owner("tui", "sess-exit-failed", ws)

    await server.local_handlers["/tui"]["tui.disconnect"](
        ws, "req-exit-failed", {"reason": "user_exit"}, "sess-exit-failed"
    )
    await binding.disconnect_handler(ws, [("tui", "sess-exit-failed")], [])

    assert handler.schedule_attempts == 2
    assert handler.scheduled == [([("tui", "sess-exit-failed")], [])]


def test_tui_session_bind_handler_cancels_pending_disconnect_cancel():
    handler = FakeMessageHandler()
    binding = build_cli_route_binding(CliRouteBindParams(path="/tui", message_handler=handler))

    binding.session_bind_handler("tui", "sess-reconnect")

    assert handler.reconnected == [("tui", "sess-reconnect")]


@pytest.mark.asyncio
async def test_config_validate_model_handler_forwards_to_agentserver():
    server = FakeGatewayServer()
    client = FakeAgentClient(
        {"provider": "OpenAI", "model": "gpt-4.1", "response": "hello"}
    )
    _register(server, agent_client=client)
    params = {
        "model_provider": "openai",
        "model": "gpt-4.1",
        "api_base": "https://api.openai.com/v1",
        "api_key": "secret",
    }

    await server.local_handlers["/tui"]["config.validate_model"](
        object(), "req-validate", params, "sess-1", "user-a"
    )

    assert len(client.requests) == 1
    request = client.requests[0]
    assert request.method == ReqMethod.CONFIG_VALIDATE_MODEL.value
    assert request.params == params
    assert request.user_id == "user-a"
    assert server.responses[-1] == {
        "id": "req-validate",
        "ok": True,
        "payload": {"provider": "OpenAI", "model": "gpt-4.1", "response": "hello"},
        "error": None,
        "code": None,
    }


@pytest.mark.asyncio
async def test_command_model_switch_sends_scoped_agent_reload(monkeypatch):
    server = FakeGatewayServer()
    sent_envs = []
    defaults = [
        {
            "alias": "glm",
            "model_client_config": {
                "api_key": "key",
                "api_base": "https://example.test/v1",
                "model_name": "GLM-5",
                "client_provider": "openai",
            },
            "model_config_obj": {},
        },
        {
            "alias": "other",
            "model_client_config": {
                "api_key": "key",
                "api_base": "https://example.test/v1",
                "model_name": "other-model",
                "client_provider": "openai",
            },
            "model_config_obj": {},
        },
    ]

    async def fake_send_tui_agent_request(_client, env, *, label):
        sent_envs.append((env, label))

    def fake_update_config(mutator, **kwargs):
        del kwargs
        data = {"models": {"defaults": [dict(item) for item in defaults]}}
        return mutator(data)

    monkeypatch.setattr(tui_connect_module, "_send_tui_agent_request", fake_send_tui_agent_request)
    monkeypatch.setattr(tui_models_handlers, "update_config", fake_update_config)
    monkeypatch.setattr(
        tui_models_handlers, "get_config_raw", lambda: {"models": {"defaults": defaults}}
    )
    monkeypatch.setattr(
        tui_models_handlers, "get_config", lambda: {"models": {"defaults": defaults}}
    )
    _register(
        server,
        agent_client=_offline_local_client(),
        force_local_config=True,
        message_handler=None,
        on_config_saved=None,
    )

    await server.local_handlers["/tui"]["command.model"](
        object(), "req-switch", {"model": "glm"}, "tui_session_1"
    )
    await asyncio.sleep(0)

    assert server.responses[-1] == {
        "id": "req-switch",
        "ok": True,
        "payload": {
            "current": "GLM-5",
            "requested": "glm",
            "type": "switched",
            "applied": True,
        },
        "error": None,
        "code": None,
    }
    assert len(sent_envs) == 1
    env, label = sent_envs[0]
    assert label == "command.model.switch"
    assert env.params["target_channel_id"] == "tui"
    assert env.params["target_session_id"] == "tui_session_1"
    assert env.params["reason"] == "model_switch"


def _model_entry(alias, model_name, *, api_key="key", api_base="https://example.test/v1"):
    return {
        "alias": alias,
        "model_client_config": {
            "api_key": api_key,
            "api_base": api_base,
            "model_name": model_name,
            "client_provider": "openai",
        },
        "model_config_obj": {},
    }


def _patch_local_model_config(monkeypatch, defaults):
    async def fake_send_tui_agent_request(_client, env, *, label):
        del env, label

    def fake_update_config(mutator, **kwargs):
        del kwargs
        return mutator({"models": {"defaults": defaults}})

    monkeypatch.setattr(tui_connect_module, "_send_tui_agent_request", fake_send_tui_agent_request)
    monkeypatch.setattr(tui_models_handlers, "update_config", fake_update_config)
    monkeypatch.setattr(
        tui_models_handlers, "get_config_raw", lambda: {"models": {"defaults": defaults}}
    )
    monkeypatch.setattr(
        tui_models_handlers, "get_config", lambda: {"models": {"defaults": defaults}}
    )


@pytest.mark.asyncio
async def test_command_model_delete_matches_by_name_when_index_drifted(monkeypatch):
    server = FakeGatewayServer()
    current_defaults = [
        _model_entry("other", "GLM-5.2"),
        _model_entry("glm", "GLM-5"),
    ]
    _patch_local_model_config(monkeypatch, current_defaults)
    _register(server, agent_client=_offline_local_client(), force_local_config=True)

    await server.local_handlers["/tui"]["command.model"](
        object(),
        "req-delete",
        {"action": "delete_model", "index": 0, "model": "GLM-5"},
        "tui_session_1",
    )
    await asyncio.sleep(0)

    assert server.responses[-1]["ok"] is True
    assert server.responses[-1]["payload"]["name"] == "GLM-5"
    assert server.responses[-1]["payload"]["type"] == "model_deleted"
    assert len(current_defaults) == 1
    assert current_defaults[0]["model_client_config"]["model_name"] == "GLM-5.2"


@pytest.mark.asyncio
async def test_command_model_delete_rejects_when_index_and_name_mismatch(monkeypatch):
    server = FakeGatewayServer()
    current_defaults = [
        _model_entry("a", "GLM", api_key="key1", api_base="https://a.test/v1"),
        _model_entry("b", "GLM", api_key="key2", api_base="https://b.test/v1"),
        _model_entry("c", "OTHER", api_key="key3", api_base="https://c.test/v1"),
    ]
    _patch_local_model_config(monkeypatch, current_defaults)
    _register(server, agent_client=_offline_local_client(), force_local_config=True)

    await server.local_handlers["/tui"]["command.model"](
        object(),
        "req-delete",
        {"action": "delete_model", "index": 2, "model": "GLM"},
        "tui_session_1",
    )
    await asyncio.sleep(0)

    assert server.responses[-1]["ok"] is False
    error = server.responses[-1]["error"].lower()
    assert "refresh" in error or "changed" in error
    assert len(current_defaults) == 3


@pytest.mark.asyncio
async def test_command_model_lists_agentos_models_without_defaults(monkeypatch):
    server = FakeGatewayServer()
    agentos_models = [_model_entry("backup", "backup-model", api_key="backup-key", api_base="test-endpoint")]
    monkeypatch.setattr(tui_models_handlers, "get_model_names", lambda: [])
    monkeypatch.setattr(
        tui_models_handlers,
        "get_config_raw",
        lambda: {"models": {"defaults": [], "agentos": agentos_models}},
    )
    _register(server, agent_client=_offline_local_client(), force_local_config=True)

    await server.local_handlers["/tui"]["command.model"](
        object(), "req-model-list", {}, "tui_session_1"
    )

    payload = server.responses[-1]["payload"]
    assert payload["available_models"] == ["backup"]
    assert payload["models"] == [
        {
            "index": "a0",
            "name": "backup",
            "alias": "backup",
            "model_name": "backup-model",
            "model_provider": "openai",
            "api_base": "test-endpoint",
            "reasoning_level": "",
            "api_key_suffix": "-key",
            "is_current": False,
            "is_agentos": True,
        }
    ]


@pytest.mark.asyncio
async def test_session_list_returns_agent_timeout_before_tui_request_timeout(monkeypatch):
    server = FakeGatewayServer()

    class HangingAgentClient:
        async def send_request(self, env):
            del env
            await asyncio.Event().wait()

    monkeypatch.setattr(
        "agent_gateway.routing.agent_request_timeout._TUI_DEFAULT_UNARY_TIMEOUT_SECONDS",
        0.01,
        raising=False,
    )
    _register(server, agent_client=HangingAgentClient())

    await asyncio.wait_for(
        server.local_handlers["/tui"]["session.list"](
            object(), "req-session-list", {"limit": 10}, "sess-1"
        ),
        timeout=0.2,
    )

    assert server.responses[-1] == {
        "id": "req-session-list",
        "ok": False,
        "payload": {},
        "error": "AgentServer request timed out",
        "code": "AGENT_SERVER_TIMEOUT",
    }


@pytest.mark.asyncio
async def test_session_color_set_unavailable_when_agent_offline() -> None:
    server = FakeGatewayServer()
    _register(server, agent_client=_offline_local_client())

    await server.local_handlers["/tui"]["session.color_set"](
        object(), "req-color", {"session_id": "sess-1"}, "sess-1"
    )

    _assert_control_method_unavailable(server)


@pytest.mark.asyncio
async def test_session_list_unavailable_when_agent_offline() -> None:
    server = FakeGatewayServer()
    _register(server, agent_client=_offline_local_client())

    await server.local_handlers["/tui"]["session.list"](
        object(), "req-list", {"all_projects": True}, "current"
    )

    _assert_control_method_unavailable(server)


@pytest.mark.asyncio
async def test_session_preview_unavailable_when_agent_offline() -> None:
    server = FakeGatewayServer()
    _register(server, agent_client=_offline_local_client())

    await server.local_handlers["/tui"]["session.preview"](
        object(), "req-preview", {"session_id": "sess-1"}, "sess-1"
    )

    _assert_control_method_unavailable(server)


@pytest.mark.asyncio
async def test_session_delete_unavailable_when_agent_offline() -> None:
    server = FakeGatewayServer()
    _register(server, agent_client=_offline_local_client())

    await server.local_handlers["/tui"]["session.delete"](
        object(), "req-del", {"session_id": "sess-del"}, "sess-del"
    )

    _assert_control_method_unavailable(server)


@pytest.mark.asyncio
async def test_session_rebind_project_does_not_read_gateway_state_for_remote_client() -> None:
    server = FakeGatewayServer()

    class RemoteAgentClient:
        server_ready = False

    _register(server, agent_client=RemoteAgentClient())

    await server.local_handlers["/tui"]["session.rebind_project"](
        object(),
        "req-remote-rebind",
        {"session_id": "sess-1", "project_dir": "/remote/user/project"},
        "sess-1",
    )

    assert server.responses[-1]["ok"] is False
    assert server.responses[-1]["code"] == "SERVICE_UNAVAILABLE"


class _DictOwnedCronController:
    def __init__(self) -> None:
        self.job = {
            "id": "cron-dict-owner",
            "user_id": "user-owner",
            "description": "old",
            "work_mode": "work",
            "session_id": "sess-1",
        }
        self.update_calls: list[tuple[str, dict]] = []

    async def get_job(self, job_id, **_kwargs):
        return dict(self.job) if job_id == self.job["id"] else None

    async def update_job(self, job_id, patch, **_kwargs):
        self.update_calls.append((job_id, dict(patch)))
        return {"id": job_id, **self.job, **patch}


@pytest.mark.asyncio
async def test_cron_job_update_accepts_matching_owner_with_dict_job() -> None:
    server = FakeGatewayServer()
    cron = _DictOwnedCronController()
    _register(server, cron_controller=cron)

    await server.local_handlers["/tui"]["cron.job.update"](
        object(),
        "req-cron-update",
        {"id": "cron-dict-owner", "patch": {"description": "new"}},
        "sess-1",
        "user-owner",
    )

    assert server.responses[-1]["ok"] is True, server.responses[-1]
    assert cron.update_calls[0][1]["description"] == "new"
    assert server.responses[-1]["payload"]["job"]["description"] == "new"


@pytest.mark.asyncio
async def test_cron_job_update_rejects_a_different_authenticated_owner() -> None:
    server = FakeGatewayServer()
    cron = _DictOwnedCronController()
    _register(server, cron_controller=cron)

    await server.local_handlers["/tui"]["cron.job.update"](
        object(),
        "req-cron-owner",
        {"id": "cron-dict-owner", "patch": {"description": "stolen"}},
        "sess-1",
        "user-other",
    )

    assert cron.update_calls == []
    assert server.responses[-1]["ok"] is False
    assert server.responses[-1]["error"] == "job not found"
    assert server.responses[-1]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_cron_job_update_accepts_matching_owner_with_object_job() -> None:
    class ObjectOwnedCronController:
        def __init__(self) -> None:
            self.job = SimpleNamespace(id="cron-obj-owner", user_id="user-owner")
            self.update_calls = 0

        async def get_job(self, job_id, **_kwargs):
            return self.job if job_id == self.job.id else None

        async def update_job(self, job_id, patch, **_kwargs):
            del job_id
            self.update_calls += 1
            return dict(patch)

    server = FakeGatewayServer()
    cron = ObjectOwnedCronController()
    _register(server, cron_controller=cron)

    await server.local_handlers["/tui"]["cron.job.update"](
        object(),
        "req-cron-obj",
        {"id": "cron-obj-owner", "patch": {"description": "new"}},
        "sess-1",
        "user-owner",
    )

    assert cron.update_calls == 1
    assert server.responses[-1]["ok"] is True


@pytest.mark.asyncio
async def test_agentos_cron_update_project_fields_with_dict_job(monkeypatch) -> None:
    class AgentOSClient:
        server_ready = True

    server = FakeGatewayServer()
    cron = _DictOwnedCronController()
    _register(server, agent_client=AgentOSClient(), cron_controller=cron)
    monkeypatch.setattr(
        "agent_gateway.routing.e2a_proxy.is_agentos_routing_client", lambda _client: True
    )

    async def fake_resolve_agent_cron_project_binding(**kwargs):
        assert kwargs.get("session_id") == "sess-1"
        assert kwargs["params"].get("work_mode") == "work"
        return True, {"project_id": "user-proj-1", "work_mode": "code"}

    monkeypatch.setattr(
        "agent_gateway.routing.e2a_proxy.resolve_agent_cron_project_binding",
        fake_resolve_agent_cron_project_binding,
    )

    await server.local_handlers["/tui"]["cron.job.update"](
        object(),
        "req-cron-update-proj",
        {"id": "cron-dict-owner", "patch": {"project_id": "user-proj-1"}},
        "sess-1",
        "user-owner",
    )

    assert server.responses[-1]["ok"] is True, server.responses[-1]
    patch = cron.update_calls[0][1]
    assert patch["project_id"] == "user-proj-1"
    assert patch["work_mode"] == "code"
    assert patch["_agentos_project_binding_verified"] is True


@pytest.mark.asyncio
async def test_models_list_forwards_to_agentserver():
    server = FakeGatewayServer()
    client = FakeAgentClient({"models": [{"name": "m"}]})
    _register(server, agent_client=client)

    await server.local_handlers["/tui"]["models.list"](
        object(), "req-models", {}, "sess-1", "user-a"
    )

    assert len(client.requests) == 1
    request = client.requests[0]
    assert request.method == ReqMethod.MODELS_LIST.value
    assert request.user_id == "user-a"
    assert server.responses[-1]["ok"] is True
    assert server.responses[-1]["payload"] == {"models": [{"name": "m"}]}
