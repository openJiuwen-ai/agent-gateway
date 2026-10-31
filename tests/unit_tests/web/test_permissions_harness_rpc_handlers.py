# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

import pytest

from agent_gateway.channel_manager.web.permissions_harness_rpc_handlers import (
    register_a4p_handlers,
    register_harness_handlers,
    register_permissions_handlers,
)


class _FakeChannel:
    def __init__(self):
        self.methods = {}
        self.responses = []
        self.routes = []
        self.channel_id = "web"
        self.recorded_rebound = []
        self.cleared_rebound = []

    def register_method(self, name, handler):
        self.methods[name] = handler

    async def send_response(self, ws, req_id, **kwargs):
        self.responses.append({"req_id": req_id, **kwargs})

    async def routing_keys_for_ws(self, ws):
        return self.routes

    def record_a4p_rebound(self, session_id, ws_id):
        self.recorded_rebound.append((session_id, ws_id))

    def clear_a4p_rebound(self, session_id):
        self.cleared_rebound.append(session_id)


def _register_all(agent_client=None):
    channel = _FakeChannel()
    register_permissions_handlers(channel, agent_client, lambda ref: ref)
    register_a4p_handlers(channel, agent_client, lambda ref: ref)
    register_harness_handlers(channel, agent_client, lambda ref: ref)
    return channel


def test_permission_and_a4p_routes_are_registered_in_original_order():
    channel = _register_all()

    assert list(channel.methods) == [
        "permissions.owner_scopes.get",
        "permissions.owner_scopes.set",
        "permissions.tools.get",
        "permissions.tools.set",
        "permissions.tools.update",
        "permissions.tools.update_user",
        "permissions.tools.delete",
        "permissions.rules.get",
        "permissions.rules.create",
        "permissions.rules.update",
        "permissions.rules.delete",
        "permissions.approval_overrides.get",
        "permissions.approval_overrides.delete",
        "a4p.config.get",
        "a4p.config.update",
        "a4p.authorization.reprepare",
        "a4p.authorization.complete",
        "a4p.authorization.reject",
        "a4p.authorization.pending",
        "harness.packages",
        "harness.packages.scan",
        "harness.activate",
        "harness.deactivate",
        "harness.delete",
        "rsi.dataset.validate",
        "rsi.task.create",
        "rsi.task.list",
        "rsi.task.get",
        "rsi.task.delete",
        "rsi.training.start",
        "rsi.training.pause",
        "rsi.training.resume",
        "rsi.training.terminate",
        "rsi.report.get",
        "rsi.usage.get",
        "rsi.artifact.download",
        "rsi.artifact.files.list",
        "rsi.artifact.files.get",
        "rsi.tree.get",
        "rsi.harness.install",
        "rsi.harness.versions.list",
        "rsi.harness.rollback",
        "harness.import",
        "harness.export",
    ]


@pytest.mark.asyncio
async def test_permissions_owner_scopes_set_rejects_non_dict_params(monkeypatch):
    from agent_gateway.channel_manager.web import permissions_harness_rpc_handlers

    async def proxy_unary_request(**kwargs):
        raise AssertionError("invalid params should not be forwarded")

    monkeypatch.setattr(permissions_harness_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    channel = _register_all(object())

    await channel.methods["permissions.owner_scopes.set"](
        object(), "rpc-0", "bad", "session-0"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-0",
            "ok": False,
            "error": "params must be object",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_permissions_tools_get_forwards_with_agent_client(monkeypatch):
    from agent_gateway.channel_manager.web import permissions_harness_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    monkeypatch.setattr(permissions_harness_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    channel = _register_all(SimpleNamespace(_marker="resolved-client"))

    await channel.methods["permissions.tools.get"](
        object(), "rpc-1", {"ignored": True}, "session-1", user_id="user-1"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["agent_client"]._marker == "resolved-client"
    assert forwarded[0]["params"] == {"ignored": True}
    assert forwarded[0]["req_method"].value == "permissions.tools.get"
    assert channel.responses == []


@pytest.mark.asyncio
async def test_a4p_rejects_client_that_is_not_ready(monkeypatch):
    from agent_gateway.channel_manager.web import permissions_harness_rpc_handlers

    async def proxy_unary_request(**kwargs):
        raise AssertionError("unready client should not use unary proxy")

    monkeypatch.setattr(permissions_harness_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    channel = _register_all(SimpleNamespace(server_ready=False))

    await channel.methods["a4p.config.get"](
        object(), "rpc-2", {}, "session-2"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-2",
            "ok": False,
            "error": "Agent server is not ready",
            "code": "AGENT_NOT_READY",
        }
    ]


@pytest.mark.asyncio
async def test_a4p_requires_web_authorizer_route(monkeypatch):
    from agent_gateway.channel_manager.web import permissions_harness_rpc_handlers

    client = SimpleNamespace(server_ready=True)
    channel = _register_all(client)

    await channel.methods["a4p.config.get"](
        object(), "rpc-3", {}, "session-3"
    )

    assert channel.responses == [
        {
            "req_id": "rpc-3",
            "ok": False,
            "error": "No Web authorizer route is registered for this session",
            "code": "A4P_WEB_SESSION_REQUIRED",
        }
    ]


@pytest.mark.asyncio
async def test_a4p_success_forwards_authorizer_request(monkeypatch):
    from agent_gateway.channel_manager.web import permissions_harness_rpc_handlers

    route = SimpleNamespace(
        session_id="session-4",
        app_id="app-4",
        agent_ref=SimpleNamespace(mode="agentos", id="agent-4"),
    )
    ws = SimpleNamespace(_jiuwen_ws_id="ws-4")
    client = SimpleNamespace(server_ready=True, sent=[])

    async def send_request(env):
        client.sent.append(env)
        return SimpleNamespace(ok=True, payload={"status": "complete"})

    client.send_request = send_request
    channel = _register_all(client)
    channel.routes = [route]

    async def e2a_from_agent_fields(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(permissions_harness_rpc_handlers, "e2a_from_agent_fields", e2a_from_agent_fields)

    await channel.methods["a4p.authorization.complete"](
        ws, "rpc-4", {"decision": "allow"}, "session-4"
    )

    assert channel.recorded_rebound == [("session-4", "ws-4")]
    assert channel.responses == [
        {
            "req_id": "rpc-4",
            "ok": True,
            "payload": {"status": "complete"},
        }
    ]


@pytest.mark.asyncio
async def test_a4p_failure_clears_rebound_and_maps_response_error(monkeypatch):
    from agent_gateway.channel_manager.web import permissions_harness_rpc_handlers

    route = SimpleNamespace(
        session_id="session-5",
        app_id="app-5",
        agent_ref=SimpleNamespace(mode="agentos", id="agent-5"),
    )
    ws = SimpleNamespace(_jiuwen_ws_id="ws-5")
    client = SimpleNamespace(server_ready=True, sent=[])

    async def send_request(env):
        client.sent.append(env)
        return SimpleNamespace(ok=False, payload={"error": "reject failed", "code": "BAD_REQUEST"})

    client.send_request = send_request
    channel = _register_all(client)
    channel.routes = [route]

    async def e2a_from_agent_fields(**kwargs):
        return SimpleNamespace(**kwargs)

    monkeypatch.setattr(permissions_harness_rpc_handlers, "e2a_from_agent_fields", e2a_from_agent_fields)

    await channel.methods["a4p.authorization.reject"](
        ws, "rpc-5", {}, "session-5"
    )

    assert channel.cleared_rebound == ["session-5"]
    assert channel.responses == [
        {
            "req_id": "rpc-5",
            "ok": False,
            "error": "reject failed",
            "code": "BAD_REQUEST",
        }
    ]


@pytest.mark.asyncio
async def test_harness_rsi_requests_inject_session_and_download_user(monkeypatch):
    from agent_gateway.channel_manager.web import permissions_harness_rpc_handlers

    forwarded = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    monkeypatch.setattr(permissions_harness_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    channel = _register_all(object())

    await channel.methods["rsi.artifact.download"](
        object(), "rpc-6", {"artifact_id": "file-6"}, "session-6", user_id="user-6"
    )

    assert len(forwarded) == 1
    assert forwarded[0]["params"] == {
        "artifact_id": "file-6",
        "session_id": "session-6",
        "_download_user_id": "user-6",
    }
    assert forwarded[0]["req_method"].value == "rsi.artifact.download"
    assert forwarded[0]["label"] == "harness"
    assert channel.responses == []


@pytest.mark.asyncio
async def test_harness_import_uploads_oversized_base64_archive(monkeypatch):
    from agent_gateway.channel_manager.web import permissions_harness_rpc_handlers

    forwarded = []
    uploads = []

    async def proxy_unary_request(**kwargs):
        forwarded.append(kwargs)

    async def upload_file_bytes_via_e2a(archive, path, **kwargs):
        uploads.append((archive, path, kwargs))
        return True, {"path": "auto-harness/temp/uploads/uploaded.zip"}

    monkeypatch.setattr(permissions_harness_rpc_handlers, "proxy_unary_request", proxy_unary_request)
    monkeypatch.setattr(permissions_harness_rpc_handlers, "upload_file_bytes_via_e2a", upload_file_bytes_via_e2a)
    monkeypatch.setattr(permissions_harness_rpc_handlers, "E2A_PAYLOAD_MAX_BYTES", 1)
    channel = _register_all(SimpleNamespace(server_ready=True, _marker="ready-client"))

    await channel.methods["harness.import"](
        object(), "rpc-7", {"file_content": "AAAA", "name": "harness.zip"}, "session-7", user_id="user-7"
    )

    assert len(uploads) == 1
    assert uploads[0][1].startswith("auto-harness/temp/uploads/harness_")
    assert uploads[0][2]["user_id"] == "user-7"
    assert forwarded[0]["params"] == {
        "name": "harness.zip",
        "uploaded_path": "auto-harness/temp/uploads/uploaded.zip",
    }
    assert "file_content" not in forwarded[0]["params"]
    assert forwarded[0]["req_method"].value == "harness.packages.import"
    assert forwarded[0]["label"] == "harness.import"
    assert forwarded[0]["preserve_error_payload"] is True
    assert channel.responses == []
