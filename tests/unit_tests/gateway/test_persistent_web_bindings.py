"""Exercise owner RPCs against the same saved config and live manager used at startup."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import yaml

from agent_gateway.channel_manager.channel_manager import ChannelManager
from agent_gateway.channel_manager.web.persistent_bindings import register_persistent_bindings
from agent_gateway.channel_manager.web.app_web_handlers import _normalize_feishu_conf
from agent_gateway.common import config


@pytest.fixture
def owner(tmp_path, monkeypatch):
    path = tmp_path / "config.yaml"
    path.write_text("channels: {}\n")
    monkeypatch.setattr(config, "CONFIG_YAML_PATH", path)
    monkeypatch.delenv("JIUWENSWARM_EDITION", raising=False)
    methods = {}

    def register(name, handler, *, local_only):
        assert local_only
        methods[name] = handler

    channel = SimpleNamespace(register_method=register, send_response=AsyncMock())
    cm = ChannelManager(SimpleNamespace())
    service = SimpleNamespace(send_request=AsyncMock(return_value=SimpleNamespace(
        ok=True, payload={"agent_id": "mate"},
    )))
    settings = register_persistent_bindings(channel, lambda: cm, lambda: service, _normalize_feishu_conf, MagicMock())

    async def rpc(method, params):
        await methods[method](None, "req", params, None)
        return channel.send_response.await_args.kwargs

    async def invoke(method, params):
        return await rpc("channel." + method, params)

    return SimpleNamespace(invoke=invoke, rpc=rpc, cm=cm, service=service, settings=settings, path=path)


@pytest.mark.asyncio
@pytest.mark.parametrize("platform,id_key,secret_key", [
    ("feishu", "app_id", "app_secret"), ("dingtalk", "client_id", "client_secret"),
])
async def test_owner_bind_settings_unbind(owner, platform, id_key, secret_key):
    params = {"config_id": "colleague", id_key: "bot", secret_key: "secret"}
    result = await owner.invoke(platform + ".bind_agent", params)
    assert result == {"ok": True, "payload": {"binding": {id_key: "bot", "persistent_agent_id": "mate"}}}
    attach = owner.service.send_request.await_args_list[0].args[0]
    assert attach.method == "jiuwenmate.connection.attach"
    assert attach.params == {"config_id": "colleague", "channel": platform, "connection_key": "bot"}
    stored = yaml.safe_load(owner.path.read_text())["channels"][platform]
    assert stored == owner.cm.get_conf(platform)
    assert stored["apps"][0][secret_key] == "secret"
    assert stored["apps"][0]["persistent_agent_id"] == "mate"
    await owner.settings(owner.cm, platform, {"apps": [{id_key: "bot", "name": "renamed"}]})
    assert owner.cm.get_conf(platform)["apps"][0][secret_key] == "secret"
    if platform == "feishu":
        assert owner.service.send_request.await_args.args[0].method == "jiuwenmate.feishu.prepare"
        listing = await owner.invoke("feishu.get_bindings", {})
        assert listing["payload"] == {"agent_ids": ["mate"]}
        revoked = await owner.invoke("feishu.revoke", {"persistent_agent_id": "mate"})
        assert revoked["payload"] == {"revoked": True}
        assert not owner.cm.get_conf(platform)["apps"][0]["enabled"]

    assert (await owner.invoke(platform + ".unbind_agent", {id_key: "bot"}))["payload"] == {"unbound": True}
    assert not yaml.safe_load(owner.path.read_text())["channels"][platform]["apps"]
    assert not owner.cm.get_conf(platform)["apps"]


@pytest.mark.asyncio
@pytest.mark.parametrize("response,code", [
    (SimpleNamespace(ok=False, payload={"code": "CONFLICT", "error": "busy"}), "CONFLICT"),
    (SimpleNamespace(ok=True, payload={}), "SERVICE_UNAVAILABLE"),
])
async def test_attach_failure_never_writes_or_restarts(owner, response, code):
    owner.service.send_request.return_value = response
    result = await owner.invoke("dingtalk.bind_agent", {
        "client_id": "bot", "client_secret": "secret", "config_id": "mate",
    })
    assert not result["ok"] and result["code"] == code
    assert yaml.safe_load(owner.path.read_text())["channels"] == {}
    assert owner.cm.get_conf("dingtalk") == {}


@pytest.mark.asyncio
async def test_saved_binding_retries_failed_restart(owner):
    owner.cm._on_config_updated = AsyncMock(side_effect=RuntimeError("restart"))
    params = {"client_id": "bot", "client_secret": "secret", "config_id": "mate"}
    result = await owner.invoke("dingtalk.bind_agent", params)
    assert result["code"] == "CHANNEL_APPLY_FAILED"
    assert yaml.safe_load(owner.path.read_text())["channels"]["dingtalk"]["apps"][0]["client_id"] == "bot"
    owner.cm._on_config_updated = AsyncMock()
    assert (await owner.invoke("dingtalk.bind_agent", params))["ok"]
    owner.cm._on_config_updated.assert_awaited_once()


@pytest.mark.asyncio
async def test_http_binding_never_forwards_owner_credentials_to_chat(owner):
    from agent_gateway.channel_manager.web.invoke import dispatch_web_request

    handler = AsyncMock()
    channel = SimpleNamespace(
        _method_handlers={"channel.feishu.bind_agent": handler},
        _local_only_methods={"channel.feishu.bind_agent"},
        invoke_method_handler=AsyncMock(),
        _on_message_cb=AsyncMock(),
    )
    await dispatch_web_request(
        channel, method="channel.feishu.bind_agent", request_id="req", params={"app_secret": "secret"},
        session_id="session", user_message=SimpleNamespace(metadata={}), outbound=None,
    )
    channel.invoke_method_handler.assert_awaited_once()
    channel._on_message_cb.assert_not_awaited()


@pytest.mark.asyncio
async def test_enterprise_owner_web_cannot_change_centrally_managed_config(owner, monkeypatch):
    monkeypatch.setenv("JIUWENSWARM_EDITION", "enterprise")
    result = await owner.invoke("dingtalk.bind_agent", {
        "client_id": "bot", "client_secret": "secret", "config_id": "mate",
    })
    assert result["code"] == "FORBIDDEN"
    owner.service.send_request.assert_not_awaited()


@pytest.mark.asyncio
async def test_welink_status_revoke_and_colleague_delete_keep_shared_reader(owner):
    section = {
        "account": "reader", "reader_credential_dir": "/reader", "cli_authorized": True,
        "bindings": [
            {"persistent_agent_id": "mate", "config_id": "colleague", "assistant_credential_id": "ak1",
             "assistant_secret": "secret1", "credential_dir": "/assistant1"},
            {"persistent_agent_id": "other", "config_id": "second", "assistant_credential_id": "ak2",
             "assistant_secret": "secret2", "credential_dir": "/assistant2"},
        ],
    }
    owner.path.write_text(yaml.safe_dump({"channels": {"welink": section}}))
    await owner.cm.set_conf("welink", section)
    listing = await owner.invoke("welink.get_bindings", {"config_id": "second"})
    assert listing["ok"] and listing["payload"]["persistent_agent_id"] == "other"
    assert "secret2" not in str(listing)
    revoked = await owner.invoke("welink.revoke", {"config_id": "second"})
    assert revoked == {"ok": True, "payload": {"revoked": True}}
    assert [item["persistent_agent_id"] for item in owner.cm.get_conf("welink")["bindings"]] == ["mate"]

    removed = await owner.rpc("jiuwenmate.colleague.delete", {"agent_id": "colleague", "mode": "purge"})
    assert removed == {"ok": True, "payload": {"deleted": True}}
    saved = yaml.safe_load(owner.path.read_text())["channels"]["welink"]
    assert saved == owner.cm.get_conf("welink")
    assert saved["bindings"] == [] and saved["reader_credential_dir"] == "/reader"
    assert owner.service.send_request.await_args.args[0].method == "jiuwenmate.colleague.remove"


@pytest.mark.asyncio
async def test_welink_manual_credential_attaches_runtime_agent_and_hides_secret(owner):
    saved = await owner.invoke("welink.assistant_credential_save", {
        "config_id": "colleague", "account": "reader", "assistant_credential_id": "ak",
        "assistant_secret": "private-test-secret",
    })
    assert saved["ok"] and "private-test-secret" not in str(saved)
    assert [call.args[0].method for call in owner.service.send_request.await_args_list] == [
        "jiuwenmate.colleague.connections", "jiuwenmate.connection.attach",
    ]
    binding = owner.cm.get_conf("welink")["bindings"][0]
    assert binding["persistent_agent_id"] == "mate" and binding["config_id"] == "colleague"
    assert binding["assistant_secret"] == "private-test-secret"


def test_public_colleague_methods_forward_but_owner_and_internal_operations_stay_local():
    from openjiuwen_protocol.message import ReqMethod
    from agent_gateway.channel_manager.web.app_web_handlers import (
        _FORWARD_REQ_METHODS, _FORWARD_NO_LOCAL_HANDLER_METHODS, _JIUWENMATE_FORWARD_METHODS,
    )
    for name in _JIUWENMATE_FORWARD_METHODS:
        assert ReqMethod(name).value == name
        assert name in _FORWARD_REQ_METHODS and name in _FORWARD_NO_LOCAL_HANDLER_METHODS
    for name in ("jiuwenmate.colleague.delete", "jiuwenmate.connection.attach", "jiuwenmate.colleague.remove",
                 "jiuwenmate.welink.inbound", "jiuwenmate.dingtalk.inbound", "jiuwenmate.feishu.inbound"):
        assert name not in _FORWARD_REQ_METHODS and name not in _FORWARD_NO_LOCAL_HANDLER_METHODS
