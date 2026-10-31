# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.config_saved import ConfigSavedContext, handle_config_saved
from openjiuwen_protocol.message import ReqMethod


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def set_or_update_server_config(self, *, config, env):
        self.config = config
        self.env = env

    async def send_request(self, env):
        self.requests.append(env)
        return self.responses.pop(0)


class FakeMessageHandler:
    def __init__(self):
        self.saved = []

    def update_evolution_auto_save(self, config):
        self.saved.append(config)


async def test_handle_config_saved_applies_reload_and_side_effects():
    client = FakeClient(
        [
            SimpleNamespace(ok=True, payload={}),
            SimpleNamespace(ok=True, payload={}),
        ]
    )
    message_handler = FakeMessageHandler()
    prewarm_calls = []
    def schedule_agent_prewarm_sync(name, *, delay_seconds):
        prewarm_calls.append((name, delay_seconds))

    context = ConfigSavedContext(
        client=client,
        message_handler=message_handler,
        cron_controller=None,
        restart_request=None,
        schedule_agent_prewarm_sync=schedule_agent_prewarm_sync,
        schedule_gateway_restart=None,
    )

    result = await handle_config_saved(
        context,
        updated_env_keys={"MODEL_PROVIDER"},
        env_updates={"MODEL_PROVIDER": "qwen"},
        config_payload={"model": "qwen"},
        reload_options={},
    )
    assert result is True
    assert client.requests[0].method == ReqMethod.AGENT_RELOAD_CONFIG.value
    assert client.requests[1].method == ReqMethod.BROWSER_RUNTIME_RESTART.value
    assert message_handler.saved == [{"model": "qwen"}]
    assert prewarm_calls[0][0] == "agent-prewarm-sync-after-config"


async def test_handle_config_saved_without_agent_reload_is_quiet():
    client = FakeClient([])
    message_handler = FakeMessageHandler()
    restarts = []
    context = ConfigSavedContext(
        client=client,
        message_handler=message_handler,
        cron_controller=None,
        restart_request=None,
        schedule_agent_prewarm_sync=lambda *args, **kwargs: None,
        schedule_gateway_restart=restarts.append,
    )

    result = await handle_config_saved(
        context,
        updated_env_keys=set(),
        env_updates={},
        config_payload={},
        reload_options={"reload_scopes": {"locale"}},
    )
    assert result is True
    assert client.requests == []
    assert restarts == []


async def test_handle_config_failed_reload_schedules_restart():
    client = FakeClient(
        [SimpleNamespace(ok=False, payload={"error": "boom"}) for _ in range(4)]
    )
    message_handler = FakeMessageHandler()
    restarts = []
    context = ConfigSavedContext(
        client=client,
        message_handler=message_handler,
        cron_controller=None,
        restart_request="restart-request",
        schedule_agent_prewarm_sync=lambda *args, **kwargs: None,
        schedule_gateway_restart=restarts.append,
    )

    result = await handle_config_saved(
        context,
        updated_env_keys=set(),
        env_updates={},
        config_payload={},
        reload_options={},
    )
    assert result is False
    assert restarts == ["restart-request"]
