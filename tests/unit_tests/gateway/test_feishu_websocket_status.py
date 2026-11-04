import asyncio
import logging

import pytest
from unittest.mock import MagicMock

from agent_gateway.channel_manager.im_platforms.feishu import feishu_connect
from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import (
    FeishuChannel,
    FeishuConfig,
    _FeishuWebSocketLogHandler,
)


def _channel() -> FeishuChannel:
    return FeishuChannel(
        FeishuConfig(enabled=True, app_id="app-test"),
        MagicMock(),
    )


def _sdk_record(message: str, *, thread_id: int, level: int = logging.INFO) -> logging.LogRecord:
    record = logging.LogRecord("Lark", level, "lark/ws/client.py", 1, message, (), None)
    record.thread = thread_id
    return record


def test_sdk_connected_log_marks_handshake_without_logging_connection_url(monkeypatch):
    channel = _channel()
    handler = _FeishuWebSocketLogHandler(channel, thread_id=17)
    logged = MagicMock()
    monkeypatch.setattr(feishu_connect.logger, "info", logged)

    handler.emit(_sdk_record("connected to wss://example.invalid/?secret=hidden", thread_id=17))

    assert channel._websocket_connected.is_set()
    logged.assert_called_once()
    log_message = logged.call_args.args
    assert "握手成功" in log_message[0]
    assert "app-test" in log_message
    assert "wss://example.invalid" not in str(log_message)
    assert "secret=hidden" not in str(log_message)


def test_sdk_failure_is_reported_but_unrelated_thread_is_ignored(monkeypatch):
    channel = _channel()
    handler = _FeishuWebSocketLogHandler(channel, thread_id=17)
    channel._websocket_connected.set()
    logged = MagicMock()
    monkeypatch.setattr(feishu_connect.logger, "error", logged)

    handler.emit(_sdk_record("connected to wss://unrelated", thread_id=99))
    assert channel._websocket_connected.is_set()
    logged.assert_not_called()

    handler.emit(
        _sdk_record(
            "connect failed, err: credentials should not be copied to gateway logs",
            thread_id=17,
            level=logging.ERROR,
        )
    )

    assert not channel._websocket_connected.is_set()
    logged.assert_called_once()
    log_message = logged.call_args.args
    assert "连接失败" in log_message[0]
    assert "app-test" in log_message
    assert "credentials should not be copied" not in str(log_message)


def test_message_callback_warns_when_main_loop_is_unavailable(monkeypatch):
    channel = _channel()
    logged = MagicMock()
    monkeypatch.setattr(feishu_connect.logger, "warning", logged)

    channel._on_message_sync(object())

    logged.assert_called_once()
    assert "主事件循环不可用" in logged.call_args.args[0]
    assert "app-test" in logged.call_args.args


def test_sdk_disconnect_is_not_a_handshake_and_reconnect_restores_only_its_app():
    first, second = _channel(), _channel()
    handlers = [_FeishuWebSocketLogHandler(first, 17), _FeishuWebSocketLogHandler(second, 18)]

    def emit(message, thread_id):
        for handler in handlers:
            handler.emit(_sdk_record(message, thread_id=thread_id))

    emit("connected to wss://example.invalid", 18)
    emit("disconnected to wss://example.invalid", 17)
    assert not first._websocket_connected.is_set()
    assert second._websocket_connected.is_set()
    emit("connected to wss://example.invalid", 17)
    assert first._websocket_connected.is_set()
    emit("disconnected to wss://example.invalid", 17)
    assert not first._websocket_connected.is_set()
    assert second._websocket_connected.is_set()


@pytest.mark.asyncio
async def test_multiple_channels_start_without_waiting_for_sdk_client(monkeypatch):
    channels = [_channel(), _channel()]
    for channel in channels:
        channel.config.app_secret = "test"
        monkeypatch.setattr(channel, "_initialize_api_client", MagicMock())
    # Model two SDK threads that have not constructed a WebSocket client yet.
    started = MagicMock()
    monkeypatch.setattr(feishu_connect.threading.Thread, "start", started)
    monkeypatch.setattr(feishu_connect.time, "sleep", lambda *_: pytest.fail("blocked the main loop"))
    tasks = [asyncio.create_task(ch.start()) for ch in channels]
    try:
        await asyncio.sleep(0)
        assert started.call_count == 2
        assert all(not task.done() for task in tasks)
        assert all(not ch._websocket_connected.is_set() for ch in channels)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_enterprise_bot_identity_is_saved_under_its_bot_key(monkeypatch):
    from unittest.mock import AsyncMock
    from agent_gateway.config.channel import access

    channel = FeishuChannel(FeishuConfig(app_id="app-test", channel_id="feishu_enterprise:app-test",
                                         bot_key="sales"), MagicMock())
    channel._main_loop = asyncio.get_running_loop()
    post, get = MagicMock(), MagicMock()
    post.return_value.json.return_value = {"code": 0, "tenant_access_token": "test"}
    get.return_value.json.return_value = {"code": 0, "bot": {"open_id": "ou_test"}}
    monkeypatch.setattr(feishu_connect.requests, "post", post)
    monkeypatch.setattr(feishu_connect.requests, "get", get)
    save = AsyncMock()
    monkeypatch.setattr(access, "update_channel_subsection_in_config", save)
    await asyncio.to_thread(channel._fetch_bot_open_id)
    await asyncio.sleep(0)
    save.assert_awaited_once_with("feishu_enterprise", "sales", {"bot_open_id": "ou_test"})
