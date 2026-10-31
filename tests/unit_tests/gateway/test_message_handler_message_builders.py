# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.message_handler.message_handler import MessageHandler


def test_build_queued_chat_send_message_preserves_request_identity():
    msg = SimpleNamespace(
        id="req-1",
        channel_id="feishu",
        session_id="sess-1",
        user_id="user-1",
    )
    built = MessageHandler._build_queued_chat_send_message(
        msg,
        " continue the task ",
        [{"path": "a.txt"}],
        original_request=" Finish the PR ",
    )

    assert built.id.startswith("req_")
    assert built.id.endswith("_req-1")
    assert built.type == "req"
    assert built.channel_id == "feishu"
    assert built.session_id == "sess-1"
    assert built.ok is True
    assert built.is_stream is True
    assert built.user_id == "user-1"
    assert built.params["supplement_input"] == " continue the task "
    assert built.params["original_request"] == " Finish the PR "
    assert built.params["attachments"] == [{"path": "a.txt"}]
    assert built.params["is_supplement"] is True
    assert "continue the task" in built.params["query"]
    assert "Finish the PR" in built.params["query"]


def test_build_error_out_message_keeps_code_and_metadata():
    error = RuntimeError("agent request failed")
    error.code = "agent_unavailable"
    msg = SimpleNamespace(
        id="req-error",
        channel_id="web",
        session_id="sess-error",
        metadata={"channel": "web"},
        app_id="app-1",
    )

    built = MessageHandler._build_error_out_message(msg, error)

    assert built.id == "req-error"
    assert built.type == "res"
    assert built.channel_id == "web"
    assert built.session_id == "sess-error"
    assert built.ok is False
    assert built.payload == {"error": "agent request failed", "code": "agent_unavailable"}
    assert built.metadata == {"channel": "web"}
    assert built.app_id == "app-1"


def test_build_tool_result_message_normalizes_cancelled_tool_info():
    built = MessageHandler._build_tool_result_message(
        "feishu",
        "sess-2",
        {"tool_name": "search", "tool_call_id": "call-1", "result": "done"},
        {"request": "req-2"},
    )

    assert built.id.startswith("tool_result_")
    assert built.type == "event"
    assert built.channel_id == "feishu"
    assert built.session_id == "sess-2"
    assert built.ok is True
    assert built.payload["tool_result"]["tool_name"] == "search"
    assert built.payload["tool_result"]["tool_call_id"] == "call-1"
    assert built.payload["tool_result"]["result"] == "done"
    assert built.payload["tool_result"]["status"] == "error"
    assert built.metadata == {"request": "req-2"}
