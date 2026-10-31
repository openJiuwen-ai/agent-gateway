# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

from types import SimpleNamespace

from agent_gateway.message_handler.message_handler import MessageHandler
from openjiuwen_protocol.message import ReqMethod


def test_terminal_stream_chunk_accepts_empty_sentinel():
    assert MessageHandler._is_terminal_stream_chunk(SimpleNamespace(is_complete=True)) is True
    assert MessageHandler._is_terminal_stream_chunk(
        SimpleNamespace(is_complete=True, payload={"is_complete": True})
    ) is True
    assert MessageHandler._is_terminal_stream_chunk(
        SimpleNamespace(is_complete=False)
    ) is False
    assert MessageHandler._is_terminal_stream_chunk(
        SimpleNamespace(is_complete=True, payload={"is_complete": True, "error": ""})
    ) is False


def test_non_stream_rpc_parallelism_excludes_chat_methods():
    assert MessageHandler._non_stream_rpc_may_run_parallel(
        SimpleNamespace(method="history.get")
    ) is True
    assert MessageHandler._non_stream_rpc_may_run_parallel(
        SimpleNamespace(method=ReqMethod.CHAT_SEND)
    ) is False
    assert MessageHandler._non_stream_rpc_may_run_parallel(
        SimpleNamespace(method="")
    ) is False


def test_before_chat_hook_applies_to_chat_request_methods():
    assert MessageHandler._should_trigger_before_chat_request_hook(
        SimpleNamespace(req_method=ReqMethod.CHAT_SEND)
    ) is True
    assert MessageHandler._should_trigger_before_chat_request_hook(
        SimpleNamespace(req_method=ReqMethod.CHAT_RESUME)
    ) is True
    assert MessageHandler._should_trigger_before_chat_request_hook(
        SimpleNamespace(req_method=ReqMethod.CHAT_CANCEL)
    ) is False


def test_processing_status_skips_attach_only_streams():
    normal = SimpleNamespace(req_method=ReqMethod.CHAT_SEND, params={})
    attach_only = SimpleNamespace(
        req_method=ReqMethod.CHAT_SEND,
        params={"attach_only": True},
    )

    assert MessageHandler._should_emit_processing_status_for_stream(normal) is True
    assert MessageHandler._should_emit_processing_status_for_stream(attach_only) is False


def test_expert_stream_route_requires_team_mode_and_target():
    params = {"target_team_id": "team-1", "mode": "team.plan.normal"}
    assert MessageHandler._expert_stream_route(params) == {
        "team_id": "team-1",
        "team_name": "team-1",
        "source": "org_expert_direct",
    }
    assert MessageHandler._expert_stream_route({"target_team_id": "team-1"}) == {}
    assert MessageHandler._expert_stream_route(None) == {}


def test_stream_method_value_normalizes_enum_string_and_none():
    assert MessageHandler._stream_method_value(
        SimpleNamespace(req_method=ReqMethod.CHAT_SEND)
    ) == "chat.send"
    assert MessageHandler._stream_method_value(
        SimpleNamespace(req_method="history.get")
    ) == "history.get"
    assert MessageHandler._stream_method_value(SimpleNamespace(req_method=None)) == ""


def test_snapshot_hook_watch_params_reads_visible_keys():
    msg = SimpleNamespace(params={"query": "hello", "model_name": "model", "other": "x"})
    assert MessageHandler._snapshot_hook_watch_params(msg) == {
        "query": "hello",
        "model_name": "model",
    }
