import asyncio
from types import SimpleNamespace

from openjiuwen_protocol.message import Message

from agent_gateway.message_handler.command_parser.slash_command import (
    ParsedControlAction,
)
from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.message_handler.prompts.review_prompt import build_review_prompt
from agent_gateway.message_handler.prompts.security_review_prompt import GitPreExecError
from agent_gateway.message_handler.review_prompt_injection import (
    InboundReviewPromptResult,
    apply_inbound_review_prompt,
    inject_review_prompt,
    inject_security_review_prompt,
)


def _make_message():
    return Message(
        id="msg-1",
        type="req",
        channel_id="wechat",
        session_id="session-1",
        params={},
        timestamp=0.0,
        ok=True,
    )


def test_inject_review_prompt_sets_query_and_continues():
    handler = object.__new__(MessageHandler)
    parsed = SimpleNamespace(pr_arg="123 focus on security")
    msg = _make_message()

    result = asyncio.run(
        inject_review_prompt(handler, msg, parsed, channel_type="wechat")
    )

    assert result is False
    assert msg.params["query"] == build_review_prompt("123 focus on security")


def test_inject_security_review_prompt_sets_query_and_continues(monkeypatch):
    handler = object.__new__(MessageHandler)
    parsed = SimpleNamespace(security_review_arg=" focus on auth ")
    msg = _make_message()
    msg.metadata = {"cwd": "D:/repo"}
    monkeypatch.setattr(
        "agent_gateway.message_handler.review_prompt_injection.build_security_review_prompt",
        lambda extra_arg, cwd: f"prompt:{extra_arg}:{cwd}",
    )

    result = asyncio.run(
        inject_security_review_prompt(handler, msg, parsed, channel_type="wechat")
    )

    assert result is False
    assert msg.params["query"] == "prompt: focus on auth :D:/repo"


def test_inject_security_review_prompt_aborts_on_git_pre_exec_error(monkeypatch):
    handler = object.__new__(MessageHandler)
    handler.notice_calls = []

    async def send_notice(*args):
        handler.notice_calls.append(args)

    handler.send_channel_notice = send_notice
    parsed = SimpleNamespace(security_review_arg=" focus on auth ")
    msg = _make_message()
    msg.metadata = {"cwd": "D:/repo"}
    monkeypatch.setattr(
        "agent_gateway.message_handler.review_prompt_injection.build_security_review_prompt",
        lambda extra_arg, cwd: (_ for _ in ()).throw(
            GitPreExecError("git status failed")
        ),
    )

    result = asyncio.run(
        inject_security_review_prompt(handler, msg, parsed, channel_type="wechat")
    )

    assert result is True
    assert handler.notice_calls == [
        (
            {"id": "msg-1", "meta_data": {"cwd": "D:/repo"}},
            "wechat",
            "session-1",
            "/security-review 无法执行：git status failed",
        )
    ]
    assert "query" not in msg.params


def test_apply_inbound_review_prompt_rejects_review_bad():
    handler = object.__new__(MessageHandler)
    handler.notice_calls = []

    async def send_notice(*args):
        handler.notice_calls.append(args)

    handler.send_channel_notice = send_notice
    parsed = SimpleNamespace(action=ParsedControlAction.REVIEW_BAD)
    msg = _make_message()
    msg.metadata = {"cwd": "D:/repo"}

    result = asyncio.run(apply_inbound_review_prompt(handler, msg, parsed))

    assert result == InboundReviewPromptResult(skip=True, prompt=None)
    assert len(handler.notice_calls) == 1
    assert msg.params == {}


def test_apply_inbound_review_prompt_injects_review_ok():
    handler = object.__new__(MessageHandler)
    parsed = SimpleNamespace(
        action=ParsedControlAction.REVIEW_OK,
        pr_arg="123 focus on security",
    )
    msg = _make_message()
    msg.params = {"attachments": [], "content": "original"}

    result = asyncio.run(apply_inbound_review_prompt(handler, msg, parsed))

    prompt = build_review_prompt("123 focus on security")
    assert result == InboundReviewPromptResult(skip=False, prompt=prompt)
    assert msg.params == {
        "attachments": [],
        "content": prompt,
        "query": prompt,
    }


def test_apply_inbound_review_prompt_rejects_security_review_bad():
    handler = object.__new__(MessageHandler)
    handler.notice_calls = []

    async def send_notice(*args):
        handler.notice_calls.append(args)

    handler.send_channel_notice = send_notice
    parsed = SimpleNamespace(action=ParsedControlAction.SECURITY_REVIEW_BAD)
    msg = _make_message()
    msg.metadata = {"cwd": "D:/repo"}

    result = asyncio.run(apply_inbound_review_prompt(handler, msg, parsed))

    assert result == InboundReviewPromptResult(skip=True, prompt=None)
    assert len(handler.notice_calls) == 1
    assert msg.params == {}


def test_apply_inbound_review_prompt_injects_security_review_ok(monkeypatch):
    handler = object.__new__(MessageHandler)
    parsed = SimpleNamespace(
        action=ParsedControlAction.SECURITY_REVIEW_OK,
        security_review_arg=" focus on auth ",
    )
    msg = _make_message()
    msg.metadata = {"cwd": "D:/repo"}
    msg.params = {"content": "original"}
    monkeypatch.setattr(
        "agent_gateway.message_handler.review_prompt_injection.build_security_review_prompt",
        lambda extra_arg, cwd: f"prompt:{extra_arg}:{cwd}",
    )

    result = asyncio.run(apply_inbound_review_prompt(handler, msg, parsed))

    expected = "prompt: focus on auth :D:/repo"
    assert result == InboundReviewPromptResult(skip=False, prompt=expected)
    assert msg.params == {"content": expected, "query": expected}


def test_apply_inbound_review_prompt_aborts_on_security_git_pre_exec_error(
    monkeypatch,
):
    handler = object.__new__(MessageHandler)
    handler.notice_calls = []

    async def send_notice(*args):
        handler.notice_calls.append(args)

    handler.send_channel_notice = send_notice
    parsed = SimpleNamespace(
        action=ParsedControlAction.SECURITY_REVIEW_OK,
        security_review_arg=" focus on auth ",
    )
    msg = _make_message()
    msg.metadata = {"cwd": "D:/repo"}
    msg.params = {}
    monkeypatch.setattr(
        "agent_gateway.message_handler.review_prompt_injection.build_security_review_prompt",
        lambda extra_arg, cwd: (_ for _ in ()).throw(
            GitPreExecError("git status failed")
        ),
    )

    result = asyncio.run(apply_inbound_review_prompt(handler, msg, parsed))

    assert result == InboundReviewPromptResult(skip=True, prompt=None)
    assert len(handler.notice_calls) == 1
    assert msg.params == {}
