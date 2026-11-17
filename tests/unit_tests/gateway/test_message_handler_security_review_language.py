# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Tests for /security-review locale lookup and its git concurrency contract."""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from agent_gateway.message_handler import message_handler as message_handler_module
from agent_gateway.message_handler.message_handler import MessageHandler
from agent_gateway.message_handler.prompts.security_review_prompt import GitPreExecError


@pytest.fixture(autouse=True)
def _reset_message_handler_singleton():
    MessageHandler._instance = None
    yield
    MessageHandler._instance = None


def _handler(client: object) -> MessageHandler:
    return MessageHandler(client)


def _msg() -> SimpleNamespace:
    return SimpleNamespace(channel_id="web", user_id="u-1", session_id="s-1")


@pytest.mark.asyncio
@pytest.mark.parametrize("language", ["zh", "en"])
async def test_resolve_response_language_returns_valid_value(language: str) -> None:
    client = SimpleNamespace(
        send_request=AsyncMock(
            return_value=SimpleNamespace(
                ok=True,
                payload={"preferred_language": language},
            )
        )
    )
    handler = _handler(client)

    assert await handler._resolve_response_language(_msg()) == language


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        SimpleNamespace(ok=False, payload={"preferred_language": "zh"}),
        SimpleNamespace(ok=True, payload=["zh"]),
        SimpleNamespace(ok=True, payload={"preferred_language": "fr"}),
        SimpleNamespace(ok=True, payload={}),
    ],
)
async def test_resolve_response_language_rejects_invalid_payload(response) -> None:
    client = SimpleNamespace(send_request=AsyncMock(return_value=response))
    handler = _handler(client)

    assert await handler._resolve_response_language(_msg()) is None


@pytest.mark.asyncio
async def test_resolve_response_language_falls_back_on_exception() -> None:
    client = SimpleNamespace(send_request=AsyncMock(side_effect=RuntimeError("offline")))
    handler = _handler(client)

    assert await handler._resolve_response_language(_msg()) is None


@pytest.mark.asyncio
async def test_resolve_response_language_falls_back_on_timeout() -> None:
    client = SimpleNamespace(send_request=AsyncMock(side_effect=asyncio.TimeoutError))
    handler = _handler(client)

    assert await handler._resolve_response_language(_msg()) is None


@pytest.mark.asyncio
async def test_security_review_starts_git_before_slow_locale_finishes(monkeypatch) -> None:
    handler = _handler(SimpleNamespace())
    git_started = threading.Event()
    release_git = threading.Event()

    def fake_git(_cwd: str) -> dict[str, str]:
        git_started.set()
        assert release_git.wait(timeout=1.0)
        return {
            "GIT STATUS": "",
            "FILES MODIFIED": "",
            "COMMITS": "",
            "DIFF CONTENT": "",
        }

    async def fake_language(_msg: object) -> str:
        assert git_started.is_set(), "git pre-exec must start before locale lookup finishes"
        release_git.set()
        return "zh"

    monkeypatch.setattr(message_handler_module, "_run_security_review_git", fake_git)
    monkeypatch.setattr(handler, "_resolve_response_language", fake_language)

    prompt = await handler._build_security_review_prompt(_msg(), "", "/repo")

    assert "Respond in Chinese (simplified)." in prompt


@pytest.mark.asyncio
async def test_security_review_cancels_locale_task_when_git_fails(monkeypatch) -> None:
    handler = _handler(SimpleNamespace())
    language_started = threading.Event()
    language_tasks: list[asyncio.Task] = []

    def fake_git(_cwd: str) -> dict[str, str]:
        assert language_started.wait(timeout=1.0)
        raise GitPreExecError("origin/HEAD is not set")

    async def fake_language(_msg: object) -> str:
        language_tasks.append(asyncio.current_task())
        language_started.set()
        await asyncio.sleep(60)
        return "zh"

    monkeypatch.setattr(message_handler_module, "_run_security_review_git", fake_git)
    monkeypatch.setattr(handler, "_resolve_response_language", fake_language)

    with pytest.raises(GitPreExecError):
        await handler._build_security_review_prompt(_msg(), "", "/repo")

    await asyncio.sleep(0)
    assert language_tasks
    assert language_tasks[0].cancelled()
