# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.

"""build_review_prompt 单元测试."""

import pytest

from agent_gateway.message_handler.prompts.review_prompt import build_review_prompt


def test_build_review_prompt_empty_args_lists_prs() -> None:
    prompt = build_review_prompt("")
    assert "`gh pr list`" in prompt
    assert "`gh pr view <number>`" in prompt
    assert "`gh pr diff <number>`" in prompt
    assert "PR number: " in prompt
    assert prompt.rstrip().endswith("PR number:")
    assert "git log" not in prompt
    assert "git diff" not in prompt


def test_build_review_prompt_passes_through_args() -> None:
    prompt = build_review_prompt("123 focus on security")
    assert prompt.rstrip().endswith("PR number: 123 focus on security")


@pytest.mark.parametrize("local_language", ["zh", "en"])
def test_review_prompt_does_not_force_gateway_language(monkeypatch, local_language):
    from agent_gateway.common import config

    monkeypatch.setattr(config, "get_config", lambda: {"preferred_language": local_language})
    prompt = build_review_prompt("42")
    assert "Respond in Chinese (simplified)." not in prompt
    assert "Respond in English." not in prompt
    assert prompt.rstrip().endswith("PR number: 42")
