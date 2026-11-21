# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import pytest

from agent_gateway.channel_manager.im_platforms.thinking_content import (
    is_thinking_only_content,
    strip_think_tags,
)
from agent_gateway.channel_manager.im_platforms.wechat.wechat_connect import WechatChannel
from agent_gateway.channel_manager.im_platforms.wecom.wecom_connect import WecomChannel


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("before<think>secret</think>after", "beforeafter"),
        ("before<THINK>secret\nmore</THINK>after", "beforeafter"),
        ("before<think>unfinished\nsecret", "before"),
        ("plain <thinker>keep</thinker>", "plain <thinker>keep</thinker>"),
        ("", ""),
        (None, ""),
        (42, ""),
        (["answer"], ""),
        ({"content": "answer"}, ""),
    ],
)
def test_strip_think_tags(text, expected) -> None:
    assert strip_think_tags(text) == expected
    assert WechatChannel._strip_think_tags(text) == expected
    assert WecomChannel._strip_think_tags(text) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (None, True),
        (42, True),
        (["answer"], True),
        ("", True),
        (" \n\t", True),
        (".\uFF0E\u3002\u2026 \n...", True),
        ("answer...", False),
        ("...!", False),
    ],
)
def test_thinking_only_content_has_shared_channel_behavior(text, expected) -> None:
    assert is_thinking_only_content(text) is expected
    assert WechatChannel._is_thinking_only_content(text) is expected
    assert WecomChannel._is_thinking_only_content(text) is expected
