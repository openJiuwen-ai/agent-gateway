import pytest

from agent_gateway.channel_manager.im_platforms.message_content import strip_think_tags


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("before<think>secret</think>after", "beforeafter"),
        ("before<THINK>secret\nmore</THINK>after", "beforeafter"),
        ("before<think>unfinished\nsecret", "before"),
        ("plain <thinker>keep</thinker>", "plain <thinker>keep</thinker>"),
        ("", ""),
        (None, ""),
    ],
)
def test_strip_think_tags(text, expected) -> None:
    assert strip_think_tags(text) == expected
