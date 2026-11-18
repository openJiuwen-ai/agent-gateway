"""Regression tests for issue #125: group_digital_avatar must not suppress p2p emoji ack."""

import asyncio
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from agent_gateway.channel_manager.im_platforms.feishu.feishu_connect import FeishuChannel, FeishuConfig

NOW = datetime(2026, 11, 18, 9, 0, tzinfo=timezone.utc)


def _channel(group_digital_avatar: bool) -> FeishuChannel:
    channel = FeishuChannel(
        FeishuConfig(
            enabled=True,
            app_id="app-dev",
            agent_id="",
            channel_id="test-feishu",
            group_digital_avatar=group_digital_avatar,
        ),
        MagicMock(),
    )
    channel._add_reaction = AsyncMock()
    channel._parse_message_content_with_file = AsyncMock(return_value=("text", None))
    channel._process_batched_message = AsyncMock()
    return channel


def _raw_event(chat_type: str, message_id: str = "om_001"):
    message = SimpleNamespace(
        message_id=message_id,
        chat_id="oc_project",
        chat_type=chat_type,
        create_time=str(int(NOW.timestamp() * 1000)),
        message_type="text",
        content=json.dumps({"text": "你好"}),
        mentions=[],
        parent_id="",
        root_id="",
    )
    sender = SimpleNamespace(
        sender_type="user",
        sender_id=SimpleNamespace(open_id="ou_alice", user_id="u_alice", union_id="un_alice"),
        app_id=None,
    )
    return SimpleNamespace(event=SimpleNamespace(message=message, sender=sender))


async def _drain_tasks():
    for _ in range(3):
        await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("chat_type", ["p2p", "group"])
async def test_avatar_off_always_acks(chat_type):
    channel = _channel(group_digital_avatar=False)
    await channel._on_message(_raw_event(chat_type))
    await _drain_tasks()
    channel._add_reaction.assert_awaited_once()


@pytest.mark.asyncio
async def test_avatar_on_group_message_is_not_acked():
    channel = _channel(group_digital_avatar=True)
    await channel._on_message(_raw_event("group"))
    await _drain_tasks()
    channel._add_reaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_avatar_on_p2p_message_still_acks():
    channel = _channel(group_digital_avatar=True)
    await channel._on_message(_raw_event("p2p"))
    await _drain_tasks()
    channel._add_reaction.assert_awaited_once()
