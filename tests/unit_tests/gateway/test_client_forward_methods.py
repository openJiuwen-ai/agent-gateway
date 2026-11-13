# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Client requests use the default upstream path, including future methods."""

import pytest

from tests.unit_tests.channel.test_method_passthrough import (
    channel,
    test_unregistered_ws_method_preserves_envelope as check_forward,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", [
    "client.attach", "client.renew", "client.list", "client.conversation.messages",
    "client.conversations", "client.future.operation",
])
async def test_client_methods_are_plain_forwards(channel, method):
    await check_forward(channel, method, False)
