# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""MCP Apps requests do not require a gateway method catalogue."""

import pytest

from tests.unit_tests.channel.test_method_passthrough import (
    channel,
    test_unregistered_ws_method_preserves_envelope as check_forward,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("method", [
    "mcp_app.list_tools", "mcp_app.read_resource", "mcp_app.call_tool", "mcp_app.future.operation",
])
async def test_mcp_app_methods_are_plain_forwards(channel, method):
    await check_forward(channel, method, False)
