# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

import asyncio

from agent_gateway.inbound_server import InboundGatewayServer


async def test_inbound_server_delivers_queued_messages_in_order():
    seen = []
    all_done = asyncio.Event()

    async def handler(msg):
        seen.append(msg)
        if len(seen) == 2:
            all_done.set()

    server = InboundGatewayServer(handler)
    await server.start()
    await server.handle_message("msg-1")
    await server.handle_message("msg-2")
    await asyncio.wait_for(all_done.wait(), 1.0)
    await server.stop()

    assert seen == ["msg-1", "msg-2"]
    assert server._task is None


async def test_inbound_server_handler_failure_does_not_stop_serving():
    seen = []
    first_done = asyncio.Event()
    second_done = asyncio.Event()

    async def handler(msg):
        seen.append(msg)
        if msg == "fail":
            first_done.set()
            raise RuntimeError("handler failed")
        else:
            second_done.set()

    server = InboundGatewayServer(handler)
    await server.start()
    await server.handle_message("fail")
    await asyncio.wait_for(first_done.wait(), 1.0)
    await server.handle_message("after-failure")
    await asyncio.wait_for(second_done.wait(), 1.0)
    await server.stop()

    assert seen == ["fail", "after-failure"]


async def test_inbound_server_start_is_idempotent():
    async def handler(msg):
        return None

    server = InboundGatewayServer(handler)
    await server.start()
    first_task = server._task
    await server.start()

    assert server._task is first_task
    await server.stop()
