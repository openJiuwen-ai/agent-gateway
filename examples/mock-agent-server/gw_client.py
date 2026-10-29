# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Mock upstream client: send chat.send through the /tui route and print events."""

import asyncio
import json
import sys

import websockets


async def main() -> None:
    async with websockets.connect(sys.argv[1]) as ws:
        await ws.send(
            json.dumps(
                {
                    "type": "req",
                    "id": "req-chat",
                    "method": "chat.send",
                    "params": {"session_id": "sess_test", "content": "hello gateway", "mode": "agent.plan"},
                }
            )
        )
        deadline = asyncio.get_running_loop().time() + 15
        while asyncio.get_running_loop().time() < deadline:
            raw = await asyncio.wait_for(ws.recv(), timeout=15)
            frame = json.loads(raw)
            print("RECV", json.dumps(frame, ensure_ascii=False)[:400], flush=True)
            if frame.get("type") == "res" and frame.get("id") == "req-chat":
                break


asyncio.run(main())
