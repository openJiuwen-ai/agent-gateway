# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.
"""Mock AgentServer: reply with connection.ack on connect and echo requests."""

import asyncio
import json
import sys

from jiuwenswarm.common.e2a.wire_codec import (
    encode_agent_chunk_for_wire,
    encode_agent_response_for_wire,
)
from jiuwenswarm.common.schema.agent import AgentResponse, AgentResponseChunk
from websockets.legacy.server import serve


async def handler(ws):
    # Required: the gateway relies on this frame to mark server_ready as True.
    await ws.send(
        json.dumps(
            {
                "type": "event",
                "event": "connection.ack",
                "payload": {"status": "ready", "readiness": "AGENT_READY"},
            }
        )
    )
    print("MOCK_CONNECT", flush=True)
    try:
        async for raw in ws:
            data = json.loads(raw)
            req_id = data.get("request_id", "")
            ch_id = data.get("channel") or data.get("channel_id", "")
            params = data.get("params", {})
            print("MOCK_RECV", data.get("method"), json.dumps(params, ensure_ascii=False)[:300], flush=True)
            if data.get("is_stream"):
                for i, part in enumerate(["流式-1 ", "流式-2 ", "流式-3(完)"]):
                    chunk = AgentResponseChunk(
                        request_id=req_id,
                        channel_id=ch_id,
                        payload={"content": part},
                        is_complete=i == 2,
                    )
                    await ws.send(
                        json.dumps(
                            encode_agent_chunk_for_wire(chunk, response_id=req_id, sequence=i),
                            ensure_ascii=False,
                        )
                    )
            else:
                resp = AgentResponse(
                    request_id=req_id,
                    channel_id=ch_id,
                    ok=True,
                    payload={"content": f"Echo: {json.dumps(params, ensure_ascii=False)}"},
                )
                await ws.send(
                    json.dumps(encode_agent_response_for_wire(resp, response_id=req_id), ensure_ascii=False)
                )
    except Exception:  # Exit when the connection is closed.
        pass


async def main() -> None:
    port = int(sys.argv[1])
    server = await serve(handler, "127.0.0.1", port, ping_interval=None)
    print(f"MOCK_READY ws://127.0.0.1:{port}", flush=True)
    await server.wait_closed()


asyncio.run(main())
