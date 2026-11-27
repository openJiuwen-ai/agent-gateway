"""AG-UI endpoint on the existing WebChannel HTTP service."""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import aclosing
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anyio
from ag_ui.core import RunAgentInput, RunErrorEvent, RunFinishedEvent, RunStartedEvent
from ag_ui.encoder import EventEncoder
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import ValidationError

from agent_gateway.common.paths import get_user_workspace_dir
from .adapter import prepare_input, validate_run
from .backend import E2ABackend
from .events import EXECUTION_STATUS, EventMapper, execution_state
from .store import AGUIError, ThreadStore

logger = logging.getLogger(__name__)
RUN_PATH = "/api/ag-ui/run"
MAX_BODY_BYTES = 2 * 1024 * 1024
ANONYMOUS_PRINCIPAL = "anonymous"


@dataclass(frozen=True)
class AGUIConfig:
    enabled: bool = False
    allowed_origins: tuple[str, ...] = ()
    session_params: dict[str, Any] = field(default_factory=dict)
    store_path: Path | None = None
    execution_timeout: float = 600
    cancel_timeout: float = 20

    @classmethod
    def from_environment(cls) -> AGUIConfig:
        params: dict[str, Any] = {
            "role_instructions": os.getenv("AG_UI_ROLE_INSTRUCTIONS", "Help the application's user using the conversation, current application context, App tools and authorized host capabilities. Ask questions in ordinary text."),
        }
        for env, key in (("AG_UI_PROJECT_ID", "project_id"), ("AG_UI_MODEL_NAME", "model_name")):
            if os.getenv(env):
                params[key] = os.environ[env]
        return cls(
            enabled=os.getenv("AG_UI_ENABLED", "").strip() == "1",
            allowed_origins=tuple(value.strip() for value in os.getenv("AG_UI_ALLOWED_ORIGINS", "").split(",") if value.strip()),
            session_params=params,
        )


def _cors(request: Request, config: AGUIConfig) -> dict[str, str]:
    origin = request.headers.get("origin")
    if origin is None:
        return {}
    if origin == "null" or origin not in config.allowed_origins:
        raise AGUIError("Origin is not allowed", "ORIGIN_NOT_ALLOWED", 403)
    return {"Access-Control-Allow-Origin": origin, "Vary": "Origin"}


async def _principal(channel: Any, request: Request) -> str:
    result = await channel.authenticate_handshake(
        path=RUN_PATH, headers=request.headers,
        remote=request.client.host if request.client else "", channel="web",
    )
    if result is None:
        return ANONYMOUS_PRINCIPAL
    identity = str(getattr(result, "user_id", "") or "").strip()
    if not bool(getattr(result, "success", False)) or not identity:
        raise AGUIError("Unauthorized", "UNAUTHORIZED", 401)
    return identity


async def _read_run(request: Request) -> RunAgentInput:
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > MAX_BODY_BYTES:
            raise AGUIError("request body is too large", "BODY_TOO_LARGE", 413)
    try:
        return RunAgentInput.model_validate_json(body)
    except ValidationError as exc:
        # Validation details can contain complete private prompts and state.
        raise AGUIError("invalid RunAgentInput", "INVALID_INPUT", 422) from exc


def _verify_history(thread: dict, records: list[dict]) -> None:
    for message in thread["messages"]:
        matched = any(
            (message["role"] in ("user", "assistant")
                and record.get("request_id") == message["request_id"]
                and record.get("role") == message["role"]
                and (message["role"] == "user" or record.get("event_type") in (
                    "chat.final", "chat.tool_call", "chat.client_tool_request")))
            or (message["role"] == "tool" and message.get("tool_call_id") is not None
                and record.get("role") == "tool"
                and record.get("tool_call_id") == message["tool_call_id"])
            for record in records if isinstance(record, dict)
        )
        if not matched:
            raise AGUIError("host history no longer matches this thread", "HISTORY_MISMATCH", 409)


def attach_ag_ui_routes(
    app: FastAPI, channel: Any, *, config: AGUIConfig | None = None,
    backend: Any = None, store: ThreadStore | None = None,
) -> None:
    config = config or AGUIConfig.from_environment()
    if not config.enabled:
        return
    backend = backend or E2ABackend(channel)

    def get_store() -> ThreadStore:
        nonlocal store
        if store is None:
            store = ThreadStore(config.store_path or get_user_workspace_dir() / "agent" / ".checkpoint" / "ag_ui.sqlite3")
        return store

    @app.options(RUN_PATH)
    async def preflight(request: Request) -> Response:
        try:
            headers = _cors(request, config)
            requested = {value.strip().lower() for value in request.headers.get("access-control-request-headers", "").split(",") if value.strip()}
            if request.headers.get("access-control-request-method") != "POST" or not requested <= {"content-type", "authorization", "accept"}:
                raise AGUIError("preflight method or headers are not allowed", "CORS_NOT_ALLOWED", 403)
            return Response(status_code=204, headers={**headers, "Access-Control-Allow-Methods": "POST",
                "Access-Control-Allow-Headers": "Content-Type, Authorization, Accept"})
        except AGUIError as exc:
            return JSONResponse({"error": str(exc), "code": exc.code}, status_code=exc.status)

    @app.post(RUN_PATH)
    async def run_agent(request: Request) -> Response:
        headers = {"Cache-Control": "no-store", "X-Accel-Buffering": "no"}
        thread = None
        run = None
        principal = ""
        try:
            headers.update(_cors(request, config))
            principal = await _principal(channel, request)
            run = await _read_run(request)
            validate_run(run)
            db = get_store()
            thread = db.claim(principal, run.thread_id, run.run_id)
            params, added = prepare_input(run, thread)
            if thread["session_id"] is None:
                thread["session_id"] = await backend.create(principal, thread["create_token"], config.session_params)
                db.save(principal, run.thread_id, thread)
            elif thread["messages"]:
                _verify_history(thread, await backend.history(principal, thread["session_id"]))
        except BaseException as exc:
            if thread is not None and run is not None:
                get_store().save(principal, run.thread_id, thread, release=True)
            if isinstance(exc, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
                raise
            if isinstance(exc, AGUIError):
                return JSONResponse({"error": str(exc), "code": exc.code}, status_code=exc.status, headers=headers)
            logger.exception("AG-UI request preparation failed")
            return JSONResponse({"error": "Gateway request preparation failed", "code": "INTERNAL_ERROR"}, status_code=500, headers=headers)

        encoder = EventEncoder(accept=request.headers.get("accept"))
        entered = False

        async def events():
            nonlocal entered
            entered = True
            mapper = EventMapper(run.thread_id, run.run_id, thread["request_id"], params["client_tools"])
            accepted = False
            committed = False
            dispatched = False
            rejected = False
            try:
                yield encoder.encode(RunStartedEvent(thread_id=run.thread_id, run_id=run.run_id, protocol_version="1.0"))
                dispatched = True
                async with asyncio.timeout(config.execution_timeout):
                    async with aclosing(backend.stream(principal, thread["session_id"], thread["request_id"], params)) as stream:
                        terminal = None
                        async for payload in stream:
                            if payload.get("event_type") == EXECUTION_STATUS:
                                state = execution_state(payload, thread["request_id"])
                                if state == "rejected" and not accepted:
                                    rejected = True
                                    raise AGUIError("AgentServer rejected the input", "INPUT_REJECTED", 502)
                                if state == "accepted":
                                    if accepted:
                                        raise AGUIError("duplicate input receipt", "HOST_PROTOCOL_ERROR", 502)
                                    accepted = True
                                    thread["messages"].extend(added)
                                    thread["pending"] = []
                                    db.save(principal, run.thread_id, thread)
                                    continue
                            if not payload or set(payload) <= {"is_complete"}:
                                continue
                            if not accepted:
                                raise AGUIError("host did not confirm input acceptance", "HOST_PROTOCOL_ERROR", 502)
                            for event in mapper.consume(payload):
                                if isinstance(event, RunFinishedEvent):
                                    terminal = event
                                else:
                                    yield encoder.encode(event)
                        if terminal is None:
                            raise AGUIError("host stream ended without an execution outcome", "INCOMPLETE_EXECUTION", 502)
                        ref = mapper.message_ref()
                        if ref:
                            thread["messages"].append(ref)
                        thread["pending"] = mapper.calls
                        db.save(principal, run.thread_id, thread)
                        committed = True
                        yield encoder.encode(terminal)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if isinstance(exc, TimeoutError):
                    exc = AGUIError("execution timed out", "EXECUTION_TIMEOUT", 504)
                logger.warning("AG-UI execution failed request_id=%s code=%s", thread["request_id"], getattr(exc, "code", "INTERNAL_ERROR"))
                yield encoder.encode(RunErrorEvent(
                    message=str(exc) if isinstance(exc, AGUIError) else "Gateway execution failed",
                    code=exc.code if isinstance(exc, AGUIError) else "INTERNAL_ERROR",
                ))
            finally:
                # Shield cleanup from Starlette's disconnect cancellation scope.
                # The persistent claim remains held if targeted cancel fails.
                with anyio.CancelScope(shield=True):
                    release = committed or not dispatched or rejected
                    if dispatched and not committed and not rejected:
                        thread["blocked"] = True
                        try:
                            release = await asyncio.wait_for(
                                backend.cancel(principal, thread["session_id"], thread["request_id"]),
                                timeout=config.cancel_timeout,
                            )
                        except Exception:
                            logger.warning("AG-UI cancellation unconfirmed request_id=%s", thread["request_id"])
                    db.save(principal, run.thread_id, thread, release=release)

        class RunResponse(StreamingResponse):
            async def __call__(self, scope, receive, send):
                try:
                    await super().__call__(scope, receive, send)
                finally:
                    # ASGI >= 2.4 reports disconnect via send() raising OSError.
                    # Explicitly close the iterator so cleanup does not depend
                    # on async-generator garbage collection in that path.
                    with anyio.CancelScope(shield=True):
                        await self.body_iterator.aclose()
                        if not entered:
                            db.save(principal, run.thread_id, thread, release=True)

        return RunResponse(events(), media_type=encoder.get_content_type(), headers=headers)
