"""Behavior checks for history conversion, application tools and SSE lifecycle."""

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from ag_ui.core import RunAgentInput
from fastapi import FastAPI

from agent_gateway.channel_manager.protocol.ag_ui.adapter import prepare_input
from agent_gateway.channel_manager.protocol.ag_ui.backend import E2ABackend
from agent_gateway.channel_manager.protocol.ag_ui.events import EventMapper
from agent_gateway.channel_manager.protocol.ag_ui.http import AGUIConfig, ANONYMOUS_PRINCIPAL, RUN_PATH, attach_ag_ui_routes
from agent_gateway.channel_manager.protocol.ag_ui.store import AGUIError, ThreadStore

pytestmark = pytest.mark.asyncio


def user(identity="u1", text="hello"):
    return {"id": identity, "role": "user", "content": text}


def body(messages=None, run_id="r1", **extra):
    return {"threadId": "thread", "runId": run_id, "protocolVersion": "1.0",
            "messages": messages if messages is not None else [user()], **extra}


def decode(response):
    assert response.status_code == 200, response.text
    return [json.loads(line[6:]) for line in response.text.splitlines() if line.startswith("data: ")]


def tool_request(call_id="call1", interrupt_id="interrupt1", **extra):
    return {"event_type": "chat.client_tool_request", "request_id": call_id,
            "interrupt_id": interrupt_id, "source": "client_tool_interrupt",
            "tool_name": "set_value", "arguments": {"value": 1}, **extra}


class Host:
    def __init__(self):
        self.creates = []
        self.inputs = []
        self.records = {}
        self.cancelled = []
        self.behavior = "text"

    async def create(self, principal, token, config):
        self.creates.append((principal, token, config))
        sid = f"internal-{len(self.creates)}"
        self.records[sid] = []
        return sid

    async def history(self, principal, sid):
        return self.records[sid]

    async def stream(self, principal, sid, request_id, params):
        self.inputs.append((principal, sid, request_id, params))
        if self.behavior == "reject":
            yield {"event_type": "execution.status", "request_id": request_id, "state": "rejected"}
            return
        if "query" in params:
            self.records[sid].append({"role": "user", "request_id": request_id})
        for answer in params.get("answers", []):
            self.records[sid].append({"role": "tool", "tool_call_id": answer["request_id"]})
        yield {"event_type": "execution.status", "request_id": request_id, "state": "accepted"}
        for answer in params.get("answers", []):
            yield {"event_type": "chat.tool_result", "tool_call_id": answer["request_id"],
                   "result": answer["result"]}
        if self.behavior == "fact-wrong-id":
            yield {"event_type": "execution.status", "request_id": "other-execution", "state": "completed", "pending_client_tools": []}
        elif self.behavior == "duplicate-accepted":
            yield {"event_type": "execution.status", "request_id": request_id, "state": "accepted"}
        elif self.behavior == "hang":
            await asyncio.Event().wait()
        elif self.behavior in ("tools", "tools-after-observations", "tools-truncated", "tools-incomplete", "observation-only"):
            if self.behavior == "tools-after-observations":
                yield {"event_type": "chat.tool_call", "tool_call": {
                    "name": "set_value", "tool_call_id": "rejected-call"}}
                yield {"event_type": "chat.tool_result", "tool_name": "set_value",
                       "tool_call_id": "rejected-call", "raw_output": {
                           "error": "Deferred tool 'set_value' must be found with tool_search before it can be called."}}
                yield {"event_type": "chat.tool_call", "tool_call": {
                    "name": "tool_search", "tool_call_id": "search-call"}}
                yield {"event_type": "chat.tool_result", "tool_name": "tool_search",
                       "tool_call_id": "search-call", "result": "set_value"}
            self.records[sid].append({"role": "assistant", "request_id": request_id, "event_type": "chat.client_tool_request"})
            for number in (1, 2):
                # Observations and execution requests can coexist on the stream.
                yield {"event_type": "chat.tool_call", "tool_call": {
                    "id": f"call{number}", "name": "set_value", "arguments": {"value": number}}}
                if self.behavior != "observation-only":
                    yield tool_request(f"call{number}", f"interrupt{number}", arguments={"value": number})
            if self.behavior != "tools-truncated":
                numbers = (1,) if self.behavior == "tools-incomplete" else (1, 2)
                yield {"event_type": "execution.status", "request_id": request_id,
                       "state": "waiting_client_tools", "pending_client_tools": [
                           {"request_id": f"call{n}", "interrupt_id": f"interrupt{n}"} for n in numbers]}
        elif self.behavior in ("interaction", "approval"):
            yield {"event_type": "chat.ask_user_question" if self.behavior == "interaction" else "plan.approval_required"}
        else:
            yield {"event_type": "chat.delta", "content": "hello"}
            if self.behavior == "truncated":
                return
            self.records[sid].append({"role": "assistant", "request_id": request_id, "event_type": "chat.final"})
            yield {"event_type": "chat.final", "content": "hello world"}
            if self.behavior == "legacy-final":
                yield {"event_type": "chat.final", "content": "hello world", "completion_state": "completed"}
                yield {"event_type": "chat.run_finished", "status": "success", "pending_tool_call_ids": []}
                return
            yield {"event_type": "execution.status", "request_id": request_id,
                   "state": "completed", "pending_client_tools": []}
            if self.behavior == "error-after-completed":
                yield {"event_type": "chat.error", "error": "late execution failure"}

    async def cancel(self, principal, sid, request_id):
        self.cancelled.append((principal, sid, request_id))
        return True


class Channel:
    async def authenticate_handshake(self, *, headers, **kwargs):
        token = headers.get("authorization", "")
        return SimpleNamespace(success=token.startswith("Bearer "), user_id=token.removeprefix("Bearer "))


def make_app(tmp_path, host=None, timeout=600):
    host = host or Host()
    store = ThreadStore(tmp_path / "mapping.sqlite3")
    app = FastAPI()
    attach_ag_ui_routes(app, Channel(), backend=host, store=store, config=AGUIConfig(
        enabled=True, allowed_origins=("https://app.example",), execution_timeout=timeout,
    ))
    return app, host, store


def client(app, token="alice"):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test",
                             headers={"Authorization": f"Bearer {token}"})


async def test_text_two_rounds_restart_and_identity_isolation(tmp_path):
    app, host, _ = make_app(tmp_path)
    async with client(app) as http:
        first = decode(await http.post(RUN_PATH, json=body()))
    text = [event for event in first if event["type"] == "TEXT_MESSAGE_CONTENT"]
    assert "".join(event["delta"] for event in text) == "hello world"
    assert first[-1]["outcome"]["type"] == "success"
    message_id = next(event["messageId"] for event in first if event["type"] == "TEXT_MESSAGE_START")
    messages = [user(), {"id": message_id, "role": "assistant", "content": "hello world"}, user("u2", "next")]
    app, _, _ = make_app(tmp_path, host)
    async with client(app) as http:
        decode(await http.post(RUN_PATH, json=body(messages, "r2")))
        duplicate = await http.post(RUN_PATH, json=body(messages, "r2"))
        assert duplicate.status_code == 409
        assert duplicate.json()["code"] == "DUPLICATE_RUN"
    assert len(host.creates) == 1
    assert host.inputs[1][1] == host.inputs[0][1]
    assert host.inputs[1][3]["query"] == "next"
    assert "app_message_ids" not in host.inputs[1][3]
    async with client(app, "bob") as http:
        decode(await http.post(RUN_PATH, json=body()))
    assert host.inputs[-1][1] != host.inputs[0][1]
    assert host.inputs[-1][2] != host.inputs[0][2]


@pytest.mark.parametrize("result", ["success", "failed", "denied"])
@pytest.mark.parametrize("with_user", [False, True])
@pytest.mark.parametrize("observations", [False, True])
async def test_app_tools_batch_and_latest_state(tmp_path, result, with_user, observations):
    app, host, _ = make_app(tmp_path)
    host.behavior = "tools-after-observations" if observations else "tools"
    tools = [{"name": "set_value", "description": "Set form value", "parameters": {"type": "object"}}]
    async with client(app) as http:
        first = decode(await http.post(RUN_PATH, json=body(tools=tools, state={"value": 0})))
        assert first[-1]["outcome"]["pendingToolCallIds"] == ["call1", "call2"]
        assistant_id = next(event["parentMessageId"] for event in first if event["type"] == "TOOL_CALL_START")
        old = [user(), {"id": assistant_id, "role": "assistant", "toolCalls": [
            {"id": f"call{number}", "type": "function", "function": {"name": "set_value", "arguments": json.dumps({"value": number})}}
            for number in (1, 2)]}]
        results = [{"id": f"result{n}", "role": "tool", "toolCallId": f"call{n}", "content": result} for n in (1, 2)]
        partial = await http.post(RUN_PATH, json=body(old + results[:1], "partial", state={"value": 1}))
        assert partial.status_code == 409
        invalid = await http.post(RUN_PATH, json=body(old + [user("u2")] + results, "wrong-order", state={}))
        assert invalid.status_code == 400
    # Reopen the store between yielding and answering the native interrupts.
    app, _, _ = make_app(tmp_path, host)
    host.behavior = "text"
    messages = old + results + ([user("u2", "summarize")] if with_user else [])
    async with client(app) as http:
        if with_user:
            response = await http.post(RUN_PATH, json=body(messages, "mixed", state={"value": 2}))
            assert response.status_code == 400
            assert response.json()["code"] == "UNSUPPORTED_MIXED_INPUT"
            assert len(host.inputs) == 1
        events = decode(await http.post(RUN_PATH, json=body(old + results, "r2", tools=[], state={"value": 2},
                context=[{"description": "scene", "value": "form"}], forwardedProps={"user_id": "mallory"})))
        assert events[-1]["type"] == "RUN_FINISHED"
    assert len(host.inputs) == 2
    params = host.inputs[-1][3]
    assert params["run_context"]["state"] == {"value": 2}
    assert params["run_context"]["context"] == [{"description": "scene", "value": "form"}]
    assert params["run_context"]["metadata"] == {"user_id": "mallory"}
    assert "user_id" not in params
    assert "query" not in params
    assert "app_tool_results" not in params
    assert params["source"] == "client_tool_interrupt"
    assert params["answers"] == [
        {"interrupt_id": f"interrupt{n}", "request_id": f"call{n}",
         "result": {"content": [{"type": "text", "text": result}], "is_error": False}}
        for n in (1, 2)]
    assert "app_message_ids" not in params
    assert host.inputs[-1][2] not in ("call1", "call2", host.inputs[0][2])
    assert len([event for event in first if event["type"] == "TOOL_CALL_START"]) == 2
    # A third turn verifies persisted tool results through native history IDs,
    # without requiring external message IDs in the host history.
    mid = next(event["messageId"] for event in events if event["type"] == "TEXT_MESSAGE_START")
    continued = old + results + [{"id": mid, "role": "assistant", "content": "hello world"}, user("u3")]
    app, _, _ = make_app(tmp_path, host)
    async with client(app) as http:
        final = decode(await http.post(RUN_PATH, json=body(continued, "r3")))
    assert final[-1]["type"] == "RUN_FINISHED"


@pytest.mark.parametrize("messages", [[], [user(), user("u2")],
    [{"id": "a", "role": "assistant", "content": "external"}, user()],
    [user(), user()], [{"id": "u", "role": "user", "content": [{"type": "text", "text": "x"}]}]])
async def test_invalid_first_inputs_do_not_create_session(tmp_path, messages):
    app, host, _ = make_app(tmp_path)
    async with client(app) as http:
        assert (await http.post(RUN_PATH, json=body(messages))).status_code in (400, 422)
    assert not host.creates


async def test_auth_cors_preflight_and_disabled_route(tmp_path):
    app, host, _ = make_app(tmp_path)
    async with client(app) as http:
        assert (await http.post(RUN_PATH, json=body(), headers={"Origin": "https://app.example:444"})).status_code == 403
        assert (await http.post(RUN_PATH, json=body(), headers={"Authorization": ""})).status_code == 401
        allowed = await http.options(RUN_PATH, headers={"Origin": "https://app.example",
            "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "authorization,content-type"})
        assert allowed.status_code == 204
        assert allowed.headers["access-control-allow-origin"] == "https://app.example"
        bad = await http.options(RUN_PATH, headers={"Origin": "https://app.example",
            "Access-Control-Request-Method": "POST", "Access-Control-Request-Headers": "x-user-id"})
        assert bad.status_code == 403
    assert not host.creates
    disabled = FastAPI()
    attach_ag_ui_routes(disabled, Channel(), config=AGUIConfig())
    async with client(disabled) as http:
        assert (await http.post(RUN_PATH, json=body())).status_code == 404


@pytest.mark.parametrize("behavior", ["truncated", "interaction", "approval", "hang"])
async def test_incomplete_execution_errors_and_blocks_thread(tmp_path, behavior):
    app, host, _ = make_app(tmp_path, timeout=0.03)
    host.behavior = behavior
    async with client(app) as http:
        events = decode(await http.post(RUN_PATH, json=body()))
        assert events[-1]["type"] == "RUN_ERROR"
        assert not any(event["type"] == "RUN_FINISHED" for event in events)
        assert (await http.post(RUN_PATH, json=body(run_id="r2"))).status_code == 409
    assert host.cancelled[0][2] == host.inputs[0][2]


async def test_rejected_input_does_not_advance_history(tmp_path):
    app, host, _ = make_app(tmp_path)
    host.behavior = "reject"
    async with client(app) as http:
        assert decode(await http.post(RUN_PATH, json=body()))[-1]["type"] == "RUN_ERROR"
        host.behavior = "text"
        decode(await http.post(RUN_PATH, json=body(run_id="r2")))
    assert not host.cancelled
    assert host.inputs[-1][3]["query"] == "hello"


async def test_persistent_execution_claim_and_stable_creation_token(tmp_path):
    path = tmp_path / "routes.sqlite3"
    store = ThreadStore(path)
    first = store.claim("alice", "../../thread", "run")
    with pytest.raises(AGUIError, match="executing"):
        ThreadStore(path).claim("alice", "../../thread", "other")
    store.save("alice", "../../thread", first, release=True)
    second = ThreadStore(path).claim("alice", "../../thread", "other")
    assert first["create_token"] == second["create_token"]
    assert list(tmp_path.iterdir()) == [path]


async def test_session_create_needs_no_capability_negotiation(monkeypatch):
    backend = E2ABackend(SimpleNamespace())
    async def unary(*args):
        return {"session_id": "internal"}
    monkeypatch.setattr(backend, "unary", unary)
    assert await backend.create("alice", "token", {}) == "internal"


async def test_mapper_checks_final_and_pending_ids():
    mapper = EventMapper("t", "r", "internal", [])
    mapper.consume({"event_type": "chat.delta", "content": "prefix"})
    with pytest.raises(AGUIError, match="conflicts"):
        mapper.consume({"event_type": "chat.final", "content": "different"})
    with pytest.raises(AGUIError, match="undeclared"):
        mapper.consume(tool_request(tool_name="shell"))


@pytest.mark.parametrize("confirmed", [False, True])
async def test_actual_asgi_disconnect_cancels_exact_request(tmp_path, confirmed):
    app, host, store = make_app(tmp_path)
    host.behavior = "hang"
    async def cancel(principal, sid, request_id):
        host.cancelled.append((principal, sid, request_id))
        return confirmed
    host.cancel = cancel
    queue = asyncio.Queue()
    await queue.put({"type": "http.request", "body": json.dumps(body()).encode(), "more_body": False})
    async def receive():
        return await queue.get()
    sent = []
    async def send(message):
        sent.append(message)
    scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.3"},
             "http_version": "1.1", "scheme": "http", "method": "POST", "path": RUN_PATH,
             "raw_path": RUN_PATH.encode(), "query_string": b"", "root_path": "",
             "headers": [(b"authorization", b"Bearer alice"), (b"content-type", b"application/json")],
             "server": ("test", 80), "client": ("test", 1234)}
    task = asyncio.create_task(app(scope, receive, send))
    async with asyncio.timeout(2):
        while not host.inputs:
            await asyncio.sleep(0)
        await queue.put({"type": "http.disconnect"})
        await task
    assert host.cancelled == [("alice", host.inputs[0][1], host.inputs[0][2])]
    assert not any(b"RUN_FINISHED" in item.get("body", b"") for item in sent)
    with pytest.raises(AGUIError) as error:
        store.claim("alice", "thread", "next")
    assert error.value.code == ("HISTORY_INCOMPLETE" if confirmed else "THREAD_BUSY")


async def test_missing_client_or_host_history_is_rejected(tmp_path):
    app, host, _ = make_app(tmp_path)
    async with client(app) as http:
        first = decode(await http.post(RUN_PATH, json=body()))
        assert (await http.post(RUN_PATH, json=body([user(), user("u2")], "missing-client"))).status_code == 409
        mid = next(event["messageId"] for event in first if event["type"] == "TEXT_MESSAGE_START")
        host.records[host.inputs[0][1]] = []
        response = await http.post(RUN_PATH, json=body([user(), {"id": mid, "role": "assistant", "content": "hello world"}, user("u2")], "missing-host"))
        assert response.status_code == 409
        assert response.json()["code"] == "HISTORY_MISMATCH"
    assert len(host.inputs) == 1


async def test_real_e2a_envelopes_and_shared_stream_preparation(tmp_path):
    from e2a.agent_models import AgentResponse
    from e2a.message import ReqMethod

    envelopes = []
    class AgentClient:
        async def send_request(self, envelope, *, timeout=None):
            envelopes.append(envelope)
            payload = {"session_id": "internal"}
            if envelope.method == ReqMethod.CHAT_CANCEL.value:
                payload = {"success": True, "target_request_id": envelope.params["target_request_id"]}
            return AgentResponse(request_id=envelope.request_id, channel_id=envelope.channel, ok=True, payload=payload)

    prepared = []
    class Handler:
        async def stream_application_request(self, envelope):
            prepared.append(envelope)
            yield SimpleNamespace(request_id="interaction-id", payload={"event_type": "chat.delta", "content": "ok"})

    backend = E2ABackend(SimpleNamespace(ag_ui_agent_client=AgentClient(), ag_ui_message_handler={"value": Handler()}))
    assert await backend.create("alice", "stable-token", {"role_instructions": "Be concise"}) == "internal"
    create = envelopes[0]
    assert create.user_id == "alice"
    assert create.params["create_token"] == "stable-token"
    assert create.params == {
        "role_instructions": "Be concise", "create_token": "stable-token",
    }
    params = {"source": "client_tool_interrupt", "answers": [
        {"interrupt_id": "interrupt1", "request_id": "call1", "result": {
            "content": [{"type": "text", "text": "ok"}], "is_error": False}}]}
    chunks = [chunk async for chunk in backend.stream("alice", "internal", "execution-id", params)]
    assert chunks == [{"event_type": "chat.delta", "content": "ok"}]
    assert prepared[0].request_id == "execution-id"
    assert prepared[0].user_id == "alice"
    assert prepared[0].params == params
    assert await backend.cancel("alice", "internal", "execution-id") is True
    assert envelopes[-1].params["target_request_id"] == "execution-id"


@pytest.mark.parametrize("when", ["before-stream", "during-stream"])
async def test_asgi_send_failure_closes_iterator_without_gc(tmp_path, when):
    from starlette.requests import ClientDisconnect
    app, host, store = make_app(tmp_path)
    queue = asyncio.Queue()
    await queue.put({"type": "http.request", "body": json.dumps(body()).encode(), "more_body": False})
    async def receive():
        return await queue.get()
    async def send(message):
        if when == "before-stream" or b"TEXT_MESSAGE_CONTENT" in message.get("body", b""):
            raise OSError("connection lost")
    scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": "2.4"},
             "http_version": "1.1", "scheme": "http", "method": "POST", "path": RUN_PATH,
             "raw_path": RUN_PATH.encode(), "query_string": b"", "root_path": "",
             "headers": [(b"authorization", b"Bearer alice"), (b"content-type", b"application/json")],
             "server": ("test", 80), "client": ("test", 1234)}
    with pytest.raises(ClientDisconnect):
        await app(scope, receive, send)
    if when == "before-stream":
        assert not host.cancelled
        store.claim("alice", "thread", "next")
    else:
        assert host.cancelled == [("alice", host.inputs[0][1], host.inputs[0][2])]
        with pytest.raises(AGUIError) as error:
            store.claim("alice", "thread", "next")
        assert error.value.code == "HISTORY_INCOMPLETE"


@pytest.mark.parametrize("invalid_ids", [["c1"], ["c1", "c1"], ["c1", "unknown"]])
async def test_tool_results_validation_is_atomic(invalid_ids):
    thread = {"messages": [{"id": "u1", "role": "user"}], "pending": [{"id": "c1"}, {"id": "c2"}], "request_id": "internal"}
    results = [{"id": f"result{n}", "role": "tool", "toolCallId": call, "content": "ok"}
               for n, call in enumerate(invalid_ids)]
    run = RunAgentInput.model_validate(body([user(), *results], state={}))
    with pytest.raises(AGUIError):
        prepare_input(run, thread)
    assert thread["messages"] == [{"id": "u1", "role": "user"}]
    assert thread["pending"] == [{"id": "c1"}, {"id": "c2"}]


async def test_text_final_and_explicit_outcome_usage_can_coexist():
    mapper = EventMapper("t", "r", "internal", [])
    mapper.consume({"event_type": "chat.final", "content": "hello", "completion_state": "completed"})
    events = mapper.consume({"event_type": "execution.status", "request_id": "internal",
                             "state": "completed", "pending_client_tools": [],
                             "usage": {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7}})
    assert events[-1].model_dump(by_alias=True)["usage"] == [{"inputTokens": 5, "outputTokens": 2, "totalTokens": 7}]


async def test_message_handler_prepares_credentials_and_closes_stream():
    from agent_gateway.message_handler.message_handler import MessageHandler
    steps = []
    envelope = SimpleNamespace(request_id="execution")
    async def prepare(env):
        assert env is envelope
        steps.append("credentials")
    async def stream(env):
        assert steps == ["credentials"]
        try:
            yield SimpleNamespace(payload={"event_type": "chat.delta", "content": "ok"})
        finally:
            steps.append("closed")
    handler = SimpleNamespace(_apply_session_login_owner=prepare,
                              agent_client=SimpleNamespace(send_request_stream=stream))
    generator = MessageHandler.stream_application_request(handler, envelope)
    await anext(generator)
    await generator.aclose()
    assert steps == ["credentials", "closed"]


async def test_no_auth_configuration_allows_tokenless_runs(tmp_path):
    host = Host()
    async def authenticate(**kwargs):
        return None
    app = FastAPI()
    attach_ag_ui_routes(app, SimpleNamespace(authenticate_handshake=authenticate), backend=host,
                       store=ThreadStore(tmp_path / "no-auth.sqlite3"), config=AGUIConfig(enabled=True))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as http:
        first = decode(await http.post(RUN_PATH, json=body(forwardedProps={"user_id": "mallory"})))
        assert first[-1]["type"] == "RUN_FINISHED"
        other = decode(await http.post(RUN_PATH, json={**body(), "threadId": "other-thread"}))
        assert other[-1]["type"] == "RUN_FINISHED"
        duplicate = await http.post(RUN_PATH, json=body())
        assert duplicate.status_code == 409
        assert duplicate.json()["code"] == "DUPLICATE_RUN"
    assert host.creates[0][0] == ANONYMOUS_PRINCIPAL
    assert host.inputs[0][0] == ANONYMOUS_PRINCIPAL
    assert host.inputs[0][1] != host.inputs[1][1]


async def test_pending_tools_require_explicit_state_but_allow_null():
    thread = {"messages": [], "pending": [{"id": "c", "interrupt_id": "intr"}], "request_id": "internal"}
    messages = [{"id": "result", "role": "tool", "toolCallId": "c", "content": "denied"}]
    with pytest.raises(AGUIError, match="state"):
        prepare_input(RunAgentInput.model_validate(body(messages)), thread)
    params, _ = prepare_input(RunAgentInput.model_validate(body(messages, state=None)), thread)
    assert "query" not in params
    assert params["run_context"]["state"] is None


@pytest.mark.parametrize("behavior", ["tools-truncated", "tools-incomplete", "observation-only"])
async def test_client_tool_yield_requires_confirmed_complete_batch(tmp_path, behavior):
    app, host, _ = make_app(tmp_path)
    host.behavior = behavior
    async with client(app) as http:
        events = decode(await http.post(RUN_PATH, json=body(
            tools=[{"name": "set_value", "description": "Set", "parameters": {}}])))
    assert events[-1]["type"] == "RUN_ERROR"
    assert not any(event["type"] == "RUN_FINISHED" for event in events)
    if behavior == "observation-only":
        assert not any(event["type"] == "TOOL_CALL_START" for event in events)
    assert host.cancelled[0][2] == host.inputs[0][2]


async def test_native_request_coexists_with_observation_and_preserves_execution_identity():
    mapper = EventMapper("t", "r", "execution", [{"name": "set_value"}])
    observation = {"event_type": "chat.tool_call", "tool_call": {
        "id": "call1", "name": "set_value", "arguments": {"value": 1}}}
    assert mapper.consume(observation) == []
    events = mapper.consume(tool_request(client_tool={"input": {"value": 999}}))
    assert [event.type.value for event in events] == ["TOOL_CALL_START", "TOOL_CALL_ARGS", "TOOL_CALL_END"]
    assert json.loads(events[1].delta) == {"value": 1}
    assert mapper.consume(observation) == []
    assert mapper.message_ref() == {"id": "execution-assistant", "role": "assistant", "request_id": "execution"}
    assert mapper.calls[0]["interrupt_id"] == "interrupt1"


@pytest.mark.parametrize("extra", [
    {"request_id": ""}, {"interrupt_id": ""}, {"interrupt_id": None},
    {"source": "approval"}, {"tool_name": "shell"}, {"arguments": "bad-json"}, {"arguments": []},
])
async def test_invalid_client_tool_request_is_rejected(extra):
    mapper = EventMapper("t", "r", "execution", [{"name": "set_value"}])
    with pytest.raises(AGUIError):
        mapper.consume(tool_request(**extra))
    assert not mapper.calls


@pytest.mark.parametrize("call_id,interrupt_id", [("call1", "other"), ("other", "interrupt1")])
async def test_duplicate_client_tool_identity_is_rejected(call_id, interrupt_id):
    mapper = EventMapper("t", "r", "execution", [{"name": "set_value"}])
    mapper.consume(tool_request())
    with pytest.raises(AGUIError, match="repeated"):
        mapper.consume(tool_request(call_id, interrupt_id))


@pytest.mark.parametrize("interrupt_id", [None, "", " "])
async def test_pending_tools_without_interrupt_mapping_cannot_resume(interrupt_id):
    call = {"id": "c"}
    if interrupt_id is not None:
        call["interrupt_id"] = interrupt_id
    thread = {"messages": [], "pending": [call], "request_id": "execution"}
    run = RunAgentInput.model_validate(body([
        {"id": "result", "role": "tool", "toolCallId": "c", "content": "ok"}], state={}))
    with pytest.raises(AGUIError) as error:
        prepare_input(run, thread)
    assert error.value.code == "HISTORY_INCOMPLETE"


@pytest.mark.parametrize("as_parts", [False, True])
async def test_tool_error_and_opaque_content_use_native_result(as_parts):
    thread = {"messages": [], "pending": [{"id": "c", "interrupt_id": "intr"}], "request_id": "execution"}
    text = '{"status":"failed","data":{"slide":3}}'
    content = [{"type": "text", "text": text}] if as_parts else text
    run = RunAgentInput.model_validate(body([
        {"id": "result", "role": "tool", "toolCallId": "c", "content": content,
         "error": "App denied execution"}], state=None))
    params, _ = prepare_input(run, thread)
    assert params["answers"] == [{"request_id": "c", "interrupt_id": "intr", "result": {
        "content": [{"type": "text", "text": text}, {"type": "text", "text": "App denied execution"}],
        "is_error": True}}]


@pytest.mark.parametrize("session_id", [None, "", 123])
async def test_session_create_requires_valid_session_id(monkeypatch, session_id):
    backend = E2ABackend(SimpleNamespace())
    async def unary(*args):
        return {"session_id": session_id}
    monkeypatch.setattr(backend, "unary", unary)
    with pytest.raises(AGUIError) as error:
        await backend.create("alice", "token", {})
    assert error.value.code == "HOST_PROTOCOL_ERROR"


@pytest.mark.parametrize("behavior", ["fact-wrong-id", "duplicate-accepted", "legacy-final", "error-after-completed"])
async def test_execution_facts_do_not_infer_or_prematurely_commit_success(tmp_path, behavior):
    app, host, _ = make_app(tmp_path)
    host.behavior = behavior
    async with client(app) as http:
        events = decode(await http.post(RUN_PATH, json=body()))
    assert events[-1]["type"] == "RUN_ERROR"
    assert not any(event["type"] == "RUN_FINISHED" for event in events)
    assert host.cancelled[0][2] == host.inputs[0][2]


@pytest.mark.parametrize("state,pending", [
    ("completed", [{"request_id": "call1", "interrupt_id": "interrupt1"}]),
    ("waiting_client_tools", []),
    ("waiting_client_tools", [{"request_id": "call1", "interrupt_id": "wrong"}]),
    ("waiting_client_tools", [{"request_id": "call1", "interrupt_id": "interrupt1"}] * 2),
    ("waiting_client_tools", ["call1"]),
    ("failed", []), ("cancelled", []), ("unknown", []), ([], []),
])
async def test_execution_outcomes_check_state_and_complete_interrupt_identity(state, pending):
    mapper = EventMapper("t", "r", "execution", [{"name": "set_value"}])
    mapper.consume(tool_request())
    with pytest.raises(AGUIError):
        mapper.consume({"event_type": "execution.status", "request_id": "execution",
                        "state": state, "pending_client_tools": pending})


async def test_context_omission_null_and_untrusted_metadata_are_distinct():
    thread = {"messages": [], "pending": [], "request_id": "execution"}
    plain, _ = prepare_input(RunAgentInput.model_validate(body()), thread)
    supplied, refs = prepare_input(RunAgentInput.model_validate(body(
        state=None, context=[], forwardedProps={"execution_policy": {"tool_execution": "all"}})), thread)
    assert plain["run_context"] == {}
    assert supplied["run_context"] == {"state": None, "context": [],
        "metadata": {"execution_policy": {"tool_execution": "all"}}}
    assert set(supplied) == {"query", "client_tools", "run_context"}
    assert refs == [{"id": "u1", "role": "user", "request_id": "execution"}]


async def test_existing_session_without_contract_marker_can_continue(tmp_path):
    app, host, store = make_app(tmp_path)
    thread = store.claim("alice", "thread", "old")
    thread["session_id"] = "existing-session"
    host.records["existing-session"] = []
    store.save("alice", "thread", thread, release=True)
    async with client(app) as http:
        events = decode(await http.post(RUN_PATH, json=body()))
    assert events[-1]["type"] == "RUN_FINISHED"
    assert host.inputs[0][1] == "existing-session"
    assert not host.creates


@pytest.mark.parametrize("payload", [
    {"event_type": "execution.status", "state": "completed", "pending_client_tools": []},
    {"event_type": "execution.status", "state": "failed"},
    {"event_type": "chat.delta", "content": "premature"},
    {"event_type": "chat.input_accepted", "message_ids": ["u1"]},
])
async def test_output_without_acceptance_fact_fails_closed(tmp_path, payload):
    app, host, _ = make_app(tmp_path)
    async def stream(principal, sid, request_id, params):
        host.inputs.append((principal, sid, request_id, params))
        yield {"request_id": request_id, **payload}
    host.stream = stream
    async with client(app) as http:
        events = decode(await http.post(RUN_PATH, json=body()))
    assert events[-1]["type"] == "RUN_ERROR"
    assert events[-1]["code"] == "HOST_PROTOCOL_ERROR"
    assert not any(event["type"] == "RUN_FINISHED" for event in events)
    assert host.cancelled[0][2] == host.inputs[0][2]
