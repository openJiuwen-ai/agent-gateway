"""Request-scoped E2A events to official AG-UI 1.0 event types."""

from __future__ import annotations

import json
from typing import Any

from ag_ui.core import (
    BaseEvent, TextMessageStartEvent, TextMessageContentEvent, TextMessageEndEvent,
    ToolCallStartEvent, ToolCallArgsEvent, ToolCallEndEvent, RunFinishedEvent,
)

from .store import AGUIError

EXECUTION_STATUS = "execution.status"


def execution_state(payload: dict[str, Any], request_id: str) -> str:
    """Facts always identify the execution, never a tool interaction."""
    if (payload.get("request_id") != request_id
            or not isinstance(payload.get("state"), str)
            or payload["state"] not in {
                "accepted", "rejected", "completed", "waiting_client_tools", "failed", "cancelled"}):
        raise AGUIError("invalid execution fact", "HOST_PROTOCOL_ERROR", 502)
    return payload["state"]


class EventMapper:
    def __init__(self, thread_id: str, run_id: str, request_id: str, tools: list[dict[str, Any]]):
        self.thread_id = thread_id
        self.run_id = run_id
        self.message_id = f"{request_id}-assistant"
        self.request_id = request_id
        self.tools = {tool["name"] for tool in tools}
        self.started = False
        self.ended = False
        self.text = ""
        self.calls: list[dict[str, Any]] = []
        self.finished = False
        self.usage = None

    def message_ref(self) -> dict[str, str] | None:
        if not self.started and not self.calls:
            return None
        return {"id": self.message_id, "role": "assistant", "request_id": self.request_id}

    def _start_text(self) -> list[BaseEvent]:
        if self.started:
            return []
        self.started = True
        return [TextMessageStartEvent(message_id=self.message_id, role="assistant")]

    def _end_text(self) -> list[BaseEvent]:
        if not self.started or self.ended:
            return []
        self.ended = True
        return [TextMessageEndEvent(message_id=self.message_id)]

    def consume(self, payload: dict[str, Any]) -> list[BaseEvent]:
        kind = payload.get("event_type")
        if self.finished:
            if kind in ("chat.processing_status", "session.request_completed"):
                return []
            raise AGUIError("received output after execution ended", "HOST_PROTOCOL_ERROR", 502)
        if kind in ("chat.error", "runtime.error") or (not kind and payload.get("error")):
            raise AGUIError("AgentServer execution failed", str(payload.get("code") or "HOST_ERROR"), 502)
        if kind in {"chat.ask_user", "chat.ask_user_question", "chat.confirm", "chat.permission", "chat.interrupt",
                    "plan.approval_required",
                    "ask_user", "confirm", "permission", "interrupt"} or payload.get("interrupts"):
            raise AGUIError("host interaction is not supported", "UNSUPPORTED_HOST_INTERACTION", 502)
        if kind == "chat.delta":
            if self.ended:
                raise AGUIError("text delta after final", "HOST_PROTOCOL_ERROR", 502)
            delta = payload.get("content", "")
            if not isinstance(delta, str):
                raise AGUIError("host returned non-text output", "HOST_PROTOCOL_ERROR", 502)
            if not delta:
                return []
            events = self._start_text()
            self.text += delta
            return [*events, TextMessageContentEvent(message_id=self.message_id, delta=delta)]
        if kind == "chat.final":
            content = payload.get("content", "")
            if not isinstance(content, str) or not content.startswith(self.text):
                raise AGUIError("final text conflicts with streamed output", "HOST_PROTOCOL_ERROR", 502)
            events = []
            if content:
                events.extend(self._start_text())
                suffix = content[len(self.text):]
                if suffix:
                    events.append(TextMessageContentEvent(message_id=self.message_id, delta=suffix))
                self.text = content
            events.extend(self._end_text())
            return events
        if kind in ("chat.tool_call", "chat.tool_result"):
            # Host tool observations include backend execution, rejected calls
            # and resumed client results. Only the native interrupt below
            # requests App execution; these observations do not trigger it.
            return []
        if kind == "chat.client_tool_request":
            call_id, name = payload.get("request_id"), payload.get("tool_name")
            interrupt_id = payload.get("interrupt_id")
            if not isinstance(call_id, str) or not call_id.strip() or not isinstance(name, str) or name not in self.tools:
                raise AGUIError("host emitted an undeclared App tool", "HOST_PROTOCOL_ERROR", 502)
            if (payload.get("source") != "client_tool_interrupt"
                    or not isinstance(interrupt_id, str) or not interrupt_id.strip()):
                raise AGUIError("invalid client tool interrupt", "HOST_PROTOCOL_ERROR", 502)
            if any(old["id"] == call_id for old in self.calls):
                raise AGUIError("host repeated a tool call ID", "HOST_PROTOCOL_ERROR", 502)
            if any(old["interrupt_id"] == interrupt_id for old in self.calls):
                raise AGUIError("host repeated a tool interrupt ID", "HOST_PROTOCOL_ERROR", 502)
            arguments = payload.get("arguments")
            if isinstance(arguments, str):
                try:
                    arguments = json.loads(arguments)
                except ValueError as exc:
                    raise AGUIError("invalid tool arguments JSON", "HOST_PROTOCOL_ERROR", 502) from exc
            if not isinstance(arguments, dict):
                raise AGUIError("tool arguments must be an object", "HOST_PROTOCOL_ERROR", 502)
            self.calls.append({"id": call_id, "interrupt_id": interrupt_id})
            return [
                *self._end_text(),
                ToolCallStartEvent(tool_call_id=call_id, tool_call_name=name, parent_message_id=self.message_id),
                ToolCallArgsEvent(tool_call_id=call_id, delta=json.dumps(arguments, ensure_ascii=False)),
                ToolCallEndEvent(tool_call_id=call_id),
            ]
        if kind == "chat.usage_summary":
            self.usage = self._usage(payload.get("usage"))
        elif kind == EXECUTION_STATUS:
            state = execution_state(payload, self.request_id)
            if state in {"failed", "cancelled", "rejected"}:
                raise AGUIError("execution did not succeed", "HOST_ERROR", 502)
            if state not in {"completed", "waiting_client_tools"}:
                raise AGUIError("unexpected execution acceptance", "HOST_PROTOCOL_ERROR", 502)
            pending = payload.get("pending_client_tools")
            if not isinstance(pending, list) or any(
                    not isinstance(call, dict)
                    or not isinstance(call.get("request_id"), str)
                    or not isinstance(call.get("interrupt_id"), str) for call in pending):
                raise AGUIError("invalid pending client tools", "HOST_PROTOCOL_ERROR", 502)
            expected = {(call["id"], call["interrupt_id"]) for call in self.calls}
            actual = {(call["request_id"], call["interrupt_id"]) for call in pending}
            if (len(actual) != len(pending) or actual != expected
                    or (state == "completed" and bool(pending))
                    or (state == "waiting_client_tools" and not pending)):
                raise AGUIError("pending tools disagree with execution state", "HOST_PROTOCOL_ERROR", 502)
            if "usage" in payload:
                self.usage = self._usage(payload["usage"])
            return self._finish()
        return []

    @staticmethod
    def _usage(usage: Any) -> list[dict[str, Any]] | None:
        if usage is None:
            return None
        if not isinstance(usage, dict):
            raise AGUIError("invalid host usage", "HOST_PROTOCOL_ERROR", 502)
        fields = {"input_tokens": "inputTokens", "output_tokens": "outputTokens",
                  "total_tokens": "totalTokens", "cache_tokens": "cachedInputTokens"}
        return [{target: usage[source] for source, target in fields.items() if source in usage}]

    def _finish(self) -> list[BaseEvent]:
        ids = [call["id"] for call in self.calls]
        if not self.text and not ids:
            raise AGUIError("host produced no answer or tool call", "HOST_PROTOCOL_ERROR", 502)
        event = RunFinishedEvent(
            thread_id=self.thread_id, run_id=self.run_id,
            outcome={"type": "success", "pendingToolCallIds": ids}, usage=self.usage,
        )
        self.finished = True
        return [*self._end_text(), event]
