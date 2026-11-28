"""Translate the new user input or complete tool batch to generic E2A params."""

from __future__ import annotations

from typing import Any

from ag_ui.core import RunAgentInput

from .store import AGUIError


def validate_run(run: RunAgentInput) -> None:
    if not run.thread_id.strip() or not run.run_id.strip():
        raise AGUIError("threadId and runId must be non-empty")
    if run.protocol_version not in (None, "1.0"):
        raise AGUIError("only AG-UI 1.0 is supported", "UNSUPPORTED_PROTOCOL")
    if run.resume:
        raise AGUIError("interrupt/resume is not supported", "UNSUPPORTED_RESUME")
    ids = [m.id for m in run.messages]
    if any(not value.strip() for value in ids) or len(ids) != len(set(ids)):
        raise AGUIError("message IDs must be non-empty and unique")
    names = [tool.name for tool in run.tools or []]
    if any(not name.strip() for name in names) or len(names) != len(set(names)):
        raise AGUIError("tool names must be non-empty and unique")


def prepare_input(run: RunAgentInput, thread: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """Prepare a run after validate_run, checking its thread-dependent input."""
    synced = thread["messages"]
    if len(run.messages) < len(synced):
        raise AGUIError("client history is missing messages", "HISTORY_MISMATCH", 409)
    for old, supplied in zip(synced, run.messages):
        if old["id"] != supplied.id or old["role"] != supplied.role:
            raise AGUIError("client history IDs or order changed", "HISTORY_MISMATCH", 409)
    tail = list(run.messages[len(synced):])
    user = tail.pop() if tail and tail[-1].role == "user" else None
    if user is not None and (not isinstance(user.content, str) or not user.content.strip()):
        raise AGUIError("only non-empty text user messages are supported")
    if any(message.role != "tool" for message in tail):
        raise AGUIError("expected one text user message or a complete tool result batch")
    if tail and user is not None:
        raise AGUIError("submit tool results alone; send new user input in a later run",
                        "UNSUPPORTED_MIXED_INPUT")
    pending = {call["id"] for call in thread["pending"]}
    result_ids = [message.tool_call_id for message in tail]
    if len(result_ids) != len(set(result_ids)) or set(result_ids) != pending:
        raise AGUIError("tool results must match the entire pending batch", "TOOL_RESULTS_MISMATCH", 409)
    if pending and "state" not in run.model_fields_set:
        raise AGUIError("tool results require the current application state")
    if not tail and user is None:
        raise AGUIError("a run requires new input; state-only runs are not supported")
    # Never flatten forwardedProps into internal routing or permission fields.
    params: dict[str, Any] = {
        "client_tools": [tool.model_dump(by_alias=True) for tool in run.tools or []],
    }
    if tail:
        calls = {call["id"]: call for call in thread["pending"]}
        interrupt_ids = [call.get("interrupt_id") for call in thread["pending"]]
        if (len(calls) != len(thread["pending"])
                or any(not isinstance(value, str) or not value.strip() for value in interrupt_ids)
                or len(interrupt_ids) != len(set(interrupt_ids))):
            raise AGUIError("pending tools lack valid interrupt mappings; start a new thread",
                            "HISTORY_INCOMPLETE", 409)
        answers = []
        for message in tail:
            if isinstance(message.content, str):
                content = [{"type": "text", "text": message.content}]
            else:
                # The native wire format currently supported here is text content.
                # Reject unsupported parts rather than losing their meaning.
                if any(part.type != "text" for part in message.content):
                    raise AGUIError("only text tool results are supported")
                content = [part.model_dump(by_alias=True, exclude_none=True) for part in message.content]
            if message.error:
                content.append({"type": "text", "text": message.error})
            answers.append({
                "interrupt_id": calls[message.tool_call_id]["interrupt_id"],
                "request_id": message.tool_call_id,
                "result": {"content": content, "is_error": bool(message.error)},
            })
        params.update(source="client_tool_interrupt", answers=answers)
    if user is not None:
        params["query"] = user.content
    context: dict[str, Any] = {}
    for external, internal in (("state", "state"), ("context", "context"),
                               ("forwarded_props", "metadata")):
        if external in run.model_fields_set:
            value = getattr(run, external)
            context[internal] = [item.model_dump(by_alias=True) for item in value] if external == "context" and value is not None else value
    params["run_context"] = context
    added = [{"id": message.id, "role": message.role, "request_id": thread["request_id"]}
             for message in run.messages[len(synced):]]
    for ref, message in zip(added, run.messages[len(synced):]):
        if message.role == "tool":
            ref["tool_call_id"] = message.tool_call_id
    return params, added
