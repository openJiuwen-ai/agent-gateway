"""Gateway lifecycle orchestration: archive checks state; deletion stops work."""

from __future__ import annotations

import asyncio
import logging

from e2a.message import ReqMethod

from agent_gateway.routing.agent_client import DuplicateRequestIdError
from agent_gateway.routing.e2a_proxy import fetch_agent_unary
from agent_gateway.common.request_identity import apply_routing_metadata


_SESSION_TARGET_METHODS = frozenset({"session.archive", "session.unarchive", "session.delete"})


def _session_target_error(method: str, params: dict) -> str | None:
    """Validate Session targets. They come only from params: channels fill the
    handler's session_id argument with a request-scoped fallback.

    ``session_ids`` takes precedence. Clients such as the TUI attach their active
    Session as ``session_id`` to every request, so beside ``session_ids`` it is
    context, not a target, and is not validated.
    """
    if method not in _SESSION_TARGET_METHODS:
        return None
    if "session_ids" in params:
        ids = params["session_ids"]
        if not (isinstance(ids, list) and 1 <= len(ids) <= 100):
            return "session_ids must contain 1 to 100 IDs"
        if not all(isinstance(sid, str) and sid.strip() for sid in ids):
            return "invalid resource ID"
        return None
    if "session_id" not in params:
        return "session_id is required" if method == "session.delete" else "session_ids is required"
    single = params["session_id"]
    if not (isinstance(single, str) and single.strip()):
        return "invalid session_id"
    return None


def register_lifecycle_handlers(
    channel, resolve_client, resolve_cron, on_session_deleted=None, *, register_method=None,
    clients_for_user=None, session_event_clients=None, project_ids_for_client=None,
):
    """Register lifecycle RPCs and polling.

    TUI hooks: ``clients_for_user(user_id)`` lists connections;
    ``session_event_clients(user_id, session_id)`` lists windows bound to a
    Session; ``project_ids_for_client(user_id, ws, *, refresh=True)`` resolves a
    window's projects, and makes project events replay once per connection. The
    watcher passes ``refresh=False`` when nothing forces a new lookup.
    """
    watchers = {}

    async def call(method, params, session_id, user_id, ws=None):
        routing = getattr(ws, "_web_routing", None)
        metadata = apply_routing_metadata(None, routing) if isinstance(routing, dict) else None
        return await fetch_agent_unary(
            agent_client=resolve_client(),
            req_method=ReqMethod(method),
            params=params,
            session_id=session_id,
            user_id=user_id,
            channel_id=channel.channel_id,
            timeout_seconds=120,
            metadata=metadata,
        )

    def scope_key(user_id, ws=None):
        routing = getattr(ws, "_web_routing", None) or {}
        return (
            str(user_id or ""),
            str(routing.get("group_id") or ""),
            str(routing.get("bot_id") or ""),
            str(routing.get("gateway_id") or ""),
        )

    def scoped_clients(user_id, scope):
        if clients_for_user is not None:
            return [ws for ws in clients_for_user(user_id) if scope_key(user_id, ws) == scope]
        return [
            ws
            for ws in getattr(channel, "clients", None) or ()
            if scope_key(channel.connection_user_id(ws), ws) == scope
        ]

    async def watch(user_id, scope):
        revisions = {}
        # Per-recipient replay state: (id(ws), event, resource) -> (ws, revision).
        # Holding ws keeps a new socket that reuses its id() from inheriting it.
        delivered = {}
        polled = []  # Windows whose project membership the previous poll resolved.
        first = True
        while True:
            clients = scoped_clients(user_id, scope)
            if not clients:
                return
            live = {id(ws) for ws in clients}
            delivered = {key: value for key, value in delivered.items() if key[0] in live}
            try:
                ok, response = await call(
                    "project.lifecycle", {"events": True}, None, user_id, clients[0]
                )
                if ok:
                    # A connection may join or disconnect while the poll is in flight.
                    clients = scoped_clients(user_id, scope)
                    generations = {id(ws): getattr(ws, "_lifecycle_binding_generation", 0) for ws in clients}
                    # Resolve once per poll, including empty snapshots, so a deleted
                    # Session can retain its last known project binding. Only a new
                    # project revision or a window new to this watcher forces a
                    # lookup; otherwise the hook may answer from its cache.
                    client_projects = None
                    if project_ids_for_client is not None:
                        changed = any(
                            item["event"].startswith("project.")
                            and item["payload"]["revision"]
                            > revisions.get((item["event"], item["payload"]["resource_id"]), -1)
                            for item in response.get("events", [])
                        )
                        client_projects = list(zip(clients, await asyncio.gather(*(
                            project_ids_for_client(
                                user_id, ws, refresh=changed or all(ws is not seen for seen in polled),
                            )
                            for ws in clients
                        ))))
                        polled = clients
                    for entry in response.get("events", []):
                        payload = entry["payload"]
                        key = (entry["event"], payload["resource_id"])
                        previous = revisions.get(key, -1)
                        revisions[key] = payload["revision"]
                        scoped_project = client_projects is not None and entry["event"].startswith("project.")
                        recipient_scoped = scoped_project or (
                            session_event_clients is not None
                            and entry["event"].startswith("session.")
                            and payload.get("execution_blocked")
                        )
                        if not recipient_scoped and payload["revision"] <= previous:
                            continue
                        # Completion can precede stop/finalization cleanup.
                        completed = entry["completed"] and not _lifecycle_item_pending(entry["result"])
                        if completed and entry["kind"] in {"archive", "delete"} and payload["revision"] > previous:
                            if entry["event"].startswith("session."):
                                mark_sessions_idle([payload["resource_id"]])
                        if first and not recipient_scoped:
                            continue
                        if scoped_project:
                            recipients = [ws for ws, ids in client_projects if payload["project_id"] in ids]
                        elif session_event_clients is not None and entry["event"].startswith("session."):
                            recipients = session_event_clients(user_id, payload["resource_id"])
                        else:
                            recipients = clients
                        for ws in recipients:
                            if scope_key(user_id, ws) != scope or getattr(ws, "closed", False):
                                continue
                            generation = generations.get(id(ws))
                            seen_ws, seen_revision = delivered.get((id(ws), *key), (None, -1))
                            if recipient_scoped and (
                                getattr(ws, "_lifecycle_binding_generation", 0) != generation
                                or (seen_ws is ws and payload["revision"] <= seen_revision)
                            ):
                                continue
                            await channel.send_event(ws, entry["event"], payload)
                            # A rebind during the send voids this delivery; it retries next poll.
                            if recipient_scoped and getattr(ws, "_lifecycle_binding_generation", 0) != generation:
                                continue
                            if completed and not (
                                entry["kind"] == "delete"
                                and entry["event"].startswith("project.")
                                and not entry["result"].get("deleted")
                            ):
                                suffix = {
                                    "archive": "archived",
                                    "unarchive": "unarchived",
                                    "delete": "deleted",
                                }[entry["kind"]]
                                await channel.send_event(
                                    ws,
                                    entry["event"].split(".")[0] + "." + suffix,
                                    {
                                        **entry["result"],
                                        "project_id": payload["project_id"],
                                    },
                                )
                            if recipient_scoped:
                                delivered[(id(ws), *key)] = (ws, payload["revision"])
                    first = False
            except DuplicateRequestIdError:
                # request_id 撞号说明 id 生成器出了问题（或同一连接上请求串行
                # 堆积），不是"服务暂不可用"的正常抖动，必须可见。
                logging.getLogger(__name__).warning(
                    "lifecycle event polling dropped: duplicate request_id",
                    exc_info=True,
                )
            except Exception:
                # 后台轮询，AgentServer 短暂不可用时每 2s 都会走到这里，
                # 保持 debug，避免刷屏。
                logging.getLogger(__name__).debug(
                    "lifecycle event polling deferred", exc_info=True
                )
            await asyncio.sleep(2)

    def ensure_watch(user_id, ws=None):
        owner = scope_key(user_id, ws)
        if owner not in watchers or watchers[owner].done():
            watchers[owner] = asyncio.create_task(
                watch(user_id, owner), name="web-lifecycle-events"
            )

    channel.ensure_lifecycle_watch = ensure_watch

    def _lifecycle_item_pending(item: dict) -> bool:
        return bool(
            item.get("finalizing")
            or item.get("stop_pending")
            or (item.get("accepted") and item.get("finalizing") is not False)
        )

    def _completed_session_ids(payload: dict) -> list[str]:
        results = payload.get("results")
        items = results if isinstance(results, list) else [payload]
        session_ids = []
        for item in items:
            if not isinstance(item, dict) or not item.get("ok", True):
                continue
            if _lifecycle_item_pending(item):
                continue
            session_id = item.get("session_id")
            if session_id:
                session_ids.append(str(session_id))
        return session_ids

    def mark_sessions_idle(session_ids: list[str]) -> None:
        """Clear the cached busy flag for Sessions a lifecycle action stopped.

        Archive/delete stop runtime work in the AgentServer without emitting
        ``chat.processing_status(false)``.  Without this, a later
        ``session.get_metadata`` (for example after unarchive) would report a
        stopped Session as still processing.
        """
        clear = getattr(channel, "clear_session_busy", None)
        if not callable(clear):
            return
        for sid in dict.fromkeys(session_ids):
            clear(sid)

    def handler(method):
        async def handle(ws, req_id, params, session_id, user_id=None):
            ensure_watch(user_id, ws)
            if not isinstance(params, dict):
                await channel.send_response(
                    ws,
                    req_id,
                    ok=False,
                    error="params must be object",
                    code="BAD_REQUEST",
                )
                return
            error = _session_target_error(method, params)
            if error is not None:
                await channel.send_response(ws, req_id, ok=False, error=error, code="BAD_REQUEST")
                return
            # Browser callers cannot inject internal stage acknowledgements.
            public = {}
            for k, v in params.items():
                if k not in {
                    "_lifecycle_stage",
                    "operation_id",
                    "generation",
                    "stopped_cron_jobs",
                    "deleted_cron_jobs",
                    "completed_cron_job_ids",
                }:
                    public[k] = v
            if method in _SESSION_TARGET_METHODS and "session_ids" in public:
                # Never forward an accompanying session_id: AgentServer adds it
                # to the delete targets, and it is the caller's active Session.
                public.pop("session_id", None)
            elif method in {"session.archive", "session.unarchive"} and "session_id" in public:
                # AgentServer takes the batch field, even for one Session.
                public["session_ids"] = [public.pop("session_id")]
            if project_ids_for_client is not None and method in {"session.delete", "project.sessions.delete_archived"}:
                # Capture membership before deletion removes the metadata; a new
                # Session may not have existed when its window was bound. This is
                # context for later events: a failed lookup must not stop the deletion.
                for failure in await asyncio.gather(*(
                    project_ids_for_client(user_id, client_ws)
                    for client_ws in scoped_clients(user_id, scope_key(user_id, ws))
                ), return_exceptions=True):
                    if isinstance(failure, Exception):
                        logging.getLogger(__name__).warning(
                            "project membership lookup before %s failed", method, exc_info=failure
                        )
            ok, payload = await call(method, public, session_id, user_id, ws)
            if ok and on_session_deleted is not None and method in {
                "session.delete",
                "project.sessions.delete_archived",
            }:
                # (project_id, session_id) of each deleted conversation, before the caller hears back.
                for item in payload.get("results", [payload]):
                    if item.get("ok", True) and item.get("session_id"):
                        try:
                            await on_session_deleted(str(item.get("project_id") or ""), item["session_id"])
                        except Exception:
                            logging.getLogger(__name__).warning(
                                "session deletion follow-up failed", exc_info=True
                            )
            await channel.send_response(
                ws,
                req_id,
                ok=ok,
                payload=payload,
                error=None if ok else payload.get("error"),
                code=None if ok else payload.get("code"),
            )
            if ok and method in {
                "session.archive",
                "session.delete",
                "project.sessions.archive",
                "project.sessions.delete_archived",
            }:
                # Acceptance does not prove that file writers have stopped.
                # Pending work keeps its busy guard until the completion poll.
                mark_sessions_idle(_completed_session_ids(payload))
            if ok and method == "session.unarchive":
                project_ids = {
                    item.get("project_id") for item in payload.get("results", [payload])
                    if item.get("ok", True) and item.get("restored") and item.get("project_id")
                }
                for client_ws in scoped_clients(user_id, scope_key(user_id, ws)):
                    generation = getattr(client_ws, "_lifecycle_binding_generation", 0)
                    try:
                        subscribed = await project_ids_for_client(user_id, client_ws) if project_ids_for_client else None
                    except Exception:
                        # The request is already answered; unknown membership gets no event.
                        logging.getLogger(__name__).warning(
                            "project membership lookup after %s failed", method, exc_info=True
                        )
                        continue
                    for project_id in project_ids:
                        if generation != getattr(client_ws, "_lifecycle_binding_generation", 0):
                            break
                        if subscribed is not None and project_id not in subscribed:
                            continue
                        try:
                            await channel.send_event(client_ws, "project.restored", {"project_id": project_id})
                        except Exception:
                            logging.getLogger(__name__).debug("project restore broadcast failed", exc_info=True)
            if ok and not method.endswith(".list"):
                event = (
                    method.replace(".archive", ".archived")
                    .replace(".unarchive", ".unarchived")
                    .replace(".delete", ".deleted")
                )
                # Match the caller's user boundary; never broadcast personal IDs
                # across all tenants connected to the Gateway.
                if method.startswith("project.sessions."):
                    event = "session.archived" if method.endswith(".archive") else "session.deleted"
                if method.startswith(("session.", "project.sessions.")):
                    for item in payload.get("results", [payload]):
                        if item.get("ok", True) and item.get("session_id"):
                            if event in {"session.archived", "session.deleted"} and _lifecycle_item_pending(item):
                                continue
                            clear_expert = getattr(channel, "clear_expert_processing", None)
                            session_closed = (
                                event in {"session.archived", "session.deleted"}
                                and "operation_id" not in payload
                            )
                            if callable(clear_expert) and session_closed:
                                clear_expert(item["session_id"])
                            send_event = getattr(channel, "send_event", None)
                            if callable(send_event):
                                # Windows bound to the Session, and always the caller.
                                targets = session_event_clients(user_id, item["session_id"]) if session_event_clients else []
                                if not any(client_ws is ws for client_ws in targets):
                                    targets = [*targets, ws]
                                for client_ws in targets:
                                    if scope_key(user_id, client_ws) == scope_key(user_id, ws):
                                        await send_event(client_ws, event, item)
                elif "operation_id" not in payload and (method != "project.delete" or payload.get("deleted")):
                    send_event = getattr(channel, "send_event", None)
                    if callable(send_event):
                        await send_event(ws, event, payload)

        return handle

    for method in (
        "session.archive",
        "session.unarchive",
        "session.archived.list",
        "session.delete",
        "project.sessions.archive",
        "project.sessions.delete_archived",
    ):
        (register_method or channel.register_method)(method, handler(method))
