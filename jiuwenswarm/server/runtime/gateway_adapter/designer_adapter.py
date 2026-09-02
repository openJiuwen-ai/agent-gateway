# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""DesignerAdapter: designer.graph.* / designer.run.* executed in AgentServer."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

from jiuwenswarm.common.schema.agent import AgentRequest, AgentResponse
from jiuwenswarm.common.schema.designer_graph import (
    DesignerGraphValidationError,
    build_bootstrap_graph,
    normalize_execution_graph,
)
from jiuwenswarm.common.schema.message import ReqMethod
from jiuwenswarm.common.work_mode import DEFAULT_WEB_WORK_MODE
from jiuwenswarm.server.runtime.designer.executor import GraphExecutor
from jiuwenswarm.server.runtime.designer.graph_store import DesignerGraphStore
from jiuwenswarm.server.runtime.gateway_adapter.base import (
    GatewayAdapter,
    build_error_response,
)
from jiuwenswarm.server.runtime.session import project_store
from jiuwenswarm.server.runtime.session.work_mode import resolve_request_work_mode

logger = logging.getLogger(__name__)

_store = DesignerGraphStore()
_executor = GraphExecutor(_store)


def _ok_response(request: AgentRequest, payload: Any) -> AgentResponse:
    return AgentResponse(
        request_id=request.request_id,
        channel_id=request.channel_id,
        ok=True,
        payload=payload,
        metadata=request.metadata,
    )


def _request_params(request: AgentRequest) -> dict[str, Any]:
    return request.params if isinstance(request.params, dict) else {}


def _error(
    request: AgentRequest,
    message: str,
    code: str = "BAD_REQUEST",
) -> AgentResponse:
    return build_error_response(request, message, code=code)


def _get_graph(params: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
    graph_id = str(params.get("graph_id") or "").strip()
    if not graph_id:
        return None, "graph_id is required", "BAD_REQUEST"
    graph = _store.get_graph(graph_id)
    if graph is None:
        return None, "graph not found", "NOT_FOUND"
    return {"graph": dict(graph)}, None, None


def _list_graphs(params: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
    project_id = str(params.get("project_id") or "").strip()
    if not project_id:
        return None, "project_id is required", "BAD_REQUEST"
    project = project_store.get_project_by_id(project_id, cache_bust=True)
    if project is None or project.hidden:
        return None, "project not found", "NOT_FOUND"
    graphs = _store.list_graphs_for_project(project_id)
    return {"graphs": [dict(graph) for graph in graphs]}, None, None


def _save_graph(params: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
    raw_graph = params.get("graph")
    if not isinstance(raw_graph, dict):
        return None, "graph is required", "BAD_REQUEST"
    try:
        saved = _store.save_graph(normalize_execution_graph(raw_graph))
    except DesignerGraphValidationError as exc:
        return None, str(exc), "BAD_REQUEST"
    return {"graph": dict(saved)}, None, None


def _bootstrap_graph(
    params: dict[str, Any],
    channel_id: str,
) -> tuple[dict[str, Any] | None, str | None, str | None]:
    prompt = str(params.get("prompt") or "").strip()
    if not prompt:
        return None, "prompt is required", "BAD_REQUEST"

    project_id = str(params.get("project_id") or "").strip()
    project_payload: dict[str, Any] | None = None

    if project_id:
        project = project_store.get_project_by_id(project_id, cache_bust=True)
        if project is None or project.hidden:
            return None, "project not found", "NOT_FOUND"
    else:
        name = str(params.get("name") or prompt[:40] or "Designer Project").strip()
        work_mode, mode_error = resolve_request_work_mode(params, channel_id)
        if mode_error is not None:
            return None, f"invalid work_mode: {params.get('work_mode')!r}", mode_error
        project_dir = str(params.get("project_dir") or "").strip()
        if project_dir and not os.path.isabs(project_dir):
            return None, "project_dir must be an absolute path", "BAD_REQUEST"
        if project_dir and not os.path.isdir(project_dir):
            return None, "project directory does not exist", "PROJECT_DIR_MISSING"
        if not project_dir:
            try:
                project_dir = project_store.resolve_default_project_dir(name, work_mode)
            except ValueError as exc:
                return None, str(exc), "BAD_REQUEST"
            try:
                os.makedirs(project_dir, exist_ok=True)
            except OSError as exc:
                return None, f"failed to create project directory: {exc}", "INTERNAL_ERROR"
        try:
            project, restored = project_store.create_or_restore_project(
                name,
                project_dir,
                work_mode,
            )
        except project_store.ProjectDirConflict:
            return None, "project_dir already exists", "CONFLICT"
        except project_store.ProjectNameConflict:
            return None, "project name already exists", "CONFLICT"
        except ValueError as exc:
            return None, str(exc), "BAD_REQUEST"
        project_id = project.project_id
        project_payload = {
            "project_id": project.project_id,
            "project_dir": project.project_dir,
            "restored": restored,
            "work_mode": project.work_mode or DEFAULT_WEB_WORK_MODE,
        }

    title = params.get("title")
    graph = build_bootstrap_graph(
        project_id=project_id,
        prompt=prompt,
        title=str(title).strip() if isinstance(title, str) else None,
    )
    saved = _store.save_graph(graph)
    payload: dict[str, Any] = {"graph": dict(saved), "project_id": project_id}
    if project_payload is not None:
        payload["project"] = project_payload
    return payload, None, None


def _get_run(params: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
    run_id = str(params.get("run_id") or "").strip()
    if run_id:
        run = _store.get_run(run_id)
        if run is None:
            return None, "run not found", "NOT_FOUND"
        return {"run": dict(run)}, None, None
    graph_id = str(params.get("graph_id") or "").strip()
    if graph_id:
        run = _store.get_latest_run_for_graph(graph_id)
        if run is None:
            return None, "run not found", "NOT_FOUND"
        return {"run": dict(run)}, None, None
    return None, "run_id or graph_id is required", "BAD_REQUEST"


def _start_run(params: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
    graph_id = str(params.get("graph_id") or "").strip()
    run_id = str(params.get("run_id") or "").strip()
    graph = _store.get_graph(graph_id) if graph_id else None
    if run_id:
        existing = _store.get_run(run_id)
        if existing is None:
            return None, "run not found", "NOT_FOUND"
        if graph is None:
            graph = _store.get_graph(existing["graph_id"])
    elif graph is not None:
        run = _executor.create_run(graph)
        run_id = run["run_id"]
    else:
        return None, "graph_id or run_id is required", "BAD_REQUEST"
    if graph is None:
        return None, "graph not found", "NOT_FOUND"
    return {"run_id": run_id, "graph_id": graph["graph_id"]}, None, None


def _pause_run(params: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
    run_id = str(params.get("run_id") or "").strip()
    if not run_id:
        return None, "run_id is required", "BAD_REQUEST"
    try:
        run = _executor.pause_run(run_id)
    except KeyError:
        return None, "run not found", "NOT_FOUND"
    return {"run": dict(run)}, None, None


def _cancel_run(params: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
    run_id = str(params.get("run_id") or "").strip()
    if not run_id:
        return None, "run_id is required", "BAD_REQUEST"
    try:
        run = _executor.cancel_run(run_id)
    except KeyError:
        return None, "run not found", "NOT_FOUND"
    return {"run": dict(run)}, None, None


class DesignerAdapter(GatewayAdapter):
    """Designer execution graph adapter."""

    methods: frozenset[str] = frozenset(
        {
            ReqMethod.DESIGNER_GRAPH_GET.value,
            ReqMethod.DESIGNER_GRAPH_LIST.value,
            ReqMethod.DESIGNER_GRAPH_SAVE.value,
            ReqMethod.DESIGNER_GRAPH_BOOTSTRAP.value,
            ReqMethod.DESIGNER_RUN_START.value,
            ReqMethod.DESIGNER_RUN_GET.value,
            ReqMethod.DESIGNER_RUN_PAUSE.value,
            ReqMethod.DESIGNER_RUN_CANCEL.value,
        }
    )

    async def handle(self, request: AgentRequest) -> AgentResponse:
        method = request.req_method
        params = _request_params(request)
        try:
            if method == ReqMethod.DESIGNER_GRAPH_GET:
                payload, error, code = await asyncio.to_thread(_get_graph, params)
            elif method == ReqMethod.DESIGNER_GRAPH_LIST:
                payload, error, code = await asyncio.to_thread(_list_graphs, params)
            elif method == ReqMethod.DESIGNER_GRAPH_SAVE:
                payload, error, code = await asyncio.to_thread(_save_graph, params)
            elif method == ReqMethod.DESIGNER_GRAPH_BOOTSTRAP:
                payload, error, code = await asyncio.to_thread(
                    _bootstrap_graph,
                    params,
                    request.channel_id,
                )
            elif method == ReqMethod.DESIGNER_RUN_GET:
                payload, error, code = await asyncio.to_thread(_get_run, params)
            elif method == ReqMethod.DESIGNER_RUN_START:
                payload, error, code = await asyncio.to_thread(_start_run, params)
                if error is None and payload is not None:
                    run = await _executor.start_run(str(payload["run_id"]))
                    return _ok_response(request, {"run": dict(run)})
            elif method == ReqMethod.DESIGNER_RUN_PAUSE:
                payload, error, code = await asyncio.to_thread(_pause_run, params)
            elif method == ReqMethod.DESIGNER_RUN_CANCEL:
                payload, error, code = await asyncio.to_thread(_cancel_run, params)
            else:
                return build_error_response(
                    request,
                    f"unsupported method: {method}",
                    code="NOT_IMPLEMENTED",
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[DesignerAdapter] %s failed: %s", method, exc)
            return build_error_response(request, str(exc), code="INTERNAL_ERROR")

        if error is not None:
            return build_error_response(request, error, code=code or "BAD_REQUEST")
        return _ok_response(request, payload)
