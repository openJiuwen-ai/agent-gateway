# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Designer execution graph domain schema (canonical Python contract).

Shared between the React Flow frontend, gateway RPC handlers, and the graph
executor.  Frontend mirrors live in
``channels/web/frontend/src/features/designer/executionGraphTypes.ts``;
cross-layer literals are pinned by
``tests/unit_tests/test_designer_execution_graph_contract.py``.
"""

from __future__ import annotations

import secrets
import time
from typing import Any, TypedDict

# ── Schema version ────────────────────────────────────────────────────────────

SCHEMA_VERSION = "designer-execution-graph.v1"
"""Domain graph payload version."""

RUN_SCHEMA_VERSION = "designer-execution-run.v1"
"""Execution run state payload version."""

# ── Node types (modality, contract-pinned) ────────────────────────────────────

NODE_TYPE_TEXT = "text"
NODE_TYPE_TABLE = "table"
NODE_TYPE_IMAGE = "image"
NODE_TYPE_VIDEO = "video"
NODE_TYPE_AUDIO = "audio"

NODE_TYPES: frozenset[str] = frozenset(
    {
        NODE_TYPE_TEXT,
        NODE_TYPE_TABLE,
        NODE_TYPE_IMAGE,
        NODE_TYPE_VIDEO,
        NODE_TYPE_AUDIO,
    }
)

# ── Graph sources ─────────────────────────────────────────────────────────────

GRAPH_SOURCE_PROMPT = "prompt"
GRAPH_SOURCE_MANUAL = "manual"

GRAPH_SOURCES: frozenset[str] = frozenset(
    {
        GRAPH_SOURCE_PROMPT,
        GRAPH_SOURCE_MANUAL,
    }
)

# ── Node / run statuses ───────────────────────────────────────────────────────

NODE_STATUS_PENDING = "pending"
NODE_STATUS_RUNNING = "running"
NODE_STATUS_COMPLETED = "completed"
NODE_STATUS_FAILED = "failed"
NODE_STATUS_CANCELLED = "cancelled"

NODE_STATUSES: frozenset[str] = frozenset(
    {
        NODE_STATUS_PENDING,
        NODE_STATUS_RUNNING,
        NODE_STATUS_COMPLETED,
        NODE_STATUS_FAILED,
        NODE_STATUS_CANCELLED,
    }
)

RUN_STATUS_DRAFT = "draft"
RUN_STATUS_RUNNING = "running"
RUN_STATUS_PAUSED = "paused"
RUN_STATUS_COMPLETED = "completed"
RUN_STATUS_FAILED = "failed"
RUN_STATUS_CANCELLED = "cancelled"

RUN_STATUSES: frozenset[str] = frozenset(
    {
        RUN_STATUS_DRAFT,
        RUN_STATUS_RUNNING,
        RUN_STATUS_PAUSED,
        RUN_STATUS_COMPLETED,
        RUN_STATUS_FAILED,
        RUN_STATUS_CANCELLED,
    }
)

# ── ID helpers ────────────────────────────────────────────────────────────────

_GRAPH_ID_PREFIX = "graph_"
_RUN_ID_PREFIX = "run_"
_ID_HEX_LEN = 8


def new_graph_id() -> str:
    return f"{_GRAPH_ID_PREFIX}{secrets.token_hex(_ID_HEX_LEN)}"


def new_run_id() -> str:
    return f"{_RUN_ID_PREFIX}{secrets.token_hex(_ID_HEX_LEN)}"


def utc_now_ms() -> int:
    return int(time.time() * 1000)


# ── Typed payloads ──────────────────────────────────────────────────────────────


class AssetRef(TypedDict, total=False):
    kind: str
    uri: str
    mime_type: str
    label: str


class NodeLayout(TypedDict, total=False):
    x: float
    y: float
    width: float
    height: float


class DesignerGraphNode(TypedDict, total=False):
    id: str
    type: str
    label: str
    config: dict[str, Any]
    layout: NodeLayout
    output_ref: AssetRef | None


class DesignerGraphEdge(TypedDict, total=False):
    id: str
    source: str
    target: str
    label: str


class DesignerExecutionGraph(TypedDict, total=False):
    schema_version: str
    graph_id: str
    project_id: str
    title: str
    description: str
    source: str
    nodes: list[DesignerGraphNode]
    edges: list[DesignerGraphEdge]
    metadata: dict[str, Any]
    created_at: int
    updated_at: int


class DesignerNodeState(TypedDict, total=False):
    status: str
    started_at: int | None
    completed_at: int | None
    output_ref: AssetRef | None
    error: str | None
    blocked_by: list[str]


class DesignerExecutionRun(TypedDict, total=False):
    schema_version: str
    run_id: str
    graph_id: str
    project_id: str
    status: str
    node_states: dict[str, DesignerNodeState]
    current_node_ids: list[str]
    created_at: int
    updated_at: int


# ── Validation / normalization ────────────────────────────────────────────────


class DesignerGraphValidationError(ValueError):
    """Raised when an execution graph payload fails schema validation."""


def _require_str(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise DesignerGraphValidationError(f"{field} must be a non-empty string")
    return value.strip()


def normalize_layout(raw: Any) -> NodeLayout:
    layout: NodeLayout = {}
    if not isinstance(raw, dict):
        return layout
    for key in ("x", "y", "width", "height"):
        val = raw.get(key)
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            layout[key] = float(val)  # type: ignore[literal-required]
    return layout


def normalize_asset_ref(raw: Any) -> AssetRef | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise DesignerGraphValidationError("output_ref must be an object")
    kind = raw.get("kind")
    uri = raw.get("uri")
    if not isinstance(kind, str) or not kind.strip():
        raise DesignerGraphValidationError("output_ref.kind must be a non-empty string")
    if not isinstance(uri, str) or not uri.strip():
        raise DesignerGraphValidationError("output_ref.uri must be a non-empty string")
    ref: AssetRef = {"kind": kind.strip(), "uri": uri.strip()}
    mime_type = raw.get("mime_type")
    if isinstance(mime_type, str) and mime_type.strip():
        ref["mime_type"] = mime_type.strip()
    label = raw.get("label")
    if isinstance(label, str) and label.strip():
        ref["label"] = label.strip()
    return ref


def normalize_node(raw: Any) -> DesignerGraphNode:
    if not isinstance(raw, dict):
        raise DesignerGraphValidationError("node must be an object")
    node_id = _require_str(raw.get("id"), "node.id")
    node_type = _require_str(raw.get("type"), "node.type")
    if node_type not in NODE_TYPES:
        raise DesignerGraphValidationError(f"unsupported node type: {node_type!r}")
    label = raw.get("label")
    config = raw.get("config")
    if config is not None and not isinstance(config, dict):
        raise DesignerGraphValidationError("node.config must be an object")
    node: DesignerGraphNode = {
        "id": node_id,
        "type": node_type,
        "label": str(label).strip() if isinstance(label, str) and label.strip() else node_id,
        "config": dict(config) if isinstance(config, dict) else {},
        "layout": normalize_layout(raw.get("layout")),
    }
    if "output_ref" in raw:
        node["output_ref"] = normalize_asset_ref(raw.get("output_ref"))
    return node


def normalize_edge(raw: Any) -> DesignerGraphEdge:
    if not isinstance(raw, dict):
        raise DesignerGraphValidationError("edge must be an object")
    edge_id = _require_str(raw.get("id"), "edge.id")
    source = _require_str(raw.get("source"), "edge.source")
    target = _require_str(raw.get("target"), "edge.target")
    edge: DesignerGraphEdge = {
        "id": edge_id,
        "source": source,
        "target": target,
    }
    label = raw.get("label")
    if isinstance(label, str) and label.strip():
        edge["label"] = label.strip()
    return edge


def normalize_execution_graph(raw: Any) -> DesignerExecutionGraph:
    if not isinstance(raw, dict):
        raise DesignerGraphValidationError("graph must be an object")
    schema_version = _require_str(raw.get("schema_version"), "schema_version")
    if schema_version != SCHEMA_VERSION:
        raise DesignerGraphValidationError(
            f"unsupported schema_version: {schema_version!r} (expected {SCHEMA_VERSION!r})"
        )
    graph_id = _require_str(raw.get("graph_id"), "graph_id")
    project_id = _require_str(raw.get("project_id"), "project_id")
    title = raw.get("title")
    description = raw.get("description")
    source = raw.get("source")
    graph_source = GRAPH_SOURCE_MANUAL
    if isinstance(source, str) and source.strip():
        graph_source = source.strip()
        if graph_source not in GRAPH_SOURCES:
            raise DesignerGraphValidationError(f"unsupported source: {graph_source!r}")
    raw_nodes = raw.get("nodes")
    if not isinstance(raw_nodes, list):
        raise DesignerGraphValidationError("nodes must be an array")
    raw_edges = raw.get("edges")
    if not isinstance(raw_edges, list):
        raise DesignerGraphValidationError("edges must be an array")
    nodes = [normalize_node(item) for item in raw_nodes]
    edges = [normalize_edge(item) for item in raw_edges]
    node_ids = {node["id"] for node in nodes}
    if len(node_ids) != len(nodes):
        raise DesignerGraphValidationError("node ids must be unique")
    for edge in edges:
        if edge["source"] not in node_ids or edge["target"] not in node_ids:
            raise DesignerGraphValidationError(
                f"edge {edge['id']!r} references unknown node id"
            )
    metadata = raw.get("metadata")
    if metadata is not None and not isinstance(metadata, dict):
        raise DesignerGraphValidationError("metadata must be an object")
    now = utc_now_ms()
    created_at = raw.get("created_at")
    updated_at = raw.get("updated_at")
    return {
        "schema_version": SCHEMA_VERSION,
        "graph_id": graph_id,
        "project_id": project_id,
        "title": str(title).strip() if isinstance(title, str) and title.strip() else "Untitled",
        "description": str(description).strip() if isinstance(description, str) else "",
        "source": graph_source,
        "nodes": nodes,
        "edges": edges,
        "metadata": dict(metadata) if isinstance(metadata, dict) else {},
        "created_at": int(created_at) if isinstance(created_at, int) else now,
        "updated_at": int(updated_at) if isinstance(updated_at, int) else now,
    }


def normalize_node_state(raw: Any) -> DesignerNodeState:
    if not isinstance(raw, dict):
        raise DesignerGraphValidationError("node_state must be an object")
    status = _require_str(raw.get("status"), "node_state.status")
    if status not in NODE_STATUSES:
        raise DesignerGraphValidationError(f"unsupported node status: {status!r}")
    state: DesignerNodeState = {"status": status}
    for key in ("started_at", "completed_at"):
        val = raw.get(key)
        if val is None:
            state[key] = None  # type: ignore[literal-required]
        elif isinstance(val, int) and not isinstance(val, bool):
            state[key] = val  # type: ignore[literal-required]
        else:
            raise DesignerGraphValidationError(f"node_state.{key} must be an integer or null")
    if "output_ref" in raw:
        state["output_ref"] = normalize_asset_ref(raw.get("output_ref"))
    error = raw.get("error")
    if isinstance(error, str):
        state["error"] = error
    blocked_by = raw.get("blocked_by")
    if blocked_by is not None:
        if not isinstance(blocked_by, list) or not all(
            isinstance(item, str) for item in blocked_by
        ):
            raise DesignerGraphValidationError("node_state.blocked_by must be a string array")
        state["blocked_by"] = list(blocked_by)
    return state


def normalize_execution_run(raw: Any) -> DesignerExecutionRun:
    if not isinstance(raw, dict):
        raise DesignerGraphValidationError("run must be an object")
    schema_version = _require_str(raw.get("schema_version"), "schema_version")
    if schema_version != RUN_SCHEMA_VERSION:
        raise DesignerGraphValidationError(
            f"unsupported run schema_version: {schema_version!r} "
            f"(expected {RUN_SCHEMA_VERSION!r})"
        )
    run_id = _require_str(raw.get("run_id"), "run_id")
    graph_id = _require_str(raw.get("graph_id"), "graph_id")
    project_id = _require_str(raw.get("project_id"), "project_id")
    status = _require_str(raw.get("status"), "status")
    if status not in RUN_STATUSES:
        raise DesignerGraphValidationError(f"unsupported run status: {status!r}")
    raw_states = raw.get("node_states")
    if not isinstance(raw_states, dict):
        raise DesignerGraphValidationError("node_states must be an object")
    node_states = {
        str(node_id): normalize_node_state(state)
        for node_id, state in raw_states.items()
    }
    current_node_ids = raw.get("current_node_ids")
    if current_node_ids is None:
        current_ids: list[str] = []
    elif not isinstance(current_node_ids, list) or not all(
        isinstance(item, str) for item in current_node_ids
    ):
        raise DesignerGraphValidationError("current_node_ids must be a string array")
    else:
        current_ids = list(current_node_ids)
    now = utc_now_ms()
    created_at = raw.get("created_at")
    updated_at = raw.get("updated_at")
    return {
        "schema_version": RUN_SCHEMA_VERSION,
        "run_id": run_id,
        "graph_id": graph_id,
        "project_id": project_id,
        "status": status,
        "node_states": node_states,
        "current_node_ids": current_ids,
        "created_at": int(created_at) if isinstance(created_at, int) else now,
        "updated_at": int(updated_at) if isinstance(updated_at, int) else now,
    }


def initial_node_states(graph: DesignerExecutionGraph) -> dict[str, DesignerNodeState]:
    return {node["id"]: {"status": NODE_STATUS_PENDING} for node in graph["nodes"]}


def build_bootstrap_graph(
    *,
    project_id: str,
    prompt: str,
    title: str | None = None,
) -> DesignerExecutionGraph:
    """Create the default video-creation pipeline graph from an initial prompt."""
    graph_id = new_graph_id()
    now = utc_now_ms()
    prompt_text = prompt.strip()
    graph_title = title.strip() if isinstance(title, str) and title.strip() else prompt_text[:80]
    nodes: list[DesignerGraphNode] = [
        {
            "id": "n_brief",
            "type": NODE_TYPE_TEXT,
            "label": "项目 brief",
            "config": {"prompt": prompt_text},
            "layout": {"x": 40, "y": 120, "width": 280, "height": 160},
        },
        {
            "id": "n_character",
            "type": NODE_TYPE_IMAGE,
            "label": "角色图",
            "config": {"inputs": ["n_brief"]},
            "layout": {"x": 380, "y": 40, "width": 280, "height": 160},
        },
        {
            "id": "n_storyboard",
            "type": NODE_TYPE_TABLE,
            "label": "分镜表",
            "config": {"inputs": ["n_brief", "n_character"]},
            "layout": {"x": 380, "y": 260, "width": 280, "height": 160},
        },
        {
            "id": "n_frame",
            "type": NODE_TYPE_IMAGE,
            "label": "视频帧",
            "config": {"inputs": ["n_character", "n_storyboard"]},
            "layout": {"x": 720, "y": 80, "width": 260, "height": 150},
        },
        {
            "id": "n_clip",
            "type": NODE_TYPE_VIDEO,
            "label": "视频片段",
            "config": {"inputs": ["n_character", "n_storyboard"]},
            "layout": {"x": 720, "y": 260, "width": 260, "height": 150},
        },
    ]
    edges: list[DesignerGraphEdge] = [
        {"id": "e_brief_character", "source": "n_brief", "target": "n_character"},
        {"id": "e_brief_storyboard", "source": "n_brief", "target": "n_storyboard"},
        {"id": "e_character_storyboard", "source": "n_character", "target": "n_storyboard"},
        {"id": "e_character_frame", "source": "n_character", "target": "n_frame"},
        {"id": "e_storyboard_frame", "source": "n_storyboard", "target": "n_frame"},
        {"id": "e_character_clip", "source": "n_character", "target": "n_clip"},
        {"id": "e_storyboard_clip", "source": "n_storyboard", "target": "n_clip"},
    ]
    graph: DesignerExecutionGraph = {
        "schema_version": SCHEMA_VERSION,
        "graph_id": graph_id,
        "project_id": project_id,
        "title": graph_title,
        "description": prompt_text,
        "source": GRAPH_SOURCE_PROMPT,
        "nodes": nodes,
        "edges": edges,
        "metadata": {"bootstrap": "designer.graph.bootstrap.v1"},
        "created_at": now,
        "updated_at": now,
    }
    return normalize_execution_graph(graph)
