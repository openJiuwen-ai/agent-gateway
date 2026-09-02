# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Unit tests for Designer graph store and bootstrap schema."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from jiuwenswarm.common.schema.designer_graph import (
    NODE_TYPE_IMAGE,
    NODE_TYPE_TABLE,
    NODE_TYPE_TEXT,
    NODE_TYPE_VIDEO,
    NODE_STATUS_COMPLETED,
    RUN_STATUS_COMPLETED,
    RUN_STATUS_RUNNING,
    build_bootstrap_graph,
    normalize_execution_graph,
)
from jiuwenswarm.server.runtime.designer.executor import GraphExecutor
from jiuwenswarm.server.runtime.designer.graph_store import DesignerGraphStore


@pytest.fixture()
def designer_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> DesignerGraphStore:
    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.designer.graph_store.get_agent_root_dir",
        lambda: tmp_path,
    )
    return DesignerGraphStore()


def test_bootstrap_graph_uses_modality_node_types() -> None:
    graph = build_bootstrap_graph(
        project_id="proj_test01",
        prompt="test prompt",
        title="Test Video",
    )
    assert graph["schema_version"] == "designer-execution-graph.v1"
    node_types = {node["type"] for node in graph["nodes"]}
    assert node_types <= {NODE_TYPE_TEXT, NODE_TYPE_TABLE, NODE_TYPE_IMAGE, NODE_TYPE_VIDEO}
    assert NODE_TYPE_TEXT in node_types
    assert NODE_TYPE_IMAGE in node_types
    assert all("kind" not in edge for edge in graph["edges"])


def test_graph_store_roundtrip(designer_store: DesignerGraphStore) -> None:
    graph = build_bootstrap_graph(project_id="proj_test01", prompt="roundtrip")
    saved = designer_store.save_graph(graph)
    loaded = designer_store.get_graph(saved["graph_id"])
    assert loaded is not None
    assert loaded["graph_id"] == saved["graph_id"]
    assert loaded["project_id"] == "proj_test01"


def test_fixture_file_normalizes() -> None:
    fixture = (
        Path(__file__).resolve().parents[3]
        / "jiuwenswarm"
        / "channels"
        / "web"
        / "frontend"
        / "tests"
        / "fixtures"
        / "designer-execution-graph.v1.json"
    )
    payload = json.loads(fixture.read_text(encoding="utf-8"))
    graph = normalize_execution_graph(payload)
    assert graph["title"] == "赛博朋克街景短视频"


def test_graph_store_list_by_project(designer_store: DesignerGraphStore) -> None:
    first = designer_store.save_graph(
        build_bootstrap_graph(project_id="proj_list_a", prompt="first"),
    )
    second = designer_store.save_graph(
        build_bootstrap_graph(project_id="proj_list_a", prompt="second"),
    )
    designer_store.save_graph(
        build_bootstrap_graph(project_id="proj_list_b", prompt="other project"),
    )
    graphs = designer_store.list_graphs_for_project("proj_list_a")
    graph_ids = {graph["graph_id"] for graph in graphs}
    assert graph_ids == {first["graph_id"], second["graph_id"]}


@pytest.mark.asyncio
async def test_mock_executor_completes_run(designer_store: DesignerGraphStore) -> None:
    graph = designer_store.save_graph(
        build_bootstrap_graph(project_id="proj_exec01", prompt="execute me"),
    )
    executor = GraphExecutor(designer_store)
    run = executor.create_run(graph)
    started = await executor.start_run(run["run_id"])
    assert started["status"] == RUN_STATUS_RUNNING
    task = executor._tasks.get(run["run_id"])
    if task is not None:
        await task
    finished = designer_store.get_run(run["run_id"])
    assert finished is not None
    assert finished["status"] == RUN_STATUS_COMPLETED
    node_states = finished["node_states"]
    assert all(
        state.get("status") == NODE_STATUS_COMPLETED for state in node_states.values()
    )
