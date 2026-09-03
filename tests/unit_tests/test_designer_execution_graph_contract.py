# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Cross-layer contract tests for Designer execution graph literals."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from jiuwenswarm.common.schema.designer_graph import (
    NODE_TYPES,
    NODE_TYPE_AUDIO,
    NODE_TYPE_IMAGE,
    NODE_TYPE_TABLE,
    NODE_TYPE_TEXT,
    NODE_TYPE_VIDEO,
    RUN_SCHEMA_VERSION,
    SCHEMA_VERSION,
    normalize_execution_graph,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_TS = (
    _REPO_ROOT
    / "jiuwenswarm"
    / "channels"
    / "web"
    / "frontend"
    / "src"
    / "features"
    / "designer"
    / "executionGraphTypes.ts"
)
_FIXTURE = (
    _REPO_ROOT
    / "jiuwenswarm"
    / "channels"
    / "web"
    / "frontend"
    / "tests"
    / "fixtures"
    / "designer-execution-graph.v1.json"
)


def _extract_ts_const(path: Path, name: str) -> str:
    text = path.read_text(encoding="utf-8")
    pattern = rf"export const {re.escape(name)}\s*=\s*['\"]([^'\"]+)['\"]"
    match = re.search(pattern, text)
    assert match is not None, f"{name} not found in {path}"
    return match.group(1)


def _extract_ts_const_array(path: Path, name: str) -> list[str]:
    text = path.read_text(encoding="utf-8")
    start = text.find(f"export const {name}")
    assert start != -1, f"{name} array not found in {path}"
    bracket_start = text.find("[", start)
    bracket_end = text.find("] as const;", bracket_start)
    assert bracket_start != -1 and bracket_end != -1, f"{name} array body not found in {path}"
    body = text[bracket_start:bracket_end]
    literals = re.findall(r"['\"]([^'\"]+)['\"]", body)
    if literals:
        return literals
    refs = re.findall(r"DESIGNER_[A-Z0-9_]+", body)
    return [_extract_ts_const(path, ref) for ref in refs]


@pytest.mark.parametrize(
    ("python_const", "ts_const"),
    [
        (SCHEMA_VERSION, "DESIGNER_GRAPH_SCHEMA_VERSION"),
        (RUN_SCHEMA_VERSION, "DESIGNER_RUN_SCHEMA_VERSION"),
        (NODE_TYPE_TEXT, "DESIGNER_NODE_TYPE_TEXT"),
        (NODE_TYPE_TABLE, "DESIGNER_NODE_TYPE_TABLE"),
        (NODE_TYPE_IMAGE, "DESIGNER_NODE_TYPE_IMAGE"),
        (NODE_TYPE_VIDEO, "DESIGNER_NODE_TYPE_VIDEO"),
        (NODE_TYPE_AUDIO, "DESIGNER_NODE_TYPE_AUDIO"),
    ],
)
def test_designer_literal_contract(python_const: str, ts_const: str) -> None:
    assert _extract_ts_const(_TS, ts_const) == python_const


def test_designer_node_types_contract() -> None:
    ts_types = set(_extract_ts_const_array(_TS, "DESIGNER_NODE_TYPES"))
    assert ts_types == set(NODE_TYPES)


def test_designer_fixture_normalizes() -> None:
    payload = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    graph = normalize_execution_graph(payload)
    assert graph["schema_version"] == SCHEMA_VERSION
    assert len(graph["nodes"]) == 10
    assert len(graph["edges"]) == 14
    assert {node["type"] for node in graph["nodes"]} == {"text", "table", "image", "video"}
    labels = {node["label"] for node in graph["nodes"]}
    assert "最终视频" in labels
    assert "视频片段1首帧" in labels
    assert "视频片段3" in labels
    edge_pairs = {(edge["source"], edge["target"]) for edge in graph["edges"]}
    assert ("n_character", "n_storyboard") not in edge_pairs
    assert ("n_storyboard", "n_clip_1") not in edge_pairs
    assert ("n_clip_1", "n_final") in edge_pairs