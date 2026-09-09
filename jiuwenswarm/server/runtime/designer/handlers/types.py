# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

from dataclasses import dataclass

from jiuwenswarm.common.schema.designer_graph import (
    AssetRef,
    DesignerExecutionGraph,
    DesignerExecutionRun,
)


@dataclass(frozen=True)
class NodeExecutionContext:
    graph: DesignerExecutionGraph
    run_id: str
    node_id: str
    run: DesignerExecutionRun | None = None


@dataclass(frozen=True)
class NodeResult:
    output_ref: AssetRef | None = None
    output_refs: list[AssetRef] | None = None
    message: str = ""
