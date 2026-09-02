# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Node handler registry for Designer graph execution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from jiuwenswarm.common.schema.designer_graph import (
    AssetRef,
    DesignerExecutionGraph,
    DesignerGraphNode,
    NODE_TYPE_AUDIO,
    NODE_TYPE_IMAGE,
    NODE_TYPE_TABLE,
    NODE_TYPE_TEXT,
    NODE_TYPE_VIDEO,
)


@dataclass(frozen=True)
class NodeExecutionContext:
    graph: DesignerExecutionGraph
    run_id: str
    node_id: str


@dataclass(frozen=True)
class NodeResult:
    output_ref: AssetRef | None = None
    message: str = ""


class NodeHandler(Protocol):
    async def execute(self, node: DesignerGraphNode, ctx: NodeExecutionContext) -> NodeResult:
        """Execute a single graph node."""


class MockNodeHandler:
    """Placeholder handler that returns a synthetic asset reference."""

    async def execute(self, node: DesignerGraphNode, ctx: NodeExecutionContext) -> NodeResult:
        node_type = str(node.get("type") or "unknown")
        node_id = str(node.get("id") or ctx.node_id)
        output_ref: AssetRef = {
            "kind": node_type,
            "uri": f"designer://mock/{ctx.run_id}/{node_id}",
            "label": str(node.get("label") or node_id),
        }
        return NodeResult(
            output_ref=output_ref,
            message=f"mock completed {node_type}",
        )


NODE_HANDLERS: dict[str, NodeHandler] = {
    NODE_TYPE_TEXT: MockNodeHandler(),
    NODE_TYPE_TABLE: MockNodeHandler(),
    NODE_TYPE_IMAGE: MockNodeHandler(),
    NODE_TYPE_VIDEO: MockNodeHandler(),
    NODE_TYPE_AUDIO: MockNodeHandler(),
}


def get_node_handler(node_type: str) -> NodeHandler:
    handler = NODE_HANDLERS.get(node_type)
    if handler is None:
        raise KeyError(f"no handler registered for node type: {node_type!r}")
    return handler
