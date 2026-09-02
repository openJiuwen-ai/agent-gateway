# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Designer graph executor — mock orchestration for the foundation layer."""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict, deque
from typing import Callable

from jiuwenswarm.common.schema.designer_graph import (
    DesignerExecutionGraph,
    DesignerExecutionRun,
    DesignerGraphNode,
    DesignerNodeState,
    NODE_STATUS_CANCELLED,
    NODE_STATUS_COMPLETED,
    NODE_STATUS_FAILED,
    NODE_STATUS_PENDING,
    NODE_STATUS_RUNNING,
    RUN_STATUS_CANCELLED,
    RUN_STATUS_COMPLETED,
    RUN_STATUS_DRAFT,
    RUN_STATUS_FAILED,
    RUN_STATUS_PAUSED,
    RUN_STATUS_RUNNING,
    initial_node_states,
    new_run_id,
    utc_now_ms,
)
from jiuwenswarm.server.runtime.designer.graph_store import DesignerGraphStore
from jiuwenswarm.server.runtime.designer.handlers import (
    NodeExecutionContext,
    get_node_handler,
)

logger = logging.getLogger(__name__)

RunUpdateCallback = Callable[[DesignerExecutionRun], None]

_MOCK_NODE_DELAY_SECONDS = 0.35


class GraphExecutor:
    """Topological mock executor with sync-edge barrier semantics."""

    def __init__(self, store: DesignerGraphStore | None = None) -> None:
        self._store = store or DesignerGraphStore()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._pause_flags: dict[str, asyncio.Event] = {}
        self._cancel_flags: dict[str, asyncio.Event] = {}

    def create_run(self, graph: DesignerExecutionGraph) -> DesignerExecutionRun:
        now = utc_now_ms()
        run: DesignerExecutionRun = {
            "schema_version": "designer-execution-run.v1",
            "run_id": new_run_id(),
            "graph_id": graph["graph_id"],
            "project_id": graph["project_id"],
            "status": RUN_STATUS_DRAFT,
            "node_states": initial_node_states(graph),
            "current_node_ids": [],
            "created_at": now,
            "updated_at": now,
        }
        return self._store.save_run(run)

    async def start_run(
        self,
        run_id: str,
        *,
        on_update: RunUpdateCallback | None = None,
    ) -> DesignerExecutionRun:
        run = self._require_run(run_id)
        if run["status"] in {RUN_STATUS_RUNNING, RUN_STATUS_COMPLETED}:
            return run
        graph = self._require_graph(run["graph_id"])
        run["status"] = RUN_STATUS_RUNNING
        run["updated_at"] = utc_now_ms()
        run = self._store.save_run(run)
        self._pause_flags[run_id] = asyncio.Event()
        self._pause_flags[run_id].set()
        self._cancel_flags[run_id] = asyncio.Event()
        task = asyncio.create_task(
            self._execute_run(graph, run, on_update=on_update),
            name=f"designer-run-{run_id}",
        )
        self._tasks[run_id] = task
        return run

    def pause_run(self, run_id: str) -> DesignerExecutionRun:
        run = self._require_run(run_id)
        pause_flag = self._pause_flags.get(run_id)
        if pause_flag is not None:
            pause_flag.clear()
        run["status"] = RUN_STATUS_PAUSED
        run["updated_at"] = utc_now_ms()
        return self._store.save_run(run)

    def cancel_run(self, run_id: str) -> DesignerExecutionRun:
        run = self._require_run(run_id)
        cancel_flag = self._cancel_flags.get(run_id)
        if cancel_flag is not None:
            cancel_flag.set()
        task = self._tasks.pop(run_id, None)
        if task is not None and not task.done():
            task.cancel()
        for node_id, state in run.get("node_states", {}).items():
            if state.get("status") in {NODE_STATUS_PENDING, NODE_STATUS_RUNNING}:
                state["status"] = NODE_STATUS_CANCELLED
        run["status"] = RUN_STATUS_CANCELLED
        run["current_node_ids"] = []
        run["updated_at"] = utc_now_ms()
        return self._store.save_run(run)

    def _require_run(self, run_id: str) -> DesignerExecutionRun:
        run = self._store.get_run(run_id)
        if run is None:
            raise KeyError(f"designer run not found: {run_id}")
        return run

    def _require_graph(self, graph_id: str) -> DesignerExecutionGraph:
        graph = self._store.get_graph(graph_id)
        if graph is None:
            raise KeyError(f"designer graph not found: {graph_id}")
        return graph

    async def _execute_run(
        self,
        graph: DesignerExecutionGraph,
        run: DesignerExecutionRun,
        *,
        on_update: RunUpdateCallback | None,
    ) -> None:
        run_id = run["run_id"]
        order = _topological_order(graph)
        try:
            for node_id in order:
                await self._await_pause(run_id)
                if self._is_cancelled(run_id):
                    break
                node = _node_by_id(graph, node_id)
                await self._run_single_node(graph, run, node, on_update=on_update)
            if self._is_cancelled(run_id):
                return
            statuses = {state.get("status") for state in run["node_states"].values()}
            if NODE_STATUS_FAILED in statuses:
                run["status"] = RUN_STATUS_FAILED
            else:
                run["status"] = RUN_STATUS_COMPLETED
            run["current_node_ids"] = []
            run["updated_at"] = utc_now_ms()
            self._publish(run, on_update)
            self._store.save_run(run)
        except asyncio.CancelledError:
            run = self.cancel_run(run_id)
            self._publish(run, on_update)
            raise
        except Exception as exc:  # noqa: BLE001
            logger.exception("Designer run %s failed: %s", run_id, exc)
            run["status"] = RUN_STATUS_FAILED
            run["updated_at"] = utc_now_ms()
            self._publish(run, on_update)
            self._store.save_run(run)
        finally:
            self._tasks.pop(run_id, None)
            self._pause_flags.pop(run_id, None)
            self._cancel_flags.pop(run_id, None)

    async def _run_single_node(
        self,
        graph: DesignerExecutionGraph,
        run: DesignerExecutionRun,
        node: DesignerGraphNode,
        *,
        on_update: RunUpdateCallback | None,
    ) -> None:
        node_id = node["id"]
        started_at = utc_now_ms()
        self._set_node_state(
            run,
            node_id,
            {"status": NODE_STATUS_RUNNING, "started_at": started_at, "error": None},
        )
        run["current_node_ids"] = [node_id]
        self._publish(run, on_update)
        try:
            handler = get_node_handler(str(node.get("type") or ""))
            ctx = NodeExecutionContext(graph=graph, run_id=run["run_id"], node_id=node_id)
            result = await handler.execute(node, ctx)
            await asyncio.sleep(_MOCK_NODE_DELAY_SECONDS)
            if self._is_cancelled(run["run_id"]):
                return
            self._set_node_state(
                run,
                node_id,
                {
                    "status": NODE_STATUS_COMPLETED,
                    "started_at": started_at,
                    "completed_at": utc_now_ms(),
                    "output_ref": result.output_ref,
                    "error": None,
                },
            )
        except Exception as exc:  # noqa: BLE001
            self._set_node_state(
                run,
                node_id,
                {
                    "status": NODE_STATUS_FAILED,
                    "started_at": started_at,
                    "completed_at": utc_now_ms(),
                    "error": str(exc),
                },
            )
            run["status"] = RUN_STATUS_FAILED
        finally:
            run["updated_at"] = utc_now_ms()
            self._publish(run, on_update)
            self._store.save_run(run)

    async def _await_pause(self, run_id: str) -> None:
        pause_flag = self._pause_flags.get(run_id)
        if pause_flag is None:
            return
        await pause_flag.wait()

    def _is_cancelled(self, run_id: str) -> bool:
        cancel_flag = self._cancel_flags.get(run_id)
        return cancel_flag is not None and cancel_flag.is_set()

    @staticmethod
    def _set_node_state(
        run: DesignerExecutionRun,
        node_id: str,
        patch: DesignerNodeState,
    ) -> None:
        states = run.setdefault("node_states", {})
        current = dict(states.get(node_id) or {"status": NODE_STATUS_PENDING})
        current.update(patch)
        states[node_id] = current

    @staticmethod
    def _publish(
        run: DesignerExecutionRun,
        on_update: RunUpdateCallback | None,
    ) -> None:
        if on_update is not None:
            on_update(run)


def _node_by_id(graph: DesignerExecutionGraph, node_id: str) -> DesignerGraphNode:
    for node in graph.get("nodes", []):
        if node.get("id") == node_id:
            return node
    raise KeyError(f"node not found: {node_id}")


def _incoming_edges(graph: DesignerExecutionGraph) -> dict[str, list[str]]:
    incoming: dict[str, list[str]] = defaultdict(list)
    for edge in graph.get("edges", []):
        target = edge.get("target")
        source = edge.get("source")
        if isinstance(target, str) and isinstance(source, str):
            incoming[target].append(source)
    return incoming


def _topological_order(graph: DesignerExecutionGraph) -> list[str]:
    incoming = _incoming_edges(graph)
    indegree = {node["id"]: len(incoming.get(node["id"], [])) for node in graph["nodes"]}
    outgoing: dict[str, list[str]] = defaultdict(list)
    for edge in graph.get("edges", []):
        source = edge.get("source")
        target = edge.get("target")
        if isinstance(source, str) and isinstance(target, str):
            outgoing[source].append(target)
    queue = deque(node_id for node_id, degree in indegree.items() if degree == 0)
    order: list[str] = []
    while queue:
        node_id = queue.popleft()
        order.append(node_id)
        for target_id in outgoing.get(node_id, []):
            indegree[target_id] -= 1
            if indegree[target_id] == 0:
                queue.append(target_id)
    if len(order) != len(graph.get("nodes", [])):
        raise ValueError("designer graph contains a cycle")
    return order
