# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Designer graph executor — wave scheduler or agent-owned graph."""

from __future__ import annotations

import asyncio
import logging
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Protocol

from jiuwenswarm.common.schema.designer_graph import (
    DesignerExecutionGraph,
    DesignerExecutionRun,
    DesignerGraphNode,
    DesignerNodeState,
    MAX_SHOT_CLIP_NODES,
    NODE_ROLE_CLIP,
    NODE_ROLE_COMPOSE,
    NODE_ROLE_FRAME,
    NODE_ROLE_STORYBOARD,
    NODE_STATUS_CANCELLED,
    NODE_STATUS_COMPLETED,
    NODE_STATUS_FAILED,
    NODE_STATUS_PENDING,
    NODE_STATUS_RUNNING,
    NODE_TYPE_IMAGE,
    NODE_TYPE_VIDEO,
    RUN_STATUS_CANCELLED,
    RUN_STATUS_COMPLETED,
    RUN_STATUS_DRAFT,
    RUN_STATUS_FAILED,
    RUN_STATUS_PAUSED,
    RUN_STATUS_RUNNING,
    apply_graph_patch,
    apply_shot_generate_prompts,
    clip_node_id,
    data_predecessors,
    expand_shot_nodes,
    frame_node_id,
    graph_uses_agent_scheduler,
    initial_node_states,
    new_run_id,
    node_role,
    node_uses_agent_runtime,
    sync_groups,
    utc_now_ms,
)
from jiuwenswarm.common.schema.message import EventType
from jiuwenswarm.server.runtime.designer.a2a_collab import collaborate_ready_wave
from jiuwenswarm.server.runtime.designer.graph_store import DesignerGraphStore
from jiuwenswarm.server.runtime.designer.handlers import (
    NodeExecutionContext,
    get_node_handler,
)
from jiuwenswarm.server.runtime.designer.node_agent import NodeAgentHost, NodeAgentRunner

logger = logging.getLogger(__name__)

GraphUpdateCallback = Callable[[DesignerExecutionGraph], None]


class RunUpdateCallback(Protocol):
    def __call__(
        self,
        run: DesignerExecutionRun,
        node_id: str | None = None,
    ) -> None: ...

_MOCK_NODE_DELAY_SECONDS = 0.35
_MAX_CONCURRENT_NODE_AGENTS = 6
_TERMINAL_NODE_STATUSES = {
    NODE_STATUS_COMPLETED,
    NODE_STATUS_FAILED,
    NODE_STATUS_CANCELLED,
}


@dataclass(frozen=True)
class NodeEvent:
    event: str
    run: DesignerExecutionRun
    node_id: str | None = None


class GraphExecutor:
    """Wave scheduler for handler graphs; agent scheduler for node Agents."""

    def __init__(
        self,
        store: DesignerGraphStore | None = None,
        *,
        runner: NodeAgentRunner | None = None,
    ) -> None:
        self._store = store or DesignerGraphStore()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._node_workers: dict[str, dict[str, asyncio.Task[None]]] = {}
        self._pause_flags: dict[str, asyncio.Event] = {}
        self._cancel_flags: dict[str, asyncio.Event] = {}
        self._state_locks: dict[str, asyncio.Lock] = {}
        self._on_updates: dict[str, RunUpdateCallback] = {}
        self._on_graph_updates: dict[str, GraphUpdateCallback] = {}
        self._live_runs: dict[str, DesignerExecutionRun] = {}
        self._host = NodeAgentHost(self, runner=runner)

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

    def create_rerun(
        self,
        graph: DesignerExecutionGraph,
        *,
        source_run: DesignerExecutionRun,
        node_id: str,
    ) -> DesignerExecutionRun:
        """Copy a finished run and reset one node so only that node executes again."""
        node_ids = {node["id"] for node in graph.get("nodes", [])}
        if node_id not in node_ids:
            raise KeyError(f"node not found: {node_id}")
        incoming = data_predecessors(graph)
        groups = sync_groups(graph)
        source_states = source_run.get("node_states") or {}
        for pred in incoming.get(node_id, []):
            members = groups.get(pred, frozenset({pred}))
            for member in members:
                if (source_states.get(member) or {}).get("status") != NODE_STATUS_COMPLETED:
                    raise ValueError(f"upstream not ready: {member}")
        now = utc_now_ms()
        states = deepcopy(source_states)
        for node in graph.get("nodes", []):
            states.setdefault(node["id"], {"status": NODE_STATUS_PENDING})
        previous = states.get(node_id) or {}
        kept_ref = previous.get("output_ref") if _usable_ref(previous.get("output_ref")) else None
        kept_refs = [
            ref for ref in (previous.get("output_refs") or []) if _usable_ref(ref)
        ]
        if kept_ref is not None and not kept_refs:
            kept_refs = [kept_ref]
        target_node = _node_by_id(graph, node_id)
        target_type = str(target_node.get("type") or "")
        if (
            target_type in {NODE_TYPE_IMAGE, NODE_TYPE_VIDEO}
            and _is_fallback_text_ref(kept_ref)
        ) or node_role(target_node) == NODE_ROLE_COMPOSE:
            kept_ref = None
            kept_refs = []
        states[node_id] = {
            "status": NODE_STATUS_PENDING,
            "started_at": None,
            "completed_at": None,
            "output_ref": kept_ref,
            "output_refs": kept_refs,
            "error": None,
            "blocked_by": [],
        }
        run: DesignerExecutionRun = {
            "schema_version": "designer-execution-run.v1",
            "run_id": new_run_id(),
            "graph_id": graph["graph_id"],
            "project_id": graph["project_id"],
            "status": RUN_STATUS_DRAFT,
            "node_states": states,
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
        on_graph_update: GraphUpdateCallback | None = None,
    ) -> DesignerExecutionRun:
        run = self._require_run(run_id)
        if run["status"] in {RUN_STATUS_RUNNING, RUN_STATUS_COMPLETED}:
            return run
        graph = self._require_graph(run["graph_id"])
        run["status"] = RUN_STATUS_RUNNING
        run["updated_at"] = utc_now_ms()
        run = self._store.save_run(run)
        self._live_runs[run_id] = run
        self._pause_flags[run_id] = asyncio.Event()
        self._pause_flags[run_id].set()
        self._cancel_flags[run_id] = asyncio.Event()
        self._state_locks[run_id] = asyncio.Lock()
        self._node_workers[run_id] = {}
        if on_update is not None:
            self._on_updates[run_id] = on_update
        if on_graph_update is not None:
            self._on_graph_updates[run_id] = on_graph_update
        task = asyncio.create_task(
            self._execute_run(graph, run, on_update=on_update),
            name=f"designer-run-{run_id}",
        )
        self._tasks[run_id] = task
        return run

    async def run(self, graph: DesignerExecutionGraph, run_id: str) -> AsyncIterator[NodeEvent]:
        """Drive a run and yield node/run events as they happen."""
        queue: asyncio.Queue[NodeEvent] = asyncio.Queue()

        def on_update(updated: DesignerExecutionRun, node_id: str | None = None) -> None:
            event = (
                EventType.DESIGNER_NODE_UPDATED.value
                if node_id
                else EventType.DESIGNER_RUN_UPDATED.value
            )
            queue.put_nowait(
                NodeEvent(
                    event=event,
                    run=deepcopy(updated),
                    node_id=node_id,
                )
            )

        existing = self._tasks.get(run_id)
        if existing is None or existing.done():
            started = await self.start_run(run_id, on_update=on_update)
            queue.put_nowait(
                NodeEvent(event=EventType.DESIGNER_RUN_UPDATED.value, run=deepcopy(started))
            )
        task = self._tasks.get(run_id)
        if task is None:
            return
        while True:
            if task.done() and queue.empty():
                break
            try:
                event = await asyncio.wait_for(queue.get(), timeout=0.05)
            except TimeoutError:
                continue
            yield event
        await task

    def pause_run(self, run_id: str) -> DesignerExecutionRun:
        run = self._require_run(run_id)
        pause_flag = self._pause_flags.get(run_id)
        if pause_flag is not None:
            pause_flag.clear()
        run["status"] = RUN_STATUS_PAUSED
        run["updated_at"] = utc_now_ms()
        return self._store.save_run(run)

    def choose_output(
        self,
        run_id: str,
        node_id: str,
        choice: str,
    ) -> DesignerExecutionRun:
        """Keep the original output or promote the regenerated candidate."""
        run = self._require_run(run_id)
        states = run.setdefault("node_states", {})
        state = states.get(node_id)
        if not isinstance(state, dict):
            raise KeyError(f"node not found: {node_id}")
        if state.get("status") == NODE_STATUS_RUNNING:
            raise ValueError("node is still running")
        decided = str(choice or "").strip()
        if decided not in {"original", "new"}:
            raise ValueError("choice must be original or new")
        candidate = state.get("candidate_output_ref")
        if not _usable_ref(candidate):
            raise ValueError("no pending revision")
        if decided == "new":
            refs = [ref for ref in (state.get("candidate_output_refs") or []) if _usable_ref(ref)]
            state["output_ref"] = candidate
            state["output_refs"] = refs or [candidate]
        state["candidate_output_ref"] = None
        state["candidate_output_refs"] = []
        run["updated_at"] = utc_now_ms()
        return self._store.save_run(run)

    def cancel_run(self, run_id: str) -> DesignerExecutionRun:
        run = self._require_run(run_id)
        cancel_flag = self._cancel_flags.get(run_id)
        if cancel_flag is not None:
            cancel_flag.set()
        workers = self._node_workers.pop(run_id, {})
        for worker in workers.values():
            if not worker.done():
                worker.cancel()
        self._host.drop_run(run_id)
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

    async def spawn_node_agent(self, run_id: str, node_id: str) -> str:
        """Start one node's Agent. Allowed even if data-predecessors are incomplete."""
        target = str(node_id or "").strip()
        if not target:
            return "node_id is required"
        if self._is_cancelled(run_id):
            return "run cancelled"
        run = self._require_run(run_id)
        if run.get("status") not in {RUN_STATUS_RUNNING, RUN_STATUS_PAUSED}:
            return f"run is {run.get('status')}"
        graph = self._require_graph(str(run.get("graph_id") or ""))
        try:
            _node_by_id(graph, target)
        except KeyError:
            return f"node not found: {target}"
        workers = self._node_workers.setdefault(run_id, {})
        existing = workers.get(target)
        if existing is not None and not existing.done():
            return f"already running: {target}"
        running_count = sum(1 for item in workers.values() if not item.done())
        if running_count >= _MAX_CONCURRENT_NODE_AGENTS:
            return "too many concurrent node agents"
        lock = self._state_locks.setdefault(run_id, asyncio.Lock())
        async with lock:
            run = self._require_run(run_id)
            states = run.setdefault("node_states", {})
            current = dict(states.get(target) or {"status": NODE_STATUS_PENDING})
            if current.get("status") == NODE_STATUS_RUNNING:
                return f"already running: {target}"
            if current.get("status") in _TERMINAL_NODE_STATUSES:
                current["status"] = NODE_STATUS_PENDING
                current["error"] = None
                current["completed_at"] = None
            states[target] = current
            current_ids = list(run.get("current_node_ids") or [])
            if target not in current_ids:
                current_ids.append(target)
                run["current_node_ids"] = current_ids
            run["updated_at"] = utc_now_ms()
            self._store.save_run(run)
        on_update = self._on_updates.get(run_id)
        task = asyncio.create_task(
            self._run_spawned_node(run_id, target, on_update=on_update),
            name=f"designer-node-{run_id}-{target}",
        )
        workers[target] = task
        return f"started {target}"

    def apply_agent_graph_patch(
        self,
        graph_id: str,
        patch: dict[str, Any],
    ) -> DesignerExecutionGraph:
        graph = self._require_graph(graph_id)
        saved = self._store.save_graph(apply_graph_patch(graph, patch))
        node_ids = {node["id"] for node in saved.get("nodes") or []}
        for run_id, run in list(self._live_runs.items()):
            if run.get("graph_id") != graph_id:
                continue
            states = run.setdefault("node_states", {})
            for node_id in node_ids:
                states.setdefault(node_id, {"status": NODE_STATUS_PENDING})
            run["updated_at"] = utc_now_ms()
            self._store.save_run(run)
            callback = self._on_graph_updates.get(run_id)
            if callback is not None:
                callback(deepcopy(saved))
        return saved

    def load_graph_snapshot(self, graph_id: str, run_id: str) -> dict[str, Any]:
        graph = self._require_graph(graph_id)
        run = self._store.get_run(run_id)
        return {
            "graph": deepcopy(graph),
            "run": deepcopy(run) if run else None,
        }

    def _require_run(self, run_id: str) -> DesignerExecutionRun:
        live = self._live_runs.get(run_id)
        if live is not None:
            return live
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
        if graph_uses_agent_scheduler(graph):
            await self._execute_agent_run(graph, run, on_update=on_update)
            return
        await self._execute_wave_run(graph, run, on_update=on_update)

    async def _execute_agent_run(
        self,
        graph: DesignerExecutionGraph,
        run: DesignerExecutionRun,
        *,
        on_update: RunUpdateCallback | None,
    ) -> None:
        run_id = run["run_id"]
        try:
            incoming = data_predecessors(graph)
            groups = sync_groups(graph)
            ready_ids = [
                node["id"]
                for node in graph.get("nodes") or []
                if _is_ready(node["id"], run, incoming, groups)
            ]
            if not ready_ids:
                pending = any(
                    (run.get("node_states") or {}).get(node["id"], {}).get("status")
                    == NODE_STATUS_PENDING
                    for node in graph.get("nodes") or []
                )
                run["status"] = RUN_STATUS_FAILED if pending else RUN_STATUS_COMPLETED
                run["current_node_ids"] = []
                run["updated_at"] = utc_now_ms()
                self._publish(run, on_update)
                self._store.save_run(run)
                return
            run["current_node_ids"] = list(ready_ids)
            run["updated_at"] = utc_now_ms()
            self._publish(run, on_update)
            self._store.save_run(run)
            for node_id in ready_ids:
                await self.spawn_node_agent(run_id, node_id)
            await self._wait_agent_workers(run_id)
            if self._is_cancelled(run_id):
                return
            run = self._require_run(run_id)
            statuses = {state.get("status") for state in (run.get("node_states") or {}).values()}
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
            logger.exception("Designer agent run %s failed: %s", run_id, exc)
            run["status"] = RUN_STATUS_FAILED
            run["updated_at"] = utc_now_ms()
            self._publish(run, on_update)
            self._store.save_run(run)
        finally:
            self._cleanup_run(run_id)

    async def _execute_wave_run(
        self,
        graph: DesignerExecutionGraph,
        run: DesignerExecutionRun,
        *,
        on_update: RunUpdateCallback | None,
    ) -> None:
        run_id = run["run_id"]
        remaining = {
            node["id"]
            for node in graph.get("nodes", [])
            if (run.get("node_states") or {}).get(node["id"], {}).get("status")
            not in _TERMINAL_NODE_STATUSES
        }
        incoming = data_predecessors(graph)
        groups = sync_groups(graph)
        graph, remaining, incoming, groups = self._expand_clips_if_needed(
            graph, run, remaining, on_update=on_update
        )
        try:
            while remaining:
                await self._await_pause(run_id)
                if self._is_cancelled(run_id):
                    break
                ready_ids = [
                    node_id
                    for node_id in remaining
                    if _is_ready(node_id, run, incoming, groups)
                ]
                if not ready_ids:
                    run["status"] = RUN_STATUS_FAILED
                    run["updated_at"] = utc_now_ms()
                    self._publish(run, on_update)
                    self._store.save_run(run)
                    return
                run["current_node_ids"] = list(ready_ids)
                run["updated_at"] = utc_now_ms()
                self._publish(run, on_update)
                self._store.save_run(run)
                await collaborate_ready_wave(graph, run, ready_ids)
                await asyncio.gather(
                    *(
                        self._run_single_node(
                            graph,
                            run,
                            _node_by_id(graph, node_id),
                            on_update=on_update,
                        )
                        for node_id in ready_ids
                    )
                )
                remaining -= {
                    node_id
                    for node_id in ready_ids
                    if run["node_states"].get(node_id, {}).get("status") in _TERMINAL_NODE_STATUSES
                }
                graph, remaining, incoming, groups = self._expand_clips_if_needed(
                    graph, run, remaining, on_update=on_update
                )
                if run.get("status") == RUN_STATUS_FAILED or self._is_cancelled(run_id):
                    break
            if self._is_cancelled(run_id):
                return
            statuses = {state.get("status") for state in run["node_states"].values()}
            if NODE_STATUS_FAILED in statuses or remaining:
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
            self._cleanup_run(run_id)

    def reconcile_loaded_graph(self, graph: DesignerExecutionGraph) -> DesignerExecutionGraph:
        """Expand per-shot keyframe nodes when a completed storyboard already exists."""
        graph_id = str(graph.get("graph_id") or "")
        run = None
        for live in self._live_runs.values():
            if str(live.get("graph_id") or "") == graph_id:
                run = live
                break
        if run is None:
            run = self._store.get_latest_run_for_graph(graph_id)
        if run is None:
            return graph
        expanded, _, _, _ = self._expand_clips_if_needed(
            graph, run, set(), on_update=None
        )
        return expanded

    def _expand_clips_if_needed(
        self,
        graph: DesignerExecutionGraph,
        run: DesignerExecutionRun,
        remaining: set[str],
        *,
        on_update: RunUpdateCallback | None,
    ) -> tuple[
        DesignerExecutionGraph,
        set[str],
        dict[str, list[str]],
        dict[str, frozenset[str]],
    ]:
        shot_rows = self._completed_storyboard_shots(graph, run)
        if shot_rows is None:
            return graph, remaining, data_predecessors(graph), sync_groups(graph)
        shot_count = max(1, min(len(shot_rows) or 1, MAX_SHOT_CLIP_NODES))
        from jiuwenswarm.server.runtime.designer.handlers.text_nodes import shot_generate_prompt

        prompts = [shot_generate_prompt(shot) for shot in shot_rows]
        has_clip_pipeline = any(
            node_role(node) in {NODE_ROLE_CLIP, NODE_ROLE_COMPOSE}
            or str(node.get("id") or "") in {"n_clip", "n_compose"}
            or str(node.get("id") or "").startswith("n_clip_")
            for node in graph.get("nodes") or []
        )
        if not has_clip_pipeline:
            return graph, remaining, data_predecessors(graph), sync_groups(graph)
        current_clip_ids = {
            str(node.get("id") or "")
            for node in graph.get("nodes") or []
            if node_role(node) == NODE_ROLE_CLIP
        }
        current_frame_ids = {
            str(node.get("id") or "")
            for node in graph.get("nodes") or []
            if node_role(node) == NODE_ROLE_FRAME
        }
        wanted_clip_ids = {clip_node_id(index) for index in range(1, shot_count + 1)}
        wanted_frame_ids = {frame_node_id(index) for index in range(1, shot_count + 1)}
        has_compose = any(
            node_role(node) == NODE_ROLE_COMPOSE or str(node.get("id") or "") == "n_compose"
            for node in graph.get("nodes") or []
        )
        topology_matches = (
            current_clip_ids == wanted_clip_ids
            and current_frame_ids == wanted_frame_ids
            and has_compose
        )
        bundled_frames = any(
            len(_image_output_refs((run.get("node_states") or {}).get(node_id) or {})) > 1
            for node_id in (current_frame_ids | {"n_frame"})
        )
        if topology_matches and not bundled_frames:
            synced = apply_shot_generate_prompts(graph, prompts)
            saved = synced if synced is graph else self._store.save_graph(synced)
            if saved is not graph:
                callback = self._on_graph_updates.get(str(run.get("run_id") or ""))
                if callback is not None:
                    callback(deepcopy(saved))
            states = run.setdefault("node_states", {})
            for node in saved.get("nodes") or []:
                node_id = str(node.get("id") or "")
                if node_id:
                    states.setdefault(node_id, {"status": NODE_STATUS_PENDING})
            remaining = _pending_node_ids(saved, run)
            return saved, remaining, data_predecessors(saved), sync_groups(saved)

        saved = self._store.save_graph(
            apply_shot_generate_prompts(expand_shot_nodes(graph, shot_count), prompts)
        )
        live_ids = {str(node.get("id") or "") for node in saved.get("nodes") or []}
        states = run.setdefault("node_states", {})
        for node_id in live_ids:
            states.setdefault(node_id, {"status": NODE_STATUS_PENDING})
        redistribute_frame_node_states(run, shot_count)
        remaining = {
            node_id
            for node_id in live_ids
            if (states.get(node_id) or {}).get("status") not in _TERMINAL_NODE_STATUSES
        }
        run["updated_at"] = utc_now_ms()
        self._store.save_run(run)
        self._publish(run, on_update)
        callback = self._on_graph_updates.get(str(run.get("run_id") or ""))
        if callback is not None:
            callback(deepcopy(saved))
        return saved, remaining, data_predecessors(saved), sync_groups(saved)

    def _completed_storyboard_shots(
        self,
        graph: DesignerExecutionGraph,
        run: DesignerExecutionRun,
    ) -> list | None:
        from jiuwenswarm.server.runtime.designer.handlers.common import role_output_text
        from jiuwenswarm.server.runtime.designer.handlers.text_nodes import (
            storyboard_shots_or_default,
        )
        from jiuwenswarm.server.runtime.designer.handlers.types import NodeExecutionContext

        storyboard = next(
            (
                node
                for node in graph.get("nodes") or []
                if node_role(node) == NODE_ROLE_STORYBOARD
            ),
            None,
        )
        if storyboard is None:
            return []
        node_id = str(storyboard.get("id") or "")
        status = ((run.get("node_states") or {}).get(node_id) or {}).get("status")
        if status != NODE_STATUS_COMPLETED:
            return None
        ctx = NodeExecutionContext(
            graph=graph,
            run_id=str(run.get("run_id") or ""),
            node_id=node_id,
            run=run,
        )
        text = role_output_text(ctx, NODE_ROLE_STORYBOARD)
        shots = storyboard_shots_or_default(
            text,
            str(graph.get("description") or graph.get("title") or ""),
        )
        return shots[:MAX_SHOT_CLIP_NODES]

    def _completed_storyboard_shot_count(
        self,
        graph: DesignerExecutionGraph,
        run: DesignerExecutionRun,
    ) -> int | None:
        shots = self._completed_storyboard_shots(graph, run)
        if shots is None:
            return None
        return max(1, min(len(shots) or 1, MAX_SHOT_CLIP_NODES))

    async def _wait_agent_workers(self, run_id: str) -> None:
        while True:
            if self._is_cancelled(run_id):
                return
            await self._await_pause(run_id)
            workers = self._node_workers.get(run_id) or {}
            pending = [task for task in workers.values() if not task.done()]
            if not pending:
                return
            await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

    async def _run_spawned_node(
        self,
        run_id: str,
        node_id: str,
        *,
        on_update: RunUpdateCallback | None,
    ) -> None:
        try:
            await self._await_pause(run_id)
            if self._is_cancelled(run_id):
                return
            run = self._require_run(run_id)
            graph = self._require_graph(str(run.get("graph_id") or ""))
            node = _node_by_id(graph, node_id)
            await self._run_single_node(graph, run, node, on_update=on_update)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Designer spawned node %s failed in run %s", node_id, run_id)
        finally:
            workers = self._node_workers.get(run_id)
            if workers is not None:
                workers.pop(node_id, None)

    def _cleanup_run(self, run_id: str) -> None:
        self._tasks.pop(run_id, None)
        self._node_workers.pop(run_id, None)
        self._pause_flags.pop(run_id, None)
        self._cancel_flags.pop(run_id, None)
        self._state_locks.pop(run_id, None)
        self._on_updates.pop(run_id, None)
        self._on_graph_updates.pop(run_id, None)
        self._live_runs.pop(run_id, None)
        self._host.drop_run(run_id)

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
        blocked_by = list(sync_groups(graph).get(node_id, frozenset()) - {node_id})
        lock = self._state_locks.setdefault(run["run_id"], asyncio.Lock())
        async with lock:
            self._set_node_state(
                run,
                node_id,
                {
                    "status": NODE_STATUS_RUNNING,
                    "started_at": started_at,
                    "error": None,
                    "blocked_by": blocked_by,
                },
            )
            run["updated_at"] = utc_now_ms()
            self._store.save_run(run)
            self._publish(run, on_update, node_id)
        try:
            ctx = NodeExecutionContext(
                graph=graph,
                run_id=run["run_id"],
                node_id=node_id,
                run=run,
            )
            if node_uses_agent_runtime(node):
                result = await self._host.execute(node, ctx)
            else:
                handler = get_node_handler(node)
                result = await handler.execute(node, ctx)
                if _MOCK_NODE_DELAY_SECONDS:
                    await asyncio.sleep(_MOCK_NODE_DELAY_SECONDS)
            if self._is_cancelled(run["run_id"]):
                return
            refs = [ref for ref in (result.output_refs or []) if ref]
            primary = result.output_ref or (refs[0] if refs else None)
            if primary is not None and not refs:
                refs = [primary]
            if node_role(node) == NODE_ROLE_FRAME:
                image_refs = [ref for ref in refs if _ref_kind(ref) == "image"]
                if image_refs:
                    primary = image_refs[0]
                    refs = [image_refs[0]]
            current = (run.get("node_states") or {}).get(node_id) or {}
            kept = current.get("output_ref") if _usable_ref(current.get("output_ref")) else None
            kept_refs = [ref for ref in (current.get("output_refs") or []) if _usable_ref(ref)]
            if kept is not None and not kept_refs:
                kept_refs = [kept]
            incoming_uri = str((primary or {}).get("uri") or "") if primary else ""
            kept_uri = str((kept or {}).get("uri") or "") if kept else ""
            pending = bool(kept and primary and incoming_uri and incoming_uri != kept_uri)
            if pending and (
                node_role(node) == NODE_ROLE_COMPOSE or _should_auto_promote(kept, primary)
            ):
                pending = False
            accepted = kept if pending else primary
            async with lock:
                self._set_node_state(
                    run,
                    node_id,
                    {
                        "status": NODE_STATUS_COMPLETED,
                        "started_at": started_at,
                        "completed_at": utc_now_ms(),
                        "output_ref": accepted,
                        "output_refs": kept_refs if pending else refs,
                        "candidate_output_ref": primary if pending else None,
                        "candidate_output_refs": refs if pending else [],
                        "error": None,
                        "blocked_by": [],
                    },
                )
            self._persist_node_output_on_graph(
                graph,
                node_id,
                accepted if isinstance(accepted, dict) else None,
                run_id=str(run.get("run_id") or ""),
            )
            if node_role(node) == NODE_ROLE_STORYBOARD:
                live_graph = self._require_graph(str(run.get("graph_id") or graph.get("graph_id") or ""))
                self._expand_clips_if_needed(live_graph, run, set(), on_update=on_update)
        except Exception as exc:  # noqa: BLE001
            async with lock:
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
            async with lock:
                run["updated_at"] = utc_now_ms()
                self._publish(run, on_update, node_id)
                self._store.save_run(run)

    async def _await_pause(self, run_id: str) -> None:
        pause_flag = self._pause_flags.get(run_id)
        if pause_flag is None:
            return
        await pause_flag.wait()

    def _is_cancelled(self, run_id: str) -> bool:
        cancel_flag = self._cancel_flags.get(run_id)
        return cancel_flag is not None and cancel_flag.is_set()

    def _persist_node_output_on_graph(
        self,
        graph: DesignerExecutionGraph,
        node_id: str,
        ref: dict[str, Any] | None,
        *,
        run_id: str,
    ) -> None:
        """Write the completed artifact onto the graph so a restart still shows it."""
        if not node_id or not isinstance(ref, dict) or not str(ref.get("uri") or "").strip():
            return
        nodes = []
        changed = False
        for node in graph.get("nodes") or []:
            if str(node.get("id") or "") != node_id:
                nodes.append(node)
                continue
            current = node.get("output_ref")
            if current == ref:
                nodes.append(node)
                continue
            nodes.append({**node, "output_ref": dict(ref)})
            changed = True
        if not changed:
            return
        graph["nodes"] = nodes
        saved = self._store.save_graph(graph)
        graph.update(saved)
        callback = self._on_graph_updates.get(run_id)
        if callback is not None:
            callback(deepcopy(saved))

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
        node_id: str | None = None,
    ) -> None:
        if on_update is not None:
            on_update(run, node_id)


def _image_output_refs(state: dict[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    seen: set[str] = set()
    raw_refs = list(state.get("output_refs") or [])
    primary = state.get("output_ref")
    if primary is not None:
        raw_refs = [primary, *raw_refs]
    for ref in raw_refs:
        if not _usable_ref(ref) or _ref_kind(ref) != "image":
            continue
        uri = str((ref or {}).get("uri") or "")
        if not uri or uri in seen:
            continue
        seen.add(uri)
        refs.append(ref)
    return refs


def redistribute_frame_node_states(run: DesignerExecutionRun, shot_count: int) -> None:
    """Split a bundled keyframe node (many PNGs) into one image per n_frame_i."""
    count = max(1, min(int(shot_count or 1), MAX_SHOT_CLIP_NODES))
    states = run.setdefault("node_states", {})
    bundled: list[dict[str, Any]] = []
    source_status = NODE_STATUS_PENDING
    for node_id in ("n_frame", *[frame_node_id(index) for index in range(1, count + 1)]):
        state = states.get(node_id) or {}
        images = _image_output_refs(state)
        if len(images) > len(bundled):
            bundled = images
            source_status = str(state.get("status") or NODE_STATUS_PENDING)
    for index in range(1, count + 1):
        node_id = frame_node_id(index)
        state = dict(states.get(node_id) or {"status": NODE_STATUS_PENDING})
        images = _image_output_refs(state)
        if index <= len(bundled):
            ref = bundled[index - 1]
            state["output_ref"] = ref
            state["output_refs"] = [ref]
            if source_status == NODE_STATUS_COMPLETED and state.get("status") == NODE_STATUS_PENDING:
                state["status"] = NODE_STATUS_COMPLETED
                state["error"] = None
        elif len(images) > 1:
            state["output_ref"] = images[0]
            state["output_refs"] = [images[0]]
        elif images:
            state["output_ref"] = images[0]
            state["output_refs"] = [images[0]]
        states[node_id] = state


def _usable_ref(ref: object) -> bool:
    if not isinstance(ref, dict):
        return False
    uri = str(ref.get("uri") or "").strip()
    return bool(uri) and not uri.startswith("designer://")


_TEXT_FALLBACK_KINDS = {"text", "table"}
_MEDIA_KINDS = {"image", "video", "audio"}
_MEDIA_SUFFIXES = (
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".gif",
    ".bmp",
    ".mp4",
    ".webm",
    ".mov",
    ".m4v",
)


def _ref_kind(ref: object) -> str:
    if not isinstance(ref, dict):
        return ""
    kind = str(ref.get("kind") or "").strip().lower()
    if kind:
        return kind
    mime = str(ref.get("mime_type") or "").strip().lower()
    if mime.startswith("image/"):
        return "image"
    if mime.startswith("video/"):
        return "video"
    if mime.startswith("audio/"):
        return "audio"
    if mime.startswith("text/"):
        return "text"
    label = f"{ref.get('label') or ''} {ref.get('uri') or ''}".lower()
    if ".md" in label:
        return "text"
    if any(ext in label for ext in _MEDIA_SUFFIXES):
        return "video" if any(ext in label for ext in (".mp4", ".webm", ".mov", ".m4v")) else "image"
    return ""


def _is_fallback_text_ref(ref: object) -> bool:
    return _ref_kind(ref) in _TEXT_FALLBACK_KINDS


def _is_media_ref(ref: object) -> bool:
    return _ref_kind(ref) in _MEDIA_KINDS


def _should_auto_promote(kept: object, primary: object) -> bool:
    """Replace fallback notes with media, and replace regenerated markdown/tables in place."""
    if _is_fallback_text_ref(kept) and _is_media_ref(primary):
        return True
    return _is_fallback_text_ref(kept) and _is_fallback_text_ref(primary)


def _node_by_id(graph: DesignerExecutionGraph, node_id: str) -> DesignerGraphNode:
    for node in graph.get("nodes", []):
        if node.get("id") == node_id:
            return node
    raise KeyError(f"node not found: {node_id}")


def _pending_node_ids(graph: DesignerExecutionGraph, run: DesignerExecutionRun) -> set[str]:
    states = run.get("node_states") or {}
    pending: set[str] = set()
    for node in graph.get("nodes") or []:
        node_id = str(node.get("id") or "")
        if not node_id:
            continue
        if (states.get(node_id) or {}).get("status") not in _TERMINAL_NODE_STATUSES:
            pending.add(node_id)
    return pending


def _is_ready(
    node_id: str,
    run: DesignerExecutionRun,
    incoming: dict[str, list[str]],
    groups: dict[str, frozenset[str]],
) -> bool:
    state = run.get("node_states", {}).get(node_id) or {}
    if state.get("status") != NODE_STATUS_PENDING:
        return False
    preds = incoming.get(node_id, [])
    for pred in preds:
        group = groups.get(pred, frozenset({pred}))
        for member in group:
            member_status = (run.get("node_states", {}).get(member) or {}).get("status")
            if member_status != NODE_STATUS_COMPLETED:
                return False
    return True
