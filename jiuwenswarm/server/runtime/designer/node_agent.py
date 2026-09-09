# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Per-node DeepAgent host for Designer: AgentTemplate persona + graph tools."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol

from jiuwenswarm.common.schema.designer_graph import (
    DESIGNER_AGENT_GROUP_NAME,
    AssetRef,
    DesignerExecutionGraph,
    DesignerGraphNode,
    data_predecessors,
    node_agent_template,
    node_role,
)
from jiuwenswarm.common.utils import get_agent_workspace_dir
from jiuwenswarm.server.runtime.designer.handlers.common import (
    file_output_ref,
    graph_prompt,
    write_workspace_text,
)
from jiuwenswarm.server.runtime.designer.handlers.types import NodeExecutionContext, NodeResult

logger = logging.getLogger(__name__)

NodeAgentRunner = Callable[
    [DesignerGraphNode, NodeExecutionContext, "DesignerGraphToolkit"],
    Awaitable[NodeResult],
]


class DesignerNodeSpawner(Protocol):
    async def spawn_node_agent(self, run_id: str, node_id: str) -> str: ...

    def apply_agent_graph_patch(self, graph_id: str, patch: dict[str, Any]) -> DesignerExecutionGraph: ...

    def load_graph_snapshot(self, graph_id: str, run_id: str) -> dict[str, Any]: ...


def designer_agent_group_dir() -> Path:
    """Return the built-in Designer AgentGroup directory."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        candidate = (
            parent
            / "resources"
            / "agent"
            / "workspace"
            / "plugins"
            / "agent_groups"
            / DESIGNER_AGENT_GROUP_NAME
        )
        if candidate.is_dir() and (candidate / "manifest.json").is_file():
            return candidate
    raise FileNotFoundError("built-in designer AgentGroup not found")


def parse_agent_template_ref(ref: str) -> tuple[str | None, str]:
    value = str(ref or "").strip()
    if "/" in value:
        group, _, member = value.partition("/")
        group = group.strip()
        member = member.strip()
        if group and member:
            return group, member
    return None, value


def flatten_template_prompt(template: Any, *, language: str = "cn") -> str:
    from jiuwenswarm.agents.swarm.assembly import (
        _render_prompt_text,
        _select_prompt_content,
    )

    sections = list(getattr(template, "prompt_sections", None) or [])
    rendered: list[str] = []
    workspace = str(get_agent_workspace_dir())
    for section in sorted(sections, key=lambda item: getattr(item, "priority", 0)):
        content = getattr(section, "content", None) or {}
        if not isinstance(content, dict):
            continue
        params = {
            "language": language,
            "workspace": workspace,
            **dict(getattr(section, "render_params", None) or {}),
        }
        text = _render_prompt_text(
            _select_prompt_content(content, language),
            params,
        ).strip()
        if text:
            rendered.append(text)
    return "\n\n".join(rendered)


_DESIGNER_MEMBERS = frozenset(
    {"leader", "character", "scene", "storyboard", "frame", "clip"}
)


def _resolve_group_package_dir(group_name: str) -> Path | None:
    if group_name == DESIGNER_AGENT_GROUP_NAME:
        try:
            return designer_agent_group_dir()
        except FileNotFoundError:
            pass
    from jiuwenswarm.server.runtime.extension_package_manager import (
        get_equipment_resources_agent_groups_dir,
        resolve_agent_group_dir,
    )

    resources = get_equipment_resources_agent_groups_dir()
    if resources is not None:
        candidate = resources / group_name
        if candidate.is_dir() and (candidate / "manifest.json").is_file():
            return candidate
    try:
        return resolve_agent_group_dir(group_name)
    except ValueError:
        return None


def load_node_agent_template(node: DesignerGraphNode) -> Any | None:
    """Load the AgentTemplateSpec for a node, or None if unavailable."""
    ref = node_agent_template(node)
    if not ref:
        return None
    group_name, member_id = parse_agent_template_ref(ref)
    if group_name is None and member_id in _DESIGNER_MEMBERS:
        group_name = DESIGNER_AGENT_GROUP_NAME
    if group_name:
        from jiuwenswarm.agents.swarm.agent_group import load_agent_group_package

        package_dir = _resolve_group_package_dir(group_name)
        if package_dir is None:
            return None
        try:
            return load_agent_group_package(package_dir).get(member_id)
        except (OSError, ValueError):
            logger.exception("failed to load AgentGroup %s", group_name)
            return None
    from openjiuwen.harness.resources import load_agent_template_package
    from jiuwenswarm.server.runtime.extension_package_manager import resolve_agent_template_dir

    try:
        package_dir = resolve_agent_template_dir(member_id)
    except ValueError:
        return None
    try:
        return load_agent_template_package(package_dir / "manifest.json")
    except (OSError, ValueError):
        logger.exception("failed to load AgentTemplate %s", member_id)
        return None


def build_node_user_query(node: DesignerGraphNode, ctx: NodeExecutionContext) -> str:
    graph = ctx.graph
    incoming = data_predecessors(graph)
    preds = incoming.get(node["id"], [])
    suggested = [
        edge.get("target")
        for edge in graph.get("edges") or []
        if edge.get("source") == node["id"]
    ]
    snapshot = {
        "node_id": node["id"],
        "role": node_role(node),
        "label": node.get("label"),
        "type": node.get("type"),
        "prompt": graph_prompt(graph, node),
        "agent_template": node_agent_template(node),
        "upstream_node_ids": preds,
        "suggested_next_node_ids": [item for item in suggested if isinstance(item, str)],
        "upstream_outputs": _upstream_outputs(ctx, preds),
    }
    return (
        "执行当前设计节点。先看 JSON 上下文，再用工具读图/改图/拉起同伴，最后 "
        "designer_node_complete。\n\n"
        f"```json\n{json.dumps(snapshot, ensure_ascii=False, indent=2)}\n```"
    )


def _upstream_outputs(ctx: NodeExecutionContext, pred_ids: list[str]) -> list[dict[str, Any]]:
    run = ctx.run or {}
    states = run.get("node_states") or {}
    items: list[dict[str, Any]] = []
    for node_id in pred_ids:
        state = states.get(node_id) or {}
        ref = state.get("output_ref") or {}
        items.append(
            {
                "node_id": node_id,
                "status": state.get("status"),
                "output_ref": ref if isinstance(ref, dict) else {},
            }
        )
    return items


@dataclass
class DesignerGraphToolkit:
    """In-process graph tools bound to the active node/run."""

    spawner: DesignerNodeSpawner
    ctx: NodeExecutionContext
    completed: NodeResult | None = None
    spawned: list[str] = field(default_factory=list)

    def graph_get(self) -> dict[str, Any]:
        graph_id = str(self.ctx.graph.get("graph_id") or "")
        return self.spawner.load_graph_snapshot(graph_id, self.ctx.run_id)

    def graph_patch(self, patch: dict[str, Any]) -> dict[str, Any]:
        graph_id = str(self.ctx.graph.get("graph_id") or "")
        graph = self.spawner.apply_agent_graph_patch(graph_id, patch)
        return {
            "graph_id": graph.get("graph_id"),
            "nodes": [node.get("id") for node in graph.get("nodes") or []],
        }

    async def node_run(self, node_id: str) -> str:
        target = str(node_id or "").strip()
        if not target:
            return "node_id is required"
        if target == self.ctx.node_id:
            return "cannot run the current node"
        result = await self.spawner.spawn_node_agent(self.ctx.run_id, target)
        self.spawned.append(target)
        return result

    def node_complete(
        self,
        *,
        uri: str = "",
        kind: str = "",
        mime_type: str = "",
        label: str = "",
        extra_uris: list[str] | None = None,
        text: str = "",
    ) -> str:
        refs: list[AssetRef] = []
        node = _node_from_ctx(self.ctx)
        if text.strip() and not uri.strip():
            path = write_workspace_text(
                f"designer_agent_{self.ctx.run_id}_{self.ctx.node_id}",
                text,
            )
            refs.append(
                file_output_ref(
                    path,
                    kind=kind.strip() or str(node.get("type") or "text"),
                    mime_type=mime_type.strip() or "text/markdown",
                )
            )
        elif uri.strip():
            refs.append(
                {
                    "kind": kind.strip() or "file",
                    "uri": uri.strip(),
                    "mime_type": mime_type.strip() or "application/octet-stream",
                    "label": label.strip() or Path(uri).name,
                }
            )
        for extra in extra_uris or []:
            extra_uri = str(extra or "").strip()
            if extra_uri:
                refs.append(
                    {
                        "kind": kind.strip() or "file",
                        "uri": extra_uri,
                        "label": Path(extra_uri).name,
                    }
                )
        if not refs:
            return "complete requires uri or text"
        self.completed = NodeResult(
            output_ref=refs[0],
            output_refs=refs,
            message="node completed",
        )
        return f"completed {refs[0].get('uri')}"


def _node_from_ctx(ctx: NodeExecutionContext) -> DesignerGraphNode:
    for node in ctx.graph.get("nodes") or []:
        if node.get("id") == ctx.node_id:
            return node
    return {"id": ctx.node_id, "type": "text", "label": ctx.node_id}


def build_designer_tools(toolkit: DesignerGraphToolkit) -> list[Any]:
    from openjiuwen.core.foundation.tool import LocalFunction, ToolCard

    def make_tool(name: str, description: str, input_params: dict[str, Any], func: Any) -> Any:
        card = ToolCard(name=name, description=description, input_params=input_params)
        return LocalFunction(card=card, func=func)

    async def graph_get(_inputs: dict[str, Any] | None = None) -> str:
        return json.dumps(toolkit.graph_get(), ensure_ascii=False)

    async def graph_patch(inputs: dict[str, Any] | None = None) -> str:
        payload = inputs or {}
        patch = payload.get("patch")
        if isinstance(patch, str):
            patch = json.loads(patch)
        if not isinstance(patch, dict):
            return "patch must be an object"
        return json.dumps(toolkit.graph_patch(patch), ensure_ascii=False)

    async def node_run(inputs: dict[str, Any] | None = None) -> str:
        payload = inputs or {}
        return await toolkit.node_run(str(payload.get("node_id") or ""))

    async def node_complete(inputs: dict[str, Any] | None = None) -> str:
        payload = inputs or {}
        extra = payload.get("extra_uris") or payload.get("extraUris") or []
        if isinstance(extra, str):
            extra = [item.strip() for item in extra.split(",") if item.strip()]
        return toolkit.node_complete(
            uri=str(payload.get("uri") or ""),
            kind=str(payload.get("kind") or ""),
            mime_type=str(payload.get("mime_type") or payload.get("mimeType") or ""),
            label=str(payload.get("label") or ""),
            extra_uris=list(extra) if isinstance(extra, list) else None,
            text=str(payload.get("text") or ""),
        )

    return [
        make_tool(
            "designer_graph_get",
            "读取当前设计图、运行态和各节点产出。",
            {"type": "object", "properties": {}},
            graph_get,
        ),
        make_tool(
            "designer_graph_patch",
            "按 Designer graph patch 增删节点或边。",
            {
                "type": "object",
                "properties": {"patch": {"type": "object"}},
                "required": ["patch"],
            },
            graph_patch,
        ),
        make_tool(
            "designer_node_run",
            "启动另一个节点上的 Agent。",
            {
                "type": "object",
                "properties": {"node_id": {"type": "string"}},
                "required": ["node_id"],
            },
            node_run,
        ),
        make_tool(
            "designer_node_complete",
            "提交当前节点产物。可给 file uri，或 text 写成工作区 Markdown。",
            {
                "type": "object",
                "properties": {
                    "uri": {"type": "string"},
                    "kind": {"type": "string"},
                    "mime_type": {"type": "string"},
                    "label": {"type": "string"},
                    "text": {"type": "string"},
                    "extra_uris": {"type": "array", "items": {"type": "string"}},
                },
            },
            node_complete,
        ),
    ]


class NodeAgentHost:
    """Run one Designer node as a DeepAgent, with handler fallback."""

    def __init__(
        self,
        spawner: DesignerNodeSpawner,
        *,
        runner: NodeAgentRunner | None = None,
    ) -> None:
        self._spawner = spawner
        self._runner = runner
        self._agents: dict[str, Any] = {}

    def agent_key(self, run_id: str, node_id: str) -> str:
        return f"designer:{run_id}:{node_id}"

    def drop_run(self, run_id: str) -> None:
        prefix = f"designer:{run_id}:"
        for key in [item for item in self._agents if item.startswith(prefix)]:
            self._agents.pop(key, None)

    async def execute(
        self,
        node: DesignerGraphNode,
        ctx: NodeExecutionContext,
    ) -> NodeResult:
        toolkit = DesignerGraphToolkit(self._spawner, ctx)
        if self._runner is not None:
            return await self._runner(node, ctx, toolkit)
        try:
            return await self._run_deep_agent(node, ctx, toolkit)
        except Exception:
            logger.exception(
                "Designer node agent failed; falling back to handler. node=%s",
                ctx.node_id,
            )
            from jiuwenswarm.server.runtime.designer.handlers import get_node_handler

            return await get_node_handler(node).execute(node, ctx)

    async def _run_deep_agent(
        self,
        node: DesignerGraphNode,
        ctx: NodeExecutionContext,
        toolkit: DesignerGraphToolkit,
    ) -> NodeResult:
        from openjiuwen.core.foundation.llm import Model, ModelClientConfig
        from openjiuwen.core.single_agent import AgentCard
        from openjiuwen.harness.factory import create_deep_agent
        from openjiuwen.harness.schema.config import Workspace

        from jiuwenswarm.common.config import get_config, get_default_models

        template = load_node_agent_template(node)
        persona = flatten_template_prompt(template) if template else ""
        system_prompt = persona or "你是设计画布上的节点 Agent。用工具完成任务并提交产物。"
        query = build_node_user_query(node, ctx)
        tools = build_designer_tools(toolkit)

        key = self.agent_key(ctx.run_id, ctx.node_id)
        agent = self._agents.get(key)
        if agent is None:
            entries = get_default_models(get_config())
            entry = next((item for item in entries if item.get("is_default") is True), None)
            if entry is None and entries:
                entry = entries[0]
            client = (entry or {}).get("model_client_config") if isinstance(entry, dict) else {}
            if not isinstance(client, dict):
                raise RuntimeError("no model configured for designer node agent")
            api_key = str(client.get("api_key") or "").strip()
            model_name = str(client.get("model_name") or "").strip()
            if not api_key or not model_name:
                raise RuntimeError("incomplete model config for designer node agent")
            kwargs: dict[str, object] = {
                "api_key": api_key,
                "api_base": str(client.get("api_base") or "").strip(),
                "client_provider": str(client.get("client_provider") or "").strip(),
            }
            profile = str(client.get("endpoint_profile") or "").strip()
            if profile:
                kwargs["endpoint_profile"] = profile
            model = Model(model_client_config=ModelClientConfig(**kwargs))
            workspace_dir = get_agent_workspace_dir()
            workspace_dir.mkdir(parents=True, exist_ok=True)
            card_name = str(
                getattr(getattr(template, "agent_card", None), "name", "")
                or node.get("label")
                or ctx.node_id
            )
            agent = create_deep_agent(
                model=model,
                card=AgentCard(name=card_name, id=key, description=card_name),
                system_prompt=system_prompt,
                tools=tools,
                workspace=Workspace(root_path=str(workspace_dir)),
                enable_task_loop=True,
                max_iterations=12,
                add_general_purpose_agent=False,
            )
            ensure = getattr(agent, "ensure_initialized", None)
            if callable(ensure):
                maybe = ensure()
                if hasattr(maybe, "__await__"):
                    await maybe
            self._agents[key] = agent

        invoke = getattr(agent, "invoke", None)
        if not callable(invoke):
            raise RuntimeError("DeepAgent has no invoke")
        result = invoke({"query": query, "conversation_id": key})
        if hasattr(result, "__await__"):
            result = await result
        if toolkit.completed is not None:
            return toolkit.completed
        text = ""
        if isinstance(result, dict):
            text = str(result.get("output") or result.get("content") or "")
        elif isinstance(result, str):
            text = result
        else:
            text = str(getattr(result, "content", "") or result)
        if not text.strip():
            raise RuntimeError("node agent returned empty output")
        path = write_workspace_text(f"designer_agent_{ctx.run_id}_{ctx.node_id}", text)
        return NodeResult(
            output_ref=file_output_ref(
                path,
                kind=str(node.get("type") or "text"),
                mime_type="text/markdown",
            ),
            message="node agent text fallback",
        )
