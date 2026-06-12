"""Agent-facing Symphony tools."""

from __future__ import annotations

import asyncio
import inspect
import logging
from typing import Any, Callable

from openjiuwen.core.foundation.tool import LocalFunction, Tool, ToolCard

from jiuwenswarm.extensions.registry import ExtensionRegistry
from jiuwenswarm.symphony.config import load_symphony_config

logger = logging.getLogger(__name__)


class SymphonyToolkit:
    """Expose Symphony extension RPC methods as model-callable tools."""

    @staticmethod
    def _resolve_timeout_s(default_s: float = 1800.0) -> float:
        return default_s

    async def _call_rpc(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        logger.info(
            "[SymphonyToolkit] calling RPC: method=%s params_keys=%s",
            method,
            sorted(params),
        )
        try:
            registry = ExtensionRegistry.get_instance()
        except RuntimeError as exc:
            return {
                "success": False,
                "detail": f"Symphony extension RPC unavailable: {method}: {exc}",
            }

        handler = registry.get_rpc_handler(method)
        if handler is None:
            return {
                "success": False,
                "detail": f"Symphony extension RPC unavailable: {method}: handler not registered",
            }

        timeout_s = self._resolve_timeout_s()
        try:
            result = handler(params, request=None)
            payload = await asyncio.wait_for(
                result if inspect.isawaitable(result) else _return_value(result),
                timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            return {"success": False, "detail": f"{method}: timeout after {timeout_s}s"}
        except Exception as exc:  # noqa: BLE001
            logger.exception("Symphony RPC failed: %s", method)
            return {"success": False, "detail": f"{method}: {exc}"}

        return payload if isinstance(payload, dict) else {"success": True, "result": payload}

    async def score_status(self) -> dict[str, Any]:
        return await self._call_rpc("symphony.score_status", {})

    async def refresh_score(self) -> dict[str, Any]:
        return await self._call_rpc("symphony.build_score", {})

    @staticmethod
    def _score_needs_build(status: dict[str, Any]) -> bool:
        if not bool(status.get("exists", False)):
            return True
        if bool(status.get("stale", False)):
            return True
        for key in ("added_count", "changed_count", "removed_count"):
            try:
                if int(status.get(key) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                continue
        return False

    @staticmethod
    def _score_summary_markdown(
        status: dict[str, Any],
        update: dict[str, Any] | None,
    ) -> str:
        lines = ["## Symphony score", ""]
        if status.get("success"):
            state = "stale" if status.get("stale") else "fresh"
            if not status.get("exists"):
                state = "missing"
            reason = str(status.get("reason") or "").strip()
            lines.append(f"- Status: `{state}`")
            if reason:
                lines.append(f"- Detail: {reason}")
            for key, label in (
                ("added_count", "Added"),
                ("changed_count", "Changed"),
                ("removed_count", "Removed"),
            ):
                value = status.get(key)
                if value not in (None, ""):
                    lines.append(f"- {label}: `{value}`")
        else:
            detail = str(status.get("detail") or "score status failed").strip()
            lines.append("- Status: `failed`")
            lines.append(f"- Detail: {detail}")
        if update is not None:
            update_state = "succeeded" if update.get("success") else "failed"
            lines.append(f"- Update: `{update_state}`")
            detail = str(update.get("detail") or update.get("reason") or "").strip()
            if detail:
                lines.append(f"- Update detail: {detail}")
        else:
            lines.append("- Update: `not required`")
        return "\n".join(lines)

    @classmethod
    def _attach_display_payload(
        cls,
        payload: dict[str, Any],
        status: dict[str, Any],
        update: dict[str, Any] | None,
    ) -> None:
        score_markdown = cls._score_summary_markdown(status, update)
        presentation = payload.get("presentation")
        presentation_markdown = (
            presentation.get("markdown") if isinstance(presentation, dict) else None
        )
        presentation_mermaid = (
            presentation.get("mermaid") if isinstance(presentation, dict) else None
        )
        rendered = (
            payload.get("content")
            or payload.get("markdown")
            or presentation_markdown
        )
        mermaid = payload.get("mermaid") or presentation_mermaid
        if isinstance(mermaid, str) and mermaid.strip():
            payload.setdefault("mermaid", mermaid.strip())
        if not isinstance(rendered, str):
            rendered = ""
        rendered = rendered.strip()
        combined = f"{score_markdown}\n\n{rendered}".strip() if rendered else score_markdown
        payload["content"] = combined
        payload["markdown"] = combined
        payload["summary"] = combined
        payload.setdefault("display_format", "markdown")
        payload.setdefault("direct_display", True)

    @staticmethod
    def _primary_plan(payload: dict[str, Any]) -> dict[str, Any]:
        for key in ("recommended_plans", "plans"):
            plans = payload.get(key)
            if not isinstance(plans, list):
                continue
            for plan in plans:
                if isinstance(plan, dict):
                    return plan
        return {}

    @classmethod
    def _planning_payload(cls, payload: dict[str, Any]) -> dict[str, Any]:
        result = payload.get("result")
        return result if isinstance(result, dict) else payload

    @classmethod
    def _needs_external_skill_discovery(cls, payload: dict[str, Any]) -> bool:
        planning_payload = cls._planning_payload(payload)
        plan = cls._primary_plan(planning_payload)
        status = str(
            plan.get("status")
            or planning_payload.get("status")
            or payload.get("status")
            or ""
        ).strip().lower()
        missing_inputs = (
            plan.get("missing_inputs")
            or planning_payload.get("missing_inputs")
            or []
        )
        if status == "needs_input" or missing_inputs:
            return False
        if status == "no_plan":
            return True

        steps = plan.get("steps") if isinstance(plan, dict) else []
        execution_graph = planning_payload.get("execution_graph")
        if not isinstance(execution_graph, dict):
            execution_graph = payload.get("execution_graph")
        graph_nodes = (
            execution_graph.get("nodes")
            if isinstance(execution_graph, dict)
            else []
        )
        return not steps and not graph_nodes

    @classmethod
    def _attach_followup_control(cls, payload: dict[str, Any]) -> None:
        if cls._needs_external_skill_discovery(payload):
            payload["continue_after_display"] = True
            payload["followup_action"] = "external_skill_discovery"
            return
        payload.setdefault("continue_after_display", False)

    @staticmethod
    def _failure_detail(payload: dict[str, Any], fallback: str) -> str:
        return str(
            payload.get("detail")
            or payload.get("reason")
            or payload.get("error")
            or fallback
        ).strip()

    async def plan(self, query: str, mode: str | None = None) -> dict[str, Any]:
        status = await self.score_status()
        if not status.get("success"):
            detail = self._failure_detail(status, "symphony.score_status failed")
            return {
                "success": False,
                "detail": f"symphony.score_status failed before planning: {detail}",
                "score_status": status,
            }
        update: dict[str, Any] | None = None
        if status.get("success") and self._score_needs_build(status):
            update = await self.refresh_score()
            if not update.get("success"):
                detail = self._failure_detail(update, "symphony.build_score failed")
                return {
                    "success": False,
                    "detail": f"symphony.build_score failed before planning: {detail}",
                    "score_status": status,
                    "score_build": update,
                }

        params: dict[str, Any] = {
            "query": str(query or "").strip(),
        }
        mode_text = str(mode or "").strip()
        if mode_text:
            params["mode"] = mode_text
        payload = await self._call_rpc("symphony.plan", params)
        if isinstance(payload, dict):
            payload.setdefault("score_status", status)
            if update is not None:
                payload.setdefault("score_build", update)
            self._attach_followup_control(payload)
            self._attach_display_payload(payload, status, update)
        return payload

    @staticmethod
    def is_enabled() -> bool:
        try:
            return bool(load_symphony_config().enabled)
        except Exception as exc:  # noqa: BLE001
            logger.warning("failed to load Symphony config; tools disabled: %s", exc)
            return False

    def get_tools(self) -> list[Tool]:
        if not self.is_enabled():
            return []

        def make_tool(
            name: str,
            description: str,
            input_params: dict[str, Any],
            func: Callable[..., Any],
        ) -> Tool:
            card = ToolCard(
                id=name,
                name=name,
                description=description,
                input_params=input_params,
            )
            return LocalFunction(card=card, func=func)

        return [
            make_tool(
                "symphony_read_score",
                "Read whether the Symphony score exists or is stale before composing skill execution.",
                {"type": "object", "properties": {}},
                self.score_status,
            ),
            make_tool(
                "symphony_refresh_score",
                "Extract installed skill features and refresh the Symphony score.",
                {"type": "object", "properties": {}},
                self.refresh_score,
            ),
            make_tool(
                "symphony_compose_score",
                (
                    "MUST call before answering when the user says to use skill(s) "
                    "or 技能, or when skill capabilities, skill chaining, skill ordering, "
                    "or a specialized toolchain could help complete the task. Do not manually "
                    "list skill names or choose a skill chain before calling this tool. This is "
                    "the Symphony entrypoint: it reads the score, refreshes stale or missing "
                    "scores, then composes the skill execution graph. If no suitable candidates "
                    "or a missing capability is reported, use search_skill to discover external "
                    "skills; when installing a discovered skill is appropriate, call install_skill, "
                    "then call symphony_refresh_score and retry this tool with the original query. "
                    "After it returns, present its content/markdown result directly to the user; "
                    "do not call individual skill tools just to manually recreate the plan. "
                    "Skip only clearly ordinary tasks that do not benefit from skill capabilities."
                ),
                {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The original user task to complete with skill capabilities.",
                        },
                        "mode": {
                            "type": "string",
                            "enum": ["fast"],
                            "description": (
                                "Optional planning mode. The current Symphony runtime "
                                "supports fast planning only."
                            ),
                        },
                    },
                    "required": ["query"],
                },
                self.plan,
            ),
        ]


async def _return_value(value: Any) -> Any:
    return value
