"""Agent-facing toolkit for installed skill retrieval."""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from openjiuwen.core.foundation.tool import LocalFunction, Tool, ToolCard

from jiuwenswarm.server.runtime.skill.skill_manager import SkillManager
from jiuwenswarm.symphony.agent import (
    build_skill_index,
    is_agentic_retrieval_enabled,
    skill_branch_explore,
    skill_branch_peek,
)


def is_skill_retrieval_enabled() -> bool:
    return is_agentic_retrieval_enabled()


class SkillRetrievalToolkit:
    """Expose agentic installed-skill tree retrieval to agents."""

    def __init__(self, manager: SkillManager) -> None:
        self._manager = manager

    async def skill_index_build(self) -> dict[str, Any]:
        return await asyncio.to_thread(build_skill_index, self._manager)

    async def skill_branch_peek(self, node_ids: list[str]) -> dict[str, Any]:
        return await asyncio.to_thread(skill_branch_peek, node_ids, self._manager)

    async def skill_branch_explore(self, node_ids: list[str]) -> dict[str, Any]:
        return await asyncio.to_thread(skill_branch_explore, node_ids, self._manager)

    def get_tools(self) -> list[Tool]:
        def make_tool(name: str, description: str, input_params: dict, func: Callable[..., Any]) -> Tool:
            card = ToolCard(
                id=name,
                name=name,
                description=description,
                input_params=input_params,
            )
            return LocalFunction(card=card, func=func)

        return [
            make_tool(
                name="skill_index_build",
                description=(
                    "Build or refresh the local tree index for installed enabled skills. "
                    "Do not call this proactively. First call skill_branch_explore or skill_branch_peek; "
                    "call skill_index_build only if those tools return a failure result that explicitly "
                    "says the index is missing or stale and instructs you to build it."
                ),
                input_params={
                    "type": "object",
                    "properties": {},
                    "required": [],
                },
                func=self.skill_index_build,
            ),
            make_tool(
                name="skill_branch_explore",
                description=(
                    "Primary skill-directory browsing tool for installed skills. "
                    "Explore one or more branch node ids to disclose the next visible skill-tree boundary. "
                    "Use this directly with first-level category ids already shown in the system prompt, "
                    "or with branch ids returned by previous tool results. Do not call this with ROOT; "
                    "ROOT is already summarized in the system prompt. "
                    "When the result contains a 'skills' section, those entries are installed skills, not branch ids; "
                    "shortlist by Name and Description, then read a returned SKILL.md only after the skill "
                    "looks likely useful. "
                    "Do not read every skill or explore skill names as branch ids. "
                    "This is not an execution tool or a full execution plan. If the index is missing or stale, "
                    "follow the returned instruction to call skill_index_build once, then retry."
                ),
                input_params={
                    "type": "object",
                    "properties": {
                        "node_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                "Branch node ids to explore. Use first-level category ids from the system "
                                "prompt or branch ids returned by previous retrieval results. Do not use ['ROOT']."
                            ),
                        },
                    },
                    "required": ["node_ids"],
                },
                func=self.skill_branch_explore,
            ),
            make_tool(
                name="skill_branch_peek",
                description=(
                    "Lightweight skill-directory preview tool for installed skills. "
                    "Use this only when you are unsure whether a branch is worth exploring. "
                    "It returns child branch summaries and coverage information; it does not return full leaf skill "
                    "details, execute skills, or plan the task. Use skill_branch_explore to disclose actual "
                    "skill entries when a branch looks relevant. If the index is missing or stale, follow the "
                    "returned instruction to call skill_index_build once, then retry."
                ),
                input_params={
                    "type": "object",
                    "properties": {
                        "node_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": (
                                "Branch node ids to preview. Use ['ROOT'] only when you need to rediscover "
                                "top-level branch summaries."
                            ),
                        },
                    },
                    "required": ["node_ids"],
                },
                func=self.skill_branch_peek,
            ),
        ]
