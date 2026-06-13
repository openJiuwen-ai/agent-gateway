"""Prompt rail for agentic installed-skill retrieval."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from openjiuwen.core.single_agent.rail.base import AgentCallbackContext
from openjiuwen.harness.prompts import PromptSection
from openjiuwen.harness.prompts.sections import SectionName
from openjiuwen.harness.rails.base import DeepAgentRail

from jiuwenswarm.symphony.agent import render_skill_retrieval_prompt

logger = logging.getLogger(__name__)

_LEGACY_LIST_SKILL_TOOL_NAMES = frozenset({"list_skill", "list_skills"})


class SkillRetrievalPromptRail(DeepAgentRail):
    """Inject lightweight skill-tree retrieval guidance into the system prompt."""

    priority = 101
    SECTION_NAME = "skill_retrieval"
    SECTION_PRIORITY = 41

    def __init__(self, *, manager: Any | None = None) -> None:
        super().__init__()
        self._manager = manager
        self._agent = None
        self.system_prompt_builder = None
        self._hidden_legacy_abilities: dict[str, Any] = {}

    def init(self, agent: Any) -> None:
        self._agent = agent
        self.system_prompt_builder = getattr(agent, "system_prompt_builder", None)

    def uninit(self, agent: Any) -> None:
        self._restore_legacy_list_skill(agent)
        if self.system_prompt_builder is not None:
            self.system_prompt_builder.remove_section(self.SECTION_NAME)
        self.system_prompt_builder = None
        self._agent = None

    async def before_model_call(self, ctx: AgentCallbackContext) -> None:
        agent = getattr(ctx, "agent", None)
        if agent is not None:
            self._agent = agent
            if self.system_prompt_builder is None:
                self.system_prompt_builder = getattr(agent, "system_prompt_builder", None)

        self._hide_legacy_list_skill()
        self._filter_legacy_list_skill_from_model_inputs(ctx)

        if self.system_prompt_builder is None:
            return

        self.system_prompt_builder.remove_section(SectionName.SKILLS)
        language = getattr(self.system_prompt_builder, "language", "cn") or "cn"
        try:
            content = await asyncio.to_thread(
                render_skill_retrieval_prompt,
                self._manager,
                language=language,
            )
        except Exception as exc:
            logger.warning("[SkillRetrievalPromptRail] render failed: %s", exc)
            content = ""

        if not content.strip():
            self.system_prompt_builder.remove_section(self.SECTION_NAME)
            return

        self.system_prompt_builder.add_section(
            PromptSection(
                name=self.SECTION_NAME,
                content={language: content},
                priority=self.SECTION_PRIORITY,
            )
        )

    async def after_model_call(self, ctx: AgentCallbackContext) -> None:
        self._restore_legacy_list_skill(getattr(ctx, "agent", None))

    async def on_model_exception(self, ctx: AgentCallbackContext) -> None:
        self._restore_legacy_list_skill(getattr(ctx, "agent", None))

    def _hide_legacy_list_skill(self) -> None:
        ability_manager = getattr(self._agent, "ability_manager", None)
        if ability_manager is None:
            return
        get_ability = getattr(ability_manager, "get", None)
        remove_ability = getattr(ability_manager, "remove", None)
        if not callable(get_ability) or not callable(remove_ability):
            return

        for name in _LEGACY_LIST_SKILL_TOOL_NAMES:
            if name in self._hidden_legacy_abilities:
                continue
            card = get_ability(name)
            if card is None:
                continue
            removed = remove_ability(name)
            if removed is not None:
                self._hidden_legacy_abilities[name] = removed

    def _restore_legacy_list_skill(self, agent: Any | None = None) -> None:
        if agent is not None:
            self._agent = agent
        ability_manager = getattr(self._agent, "ability_manager", None)
        if ability_manager is None or not self._hidden_legacy_abilities:
            return
        get_ability = getattr(ability_manager, "get", None)
        add_ability = getattr(ability_manager, "add", None)
        if not callable(get_ability) or not callable(add_ability):
            return

        for name, card in list(self._hidden_legacy_abilities.items()):
            if get_ability(name) is None:
                add_ability(card)
            self._hidden_legacy_abilities.pop(name, None)

    @staticmethod
    def _filter_legacy_list_skill_from_model_inputs(ctx: AgentCallbackContext) -> None:
        inputs = getattr(ctx, "inputs", None)
        tools = getattr(inputs, "tools", None)
        if not tools:
            return

        filtered = []
        for tool in tools:
            name = SkillRetrievalPromptRail._model_tool_name(tool)
            if name not in _LEGACY_LIST_SKILL_TOOL_NAMES:
                filtered.append(tool)
        if len(filtered) != len(tools):
            inputs.tools = filtered

    @staticmethod
    def _model_tool_name(tool: Any) -> str:
        if isinstance(tool, dict):
            function = tool.get("function")
            if isinstance(function, dict):
                return str(function.get("name", "") or "")
            return str(tool.get("name", "") or "")
        return str(getattr(tool, "name", "") or "")


__all__ = ["SkillRetrievalPromptRail"]
