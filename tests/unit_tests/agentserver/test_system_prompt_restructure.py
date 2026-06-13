from types import SimpleNamespace
from unittest.mock import patch

import pytest

from openjiuwen.core.foundation.llm import Model
from openjiuwen.core.single_agent.rail.base import AgentCallbackContext
from openjiuwen.harness.prompts.prompt_attachment_manager import (
    PromptAttachmentManager,
)
from openjiuwen.harness.prompts import PromptSection, SystemPromptBuilder

from jiuwenswarm.server.runtime.agent_adapter.interface_deep import JiuWenSwarmDeepAdapter
from jiuwenswarm.agents.harness.common.prompt.prompt_builder import build_agent_identity_prompt
from jiuwenswarm.agents.harness.common.rails import runtime_prompt_rail as _runtime_mod
from jiuwenswarm.agents.harness.common.rails import skill_retrieval_prompt_rail as _skill_retrieval_prompt_mod
from jiuwenswarm.agents.harness.common.rails.runtime_prompt_rail import RuntimePromptRail
from jiuwenswarm.agents.harness.common.rails.skill_retrieval_prompt_rail import SkillRetrievalPromptRail


class _TestableJiuWenSwarmDeepAdapter(JiuWenSwarmDeepAdapter):
    def set_workspace_dir(self, workspace_dir: str) -> None:
        self._workspace_dir = workspace_dir

    def build_configured_subagents(
        self,
        model: Model,
        config: dict,
        config_base: dict | None = None,
    ):
        return self._build_configured_subagents(model, config, config_base)


class _FakeSession:
    def get_session_id(self) -> str:
        return "sess1"


class _FakeAgent:
    def __init__(self, builder: SystemPromptBuilder) -> None:
        self.system_prompt_builder = builder
        self.prompt_attachment_manager = PromptAttachmentManager()


class _FakeAbilityManager:
    def __init__(self) -> None:
        self._items = {
            "list_skill": SimpleNamespace(name="list_skill"),
            "search_skill": SimpleNamespace(name="search_skill"),
        }

    def get(self, name: str):
        return self._items.get(name)

    def remove(self, name: str):
        return self._items.pop(name, None)

    def add(self, ability):
        self._items[ability.name] = ability


class _FakeToolAgent(_FakeAgent):
    def __init__(self, builder: SystemPromptBuilder) -> None:
        super().__init__(builder)
        self.ability_manager = _FakeAbilityManager()


def test_build_agent_identity_prompt_contains_identity_section_only(monkeypatch):
    monkeypatch.setattr(
        "jiuwenswarm.symphony.config.load_symphony_config",
        lambda: SimpleNamespace(enabled=True),
    )
    prompt = build_agent_identity_prompt(language="zh")
    prompt_inline = " ".join(prompt.split())

    assert "# 你的家" in prompt
    assert "## Symphony Routing" in prompt
    assert "`symphony_compose_score`" in prompt
    assert "skill capabilities, skill chaining, skill ordering" in prompt_inline
    assert "use `search_skill` to discover external skills" in prompt_inline
    assert "call `symphony_refresh_score`" in prompt_inline
    assert "present its returned `content` or" in prompt
    assert "# 消息说明" not in prompt


def test_build_agent_identity_prompt_omits_symphony_when_disabled(monkeypatch):
    monkeypatch.setattr(
        "jiuwenswarm.symphony.config.load_symphony_config",
        lambda: SimpleNamespace(enabled=False),
    )

    prompt = build_agent_identity_prompt(language="zh")

    assert "## Symphony Routing" not in prompt
    assert "`symphony_compose_score`" not in prompt


@pytest.mark.asyncio
async def test_runtime_time_section_participates_in_priority_order():
    builder = SystemPromptBuilder(language="cn")
    builder.add_section(PromptSection(name="identity", content={"cn": "identity"}, priority=10))
    builder.add_section(PromptSection(name="tools", content={"cn": "# 可用工具"}, priority=30))
    builder.add_section(PromptSection(name="workspace", content={"cn": "# 工作空间"}, priority=70))

    agent = _FakeAgent(builder)
    runtime_rail = RuntimePromptRail(
        language="cn",
        channel="web"
    )
    runtime_rail.init(agent)

    ctx = AgentCallbackContext(
        agent=agent,
        inputs=None,
        session=_FakeSession(),
        extra={},
    )
    await runtime_rail.before_model_call(ctx)

    prompt = builder.build()
    ordered_markers = [
        "identity",
        "# 可用工具",
        "# 工作空间",
        "# 时间说明",
    ]
    positions = [prompt.index(marker) for marker in ordered_markers]
    assert positions == sorted(positions)
    assert builder.has_section("runtime.model_answer_policy")
    assert not builder.has_section("runtime")
    assert "# 运行时状态" not in prompt


@pytest.mark.asyncio
async def test_runtime_dynamic_sections_go_to_prompt_attachment_when_manager_available(tmp_path, monkeypatch):
    monkeypatch.setattr(_runtime_mod, "get_config_dir", lambda: tmp_path)
    builder = SystemPromptBuilder(language="en")
    agent = _FakeAgent(builder)
    runtime_rail = RuntimePromptRail(language="en", channel="web")
    runtime_rail.init(agent)
    runtime_rail.set_model_name("model-x")
    runtime_rail.set_mode("agent.plan")

    ctx = AgentCallbackContext(
        agent=agent,
        inputs=None,
        session=_FakeSession(),
        extra={},
    )
    await runtime_rail.before_model_call(ctx)

    prompt = builder.build()
    assert "# Time Description" in prompt
    assert "# Runtime State" not in prompt
    assert "# Language" in prompt
    assert "# Browser Tool Policy" in prompt
    assert "# Environment" in prompt

    items = await agent.prompt_attachment_manager.collect_for_session("sess1")
    assert [item.id for item in items] == ["session.sess1.runtime.setting"]
    rendered = agent.prompt_attachment_manager.render(items)
    assert "model-x" in rendered
    assert "Always respond in English" in prompt
    assert "# Browser Tool Policy" in prompt


@pytest.mark.asyncio
async def test_runtime_git_status_attachment_clears_when_git_context_disappears(tmp_path, monkeypatch):
    monkeypatch.setattr(_runtime_mod, "get_config_dir", lambda: tmp_path)
    runtime_state = tmp_path / "runtime_state.yaml"
    runtime_state.write_text(
        "git_branch: feature/test\n"
        "git_status: M file.py\n"
        "git_recent_commits: abc init\n",
        encoding="utf-8",
    )
    builder = SystemPromptBuilder(language="en")
    agent = _FakeAgent(builder)
    runtime_rail = RuntimePromptRail(language="en", channel="web")
    runtime_rail.init(agent)
    ctx = AgentCallbackContext(
        agent=agent,
        inputs=None,
        session=_FakeSession(),
        extra={},
    )

    await runtime_rail.before_model_call(ctx)
    session_items = await agent.prompt_attachment_manager.list_by_filter(session_id="sess1")
    assert [item.id for item in session_items if item.id.endswith(".git_status")] == ["session.sess1.git_status"]

    runtime_state.write_text("git_branch: ''\n", encoding="utf-8")
    await runtime_rail.before_model_call(ctx)
    session_items = await agent.prompt_attachment_manager.list_by_filter(session_id="sess1")
    assert [item.id for item in session_items if item.id.endswith(".git_status")] == []


@pytest.mark.asyncio
async def test_runtime_prompt_uses_runtime_cwd_over_stale_trusted_dir(tmp_path):
    builder = SystemPromptBuilder(language="en")
    agent = _FakeAgent(builder)
    stale_dir = tmp_path / "missing-worktree"
    project_dir = tmp_path / "project"
    current_dir = project_dir / "current"
    extra_dir = tmp_path / "extra"
    current_dir.mkdir(parents=True)
    extra_dir.mkdir()

    runtime_rail = RuntimePromptRail(language="en", channel="tui")
    runtime_rail.init(agent)
    runtime_rail.set_trusted_dirs([str(stale_dir), str(current_dir), str(extra_dir)])
    runtime_rail.set_runtime_paths(cwd=str(current_dir), project_dir=str(project_dir))

    ctx = AgentCallbackContext(
        agent=agent,
        inputs=None,
        session=_FakeSession(),
        extra={},
    )
    await runtime_rail.before_model_call(ctx)

    prompt = builder.build()
    assert "Current project directory" not in prompt
    rendered = agent.prompt_attachment_manager.render(
        await agent.prompt_attachment_manager.list_by_filter(session_id="sess1")
    )
    assert "Current project directory" in rendered
    assert str(current_dir) in rendered
    assert str(stale_dir) not in rendered
    assert str(extra_dir) in rendered


@pytest.mark.asyncio
async def test_runtime_prompt_language_output_prefers_rail_language_over_runtime_state(
    monkeypatch,
    tmp_path,
):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "runtime_state.yaml").write_text(
        "model: test-model\nmode: team.plan\nlanguage: en\nchannel: tui\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(_runtime_mod, "get_config_dir", lambda: config_dir)

    builder = SystemPromptBuilder(language="cn")
    agent = _FakeAgent(builder)
    runtime_rail = RuntimePromptRail(language="cn", channel="tui")
    runtime_rail.init(agent)

    ctx = AgentCallbackContext(
        agent=agent,
        inputs=None,
        session=_FakeSession(),
        extra={},
    )
    await runtime_rail.before_model_call(ctx)

    prompt = builder.build()
    assert "Always respond in Chinese." in prompt
    rendered = agent.prompt_attachment_manager.render(
        await agent.prompt_attachment_manager.list_by_filter(session_id="sess1")
    )
    assert "Always respond in Chinese." not in rendered
    assert "Always respond in English." not in rendered
    assert "Always respond in English." not in prompt
    assert "当前语言：cn" in rendered


@pytest.mark.asyncio
async def test_skill_retrieval_prompt_hides_legacy_list_skill(monkeypatch):
    monkeypatch.setattr(
        _skill_retrieval_prompt_mod,
        "render_skill_retrieval_prompt",
        lambda manager, language: "# Agentic 技能检索\n使用 skill_branch_explore。",
    )
    builder = SystemPromptBuilder(language="cn")
    builder.add_section(PromptSection(name="skills", content={"cn": "旧 list_skill 提示"}, priority=40))
    agent = _FakeToolAgent(builder)
    ctx = AgentCallbackContext(
        agent=agent,
        inputs=SimpleNamespace(
            tools=[
                SimpleNamespace(name="list_skill"),
                SimpleNamespace(name="list_skills"),
                SimpleNamespace(name="skill_branch_explore"),
            ],
        ),
        session=_FakeSession(),
        extra={},
    )

    rail = SkillRetrievalPromptRail()
    rail.init(agent)
    await rail.before_model_call(ctx)

    assert [tool.name for tool in ctx.inputs.tools] == ["skill_branch_explore"]
    assert agent.ability_manager.get("list_skill") is None
    prompt = builder.build()
    assert "旧 list_skill 提示" not in prompt
    assert "Agentic 技能检索" in prompt

    await rail.after_model_call(ctx)

    assert agent.ability_manager.get("list_skill") is not None


def test_resolve_skill_mode_accepts_all_and_auto_list(monkeypatch):
    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.agent_adapter.interface_deep.is_skill_retrieval_enabled",
        lambda: False,
    )
    assert JiuWenSwarmDeepAdapter._resolve_skill_mode({"skill_mode": "all"}) == "all"
    assert JiuWenSwarmDeepAdapter._resolve_skill_mode({"skill_mode": "auto_list"}) == "auto_list"
    assert JiuWenSwarmDeepAdapter._resolve_skill_mode({"skill_mode": "invalid"}) == "all"

    monkeypatch.setattr(
        "jiuwenswarm.server.runtime.agent_adapter.interface_deep.is_skill_retrieval_enabled",
        lambda: True,
    )
    assert JiuWenSwarmDeepAdapter._resolve_skill_mode({"skill_mode": "all"}) == "auto_list"


def test_resolve_enable_task_loop_can_be_called_on_class(monkeypatch):
    monkeypatch.delenv("SKILL_CREATE", raising=False)
    assert (
        JiuWenSwarmDeepAdapter._resolve_enable_task_loop(
            {"enable_task_loop": False},
            {"evolution": {"skill_create": True}},
        )
        is True
    )
    assert (
        JiuWenSwarmDeepAdapter._resolve_enable_task_loop(
            {"enable_task_loop": False},
            {"evolution": {"skill_create": False}},
        )
        is False
    )


# DeepAdapter only builds research_agent + browser_agent (agent mode).
# code_agent / explore_agent belong to CodeAdapter.

def test_deep_adapter_subagents_includes_optional_browser_and_configured_research():
    adapter = _TestableJiuWenSwarmDeepAdapter()
    adapter.set_workspace_dir("/tmp/jiuwenswarm-workspace")
    model = object()
    config = {
        "max_iterations": 9,
        "subagents": {
            "research_agent": {"enabled": True},
            "browser_agent": {"max_iterations": 7},
        },
    }

    with (
        patch.object(adapter, "_resolve_runtime_language", return_value="cn"),
        patch.object(adapter, "_browser_runtime_enabled", return_value=True),
        patch(
            "jiuwenswarm.server.runtime.agent_adapter.interface_deep.build_research_agent_config",
            return_value="research_spec",
        ) as mock_research,
        patch(
            "jiuwenswarm.server.runtime.agent_adapter.interface_deep.build_browser_agent_config",
            return_value="browser_spec",
        ) as mock_browser,
    ):
        subagents, _ = adapter.build_configured_subagents(model, config)

    assert subagents == ["research_spec", "browser_spec"]
    mock_research.assert_called_once_with(
        model,
        workspace="/tmp/jiuwenswarm-workspace",
        language="cn",
        max_iterations=9,
    )
    mock_browser.assert_called_once_with(
        model,
        workspace="/tmp/jiuwenswarm-workspace",
        language="cn",
        max_iterations=7,
    )


def test_deep_adapter_subagents_omits_research_without_explicit_enable():
    adapter = _TestableJiuWenSwarmDeepAdapter()
    adapter.set_workspace_dir("/tmp/jiuwenswarm-workspace")
    model = object()
    config = {"max_iterations": 9}

    with (
        patch.object(adapter, "_resolve_runtime_language", return_value="cn"),
        patch.object(adapter, "_browser_runtime_enabled", return_value=True),
        patch(
            "jiuwenswarm.server.runtime.agent_adapter.interface_deep.build_research_agent_config",
            return_value="research_spec",
        ) as mock_research,
        patch(
            "jiuwenswarm.server.runtime.agent_adapter.interface_deep.build_browser_agent_config",
            return_value="browser_spec",
        ) as mock_browser,
    ):
        subagents, _ = adapter.build_configured_subagents(model, config)

    # DeepAdapter: no research_agent configured, browser enabled
    assert subagents == ["browser_spec"]
    mock_research.assert_not_called()
    mock_browser.assert_called_once()
