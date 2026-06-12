import asyncio
from types import SimpleNamespace

from jiuwenswarm.agents.harness.common.tools.symphony_toolkits import (
    SymphonyToolkit,
)
from jiuwenswarm.extensions.registry import ExtensionRegistry


class _CallbackFramework:
    @staticmethod
    def register_sync(*args, **kwargs):
        return None

    async def trigger(self, *args, **kwargs):
        return None


def setup_function():
    ExtensionRegistry.reset_instance()


def teardown_function():
    ExtensionRegistry.reset_instance()


def test_toolkit_calls_rpc_handler():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    seen = {}

    async def handler(params, request=None):
        seen.update(params)
        return {"success": True, "params": params}

    registry.register_rpc_handler("symphony.plan", handler)
    registry.register_rpc_handler(
        "symphony.score_status",
        lambda _params, request=None: {"success": True, "exists": True, "stale": False},
    )

    result = asyncio.run(SymphonyToolkit().plan("use installed skills"))

    assert result["success"] is True
    assert result["params"] == {"query": "use installed skills"}
    assert result["score_status"] == {"success": True, "exists": True, "stale": False}
    assert "## Symphony score" in result["content"]
    assert "Status: `fresh`" in result["content"]
    assert result["summary"] == result["content"]
    assert seen["query"] == "use installed skills"


def test_toolkit_passes_fast_mode_to_rpc_handler():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    seen = {}

    async def handler(params, request=None):
        del request
        seen.update(params)
        return {"success": True, "params": params}

    registry.register_rpc_handler("symphony.plan", handler)
    registry.register_rpc_handler(
        "symphony.score_status",
        lambda _params, request=None: {"success": True, "exists": True, "stale": False},
    )

    result = asyncio.run(SymphonyToolkit().plan("use installed skills", mode="fast"))

    assert result["success"] is True
    assert result["params"] == {"query": "use installed skills", "mode": "fast"}
    assert seen["mode"] == "fast"


def test_toolkit_reports_missing_handler():
    ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )

    result = asyncio.run(SymphonyToolkit().score_status())

    assert result["success"] is False
    assert "symphony.score_status" in result["detail"]


def test_toolkit_plan_refreshes_stale_score_before_planning():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    calls = []

    async def score_status(params, request=None):
        del params, request
        calls.append("score_status")
        return {"success": True, "exists": True, "stale": True}

    async def build_score(params, request=None):
        del params, request
        calls.append("build_score")
        return {"success": True, "updated": True}

    async def plan(params, request=None):
        del request
        calls.append("plan")
        return {"success": True, "params": params}

    registry.register_rpc_handler("symphony.score_status", score_status)
    registry.register_rpc_handler("symphony.build_score", build_score)
    registry.register_rpc_handler("symphony.plan", plan)

    result = asyncio.run(SymphonyToolkit().plan("compose installed skills"))

    assert calls == ["score_status", "build_score", "plan"]
    assert result["success"] is True
    assert result["score_build"] == {"success": True, "updated": True}
    assert "Status: `stale`" in result["content"]
    assert "Update: `succeeded`" in result["content"]


def test_toolkit_plan_succeeds_for_fresh_score():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    registry.register_rpc_handler(
        "symphony.score_status",
        lambda _params, request=None: {"success": True, "exists": True, "stale": False},
    )
    registry.register_rpc_handler(
        "symphony.plan",
        lambda params, request=None: {"success": True, "params": params},
    )

    result = asyncio.run(SymphonyToolkit().plan("compose installed skills"))

    assert result["success"] is True


def test_toolkit_plan_succeeds_after_refreshing_stale_score():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    registry.register_rpc_handler(
        "symphony.score_status",
        lambda _params, request=None: {"success": True, "exists": True, "stale": True},
    )
    registry.register_rpc_handler(
        "symphony.build_score",
        lambda _params, request=None: {"success": True, "updated": True},
    )
    registry.register_rpc_handler(
        "symphony.plan",
        lambda params, request=None: {"success": True, "params": params},
    )

    result = asyncio.run(SymphonyToolkit().plan("compose installed skills"))

    assert result["success"] is True


def test_toolkit_plan_returns_failure_when_score_status_fails():
    ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )

    result = asyncio.run(SymphonyToolkit().plan("compose installed skills"))

    assert result["success"] is False
    assert "symphony.score_status" in result["detail"]


def test_toolkit_plan_preserves_plan_markdown_after_score_summary():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    registry.register_rpc_handler(
        "symphony.score_status",
        lambda _params, request=None: {
            "success": True,
            "exists": True,
            "stale": False,
            "reason": "up to date",
        },
    )
    registry.register_rpc_handler(
        "symphony.plan",
        lambda _params, request=None: {
            "success": True,
            "presentation": {
                "markdown": "## Recommended Plan\n\nUse skill A, then skill B.",
                "mermaid": "flowchart LR\n  A --> B",
            },
        },
    )

    result = asyncio.run(SymphonyToolkit().plan("compose installed skills"))

    assert result["direct_display"] is True
    assert result["display_format"] == "markdown"
    assert result["content"].startswith("## Symphony score")
    assert "Detail: up to date" in result["content"]
    assert "## Recommended Plan" in result["content"]
    assert result["mermaid"] == "flowchart LR\n  A --> B"
    assert result["markdown"] == result["content"]
    assert result["summary"] == result["content"]


def test_toolkit_complete_plan_defaults_to_force_finish_display():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    registry.register_rpc_handler(
        "symphony.score_status",
        lambda _params, request=None: {"success": True, "exists": True, "stale": False},
    )
    registry.register_rpc_handler(
        "symphony.plan",
        lambda _params, request=None: {
            "success": True,
            "status": "ready",
            "recommended_plans": [
                {
                    "status": "ready",
                    "steps": [{"skill_id": "skill-a"}],
                    "missing_inputs": [],
                }
            ],
            "execution_graph": {"nodes": [{"id": "skill-a"}]},
            "presentation": {"markdown": "## Plan", "mermaid": "flowchart LR\n  A"},
        },
    )

    result = asyncio.run(SymphonyToolkit().plan("compose skill plan"))

    assert result["continue_after_display"] is False
    assert "followup_action" not in result


def test_toolkit_no_plan_continues_for_skill_discovery():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    registry.register_rpc_handler(
        "symphony.score_status",
        lambda _params, request=None: {"success": True, "exists": True, "stale": False},
    )
    registry.register_rpc_handler(
        "symphony.plan",
        lambda _params, request=None: {
            "success": True,
            "status": "no_plan",
            "recommended_plans": [{"status": "no_plan", "steps": []}],
            "execution_graph": {"nodes": []},
            "presentation": {"markdown": "## No plan", "mermaid": "flowchart LR\n  none"},
        },
    )

    result = asyncio.run(SymphonyToolkit().plan("compose missing skill plan"))

    assert result["continue_after_display"] is True
    assert result["followup_action"] == "external_skill_discovery"


def test_toolkit_needs_input_does_not_continue_for_skill_discovery():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    registry.register_rpc_handler(
        "symphony.score_status",
        lambda _params, request=None: {"success": True, "exists": True, "stale": False},
    )
    registry.register_rpc_handler(
        "symphony.plan",
        lambda _params, request=None: {
            "success": True,
            "status": "needs_input",
            "recommended_plans": [
                {
                    "status": "needs_input",
                    "steps": [],
                    "missing_inputs": [{"name": "brief", "type": "text"}],
                }
            ],
            "execution_graph": {"nodes": []},
            "presentation": {"markdown": "## Need input", "mermaid": "flowchart LR\n  none"},
        },
    )

    result = asyncio.run(SymphonyToolkit().plan("compose skill plan"))

    assert result["continue_after_display"] is False
    assert "followup_action" not in result


def test_toolkit_plan_stops_when_score_status_fails():
    registry = ExtensionRegistry.create_instance(
        callback_framework=_CallbackFramework(),
        config={},
        logger=object(),
    )
    calls = []

    async def plan(params, request=None):
        del params, request
        calls.append("plan")
        return {"success": True}

    registry.register_rpc_handler("symphony.plan", plan)

    result = asyncio.run(SymphonyToolkit().plan("compose installed skills"))

    assert result["success"] is False
    assert "symphony.score_status failed" in result["detail"]
    assert calls == []


def test_toolkit_get_tools_respects_symphony_enabled(monkeypatch):
    monkeypatch.setattr(
        "jiuwenswarm.agents.harness.common.tools.symphony_toolkits.load_symphony_config",
        lambda: SimpleNamespace(enabled=False),
    )

    assert SymphonyToolkit().get_tools() == []

    monkeypatch.setattr(
        "jiuwenswarm.agents.harness.common.tools.symphony_toolkits.load_symphony_config",
        lambda: SimpleNamespace(enabled=True),
    )

    tool_names = [tool.card.name for tool in SymphonyToolkit().get_tools()]
    assert "symphony_compose_score" in tool_names
    compose_tool = next(
        tool for tool in SymphonyToolkit().get_tools()
        if tool.card.name == "symphony_compose_score"
    )
    assert compose_tool.card.input_params["properties"]["mode"]["enum"] == ["fast"]
    description = compose_tool.card.description
    assert "skill capabilities, skill chaining, skill ordering" in description
    assert "search_skill to discover external skills" in description
    assert "install_skill" in description
    assert "symphony_refresh_score" in description
    assert "currently installed skills" not in description
    assert "currently installed skills" not in (
        compose_tool.card.input_params["properties"]["query"]["description"]
    )
