from __future__ import annotations

import ast
from pathlib import Path


def _load_skill_state_helpers():
    source_path = Path("jiuwenswarm/server/runtime/skill/skill_manager.py")
    module = ast.parse(source_path.read_text(encoding="utf-8"))
    target_names = {
        "normalize_skill_configs",
        "normalize_local_skills",
        "get_registered_skill_names",
        "get_skill_enabled",
        "set_skill_enabled",
        "list_disabled_skills",
        "list_execution_disabled_skills",
    }
    selected_nodes = []
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in target_names:
            selected_nodes.append(node)

    helper_module = ast.Module(body=selected_nodes, type_ignores=[])
    ast.fix_missing_locations(helper_module)
    namespace = {"Any": object}
    exec(compile(helper_module, str(source_path), "exec"), namespace)
    return namespace


_HELPERS = _load_skill_state_helpers()
normalize_skill_configs = _HELPERS["normalize_skill_configs"]
normalize_local_skills = _HELPERS["normalize_local_skills"]
get_registered_skill_names = _HELPERS["get_registered_skill_names"]
get_skill_enabled = _HELPERS["get_skill_enabled"]
set_skill_enabled = _HELPERS["set_skill_enabled"]
list_disabled_skills = _HELPERS["list_disabled_skills"]
list_execution_disabled_skills = _HELPERS["list_execution_disabled_skills"]


def test_normalize_skill_configs_defaults_enabled_true():
    normalized = normalize_skill_configs(
        {
            "plugin-skill": {},
            "local-skill": {"enabled": False},
            " ": {"enabled": False},
            123: {"enabled": False},
        }
    )

    assert normalized == {
        "plugin-skill": {"enabled": True},
        "local-skill": {"enabled": False},
    }


def test_normalize_skill_configs_treats_missing_enabled_as_true():
    normalized = normalize_skill_configs(
        {
            "builtin-candidate": {"note": "no enabled field"},
        }
    )

    assert normalized["builtin-candidate"]["enabled"] is True


def test_registered_skill_names_covers_installed_plugins_and_local_skills():
    state = {
        "installed_plugins": [
            {"name": "builtin-skill"},
            {"name": "market-skill"},
        ],
        "local_skills": [
            {"name": "imported-skill"},
        ],
    }

    assert get_registered_skill_names(state) == {
        "builtin-skill",
        "market-skill",
        "imported-skill",
    }


def test_normalize_local_skills_drops_stale_records():
    local_skills = [
        {"name": "kept-skill", "origin": "C:\\keep", "source": "local"},
        {"name": "stale-skill", "origin": "C:\\stale", "source": "local"},
        {"name": "", "origin": "C:\\bad", "source": "local"},
    ]

    normalized = normalize_local_skills(local_skills, {"kept-skill"})

    assert normalized == [
        {"name": "kept-skill", "origin": "C:\\keep", "source": "local"},
    ]


def test_set_skill_enabled_supports_plugin_and_local_skill_records():
    state = {
        "installed_plugins": [{"name": "builtin-skill"}],
        "local_skills": [{"name": "imported-skill"}],
    }

    set_skill_enabled(state, "builtin-skill", False)
    set_skill_enabled(state, "imported-skill", False)

    assert get_skill_enabled(state, "builtin-skill") is False
    assert get_skill_enabled(state, "imported-skill") is False
    assert list_disabled_skills(state) == ["builtin-skill", "imported-skill"]


def test_set_skill_enabled_also_supports_uninstalled_skill():
    state = {
        "installed_plugins": [],
        "local_skills": [],
    }

    set_skill_enabled(state, "builtin-candidate", False)

    assert get_skill_enabled(state, "builtin-candidate") is False
    assert list_disabled_skills(state) == ["builtin-candidate"]
    assert list_execution_disabled_skills(state) == []


def test_get_skill_enabled_defaults_true_for_legacy_state():
    legacy_state = {
        "installed_plugins": [{"name": "legacy-plugin"}],
        "local_skills": [{"name": "legacy-local"}],
    }

    assert get_skill_enabled(legacy_state, "legacy-plugin") is True
    assert get_skill_enabled(legacy_state, "legacy-local") is True


def test_skill_manager_source_wires_builtin_candidates_enabled():
    source = Path(
        "jiuwenswarm/server/runtime/skill/skill_manager.py"
    ).read_text(encoding="utf-8")

    assert 'meta["installed"] = False' in source
    assert 'meta["source"] = "builtin"' in source
    assert "self._apply_enabled_config(meta, meta.get(\"name\", \"\"))" in source
    assert 'state["local_skills"] = normalize_local_skills(' in source
