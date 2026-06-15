from __future__ import annotations

from typing import Any


def build_skill_index(
    manager: Any | None = None,
    *,
    force: bool = False,
    source: str = "tool",
) -> dict[str, Any]:
    from jiuwenswarm.server.runtime.skill.skill_manager import SkillManager

    from .build_coordinator import start_skill_index_build

    resolved_manager = manager or SkillManager()
    payload = start_skill_index_build(resolved_manager, force=force, source=source)
    return _tool_payload(payload)


def cancel_skill_index_build(manager: Any | None = None) -> dict[str, Any]:
    from jiuwenswarm.server.runtime.skill.skill_manager import SkillManager

    from .build_coordinator import cancel_skill_index_build as cancel_build

    resolved_manager = manager or SkillManager()
    payload = cancel_build(resolved_manager)
    return _tool_payload(payload)


def retrieve_skills(query: str, manager: Any | None = None) -> dict[str, Any]:
    from jiuwenswarm.server.runtime.skill.skill_manager import SkillManager

    from .retrieve_service import SkillRetrieveService

    resolved_manager = manager or SkillManager()
    payload = SkillRetrieveService(resolved_manager).retrieve(query)
    return _tool_payload(payload)


def get_skill_retrieval_status(manager: Any | None = None) -> dict[str, Any]:
    from jiuwenswarm.server.runtime.skill.skill_manager import SkillManager

    from .index_service import SkillIndexService

    resolved_manager = manager or SkillManager()
    return SkillIndexService(resolved_manager).status()


def get_skill_retrieval_tree(manager: Any | None = None, *, language: str = "cn") -> dict[str, Any]:
    from jiuwenswarm.server.runtime.skill.skill_manager import SkillManager

    from .index_service import SkillIndexService

    resolved_manager = manager or SkillManager()
    return SkillIndexService(resolved_manager).tree(language=language)


def _tool_payload(payload: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "success": bool(payload.get("success")),
        "result": str(payload.get("result") or ""),
    }
    skill_tree = payload.get("skill_tree")
    if isinstance(skill_tree, dict):
        out["skill_tree"] = skill_tree
    return out
