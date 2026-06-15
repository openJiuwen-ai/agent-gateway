from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace

from jiuwenswarm.symphony.skill_retrieval.build_coordinator import (
    cancel_skill_index_build,
    start_skill_index_build,
)
from jiuwenswarm.symphony.skill_retrieval.config import (
    BuildSettings,
    LLMSettings,
    RetrieveSettings,
    SkillRetrievalSettings,
)
from jiuwenswarm.symphony.skill_retrieval.index_service import SkillIndexService, expected_index_fingerprint
from jiuwenswarm.symphony.skill_retrieval.inventory import scan_skill_inventory


def _write_skill(root: Path, dirname: str, *, name: str | None = None, description: str = "desc") -> None:
    skill_dir = root / dirname
    skill_dir.mkdir(parents=True)
    skill_name = name or dirname
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: {skill_name}\ndescription: {description}\n---\n\nBody\n",
        encoding="utf-8",
    )


class _InventoryManager:
    def __init__(self, skills_dir: Path) -> None:
        self._skills_dir = skills_dir

    @staticmethod
    def get_local_skills() -> list[dict]:
        return []

    @staticmethod
    def get_installed_plugins() -> list[dict]:
        return [
            {"name": "disabled-plugin", "enabled": False, "skills": ["disabled-plugin"]},
            {"name": "enabled-plugin", "enabled": True, "skills": ["enabled-plugin"]},
        ]

    @staticmethod
    def get_skill_enabled(name: str) -> bool:
        return name != "disabled-skill"


def test_scan_skill_inventory_includes_all_installed_skills(tmp_path: Path) -> None:
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    _write_skill(skills_dir, "disabled-plugin")
    _write_skill(skills_dir, "disabled-skill")
    _write_skill(skills_dir, "enabled-plugin")

    inventory = scan_skill_inventory(_InventoryManager(skills_dir))

    assert [item.name for item in inventory.items] == [
        "disabled-plugin",
        "disabled-skill",
        "enabled-plugin",
    ]


def test_build_index_with_no_skills_clears_stale_index_and_records_failure(monkeypatch, tmp_path: Path) -> None:
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    artifact_root = tmp_path / "artifact"
    index_dir = artifact_root / "index"
    index_dir.mkdir(parents=True)
    (index_dir / "tree_index.yaml").write_text("nodes: []\n", encoding="utf-8")
    (index_dir / "catalog.jsonl").write_text("", encoding="utf-8")
    (index_dir / "manifest.json").write_text(json.dumps({"item_paths": ["/old/skill"]}), encoding="utf-8")
    (artifact_root / "state.json").write_text(
        json.dumps({"fingerprint": "old", "indexed_count": 1}),
        encoding="utf-8",
    )
    settings = SkillRetrievalSettings(
        enabled=True,
        artifact_root=artifact_root,
        llm=LLMSettings(model="", api_key="", base_url=""),
        build=BuildSettings(),
        retrieve=RetrieveSettings(),
    )
    monkeypatch.setattr(
        "jiuwenswarm.symphony.skill_retrieval.index_service.load_settings",
        lambda: settings,
    )

    result = SkillIndexService(SimpleNamespace(_skills_dir=skills_dir)).build_index(force=True)

    assert result["success"] is False
    assert not index_dir.exists()
    state = json.loads((artifact_root / "state.json").read_text(encoding="utf-8"))
    assert state["build"]["status"] == "failed"
    assert "No installed skills" in state["build"]["error"]


def test_cancel_without_running_build_does_not_write_cancel_state(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact"
    settings = SkillRetrievalSettings(
        enabled=True,
        artifact_root=artifact_root,
        llm=LLMSettings(model="model", api_key="key", base_url=""),
        build=BuildSettings(),
        retrieve=RetrieveSettings(),
    )
    monkeypatch.setattr(
        "jiuwenswarm.symphony.skill_retrieval.build_coordinator.load_settings",
        lambda: settings,
    )

    result = cancel_skill_index_build(SimpleNamespace(_skills_dir=tmp_path / "skills"))

    assert result["success"] is False
    assert result["build_status"] == "idle"
    assert not (artifact_root / "state.json").exists()


def test_background_build_marks_shared_state(monkeypatch, tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact"
    settings = SkillRetrievalSettings(
        enabled=True,
        artifact_root=artifact_root,
        llm=LLMSettings(model="model", api_key="key", base_url=""),
        build=BuildSettings(),
        retrieve=RetrieveSettings(),
    )
    monkeypatch.setattr(
        "jiuwenswarm.symphony.skill_retrieval.build_coordinator.load_settings",
        lambda: settings,
    )
    monkeypatch.setattr(
        "jiuwenswarm.symphony.skill_retrieval.index_service.load_settings",
        lambda: settings,
    )
    release = threading.Event()
    started = threading.Event()

    def fake_build_index(self, *, force=False, cancel_check=None, source="manual"):
        started.set()
        release.wait(timeout=1)
        return {"success": True, "result": "# ok"}

    monkeypatch.setattr(SkillIndexService, "build_index", fake_build_index)
    manager = SimpleNamespace(_skills_dir=tmp_path / "skills")

    result = start_skill_index_build(manager, force=True, source="web")
    assert started.wait(timeout=1)

    assert result["success"] is True
    assert result["background"] is True
    state = json.loads((artifact_root / "state.json").read_text(encoding="utf-8"))
    assert state["build"]["status"] == "running"
    assert state["build"]["stage"] == "queued"

    cancel_result = cancel_skill_index_build(manager)
    release.set()
    assert cancel_result["success"] is True


def test_force_build_bypasses_fresh_index_reuse(monkeypatch, tmp_path: Path) -> None:
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    _write_skill(skills_dir, "enabled-skill")
    manager = SimpleNamespace(_skills_dir=skills_dir)
    artifact_root = tmp_path / "artifact"
    settings = SkillRetrievalSettings(
        enabled=True,
        artifact_root=artifact_root,
        llm=LLMSettings(model="model", api_key="key", base_url=""),
        build=BuildSettings(),
        retrieve=RetrieveSettings(),
    )
    inventory = scan_skill_inventory(manager)
    expected = expected_index_fingerprint(inventory, settings)
    index_dir = artifact_root / "index"
    index_dir.mkdir(parents=True)
    (index_dir / "tree_index.yaml").write_text("nodes: []\n", encoding="utf-8")
    (index_dir / "catalog.jsonl").write_text("", encoding="utf-8")
    (index_dir / "manifest.json").write_text(
        json.dumps({"item_paths": inventory.item_paths}),
        encoding="utf-8",
    )
    (artifact_root / "state.json").write_text(
        json.dumps({"fingerprint": expected, "indexed_count": inventory.count}),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "jiuwenswarm.symphony.skill_retrieval.index_service.load_settings",
        lambda: settings,
    )
    calls: list[str] = []

    def fake_run_dispatch_build(*, settings, inventory, output_dir):
        calls.append("build")
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "tree_index.yaml").write_text("nodes: []\n", encoding="utf-8")
        (output_dir / "catalog.jsonl").write_text("", encoding="utf-8")
        (output_dir / "manifest.json").write_text(
            json.dumps({"item_paths": inventory.item_paths}),
            encoding="utf-8",
        )

    monkeypatch.setattr(SkillIndexService, "_run_dispatch_build", staticmethod(fake_run_dispatch_build))

    result = SkillIndexService(manager).build_index(force=True)

    assert result["success"] is True
    assert calls == ["build"]
    state = json.loads((artifact_root / "state.json").read_text(encoding="utf-8"))
    assert state["build"]["status"] == "success"


def test_missing_llm_config_records_failure(monkeypatch, tmp_path: Path) -> None:
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    _write_skill(skills_dir, "enabled-skill")
    artifact_root = tmp_path / "artifact"
    settings = SkillRetrievalSettings(
        enabled=True,
        artifact_root=artifact_root,
        llm=LLMSettings(model="", api_key="", base_url=""),
        build=BuildSettings(),
        retrieve=RetrieveSettings(),
    )
    monkeypatch.setattr(
        "jiuwenswarm.symphony.skill_retrieval.index_service.load_settings",
        lambda: settings,
    )

    result = SkillIndexService(SimpleNamespace(_skills_dir=skills_dir)).build_index(force=True)

    assert result["success"] is False
    assert "requires a model and API key" in result["result"]
    state = json.loads((artifact_root / "state.json").read_text(encoding="utf-8"))
    assert state["build"]["status"] == "failed"
    assert state["build"]["stage"] == "llm_config"


def test_tree_rejects_stale_manifest_and_uses_requested_language(monkeypatch, tmp_path: Path) -> None:
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    _write_skill(skills_dir, "current-skill")
    artifact_root = tmp_path / "artifact"
    index_dir = artifact_root / "index"
    index_dir.mkdir(parents=True)
    (index_dir / "tree_index.yaml").write_text(
        "nodes:\n"
        "  - cid: old\n"
        "    type: leaf\n"
        "    worker_id: old-skill\n",
        encoding="utf-8",
    )
    (index_dir / "catalog.jsonl").write_text("", encoding="utf-8")
    (index_dir / "manifest.json").write_text(json.dumps({"item_paths": ["/old/skill"]}), encoding="utf-8")
    (artifact_root / "state.json").write_text(json.dumps({"fingerprint": "old"}), encoding="utf-8")
    settings = SkillRetrievalSettings(
        enabled=True,
        artifact_root=artifact_root,
        llm=LLMSettings(model="model", api_key="key", base_url=""),
        build=BuildSettings(),
        retrieve=RetrieveSettings(),
    )
    monkeypatch.setattr(
        "jiuwenswarm.symphony.skill_retrieval.index_service.load_settings",
        lambda: settings,
    )

    zh = SkillIndexService(SimpleNamespace(_skills_dir=skills_dir)).tree(language="zh")
    en = SkillIndexService(SimpleNamespace(_skills_dir=skills_dir)).tree(language="en")

    assert zh["success"] is False
    assert zh["nodes"] == []
    assert "技能索引树" in zh["result"]
    assert en["success"] is False
    assert en["nodes"] == []
    assert "Skill Index Tree" in en["result"]


def test_build_error_normalizes_non_streaming_remote_model_error(monkeypatch, tmp_path: Path) -> None:
    skills_dir = tmp_path / "skills"
    skills_dir.mkdir()
    _write_skill(skills_dir, "enabled-skill")
    artifact_root = tmp_path / "artifact"
    settings = SkillRetrievalSettings(
        enabled=True,
        artifact_root=artifact_root,
        llm=LLMSettings(model="model", api_key="key", base_url=""),
        build=BuildSettings(),
        retrieve=RetrieveSettings(),
    )
    monkeypatch.setattr(
        "jiuwenswarm.symphony.skill_retrieval.index_service.load_settings",
        lambda: settings,
    )

    def raise_remote_error(*, settings, inventory, output_dir):
        raise RuntimeError("set to false for non-streaming calls")

    monkeypatch.setattr(SkillIndexService, "_run_dispatch_build", staticmethod(raise_remote_error))

    result = SkillIndexService(SimpleNamespace(_skills_dir=skills_dir)).build_index(force=True)

    assert result["success"] is False
    assert "non-streaming LLM calls" in result["result"]
    state = json.loads((artifact_root / "state.json").read_text(encoding="utf-8"))
    assert "non-streaming LLM calls" in state["build"]["error"]
