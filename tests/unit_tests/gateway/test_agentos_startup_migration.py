"""AgentOS 启动期旧共享配置迁移。"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from agent_gateway.common import paths
from agent_gateway.common.agentos_migration import migrate_legacy_agentos_config


def _render_placeholders(value):
    if isinstance(value, dict):
        return {key: _render_placeholders(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_render_placeholders(item) for item in value]
    if isinstance(value, str) and re.search(r"<<[A-Z0-9_]+>>", value):
        return "x"
    return value


def _legacy_from_template() -> dict:
    template = yaml.safe_load(
        (paths.get_bundled_resources_dir() / "config.agentos.yaml").read_text(
            encoding="utf-8"
        )
    )
    return _render_placeholders(template)


def _reset_path_cache(monkeypatch) -> None:
    monkeypatch.setattr(paths, "_initialized", False)
    monkeypatch.setattr(paths, "_config_dir", None)
    monkeypatch.setattr(paths, "_workspace_dir", None)
    monkeypatch.setattr(paths, "_root_dir", None)


def _prepare_paths(monkeypatch, tmp_path: Path):
    workspace = tmp_path / "workspace"
    gateway_dir = tmp_path / "infra" / "config"
    monkeypatch.setenv("JIUWENSWARM_RUNTIME_PROFILE", "agentos")
    monkeypatch.setenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", str(gateway_dir))
    monkeypatch.setattr(paths, "get_user_workspace_dir", lambda: workspace)
    monkeypatch.setattr(paths, "get_agent_workspace_dir", lambda: tmp_path / "agent-workspace")
    monkeypatch.setattr(paths, "get_agent_home_dir", lambda: tmp_path / "agent-home")
    _reset_path_cache(monkeypatch)
    return workspace, gateway_dir / "config.yaml"


def _prepare_same_path(monkeypatch, tmp_path: Path) -> Path:
    workspace = tmp_path / "workspace"
    monkeypatch.setenv("JIUWENSWARM_RUNTIME_PROFILE", "agentos")
    monkeypatch.delenv("JIUWENSWARM_GATEWAY_CONFIG_DIR", raising=False)
    monkeypatch.setattr(paths, "get_user_workspace_dir", lambda: workspace)
    monkeypatch.setattr(paths, "get_agent_workspace_dir", lambda: tmp_path / "agent-workspace")
    monkeypatch.setattr(paths, "get_agent_home_dir", lambda: tmp_path / "agent-home")
    _reset_path_cache(monkeypatch)
    return workspace / "config" / "config.yaml"


def test_split_premise_uses_gateway_config_dir(
    monkeypatch, tmp_path: Path
) -> None:
    workspace, target = _prepare_paths(monkeypatch, tmp_path)

    assert paths.get_config_file() == target
    assert target != workspace / "config" / "config.yaml"


def test_startup_migrates_legacy_shared_config(monkeypatch, tmp_path: Path) -> None:
    workspace, target = _prepare_paths(monkeypatch, tmp_path)
    legacy = workspace / "config" / "config.yaml"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(yaml.safe_dump(_legacy_from_template()), encoding="utf-8")

    paths.prepare_runtime_workspace(cleanup_stale_descs=False)

    migrated = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert migrated["gateway"]["agent_client"]["type"] == "agentos_router"
    assert "<<MASTER_NODE_IP>>" not in target.read_text(encoding="utf-8")


@pytest.mark.parametrize("broken", ["", "gateway:\n", "gateway: [\n"])
def test_startup_replaces_unreadable_config_from_legacy(
    monkeypatch, tmp_path: Path, broken: str
) -> None:
    workspace, target = _prepare_paths(monkeypatch, tmp_path)
    legacy = workspace / "config" / "config.yaml"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(yaml.safe_dump(_legacy_from_template()), encoding="utf-8")
    target.parent.mkdir(parents=True)
    target.write_text(broken, encoding="utf-8")

    paths.prepare_runtime_workspace(cleanup_stale_descs=False)

    migrated = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert migrated["gateway"]["agent_client"]["type"] == "agentos_router"


def test_startup_wraps_corrupt_legacy_config(monkeypatch, tmp_path: Path) -> None:
    workspace, _ = _prepare_paths(monkeypatch, tmp_path)
    legacy = workspace / "config" / "config.yaml"
    legacy.parent.mkdir(parents=True)
    legacy.write_text("channels: [\n", encoding="utf-8")

    with pytest.raises(ValueError, match="run the deploy script"):
        paths.prepare_runtime_workspace(cleanup_stale_descs=False)


def test_migration_wraps_corrupt_template(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.yaml"
    legacy.write_text("{}\n", encoding="utf-8")
    template = tmp_path / "template.yaml"
    template.write_text("channels: [\n", encoding="utf-8")

    with pytest.raises(ValueError, match="run the deploy script"):
        migrate_legacy_agentos_config(
            target=tmp_path / "target.yaml",
            legacy=legacy,
            template=template,
        )


def test_startup_keeps_existing_agentos_config(monkeypatch, tmp_path: Path) -> None:
    _, target = _prepare_paths(monkeypatch, tmp_path)
    target.parent.mkdir(parents=True)
    target.write_text(
        yaml.safe_dump({"gateway": {"agent_client": {"type": "agentos_router"}}}),
        encoding="utf-8",
    )

    paths.prepare_runtime_workspace(cleanup_stale_descs=False)

    assert "agentos_router" in target.read_text(encoding="utf-8")


def test_startup_replaces_stock_config_from_legacy(monkeypatch, tmp_path: Path) -> None:
    workspace, target = _prepare_paths(monkeypatch, tmp_path)
    legacy = workspace / "config" / "config.yaml"
    legacy.parent.mkdir(parents=True)
    legacy.write_text(yaml.safe_dump(_legacy_from_template()), encoding="utf-8")
    target.parent.mkdir(parents=True)
    target.write_text(
        yaml.safe_dump({"gateway": {"agent_client": {"type": "websocket"}}}),
        encoding="utf-8",
    )

    paths.prepare_runtime_workspace(cleanup_stale_descs=False)

    migrated = yaml.safe_load(target.read_text(encoding="utf-8"))
    assert migrated["gateway"]["agent_client"]["type"] == "agentos_router"


def test_startup_refuses_to_rewrite_shared_config_in_place(
    monkeypatch, tmp_path: Path
) -> None:
    shared = _prepare_same_path(monkeypatch, tmp_path)
    shared.parent.mkdir(parents=True)
    original = yaml.safe_dump({"gateway": {"agent_client": {"type": "websocket"}}})
    shared.write_text(original, encoding="utf-8")

    with pytest.raises(RuntimeError, match="refusing to rewrite"):
        paths.prepare_runtime_workspace(cleanup_stale_descs=False)

    assert shared.read_text(encoding="utf-8") == original
    assert not list(shared.parent.glob("config.yaml.bak.*"))


def test_startup_fails_fast_when_shared_config_is_missing(
    monkeypatch, tmp_path: Path
) -> None:
    _prepare_same_path(monkeypatch, tmp_path)

    with pytest.raises(RuntimeError, match="resolves to the same file"):
        paths.prepare_runtime_workspace(cleanup_stale_descs=False)


def test_startup_keeps_shared_agentos_config_in_place(
    monkeypatch, tmp_path: Path
) -> None:
    shared = _prepare_same_path(monkeypatch, tmp_path)
    shared.parent.mkdir(parents=True)
    shared.write_text(
        yaml.safe_dump({"gateway": {"agent_client": {"type": "agentos_router"}}}),
        encoding="utf-8",
    )

    paths.prepare_runtime_workspace(cleanup_stale_descs=False)

    assert "agentos_router" in shared.read_text(encoding="utf-8")


def test_migration_rejects_unresolved_template_placeholders(tmp_path: Path) -> None:
    legacy = tmp_path / "legacy.yaml"
    legacy.write_text(
        yaml.safe_dump({"gateway": {"auth": {}}}),
        encoding="utf-8",
    )
    template = tmp_path / "template.yaml"
    template.write_text(
        yaml.safe_dump(
            {
                "gateway": {
                    "agent_client": {"type": "agentos_router"},
                    "auth": {"token": "<<GATEWAY_TOKEN>>"},
                }
            }
        ),
        encoding="utf-8",
    )
    target = tmp_path / "target.yaml"
    target.write_text("original\n", encoding="utf-8")

    with pytest.raises(ValueError, match="template placeholders"):
        migrate_legacy_agentos_config(
            target=target,
            legacy=legacy,
            template=template,
            force=True,
        )

    assert target.read_text(encoding="utf-8") == "original\n"
    assert not list(tmp_path.glob("target.yaml.bak.*"))


def test_startup_fails_fast_without_any_config(monkeypatch, tmp_path: Path) -> None:
    _prepare_paths(monkeypatch, tmp_path)

    with pytest.raises(RuntimeError, match="Gateway config is missing"):
        paths.prepare_runtime_workspace(cleanup_stale_descs=False)
