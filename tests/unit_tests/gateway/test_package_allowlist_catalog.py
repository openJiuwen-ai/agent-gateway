"""Expert templates reject hidden Hub assets before writing the repository."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

pytest.importorskip("a2a", reason="a2a-sdk is an optional extra (pyproject a2a group); skip when not installed")

from agent_gateway.enterprise.config.manager_config.core.template import expert_package_template as templates


@pytest.mark.asyncio
async def test_hidden_asset_is_rejected_before_repository_write(monkeypatch):
    repo = AsyncMock()
    repo.get.return_value = None
    monkeypatch.setattr(templates, "require_enterprise_repository", lambda name: repo)
    with pytest.raises(ValueError, match="hidden hub asset"):
        await templates.ExpertPackageTemplateService().create({
            "template_id": "experts", "template_name": "Experts",
            "packages": [{"kind": "agent_template", "asset_id": "794db1bfe8884d6897d62d3584894a59"}],
        })
    repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_template_update_preserves_creation_time_and_deduplicates(monkeypatch):
    repo = AsyncMock()
    repo.get.return_value = {"template_id": "experts", "created_at": "2026-11-01T00:00:00Z"}
    monkeypatch.setattr(templates, "require_enterprise_repository", lambda name: repo)
    result = await templates.ExpertPackageTemplateService().create({
        "template_id": "experts", "template_name": "Experts",
        "packages": [
            {"kind": "agent_template", "asset_id": "one", "version": "v1"},
            {"kind": "agent_template", "asset_id": "one", "version": "v1"},
        ],
    })
    assert result == {"template_id": "experts"}
    repo.create.assert_not_awaited()
    keys, row = repo.update.await_args.args
    assert keys == {"template_id": "experts"}
    assert row["created_at"].year == 2026
    assert row["created_at"].day == 1
    assert row["packages"] == [{"kind": "agent_template", "asset_id": "one", "version": "v1"}]
