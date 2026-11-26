# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""Contract tests for enterprise template identifier validation."""

from types import ModuleType

import pytest

from agent_gateway.enterprise.config.manager_config.core.template import (
    agent_template,
    embedding_template,
    execution_guard_template,
    expert_package_template,
    extension_config_template,
    mcp_template,
    model_template,
    permissions_template,
    skill_prebuilt_template,
)


TEMPLATE_MODULES = (
    agent_template,
    embedding_template,
    execution_guard_template,
    expert_package_template,
    extension_config_template,
    mcp_template,
    model_template,
    permissions_template,
    skill_prebuilt_template,
)


@pytest.mark.parametrize("module", TEMPLATE_MODULES, ids=lambda module: module.__name__)
def test_template_modules_trim_ids(module: ModuleType) -> None:
    assert module.normalize_template_id("  tpl-1  ") == "tpl-1"


@pytest.mark.parametrize("module", TEMPLATE_MODULES, ids=lambda module: module.__name__)
@pytest.mark.parametrize("value", [None, "", "   "])
def test_template_modules_reject_missing_ids(module: ModuleType, value: object) -> None:
    with pytest.raises(ValueError, match="template_id is required"):
        module.normalize_template_id(value)


@pytest.mark.parametrize("module", TEMPLATE_MODULES, ids=lambda module: module.__name__)
def test_template_modules_enforce_length_boundary(module: ModuleType) -> None:
    assert module.normalize_template_id("x" * 100) == "x" * 100
    with pytest.raises(ValueError, match="at most 100 characters"):
        module.normalize_template_id("x" * 101)


def test_template_modules_use_the_shared_normalizer() -> None:
    from agent_gateway.enterprise.config.manager_config.core.template.template_id import (
        normalize_template_id,
    )

    assert all(
        module.normalize_template_id is normalize_template_id
        for module in TEMPLATE_MODULES
    )
