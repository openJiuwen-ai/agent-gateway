# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""技能详情只读 RPC 的 Gateway mapped 路由与企业白名单。"""

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

import pytest

import agent_gateway

_GATEWAY_ROOT = Path(agent_gateway.__file__).resolve().parent
_INVOKE = _GATEWAY_ROOT / "channel_manager" / "web" / "invoke.py"
_MAPPED = _GATEWAY_ROOT / "channel_manager" / "web" / "http_routes" / "mapped.py"

_SKILL_DETAIL_READ_METHODS: dict[str, tuple[str, str]] = {
    "skills.files.list": ("GET", "/skills/{name}/files"),
    "skills.files.get": ("GET", "/skills/{name}/files/content"),
    "skills.versions.list": ("GET", "/skills/{name}/versions"),
}


def _load_mapped():
    spec = importlib.util.spec_from_file_location("skills_detail_mapped_routes", _MAPPED)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _enterprise_skill_allowed() -> set[str]:
    tree = ast.parse(_INVOKE.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "_ENTERPRISE_SKILL_ALLOWED" for t in node.targets):
            continue
        assert isinstance(node.value, ast.Call)
        assert node.value.args and isinstance(node.value.args[0], (ast.Set, ast.Tuple, ast.List))
        out: set[str] = set()
        for elt in node.value.args[0].elts:
            if isinstance(elt, ast.Constant) and isinstance(elt.value, str):
                out.add(elt.value)
        return out
    raise AssertionError("_ENTERPRISE_SKILL_ALLOWED not found in invoke.py")


@pytest.mark.parametrize("method,expected", sorted(_SKILL_DETAIL_READ_METHODS.items()))
def test_skill_detail_read_methods_have_gateway_mapped_routes(
    method: str, expected: tuple[str, str]
) -> None:
    mapped = _load_mapped()
    verb, path = expected
    matched = [
        route
        for route in mapped.MAPPED_ROUTES
        if route.rpc_method == method and route.http_method == verb and route.path == path
    ]
    assert matched, f"mapped.py missing {verb} {path} → {method}"


@pytest.mark.parametrize("method", sorted(_SKILL_DETAIL_READ_METHODS))
def test_skill_detail_read_methods_allowed_in_enterprise(method: str) -> None:
    assert method in _enterprise_skill_allowed()
