# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""缺路径占位符时 REST 组装失败应映射为 BAD_REQUEST（HTTP 400），而非 500。"""

from __future__ import annotations

import pytest

from agent_gateway.channel_manager.web.web_http_routes import _envelope_from_res
from agent_gateway.extensions.runtime_management_extension.pod_rest_map import (
    RestAssemblyError,
    assemble_rest_request,
)
from e2a.models import E2AEnvelope


def _frame_bad_request(message: str) -> dict:
    payload = {"error": message, "code": "BAD_REQUEST"}
    return {
        "ok": False,
        "error": message,
        "code": "BAD_REQUEST",
        "payload": payload,
    }


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"name": None, "origin": "sds-fixture:sds-contract-skill"},
        {"name": ""},
    ],
)
def test_skills_uninstall_missing_name_raises_rest_assembly_error(params: dict) -> None:
    with pytest.raises(RestAssemblyError, match=r"\['name'\].*/skills/\{name\}"):
        assemble_rest_request(
            E2AEnvelope(
                request_id="req-1",
                channel="web",
                method="skills.uninstall",
                params=params,
            ),
            base_url="http://agent",
        )


@pytest.mark.parametrize(
    "name",
    [
        "../sds-contract-skill",
        "a/b",
        r"a\b",
        "..",
        ".",
    ],
)
def test_skills_uninstall_path_separator_name_raises_rest_assembly_error(
    name: str,
) -> None:
    """含路径分隔符/相对段的 name 不得编进 DELETE /skills/{name}（#168）。"""
    with pytest.raises(RestAssemblyError, match=r"非法路径片段"):
        assemble_rest_request(
            E2AEnvelope(
                request_id="req-1",
                channel="web",
                method="skills.uninstall",
                params={"name": name, "origin": "sds-fixture:sds-contract-skill"},
            ),
            base_url="http://agent",
        )


@pytest.mark.parametrize(
    "name",
    [
        [],
        ["sds-contract-skill"],
        {},
        True,
    ],
)
def test_skills_uninstall_non_string_name_raises_rest_assembly_error(name) -> None:
    """name=[] 等非标量不得编成 DELETE /skills/%5B%5D（#169）。"""
    with pytest.raises(RestAssemblyError, match=r"类型非法"):
        assemble_rest_request(
            E2AEnvelope(
                request_id="req-1",
                channel="web",
                method="skills.uninstall",
                params={"name": name, "origin": "sds-fixture:sds-contract-skill"},
            ),
            base_url="http://agent",
        )


def test_skills_uninstall_name_array_maps_to_http_400() -> None:
    envelope = E2AEnvelope(
        request_id="req-1",
        channel="web",
        method="skills.uninstall",
        params={"name": [], "origin": "sds-fixture:sds-contract-skill"},
    )
    with pytest.raises(RestAssemblyError, match=r"类型非法") as raised:
        assemble_rest_request(envelope, base_url="http://agent")
    body, status = _envelope_from_res(
        _frame_bad_request(str(raised.value)),
        "req-1",
        rpc_method="skills.uninstall",
    )
    assert status == 400
    assert body["error"]["code"] == "BAD_REQUEST"
    assert "list" in body["error"]["message"]


def test_skills_uninstall_path_separator_maps_to_http_400() -> None:
    envelope = E2AEnvelope(
        request_id="req-1",
        channel="web",
        method="skills.uninstall",
        params={"name": "../sds-contract-skill"},
    )
    with pytest.raises(RestAssemblyError, match=r"非法路径片段") as raised:
        assemble_rest_request(envelope, base_url="http://agent")
    body, status = _envelope_from_res(
        _frame_bad_request(str(raised.value)),
        "req-1",
        rpc_method="skills.uninstall",
    )
    assert status == 400
    assert body["error"]["code"] == "BAD_REQUEST"


def test_rest_assembly_error_maps_to_http_400() -> None:
    body, status = _envelope_from_res(
        _frame_bad_request("REST 路径缺占位符 ['name']: /skills/{name}"),
        "req-1",
        rpc_method="skills.uninstall",
    )
    assert status == 400
    assert body["ok"] is False
    assert body["error"]["code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_http_client_converts_rest_assembly_error(monkeypatch) -> None:
    from agent_gateway.extensions.runtime_management_extension import pod_http_client as mod

    client = object.__new__(mod.HttpSseAgentServerClient)
    monkeypatch.setattr(client, "_resolve_api_root", lambda _base=None: "http://agent/api/v1")

    async def _boom(envelope, api_root):
        raise RestAssemblyError("REST 路径缺占位符 ['name']: /skills/{name}")

    monkeypatch.setattr(mod, "_assemble_with_allowlist", _boom)
    response = await client.send_request(
        E2AEnvelope(
            request_id="req-uninstall",
            channel="web",
            method="skills.uninstall",
            params={"name": None},
        ),
        base_url="http://agent",
    )
    assert response.ok is False
    assert response.payload["code"] == "BAD_REQUEST"
