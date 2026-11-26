# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""企业版 Skill 上传策略查询。"""

from __future__ import annotations

import pytest

from agent_gateway.enterprise.capabilities.skill_upload import policy


class _Response:
    def __init__(self, status_code: int, body: dict | None = None) -> None:
        self.status_code = status_code
        self._body = body or {}

    def json(self) -> dict:
        return self._body


class _Client:
    def __init__(self, response: _Response | Exception) -> None:
        self.response = response

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get(self, *args, **kwargs):
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def _patch_client(monkeypatch: pytest.MonkeyPatch, response: _Response | Exception) -> None:
    monkeypatch.setattr(policy, "_manager_base", lambda: "http://manager")
    monkeypatch.setattr(
        policy.httpx,
        "AsyncClient",
        lambda **kwargs: _Client(response),
    )


@pytest.mark.asyncio
async def test_policy_allows_enabled_template(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_client(
        monkeypatch,
        _Response(
            200,
            {
                "code": 200,
                "data": {
                    "contexts": [
                        {
                            "jiuwenclaw_id": "cluster_a",
                            "agent_template_id": "tpl_a",
                            "allow_local_skill_upload": True,
                        }
                    ]
                },
            },
        ),
    )

    allowed = await policy.query_allow_local_skill_upload(
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
        agent_template_id="tpl_a",
    )

    assert allowed is True


@pytest.mark.asyncio
async def test_policy_rejects_missing_or_disabled_template(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_client(
        monkeypatch,
        _Response(
            200,
            {
                "data": {
                    "contexts": [
                        {
                            "jiuwenclaw_id": "cluster_a",
                            "agent_template_id": "tpl_b",
                            "allow_local_skill_upload": True,
                        },
                        {
                            "jiuwenclaw_id": "cluster_a",
                            "agent_template_id": "tpl_a",
                            "allow_local_skill_upload": False,
                        },
                    ]
                }
            },
        ),
    )

    assert not await policy.query_allow_local_skill_upload(
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
        agent_template_id="tpl_a",
    )
    assert not await policy.query_allow_local_skill_upload(
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
        agent_template_id="tpl_missing",
    )


@pytest.mark.asyncio
async def test_policy_raises_when_manager_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_client(monkeypatch, policy.httpx.HTTPError("boom"))

    with pytest.raises(policy.SkillUploadPolicyError) as exc:
        await policy.query_allow_local_skill_upload(
            authorization="Bearer token",
            jiuwenclaw_id="cluster_a",
            agent_template_id="tpl_a",
        )

    assert exc.value.code == policy.POLICY_UNAVAILABLE


@pytest.mark.parametrize(
    ("status_code", "expected_code"),
    [
        (401, policy.UNAUTHORIZED),
        (403, policy.FORBIDDEN),
        (404, policy.POLICY_UNAVAILABLE),
    ],
)
@pytest.mark.asyncio
async def test_policy_credentials_and_route_errors_keep_their_codes(
    monkeypatch: pytest.MonkeyPatch, status_code: int, expected_code: str
) -> None:
    """401/403 按凭据/权限失败，404 按不可用失败关闭，都不能伪装成“开关未开启”。"""
    _patch_client(monkeypatch, _Response(status_code))

    with pytest.raises(policy.SkillUploadPolicyError) as exc:
        await policy.query_allow_local_skill_upload(
            authorization="Bearer token",
            jiuwenclaw_id="cluster_a",
            agent_template_id="tpl_a",
        )

    assert exc.value.code == expected_code


@pytest.mark.parametrize(
    ("code", "expected_status"),
    [
        (policy.UNAUTHORIZED, 401),
        (policy.FORBIDDEN, 403),
        (policy.POLICY_UNAVAILABLE, 503),
    ],
)
def test_policy_codes_map_to_http_status(code: str, expected_status: int) -> None:
    from agent_gateway.channel_manager.web.web_http_routes import _envelope_from_res

    _, status = _envelope_from_res(
        {"ok": False, "code": code, "error": "policy rejected"}, "req-policy"
    )

    assert status == expected_status
