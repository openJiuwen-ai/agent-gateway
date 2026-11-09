# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""企业版 Skill 上传窄门禁：拒绝旧上传、复核 finalize、回写安装结果。"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest

import agent_gateway.enterprise as enterprise_pkg
from agent_gateway.enterprise.capabilities.skill_upload import client as upload_client
from agent_gateway.enterprise.capabilities.skill_upload import guard

UPLOAD_ID = "a" * 32


def _params() -> dict[str, str]:
    return {
        "order_num": f"apr_{UPLOAD_ID}",
        "upload_id": UPLOAD_ID,
        "expected_digest": "sha256:" + "0" * 64,
        "skill_name": "forged-name",
        "replace": False,
    }


def _authorization() -> dict[str, str | bool]:
    return {
        "order_num": f"apr_{UPLOAD_ID}",
        "upload_id": UPLOAD_ID,
        "skill_name": "approved-skill",
        "status": "approved",
        "cluster_id": "cluster_a",
        "digest": "sha256:" + "1" * 64,
        "replace": True,
    }


@pytest.mark.asyncio
async def test_non_enterprise_passes_every_method(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: False)

    for method in (
        "skills.import_upload",
        "skills.upload.stage",
        "skills.upload.finalize",
    ):
        denial = await guard.enterprise_skill_upload_denial(
            method=method,
            params=_params(),
            request_id="req-non-enterprise",
            authorization="Bearer token",
            jiuwenclaw_id="cluster_a",
        )
        assert denial is None


@pytest.mark.asyncio
async def test_import_upload_is_retired_in_enterprise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)

    denial = await guard.enterprise_skill_upload_denial(
        method="skills.import_upload",
        params=_params(),
        request_id="req-retired",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )

    assert denial == ("企业版已停止使用该上传方式", guard.RETIRED_CODE)


@pytest.mark.asyncio
async def test_stage_only_needs_enterprise_allowlist(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)

    denial = await guard.enterprise_skill_upload_denial(
        method="skills.upload.stage",
        params={"path": "/tmp/demo.zip"},
        request_id="req-stage",
        authorization=None,
        jiuwenclaw_id="cluster_a",
    )

    assert denial is None


@pytest.mark.asyncio
async def test_finalize_without_bearer_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)

    denial = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-no-token",
        authorization=None,
        jiuwenclaw_id="cluster_a",
    )

    assert denial is not None
    assert denial[1] == upload_client.CHECK_UNAVAILABLE


@pytest.mark.asyncio
async def test_finalize_rejects_pending_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(return_value={**_authorization(), "status": "pending"}),
    )

    denial = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-pending",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )

    assert denial is not None
    assert denial[1] == upload_client.NOT_APPROVED


@pytest.mark.asyncio
async def test_finalize_rejects_upload_id_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(return_value={**_authorization(), "upload_id": "b" * 32}),
    )

    denial = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-mismatch",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )

    assert denial is not None
    assert denial[1] == upload_client.NOT_APPROVED


@pytest.mark.asyncio
async def test_finalize_overwrites_client_fields_and_reports_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(return_value=_authorization()),
    )
    report = AsyncMock()
    monkeypatch.setattr(upload_client, "report_install_result", report)
    params = _params()

    denial = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=params,
        request_id="req-approved",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )

    assert denial is None
    assert params["skill_name"] == "approved-skill"
    assert params["expected_digest"] == "sha256:" + "1" * 64
    assert params["replace"] is True

    await guard.report_pending_install_result(
        "req-approved", ok=True, payload={"success": True}
    )

    report.assert_awaited_once()
    kwargs = report.await_args.kwargs
    assert kwargs["order_num"] == f"apr_{UPLOAD_ID}"
    assert kwargs["upload_id"] == UPLOAD_ID
    assert kwargs["status"] == "installed"
    assert kwargs["detail"] is None


@pytest.mark.asyncio
async def test_transient_failure_is_not_written_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(return_value=_authorization()),
    )
    report = AsyncMock()
    monkeypatch.setattr(upload_client, "report_install_result", report)

    await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-transient",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )
    await guard.report_pending_install_result(
        "req-transient",
        ok=False,
        payload={"code": "SKILL_UPLOAD_CHECK_UNAVAILABLE"},
    )

    report.assert_not_awaited()


@pytest.mark.asyncio
async def test_deterministic_failure_marks_order_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(return_value=_authorization()),
    )
    report = AsyncMock()
    monkeypatch.setattr(upload_client, "report_install_result", report)

    await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-digest-failed",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )
    await guard.report_pending_install_result(
        "req-digest-failed",
        ok=False,
        payload={
            "code": "SKILL_UPLOAD_DIGEST_MISMATCH",
            "error": "暂存内容与审批摘要不一致",
        },
    )

    report.assert_awaited_once()
    kwargs = report.await_args.kwargs
    assert kwargs["status"] == "failed"
    assert kwargs["detail"] == "暂存内容与审批摘要不一致"


@pytest.mark.asyncio
async def test_install_result_is_reported_only_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(return_value=_authorization()),
    )

    report = AsyncMock()
    monkeypatch.setattr(upload_client, "report_install_result", report)

    await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-once",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )
    await guard.report_pending_install_result(
        "req-once", ok=True, payload={"success": True}
    )
    await guard.report_pending_install_result(
        "req-once", ok=True, payload={"success": True}
    )

    report.assert_awaited_once()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("order_num", f"apr_{UPLOAD_ID}/../../admin"),
        ("order_num", "apr_" + "A" * 32),
        ("upload_id", UPLOAD_ID[:-1]),
        ("upload_id", UPLOAD_ID.upper()),
    ],
)
@pytest.mark.asyncio
async def test_finalize_rejects_malformed_ids(
    monkeypatch: pytest.MonkeyPatch, field: str, value: str
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    query = AsyncMock(return_value=_authorization())
    monkeypatch.setattr(upload_client, "query_upload_authorization", query)
    params = _params()
    params[field] = value

    denial = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=params,
        request_id="req-malformed",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )

    assert denial is not None
    assert denial[1] == "BAD_REQUEST"
    query.assert_not_awaited()


@pytest.mark.asyncio
async def test_schedule_writeback_does_not_block_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(return_value=_authorization()),
    )
    started = asyncio.Event()

    async def _writeback(**_: object) -> None:
        started.set()

    monkeypatch.setattr(upload_client, "report_install_result", _writeback)

    await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-scheduled",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )
    guard.schedule_pending_install_result(
        "req-scheduled", ok=True, payload={"success": True}
    )

    # 调度后立即返回：协程尚未开始执行，说明响应投递没有被 Manager 往返阻塞。
    assert started.is_set() is False

    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_http_entry_forwards_upload_credentials(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from unittest.mock import MagicMock

    from agent_gateway.channel_manager.base import RobotMessageRouter
    from agent_gateway.channel_manager.web import web_http_dispatch
    from agent_gateway.channel_manager.web.web_connect import (
        WebChannel,
        WebChannelConfig,
    )

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    captured = AsyncMock()
    monkeypatch.setattr(web_http_dispatch, "invoke_web_request", captured)
    monkeypatch.setattr(channel, "register_request_outbound", MagicMock())

    await web_http_dispatch.dispatch_http_request(
        channel,
        method="skills.upload.finalize",
        params={},
        headers={
            "authorization": "Bearer header-token",
            "cookie": "openjiuwen_access_token=cookie-token; jiuwenclaw_id=cluster_http",
        },
        request_id="req-http",
    )

    kwargs = captured.await_args.kwargs
    assert kwargs["method"] == "skills.upload.finalize"
    assert kwargs["authorization"] == "Bearer header-token"
    assert kwargs["jiuwenclaw_id"] == "cluster_http"


@pytest.mark.asyncio
async def test_ws_entry_runs_upload_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    import json
    from types import SimpleNamespace

    from agent_gateway.channel_manager.base import RobotMessageRouter
    from agent_gateway.channel_manager.web import invoke
    from agent_gateway.channel_manager.web.web_connect import (
        WebChannel,
        WebChannelConfig,
    )

    channel = WebChannel(WebChannelConfig(enabled=True), RobotMessageRouter())
    send_response = AsyncMock()
    monkeypatch.setattr(channel, "send_response", send_response)
    denial = AsyncMock(return_value=("denied by gate", "SKILL_UPLOAD_NOT_APPROVED"))
    monkeypatch.setattr(invoke, "enterprise_skill_upload_denial", denial)

    ws = SimpleNamespace(
        request_headers={
            "cookie": "openjiuwen_access_token=cookie-token; jiuwenclaw_id=cluster_ws",
        },
        closed=False,
    )
    await channel._handle_raw_message(  # pylint: disable=protected-access
        ws,
        json.dumps(
            {
                "type": "req",
                "id": "req-ws",
                "method": "skills.upload.finalize",
                "params": {},
            }
        ),
        {},
    )

    denial.assert_awaited_once()
    kwargs = denial.await_args.kwargs
    assert kwargs["authorization"] == "Bearer cookie-token"
    assert kwargs["jiuwenclaw_id"] == "cluster_ws"
    send_response.assert_awaited_once()
    assert send_response.await_args.kwargs["code"] == "SKILL_UPLOAD_NOT_APPROVED"


def test_upload_methods_are_forwarded_to_agentserver() -> None:
    from agent_gateway.channel_manager.web.app_web_handlers import (
        _FORWARD_NO_LOCAL_HANDLER_METHODS,
        _FORWARD_REQ_METHODS,
    )

    for method in ("skills.upload.stage", "skills.upload.finalize"):
        assert method in _FORWARD_REQ_METHODS
        assert method in _FORWARD_NO_LOCAL_HANDLER_METHODS
