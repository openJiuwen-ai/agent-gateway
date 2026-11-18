# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""企业版 Skill 上传窄门禁：拒绝旧上传、复核 finalize、回写安装结果。"""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
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
async def test_finalize_rejects_other_instance(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(return_value={**_authorization(), "cluster_id": "cluster_b"}),
    )

    denial = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-other-instance",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )

    assert denial is not None
    assert denial[1] == upload_client.NOT_APPROVED


@pytest.mark.parametrize("field", ["digest", "skill_name"])
@pytest.mark.asyncio
async def test_finalize_fails_closed_without_approved_fields(
    monkeypatch: pytest.MonkeyPatch, field: str
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(return_value={**_authorization(), field: ""}),
    )
    params = _params()

    denial = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=params,
        request_id=f"req-missing-{field}",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )

    assert denial is not None
    assert denial[1] == upload_client.CHECK_UNAVAILABLE
    # 失败关闭：缺字段时不能把空值写进 params 交给下游。
    assert params["expected_digest"] == "sha256:" + "0" * 64
    assert params["skill_name"] == "forged-name"


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


@pytest.mark.asyncio
async def test_ok_response_with_failure_payload_is_not_reported_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RPC ok=True 但 payload success=False 时不得把审批单标成 installed。"""
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
        request_id="req-payload-failed",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )
    await guard.report_pending_install_result(
        "req-payload-failed",
        ok=True,
        payload={"success": False, "detail": "安装失败"},
    )

    report.assert_not_awaited()


@pytest.mark.asyncio
async def test_ok_response_without_success_is_not_reported_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """request_id 被别的方法复用时，ok 响应不能把本次 finalize 标成 installed。"""
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
        request_id="req-other-method",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )
    await guard.report_pending_install_result(
        "req-other-method", ok=True, payload={"skills": []}
    )

    report.assert_not_awaited()


@pytest.mark.asyncio
async def test_finalize_rejects_request_id_bound_to_another_upload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """客户端复用 request_id 时不得把 A 单记录套到 B 单回包上。"""
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    second_upload = "b" * 32
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(
            side_effect=[
                _authorization(),
                {**_authorization(), "upload_id": second_upload},
            ]
        ),
    )

    first = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-bound",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )
    second = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params={
            **_params(),
            "order_num": f"apr_{second_upload}",
            "upload_id": second_upload,
        },
        request_id="req-bound",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )

    assert first is None
    assert second is not None
    assert second[1] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_finalize_allows_same_upload_to_refresh_request_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(side_effect=[_authorization(), _authorization()]),
    )

    for _ in range(2):
        denial = await guard.enterprise_skill_upload_denial(
            method="skills.upload.finalize",
            params=_params(),
            request_id="req-same-upload",
            authorization="Bearer token",
            jiuwenclaw_id="cluster_a",
        )
        assert denial is None


@pytest.mark.asyncio
async def test_expired_pending_result_is_not_written_back(
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
        request_id="req-expired",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )
    record = guard._PENDING_RESULTS["req-expired"]  # pylint: disable=protected-access
    record["created_at"] -= guard._PENDING_RESULT_TTL_S + 1  # pylint: disable=protected-access

    await guard.report_pending_install_result(
        "req-expired", ok=True, payload={"success": True}
    )

    report.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_record_does_not_block_a_new_upload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(enterprise_pkg, "is_enterprise", lambda: True)
    second_upload = "b" * 32
    monkeypatch.setattr(
        upload_client,
        "query_upload_authorization",
        AsyncMock(
            side_effect=[
                _authorization(),
                {**_authorization(), "upload_id": second_upload},
            ]
        ),
    )

    first = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params=_params(),
        request_id="req-reuse",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )
    record = guard._PENDING_RESULTS["req-reuse"]  # pylint: disable=protected-access
    record["created_at"] -= guard._PENDING_RESULT_TTL_S + 1  # pylint: disable=protected-access

    second = await guard.enterprise_skill_upload_denial(
        method="skills.upload.finalize",
        params={
            **_params(),
            "order_num": f"apr_{second_upload}",
            "upload_id": second_upload,
        },
        request_id="req-reuse",
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
    )

    assert first is None
    assert second is None
    assert (
        guard._PENDING_RESULTS["req-reuse"]["upload_id"]  # pylint: disable=protected-access
        == second_upload
    )


def _patch_install_result_http(monkeypatch: pytest.MonkeyPatch, post):
    """Patch the manager client used by one write-back call.

    ``post(call_no)`` returns a response-like object or raises. Returns the
    list of attempted call numbers so tests can assert retry counts.
    """
    calls: list[int] = []

    class _Client:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        async def __aenter__(self) -> "_Client":
            return self

        async def __aexit__(self, *exc: object) -> bool:
            return False

        async def post(self, *args: object, **kwargs: object) -> object:
            calls.append(len(calls) + 1)
            return post(len(calls))

    monkeypatch.setattr(upload_client, "_manager_base", lambda: "http://manager")
    monkeypatch.setattr(upload_client, "_INSTALL_RESULT_BACKOFF_S", 0)
    monkeypatch.setattr(
        upload_client,
        "httpx",
        SimpleNamespace(AsyncClient=_Client, TransportError=httpx.TransportError),
    )
    return calls


async def _writeback(
    status: str = "installed", detail: str | None = None
) -> None:
    await upload_client.report_install_result(
        authorization="Bearer token",
        jiuwenclaw_id="cluster_a",
        order_num=f"apr_{UPLOAD_ID}",
        upload_id=UPLOAD_ID,
        digest="sha256:" + "1" * 64,
        status=status,
        detail=detail,
    )


@pytest.mark.asyncio
async def test_schedule_writeback_ignores_unrelated_pending_records() -> None:
    """send() 热路径：只有登记过本次 finalize 的 id 才创建后台任务。"""
    guard._PENDING_RESULTS["req-other"] = {  # pylint: disable=protected-access
        "created_at": time.monotonic(),
        "upload_id": "a" * 32,
        "order_num": f"apr_{'a' * 32}",
    }
    try:
        before = len(guard._WRITEBACK_TASKS)  # pylint: disable=protected-access
        guard.schedule_pending_install_result(
            "req-unrelated", ok=True, payload={"success": True}
        )
        assert len(guard._WRITEBACK_TASKS) == before  # pylint: disable=protected-access
    finally:
        guard._PENDING_RESULTS.pop("req-other", None)  # pylint: disable=protected-access


@pytest.mark.asyncio
async def test_install_result_writeback_retries_transient_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _post(call_no: int) -> object:
        if call_no < 3:
            raise httpx.ConnectError("manager unreachable")
        return SimpleNamespace(status_code=200)

    calls = _patch_install_result_http(monkeypatch, _post)

    await _writeback()

    assert len(calls) == 3


@pytest.mark.asyncio
async def test_install_result_writeback_does_not_retry_client_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _patch_install_result_http(
        monkeypatch, lambda call_no: SimpleNamespace(status_code=403)
    )

    await _writeback(status="failed", detail="pkg invalid")

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_install_result_writeback_does_not_retry_config_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _post(call_no: int) -> object:
        raise httpx.InvalidURL("bad manager url")

    calls = _patch_install_result_http(monkeypatch, _post)

    with pytest.raises(httpx.InvalidURL):
        await _writeback()

    assert len(calls) == 1


@pytest.mark.asyncio
async def test_install_result_writeback_retries_server_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _patch_install_result_http(
        monkeypatch,
        lambda call_no: SimpleNamespace(status_code=503 if call_no < 3 else 200),
    )

    await _writeback()

    assert len(calls) == 3


@pytest.mark.asyncio
async def test_install_result_writeback_retries_rate_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = _patch_install_result_http(
        monkeypatch,
        lambda call_no: SimpleNamespace(status_code=429 if call_no < 3 else 200),
    )

    await _writeback()

    assert len(calls) == 3


@pytest.mark.asyncio
async def test_install_result_writeback_gives_up_after_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _post(call_no: int) -> object:
        raise httpx.ConnectError("manager unreachable")

    calls = _patch_install_result_http(monkeypatch, _post)

    await _writeback()

    assert len(calls) == upload_client._INSTALL_RESULT_ATTEMPTS


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
