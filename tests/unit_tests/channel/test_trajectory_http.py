# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Tests for the Gateway trajectory HTTP proxy contract."""

from __future__ import annotations

import base64
import io
import json
import logging
import zipfile
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI


from agent_gateway.channel_manager.web.trajectory_http import (
    TRAJECTORY_API_PREFIX,
    TRAJECTORY_ARCHIVE_ENTRY_NAME,
    TrajectoryHttpService,
    _MAX_BLOB_CHUNKS,
    attach_trajectory_routes,
)

_TRACE_ID = "4" * 32
_SPAN_ID = "d" * 16
_MAX_SQLITE_INTEGER = (1 << 63) - 1


def _metadata_loader(mode: str = "agent.work.normal"):
    async def _load(session_id: str, user_id: str | None = None) -> dict[str, str]:
        del user_id
        if session_id in {"session-1", "session-2"}:
            return {"session_id": session_id, "mode": mode, "team_name": ""}
        return {}

    return _load


async def _simple_session_loader(
    session_id: str,
    _user_id: str | None = None,
) -> dict[str, str]:
    return {"session_id": session_id}


async def _throwing_metadata_loader(
    _session_id: str,
    _user_id: str | None = None,
) -> dict[str, str]:
    raise RuntimeError("secret metadata path")


def _ok_fetch(result: dict | None = None):
    async def fetch(method, params, user_id=None):
        del method, params, user_id
        return True, {"result": result if result is not None else {"items": []}}

    return fetch


def _response_json(response) -> dict:
    return json.loads(bytes(response.body))


def _make_app(fetch, *, metadata_loader=None) -> FastAPI:
    app = FastAPI()
    attach_trajectory_routes(
        app,
        SimpleNamespace(),
        metadata_loader=metadata_loader or _metadata_loader(),
        fetch=fetch,
    )
    return app


class _BlobFetch:
    """Serve one immutable body through the southbound blob protocol."""

    def __init__(self, body: bytes, *, chunk_size: int = 4) -> None:
        self.body = body
        self.chunk_size = chunk_size
        self.calls: list[tuple[str, dict, str | None]] = []

    async def __call__(self, method, params, user_id=None):
        self.calls.append((method, dict(params), user_id))
        if method == 'trajectory.archive.open':
            return True, {
                "chunked": True,
                "export_id": "export-1",
                "size": len(self.body),
                "content_type": "application/zip",
            }
        if method == 'trajectory.blob.read':
            offset = int(params["offset"])
            chunk = self.body[offset:offset + self.chunk_size]
            next_offset = offset + len(chunk)
            return True, {
                "data": base64.b64encode(chunk).decode("ascii"),
                "next_offset": next_offset,
                "done": next_offset >= len(self.body),
            }
        if method == 'trajectory.blob.close':
            return True, {}
        raise AssertionError(f"unexpected trajectory method: {method}")


@pytest.mark.asyncio
async def test_http_list_detail_and_raw_preserve_contract() -> None:
    expected_raw = b'{"resourceSpans":[{"scopeSpans":[{"spans":[]}]}]}'
    list_payload = {
        "session_id": "session-1",
        "items": [{"subject_id": "main", "record_count": 1}],
        "watermark": 7,
        "store_epoch": "epoch-1",
    }
    detail_payload = {
        "session_id": "session-1",
        "subject_id": "main",
        "records": [{"record_id": f"{_TRACE_ID}:{_SPAN_ID}", "operation": "upsert"}],
        "revision": 7,
    }
    calls: list[tuple[str, dict, str | None]] = []

    async def fetch(method, params, user_id=None):
        calls.append((method, params, user_id))
        if method == 'trajectory.subjects.list':
            return True, {"result": list_payload}
        if method == 'trajectory.subject.get':
            return True, {"result": detail_payload}
        if method == 'trajectory.raw.get':
            return True, {"raw_b64": base64.b64encode(expected_raw).decode("ascii")}
        raise AssertionError(f"unexpected trajectory method: {method}")

    service = TrajectoryHttpService(metadata_loader=_metadata_loader(), fetch=fetch)
    list_response = await service.list_subjects("session-1", after_revision=0)
    detail_response = await service.get_subject(
        "session-1", "main", since_revision=2, limit=1000,
    )
    raw_response = await service.get_raw_record("session-1", _TRACE_ID, _SPAN_ID)

    assert list_response.status_code == 200
    assert list_response.headers["cache-control"] == "no-store"
    assert _response_json(list_response) == list_payload
    assert detail_response.status_code == 200
    assert detail_response.headers["cache-control"] == "no-store"
    assert _response_json(detail_response) == detail_payload
    assert raw_response.status_code == 200
    assert raw_response.headers["cache-control"] == "no-store"
    assert raw_response.headers["content-type"] == "application/json; charset=utf-8"
    assert bytes(raw_response.body) == expected_raw
    assert calls == [
        (
            'trajectory.subjects.list',
            {"session_id": "session-1", "after_revision": 0},
            None,
        ),
        (
            'trajectory.subject.get',
            {
                "session_id": "session-1",
                "subject_id": "main",
                "since_revision": 2,
                "limit": 1000,
            },
            None,
        ),
        (
            'trajectory.raw.get',
            {"session_id": "session-1", "trace_id": _TRACE_ID, "span_id": _SPAN_ID},
            None,
        ),
    ]


@pytest.mark.asyncio
async def test_http_stream_frame_page_preserves_cursor_and_limit() -> None:
    result = {
        "frames": [{"sequence": 50, "text": "w50 "}],
        "next_since_frame_seq": 51,
        "has_more": True,
        "reset": False,
    }
    calls: list[tuple[str, dict]] = []

    async def fetch(method, params, user_id=None):
        del user_id
        calls.append((method, params))
        return True, {"result": result}

    service = TrajectoryHttpService(metadata_loader=_metadata_loader(), fetch=fetch)
    response = await service.get_stream_frames("session-1", since_frame_seq=50, limit=500)

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert _response_json(response) == result
    assert calls == [(
        'trajectory.stream_frames.get',
        {"session_id": "session-1", "since_frame_seq": 50, "limit": 500},
    )]


@pytest.mark.asyncio
async def test_http_rejects_invalid_cursor_and_page_inputs_before_fetch() -> None:
    calls: list[str] = []

    async def fetch(method, params, user_id=None):
        del params, user_id
        calls.append(method)
        return True, {"result": {}}

    service = TrajectoryHttpService(metadata_loader=_metadata_loader(), fetch=fetch)
    responses = [
        await service.list_subjects("session-1", after_revision=-1),
        await service.list_subjects("session-1", after_revision=_MAX_SQLITE_INTEGER + 1),
        await service.get_subject("session-1", "", since_revision=0, limit=100),
        await service.get_subject("session-1", "main", since_revision=-1, limit=100),
        await service.get_subject("session-1", "main", since_revision=0, limit=0),
        await service.get_subject("session-1", "main", since_revision=0, limit=1001),
        await service.get_stream_frames("session-1", since_frame_seq=-1, limit=100),
        await service.get_stream_frames("session-1", since_frame_seq=0, limit=2001),
        await service.get_raw_record("session-1", "not-a-trace", _SPAN_ID),
        await service.get_raw_record("session-1", _TRACE_ID, "not-a-span"),
        await service.get_sequences("session-1", []),
        await service.get_sequences("session-1", ["not-a-hash"]),
        await service.get_sequences("session-1", ["a" * 64] * 201),
    ]

    for response in responses:
        assert response.status_code == 400
        assert response.headers["cache-control"] == "no-store"
        assert _response_json(response)["code"] == "BAD_REQUEST"
    assert calls == []


@pytest.mark.asyncio
async def test_http_forbids_auto_harness_sessions() -> None:
    harness_service = TrajectoryHttpService(
        metadata_loader=_metadata_loader("auto_harness"),
        fetch=_ok_fetch(),
    )
    harness_plan_service = TrajectoryHttpService(
        metadata_loader=_metadata_loader("auto_harness.plan"),
        fetch=_ok_fetch(),
    )

    harness_response = await harness_service.list_subjects("session-1", after_revision=0)
    harness_plan_response = await harness_plan_service.list_subjects("session-1", after_revision=0)

    for response in (harness_response, harness_plan_response):
        assert response.status_code == 403
        assert response.headers["cache-control"] == "no-store"
        assert _response_json(response)["code"] == "UNSUPPORTED_SESSION_MODE"


@pytest.mark.asyncio
async def test_http_accepts_team_sessions_with_team_name() -> None:
    async def team_metadata(session_id: str, _user_id: str | None = None) -> dict[str, str]:
        if session_id in {"session-1", "session-2"}:
            return {
                "session_id": session_id,
                "mode": "team.plan.normal",
                "team_name": "research-team",
            }
        return {}

    service = TrajectoryHttpService(
        metadata_loader=team_metadata,
        fetch=_ok_fetch({"session_id": "session-1", "items": [{"subject_id": "main"}]}),
    )
    response = await service.list_subjects("session-1", after_revision=0)

    assert response.status_code == 200
    assert _response_json(response)["session_id"] == "session-1"
    assert len(_response_json(response)["items"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        "agent.work.normal",
        "agent.work.plan",
        "agent.code.normal",
        "agent.code.plan",
    ],
)
async def test_http_accepts_new_single_agent_canonical_modes(mode: str) -> None:
    service = TrajectoryHttpService(
        metadata_loader=_metadata_loader(mode),
        fetch=_ok_fetch({"items": [{"subject_id": "main"}]}),
    )

    response = await service.list_subjects("session-1", after_revision=0)

    assert response.status_code == 200
    assert len(_response_json(response)["items"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    ["agent", "agent.fast", "agent.plan", "code", "code.normal", "code.plan"],
)
async def test_http_rejects_legacy_single_agent_mode_names(mode: str) -> None:
    service = TrajectoryHttpService(
        metadata_loader=_metadata_loader(mode),
        fetch=_ok_fetch(),
    )

    response = await service.list_subjects("session-1", after_revision=0)

    assert response.status_code == 403
    assert _response_json(response)["code"] == "UNSUPPORTED_SESSION_MODE"


@pytest.mark.asyncio
async def test_http_fails_closed_for_unknown_or_missing_session_mode() -> None:
    unknown = TrajectoryHttpService(
        metadata_loader=_metadata_loader("future.mode"),
        fetch=_ok_fetch(),
    )
    missing = TrajectoryHttpService(
        metadata_loader=_simple_session_loader,
        fetch=_ok_fetch(),
    )

    unknown_response = await unknown.list_subjects("session-1", after_revision=0)
    missing_response = await missing.list_subjects("session-1", after_revision=0)

    assert unknown_response.status_code == 403
    assert missing_response.status_code == 403
    assert _response_json(unknown_response)["code"] == "UNSUPPORTED_SESSION_MODE"
    assert _response_json(missing_response)["code"] == "UNSUPPORTED_SESSION_MODE"


@pytest.mark.asyncio
async def test_http_reports_invalid_session() -> None:
    service = TrajectoryHttpService(
        metadata_loader=_metadata_loader(),
        fetch=_ok_fetch(),
    )

    response = await service.list_subjects("../session", after_revision=0)

    assert response.status_code == 400
    assert response.headers["cache-control"] == "no-store"
    assert _response_json(response)["code"] == "BAD_REQUEST"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "code,reported_status,expected_status",
    [
        ("SERVICE_UNAVAILABLE", None, 503),
        ("AGENT_SERVER_TIMEOUT", None, 504),
        ("NOT_FOUND", 404, 404),
        ("TRAJECTORY_QUERY_FAILED", False, 500),
    ],
)
async def test_http_maps_transport_and_reported_errors(
    code: str,
    reported_status,
    expected_status: int,
) -> None:
    async def fetch(method, params, user_id=None):
        del method, params, user_id
        return False, {"error": "read failed", "code": code, "status": reported_status}

    service = TrajectoryHttpService(metadata_loader=_metadata_loader(), fetch=fetch)
    response = await service.list_subjects("session-1", after_revision=0)

    assert response.status_code == expected_status
    assert response.headers["cache-control"] == "no-store"
    assert _response_json(response) == {"error": "read failed", "code": code}


def test_attach_trajectory_routes_registers_all_paths() -> None:
    app = _make_app(_ok_fetch())
    paths = {getattr(route, "path", None) for route in app.router.routes}

    assert f"{TRAJECTORY_API_PREFIX}/sessions/{{session_id}}/subjects" in paths
    assert f"{TRAJECTORY_API_PREFIX}/sessions/{{session_id}}/stream-frames" in paths
    assert f"{TRAJECTORY_API_PREFIX}/sessions/{{session_id}}/sequences" in paths
    assert f"{TRAJECTORY_API_PREFIX}/sessions/{{session_id}}/archive" in paths
    assert f"{TRAJECTORY_API_PREFIX}/sessions/{{session_id}}/checkpoints" in paths
    assert f"{TRAJECTORY_API_PREFIX}/sessions/{{session_id}}/usage" in paths
    assert (
        f"{TRAJECTORY_API_PREFIX}/sessions/{{session_id}}/subjects/{{subject_id}}/records"
        in paths
    )
    assert (
        f"{TRAJECTORY_API_PREFIX}/sessions/{{session_id}}/traces/{{trace_id}}/spans/{{span_id}}/raw"
        in paths
    )


@pytest.mark.asyncio
async def test_archive_get_download_preserves_gateway_contract() -> None:
    archive_buffer = io.BytesIO()
    with zipfile.ZipFile(archive_buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            TRAJECTORY_ARCHIVE_ENTRY_NAME,
            b'{"type":"header"}\n{"type":"end","records":0,"lines":1}\n',
        )
    body = archive_buffer.getvalue()
    fetch = _BlobFetch(body)
    app = _make_app(fetch)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/archive",
            params={"user_id": "alice"},
        )
        missing = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-missing/archive",
        )

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["content-type"] == "application/zip"
    assert response.headers["content-disposition"] == (
        'attachment; filename="trajectory-session-1.trajectory.zip"'
    )
    assert int(response.headers["content-length"]) == len(body)
    assert response.content == body
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert archive.namelist() == [TRAJECTORY_ARCHIVE_ENTRY_NAME]
    assert missing.status_code == 404
    assert missing.headers["cache-control"] == "no-store"
    assert missing.json()["code"] == "NOT_FOUND"
    assert all(user_id == "alice" for _method, _params, user_id in fetch.calls)
    assert fetch.calls[-1][0] == 'trajectory.blob.close'


@pytest.mark.asyncio
async def test_archive_route_is_export_only() -> None:
    calls: list[str] = []

    async def fetch(method, params, user_id=None):
        del params, user_id
        calls.append(method)
        return True, {"result": {}}

    app = _make_app(fetch)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.post(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/archive",
            json={"format": "openjiuwen.trajectory.archive", "records": []},
        )

    assert response.status_code == 405
    assert response.headers["cache-control"] == "no-store"
    assert response.json()["code"] == "METHOD_NOT_ALLOWED"
    assert calls == []


@pytest.mark.asyncio
async def test_route_query_validation_keeps_no_store_header() -> None:
    app = _make_app(_ok_fetch())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        list_response = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects?after_revision=invalid",
        )
        detail_response = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects/main/records"
            "?since_revision=invalid",
        )
        huge_limit_response = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            params={"after_revision": "9" * 5000},
        )
        huge_revision_response = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects/main/records",
            params={"since_revision": "9" * 5000},
        )

    for response in (
        list_response,
        detail_response,
        huge_limit_response,
        huge_revision_response,
    ):
        assert response.status_code == 400
        assert response.headers["cache-control"] == "no-store"
    assert huge_limit_response.json() == {
        "error": "after_revision must be an integer",
        "code": "BAD_REQUEST",
    }
    assert huge_revision_response.json() == {
        "error": "since_revision must be an integer",
        "code": "BAD_REQUEST",
    }


@pytest.mark.asyncio
async def test_framework_trajectory_errors_are_json_and_non_cacheable() -> None:
    async def fetch(method, params, user_id=None):
        del params, user_id
        if method == 'trajectory.subject.get':
            return False, {"error": "subject not found", "code": "NOT_FOUND", "status": 404}
        return True, {"result": {"items": []}}

    app = _make_app(fetch)

    @app.get(f"{TRAJECTORY_API_PREFIX}/validation-probe")
    async def validation_probe(required: int):
        return {"required": required}

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        not_found = await client.get(f"{TRAJECTORY_API_PREFIX}/missing")
        method_not_allowed = await client.post(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
        )
        validation_error = await client.get(f"{TRAJECTORY_API_PREFIX}/validation-probe")
        trace_not_found = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects/main/records",
        )

    assert not_found.status_code == 404
    assert not_found.headers["cache-control"] == "no-store"
    assert not_found.json() == {"error": "trajectory route not found", "code": "NOT_FOUND"}
    assert method_not_allowed.status_code == 405
    assert method_not_allowed.headers["cache-control"] == "no-store"
    assert method_not_allowed.json() == {
        "error": "trajectory method not allowed",
        "code": "METHOD_NOT_ALLOWED",
    }
    assert validation_error.status_code == 400
    assert validation_error.headers["cache-control"] == "no-store"
    assert validation_error.json() == {"error": "invalid trajectory request", "code": "BAD_REQUEST"}
    assert trace_not_found.status_code == 404
    assert trace_not_found.headers["cache-control"] == "no-store"
    assert trace_not_found.json() == {"error": "subject not found", "code": "NOT_FOUND"}


@pytest.mark.asyncio
async def test_routes_reuse_webchannel_origin_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("JIUWENSWARM_ENABLE_ORIGIN_CHECK", "1")
    monkeypatch.setenv("JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS", "trusted.example")
    app = _make_app(_ok_fetch())
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        rejected = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            headers={"Origin": "https://evil.example"},
        )
        allowed = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            headers={"Origin": "https://trusted.example"},
        )
        same_origin_without_origin = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            headers={"Host": "trusted.example", "Sec-Fetch-Site": "same-origin"},
        )
        spoofed_host_without_origin = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            headers={
                "Host": "evil.example",
                "Sec-Fetch-Site": "same-origin",
                "X-Forwarded-Host": "trusted.example",
            },
        )
        cross_site_without_origin = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            headers={"Host": "trusted.example", "Sec-Fetch-Site": "cross-site"},
        )
        cross_site_with_allowed_origin = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            headers={
                "Host": "trusted.example",
                "Origin": "https://trusted.example",
                "Sec-Fetch-Site": "cross-site",
            },
        )
        allowed_referer_without_origin = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            headers={
                "Host": "trusted.example",
                "Referer": "https://trusted.example/app",
            },
        )

    responses = (
        rejected,
        allowed,
        same_origin_without_origin,
        spoofed_host_without_origin,
        cross_site_without_origin,
        cross_site_with_allowed_origin,
        allowed_referer_without_origin,
    )
    assert rejected.status_code == 403
    assert rejected.json()["code"] == "FORBIDDEN_ORIGIN"
    assert allowed.status_code == 200
    assert same_origin_without_origin.status_code == 200
    assert spoofed_host_without_origin.status_code == 403
    assert cross_site_without_origin.status_code == 403
    assert cross_site_with_allowed_origin.status_code == 403
    assert allowed_referer_without_origin.status_code == 200
    for response in responses:
        assert response.headers["cache-control"] == "no-store"


@pytest.mark.asyncio
async def test_http_internal_failures_return_stable_generic_messages() -> None:
    async def failing_fetch(method, params, user_id=None):
        del method, params, user_id
        raise ValueError("secret sqlite path /private/trajectory.sqlite3")

    query_service = TrajectoryHttpService(
        metadata_loader=_metadata_loader(),
        fetch=failing_fetch,
    )
    metadata_service = TrajectoryHttpService(
        metadata_loader=_throwing_metadata_loader,
        fetch=_ok_fetch(),
    )

    responses = [
        await query_service.list_subjects("session-1", after_revision=0),
        await query_service.list_subjects("session-1", after_revision=1),
        await query_service.export_archive("session-1"),
        await query_service.get_subject("session-1", "main", since_revision=0, limit=1000),
        await query_service.get_raw_record("session-1", _TRACE_ID, _SPAN_ID),
        await query_service.get_stream_frames("session-1", since_frame_seq=0, limit=100),
        await query_service.get_sequences("session-1", ["a" * 64]),
    ]
    metadata_response = await metadata_service.list_subjects("session-1", after_revision=0)

    for response in responses:
        assert response.status_code == 500
        assert response.headers["cache-control"] == "no-store"
        assert _response_json(response) == {
            "error": "trajectory query failed",
            "code": "TRAJECTORY_QUERY_FAILED",
        }
        assert b"/private/trajectory.sqlite3" not in bytes(response.body)
    assert metadata_response.status_code == 500
    assert _response_json(metadata_response) == {
        "error": "session lookup failed",
        "code": "SESSION_LOOKUP_FAILED",
    }


@pytest.mark.asyncio
async def test_missing_trajectory_status_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    async def fetch(method, params, user_id=None):
        del method, params, user_id
        return False, {"error": "trajectory query failed", "code": "TRAJECTORY_QUERY_FAILED"}

    service = TrajectoryHttpService(metadata_loader=_metadata_loader(), fetch=fetch)
    caplog.set_level(logging.WARNING, logger="agent_gateway.channel_manager.web.trajectory_http")
    response = await service.list_subjects("session-1", after_revision=0)

    assert response.status_code == 500
    assert _response_json(response) == {
        "error": "trajectory query failed",
        "code": "TRAJECTORY_QUERY_FAILED",
    }
    assert "no usable status" in caplog.text


@pytest.mark.asyncio
async def test_blob_read_without_progress_fails_closed() -> None:
    calls: list[str] = []

    async def fetch(method, params, user_id=None):
        del params, user_id
        calls.append(method)
        if method == 'trajectory.archive.open':
            return True, {"chunked": True, "export_id": "export-1", "size": 4}
        if method == 'trajectory.blob.read':
            return True, {"data": "", "next_offset": 0}
        return True, {}

    service = TrajectoryHttpService(metadata_loader=_metadata_loader(), fetch=fetch)
    response = await service.export_archive("session-1")

    assert response.status_code == 500
    assert response.headers["content-type"].startswith("application/json")
    assert _response_json(response) == {
        "error": "trajectory query failed",
        "code": "TRAJECTORY_QUERY_FAILED",
    }
    assert calls.count('trajectory.blob.read') == 1
    assert calls.count('trajectory.blob.close') == 1


@pytest.mark.asyncio
async def test_blob_read_chunk_limit_fails_closed() -> None:
    reads = 0

    async def fetch(method, params, user_id=None):
        nonlocal reads
        del user_id
        if method == 'trajectory.archive.open':
            return True, {"chunked": True, "export_id": "export-1", "size": 1}
        if method == 'trajectory.blob.read':
            reads += 1
            return True, {
                "data": "",
                "next_offset": int(params["offset"]) + 1,
                "done": False,
            }
        return True, {}

    service = TrajectoryHttpService(metadata_loader=_metadata_loader(), fetch=fetch)
    response = await service.export_archive("session-1")

    assert response.status_code == 500
    assert _response_json(response)["code"] == "TRAJECTORY_EXPORT_TOO_LARGE"
    assert reads == _MAX_BLOB_CHUNKS


@pytest.mark.asyncio
async def test_blob_read_terminal_empty_chunk_completes() -> None:
    payload_body = b"x" * 10
    calls: list[str] = []

    async def fetch(method, params, user_id=None):
        del user_id
        calls.append(method)
        if method == 'trajectory.archive.open':
            return True, {
                "chunked": True,
                "export_id": "export-1",
                "size": len(payload_body),
                "content_type": "application/zip",
            }
        if method == 'trajectory.blob.read':
            offset = int(params["offset"])
            if offset < len(payload_body):
                chunk = payload_body[offset:offset + 4]
                return True, {
                    "data": base64.b64encode(chunk).decode("ascii"),
                    "next_offset": offset + len(chunk),
                    "done": False,
                }
            return True, {"data": "", "next_offset": offset, "done": True}
        return True, {}

    service = TrajectoryHttpService(metadata_loader=_metadata_loader(), fetch=fetch)
    response = await service.export_archive("session-1")

    assert response.status_code == 200
    chunks = [chunk async for chunk in response.body_iterator]
    assert b"".join(chunks) == payload_body
    assert calls[-1] == 'trajectory.blob.close'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "read_payload",
    [
        {"data": None, "next_offset": 1},
        {"data": "", "next_offset": True},
        {"data": "not-base64!", "next_offset": 1},
    ],
)
async def test_blob_read_malformed_chunks_fail_closed(read_payload: dict) -> None:
    calls: list[str] = []

    async def fetch(method, params, user_id=None):
        del params, user_id
        calls.append(method)
        if method == 'trajectory.archive.open':
            return True, {"chunked": True, "export_id": "export-1", "size": 4}
        if method == 'trajectory.blob.read':
            return True, read_payload
        return True, {}

    service = TrajectoryHttpService(metadata_loader=_metadata_loader(), fetch=fetch)
    response = await service.export_archive("session-1")

    assert response.status_code == 500
    assert _response_json(response)["code"] == "TRAJECTORY_QUERY_FAILED"
    assert calls[-1] == 'trajectory.blob.close'


@pytest.mark.asyncio
async def test_route_user_id_is_resolved_for_multi_user_routing() -> None:
    routed_users: list[str | None] = []
    loaded_users: list[str | None] = []

    async def fetch(method, params, user_id=None):
        del method, params
        routed_users.append(user_id)
        return False, {"error": "trajectory query failed", "code": "TRAJECTORY_QUERY_FAILED"}

    async def recording_loader(session_id, user_id=None):
        loaded_users.append(user_id)
        return {"session_id": session_id, "mode": "agent.work.normal", "team_name": ""}

    app = _make_app(fetch, metadata_loader=recording_loader)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        header_response = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            headers={"x-user-id": "user-a"},
        )
        query_response = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            params={"user_id": "user-b"},
        )
        plain_response = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
        )

    assert header_response.status_code == 500
    assert query_response.status_code == 500
    assert plain_response.status_code == 500
    assert routed_users == ["user-a", "user-b", None]
    assert loaded_users == ["user-a", "user-b", None]


@pytest.mark.asyncio
async def test_route_user_id_mismatch_is_rejected() -> None:
    async def fetch(method, params, user_id=None):
        del method, params, user_id
        raise AssertionError("the read must not be forwarded")

    app = FastAPI()

    @app.middleware("http")
    async def authenticated_state(request, call_next):
        request.state.agentos_iam_user_id = "iam-uuid-1"
        request.state.agentos_username = "alice"
        return await call_next(request)

    attach_trajectory_routes(
        app,
        SimpleNamespace(),
        metadata_loader=_metadata_loader(),
        fetch=fetch,
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get(
            f"{TRAJECTORY_API_PREFIX}/sessions/session-1/subjects",
            headers={"x-user-id": "mallory"},
        )

    assert response.status_code == 403
    assert response.json()["code"] == "USER_MISMATCH"
    assert response.headers["cache-control"] == "no-store"
