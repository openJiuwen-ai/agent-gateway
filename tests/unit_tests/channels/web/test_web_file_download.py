from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

import pytest
from starlette.requests import Request
from starlette.responses import JSONResponse

from agent_gateway.channel_manager.web.container_file_http import serve_file_chunks
from agent_gateway.common.client import agent_http_bridge
from agent_gateway.common.file_download_token import WebFileDownloadManager


def _signed_token(secret: str, payload: object) -> str:
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    signature = hmac.new(
        secret.encode("utf-8"), encoded.encode("ascii"), hashlib.sha256
    ).hexdigest()
    return f"{encoded}.{signature}"


def _request(method: str = "GET", *, range_header: str = "", inline: str = "") -> Request:
    headers = [(b"range", range_header.encode("ascii"))] if range_header else []
    query_string = f"inline={inline}".encode("ascii") if inline else b""
    return Request({
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": "/file-api/download",
        "raw_path": b"/file-api/download",
        "query_string": query_string,
        "headers": headers,
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
    })


class _MemoryFile:
    def __init__(
        self,
        content: bytes,
        *,
        name: str = "media.bin",
        mime_type: str = "application/octet-stream",
    ) -> None:
        self.content = content
        self.name = name
        self.mime_type = mime_type
        self.calls: list[tuple[int, int]] = []

    async def read(self, offset: int, limit: int) -> tuple[bytes, int, str, str, bool]:
        self.calls.append((offset, limit))
        data = self.content[offset:offset + limit]
        return data, len(self.content), self.name, self.mime_type, offset + len(data) >= len(self.content)


async def _serve(
    content: bytes = b"0123456789",
    *,
    method: str = "GET",
    range_header: str = "",
    inline: str = "",
    name: str = "media.bin",
    mime_type: str = "application/octet-stream",
):
    reader = _MemoryFile(content, name=name, mime_type=mime_type)
    response = await serve_file_chunks(
        _request(method, range_header=range_header, inline=inline),
        reader.read,
    )
    return response, reader


async def _response_body(response) -> bytes:
    if hasattr(response, "body_iterator"):
        chunks = [chunk async for chunk in response.body_iterator]
        return b"".join(chunks)
    return response.body


def test_valid_token_is_accepted() -> None:
    manager = WebFileDownloadManager(secret="s" * 32)
    token = manager.generate_token("/tmp/report.xlsx", "session-1", expires_in=60)

    payload = manager.validate_token(token)
    assert payload is not None
    assert payload["path"] == "/tmp/report.xlsx"
    assert payload["exp"] == pytest.approx(int(time.time()) + 60, abs=1)
    assert payload["sid"] == "session-1"


def test_non_expiring_token_omits_exp() -> None:
    manager = WebFileDownloadManager(secret="s" * 32)
    token = manager.generate_token("/tmp/report.xlsx", "session-1", expires_in=None)

    payload = manager.validate_token(token)
    assert payload is not None
    assert payload["path"] == "/tmp/report.xlsx"
    assert "exp" not in payload
    assert payload["sid"] == "session-1"


def test_expired_token_is_rejected() -> None:
    manager = WebFileDownloadManager(secret="s" * 32)
    token = manager.generate_token("/tmp/report.xlsx", "session-1", expires_in=-1)

    assert manager.validate_token(token) is None
    assert manager.validate_token(token, check_expiry=False) is not None


def test_tampered_token_is_rejected() -> None:
    manager = WebFileDownloadManager(secret="s" * 32)
    token = manager.generate_token("/tmp/report.xlsx", expires_in=60)
    encoded, signature = token.split(".")
    altered_signature = ("0" if signature[0] != "0" else "1") + signature[1:]

    assert manager.validate_token(f"{encoded}x.{signature}") is None
    assert manager.validate_token(f"{encoded}.{altered_signature}") is None


def test_strict_legacy_token_without_expiration_remains_compatible() -> None:
    secret = "s" * 32
    manager = WebFileDownloadManager(secret=secret)
    token = _signed_token(
        secret,
        {"path": "/tmp/report.xlsx", "sid": "session-1"},
    )

    assert manager.validate_token(token) == {
        "path": "/tmp/report.xlsx",
        "sid": "session-1",
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"path": "/tmp/report.xlsx"},
        {"path": "", "sid": "session-1"},
        {"path": "/tmp/report.xlsx", "sid": ""},
        {"path": 1, "sid": "session-1"},
        {"path": "/tmp/report.xlsx", "sid": 1},
        {"path": "/tmp/report.xlsx", "sid": "session-1", "extra": "claim"},
        {
            "path": "/tmp/report.xlsx",
            "sid": "session-1",
            "download_http_base": "",
        },
        {
            "path": "/tmp/report.xlsx",
            "sid": "session-1",
            "download_http_base": "http://test.invalid",
            "extra": "claim",
        },
    ],
)
def test_only_exact_legacy_no_expiration_payload_is_accepted(
    payload: dict[str, object],
) -> None:
    secret = "s" * 32
    manager = WebFileDownloadManager(secret=secret)

    assert manager.validate_token(_signed_token(secret, payload)) is None


def test_agent_http_base_uses_trusted_resolver(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = WebFileDownloadManager(secret="s" * 32)
    token = manager.generate_token("/tmp/a.txt", "s1", expires_in=60)
    calls: list[tuple[dict, str]] = []

    def _resolve(payload: dict, endpoint: str) -> str:
        calls.append((payload, endpoint))
        return "http://agent-a:18092"

    monkeypatch.setattr(agent_http_bridge, "_agent_http_base_resolver", _resolve)

    assert agent_http_bridge.resolve_agent_http_base_for_token(
        token, endpoint="download"
    ) == "http://agent-a:18092"
    assert len(calls) == 1
    assert calls[0][0]["path"] == "/tmp/a.txt"
    assert calls[0][0]["sid"] == "s1"
    assert calls[0][1] == "download"


def test_agent_http_base_does_not_trust_unsigned_token_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = WebFileDownloadManager(secret="s" * 32)
    token = manager.generate_token("/tmp/a.txt", "s1", expires_in=60)
    payload = manager.validate_token(token)
    assert payload is not None
    # Non-routable placeholder (RFC 2606) standing in for an attacker-chosen base.
    payload["download_http_base"] = "http://metadata.invalid/latest/meta-data"
    encoded = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":")).encode("utf-8")
    ).rstrip(b"=").decode("ascii")
    forged_token = f"{encoded}.attacker-controlled-signature"
    monkeypatch.setattr(agent_http_bridge, "_agent_http_base_resolver", None)

    assert agent_http_bridge.resolve_agent_http_base_for_token(
        forged_token, endpoint="download"
    ) == ""


@pytest.mark.asyncio
async def test_head_returns_metadata_without_streaming_body() -> None:
    response, reader = await _serve(
        method="HEAD",
        name="report final.pdf",
        mime_type="application/pdf",
    )

    assert response.status_code == 200
    assert response.headers["Content-Type"] == "application/pdf"
    assert response.headers["Content-Length"] == "10"
    assert response.headers["Accept-Ranges"] == "bytes"
    assert response.headers["Content-Disposition"] == (
        "attachment; filename*=UTF-8''report%20final.pdf"
    )
    assert response.body == b""
    assert reader.calls == [(0, 1)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("mime_type", "name", "expected_disposition"),
    [
        (
            "image/png",
            "preview sample.png",
            "inline; filename*=UTF-8''preview%20sample.png",
        ),
        ("audio/mpeg", "preview.mp3", "inline; filename*=UTF-8''preview.mp3"),
        ("video/mp4", "preview.mp4", "inline; filename*=UTF-8''preview.mp4"),
    ],
)
async def test_inline_is_allowed_only_for_safe_media_types(
    mime_type: str,
    name: str,
    expected_disposition: str,
) -> None:
    response, _ = await _serve(inline="1", name=name, mime_type=mime_type)

    assert response.status_code == 200
    assert response.headers["Content-Disposition"] == expected_disposition
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Content-Security-Policy"] == "sandbox"
    assert response.headers["Cache-Control"] == "no-store"
    assert await _response_body(response) == b"0123456789"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mime_type",
    ["application/pdf", "text/html", "image/svg+xml", "application/octet-stream"],
)
async def test_inline_falls_back_to_attachment_for_active_or_unknown_content(
    mime_type: str,
) -> None:
    response, _ = await _serve(inline="true", mime_type=mime_type)

    assert response.status_code == 200
    assert response.headers["Content-Disposition"] == (
        "attachment; filename*=UTF-8''media.bin"
    )
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["Content-Security-Policy"] == "sandbox"


@pytest.mark.asyncio
async def test_safe_media_defaults_to_attachment_without_inline() -> None:
    response, _ = await _serve(name="image.png", mime_type="image/png")

    assert response.headers["Content-Disposition"] == (
        "attachment; filename*=UTF-8''image.png"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("range_header", "expected_body", "expected_content_range", "expected_read"),
    [
        ("bytes=2-5", b"2345", "bytes 2-5/10", (2, 4)),
        ("bytes=-3", b"789", "bytes 7-9/10", (7, 3)),
        ("bytes=4-", b"456789", "bytes 4-9/10", (4, 6)),
    ],
)
async def test_download_serves_single_byte_range(
    range_header: str,
    expected_body: bytes,
    expected_content_range: str,
    expected_read: tuple[int, int],
) -> None:
    response, reader = await _serve(range_header=range_header, inline="1")

    assert response.status_code == 206
    assert response.headers["Content-Length"] == str(len(expected_body))
    assert response.headers["Accept-Ranges"] == "bytes"
    assert response.headers["Content-Range"] == expected_content_range
    assert await _response_body(response) == expected_body
    assert reader.calls == [(0, 1), expected_read]


@pytest.mark.asyncio
async def test_head_honors_range_without_streaming_body() -> None:
    response, reader = await _serve(method="HEAD", range_header="bytes=2-5")

    assert response.status_code == 206
    assert response.headers["Content-Length"] == "4"
    assert response.headers["Content-Range"] == "bytes 2-5/10"
    assert response.body == b""
    assert reader.calls == [(0, 1)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "range_header",
    ["items=0-1", "bytes=10-12", "bytes=0-1,3-4"],
)
async def test_download_rejects_invalid_byte_range(range_header: str) -> None:
    response, reader = await _serve(range_header=range_header)

    assert response.status_code == 416
    assert response.headers["Content-Range"] == "bytes */10"
    assert response.headers["Content-Length"] == "0"
    assert response.body == b""
    assert reader.calls == [(0, 1)]


@pytest.mark.asyncio
async def test_read_failure_status_is_preserved() -> None:
    async def _forbidden(_offset: int, _limit: int) -> JSONResponse:
        return JSONResponse(status_code=403, content={"error": "path_outside_workspace"})

    response = await serve_file_chunks(_request(), _forbidden)

    assert response.status_code == 403
    assert json.loads(response.body) == {"error": "path_outside_workspace"}
