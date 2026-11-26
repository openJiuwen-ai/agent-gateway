# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import hashlib
import io
import zipfile
from dataclasses import replace
from functools import partial
from types import SimpleNamespace

import httpx
import pytest

from agent_gateway.extensions.sdk import connector_source, skill_source

_URL = "https://example.invalid/package.zip"


class _Chunks(httpx.AsyncByteStream):
    def __init__(self, *chunks):
        self.chunks = chunks
        self.read_count = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.read_count += 1
            yield chunk


@pytest.fixture(params=["skill", "connector"])
def source(request, monkeypatch):
    if request.param == "skill":
        descriptor = skill_source.ArtifactDescriptor("source", "package", "1", _URL)
        provider = skill_source.SkillSourceProvider
        policy = skill_source.DownloadPolicy
        error = skill_source.SkillDownloadError
    else:
        descriptor = connector_source.ConnectorArtifactDescriptor("source", "package", "1", _URL)
        provider = connector_source.ConnectorSourceProvider
        policy = connector_source.DownloadPolicy
        error = connector_source.ConnectorDownloadError

    async def download(stream, headers, limit, sha256=None):
        transport = httpx.MockTransport(lambda _: httpx.Response(200, headers=headers, stream=stream))
        monkeypatch.setattr(httpx, "AsyncClient", partial(httpx.AsyncClient, transport=transport))
        settings = SimpleNamespace(allow_insecure_http=False, verify_ssl=True)
        return await provider.download_artifact(
            settings, replace(descriptor, content_sha256=sha256), policy(max_bytes=limit)
        )

    return download, error


@pytest.mark.asyncio
async def test_download_accepts_exact_size_limit(source):
    download, _ = source
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("package.txt", "ok")
    body = buffer.getvalue()
    stream = _Chunks(body[:3], body[3:])
    assert await download(stream, {"content-length": str(len(body))}, len(body), hashlib.sha256(body).hexdigest()) == body


@pytest.mark.asyncio
async def test_download_rejects_declared_size_before_reading(source):
    download, error = source
    stream = _Chunks(b"unused")
    with pytest.raises(error) as exc_info:
        await download(stream, {"content-length": "11"}, 10)
    assert exc_info.value.code == "content_too_large"
    assert stream.read_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [{}, {"content-length": "unknown"}, {"content-length": "1"}])
async def test_download_stops_when_stream_exceeds_limit(source, headers):
    download, error = source
    stream = _Chunks(b"ab", b"c", b"must not be read")
    with pytest.raises(error) as exc_info:
        await download(stream, headers, 2)
    assert exc_info.value.code == "content_too_large"
    assert stream.read_count == 2
