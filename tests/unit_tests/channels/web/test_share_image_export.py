# Copyright (c) Huawei Technologies Co., Ltd. 2025-2026. All rights reserved.

"""Gateway share-image jobs, rendering, validation, and personal ASGI API."""

from __future__ import annotations

import io
import json
import logging
import shutil
import struct
import subprocess
import sys
import threading
import time
import types
import zipfile
import zlib
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from agent_gateway.channel_manager.web import personal_share_api as api
from agent_gateway.channel_manager.web import share_image_export
from agent_gateway.channel_manager.web.share_image_export import (
    ShareImageExportManager,
    _validate_png,
    _validate_zip,
)


def _png_chunk(chunk_type: bytes, data: bytes) -> bytes:
    return (
        struct.pack(">I", len(data))
        + chunk_type
        + data
        + struct.pack(">I", zlib.crc32(chunk_type + data))
    )


def _test_png(*, height: int = 1) -> bytes:
    ihdr = struct.pack(">IIBBBBB", 2250, height, 8, 6, 0, 0, 0)
    pixels = b"\x00" + b"\x00\x00\x00\xff" * 2250
    return (
        b"\x89PNG\r\n\x1a\n"
        + _png_chunk(b"IHDR", ihdr)
        + _png_chunk(b"IDAT", zlib.compress(pixels))
        + _png_chunk(b"IEND", b"")
    )


def _test_zip_bytes() -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("share-part-01-of-02.png", _test_png())
        archive.writestr("share-part-02-of-02.png", _test_png())
    return output.getvalue()


def _write_test_zip(path: Path) -> None:
    Path(path).write_bytes(_test_zip_bytes())


def _wait_for_terminal_state(manager: ShareImageExportManager, job_id: str) -> dict:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        status = manager.get_status(job_id)
        assert status is not None
        if status["state"] in {"completed", "failed"}:
            return status
        time.sleep(0.01)
    raise AssertionError("share image export job did not finish")


def _cleanup_manager(manager: ShareImageExportManager) -> None:
    for job in list(manager._jobs.values()):
        shutil.rmtree(job.directory, ignore_errors=True)


def test_share_image_module_cli_is_executable(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[4]
    module = "agent_gateway.channel_manager.web.share_image_export"
    help_result = subprocess.run(
        [sys.executable, "-m", module, "--help"],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert help_result.returncode == 0, help_result.stderr
    assert "--allow-browser-render" in help_result.stdout

    result = tmp_path / "share.png"
    result.write_bytes(_test_png())
    validated = subprocess.run(
        [sys.executable, "-m", module, "--validate-result", str(result)],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert validated.returncode == 0, validated.stderr
    events = [json.loads(line) for line in validated.stdout.splitlines() if line.startswith("{")]
    assert events[-1] == {"phase": "completed", "filename": "share.png"}

    result.write_bytes(b"not a PNG")
    invalid = subprocess.run(
        [sys.executable, "-m", module, "--validate-result", str(result)],
        cwd=root,
        capture_output=True,
        text=True,
        timeout=45,
    )
    assert invalid.returncode != 0


@pytest.mark.parametrize("error", [None, "render_failed\n下一行"])
def test_share_image_cli_flushes_json_protocol_events(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    error: str | None,
) -> None:
    events: list[dict[str, str]] = []

    class ProtocolOutput:
        pending = ""

        def write(self, text: str) -> int:
            self.pending += text
            return len(text)

        def flush(self) -> None:
            assert self.pending.endswith("\n")
            assert len(self.pending.splitlines()) == 1
            events.append(json.loads(self.pending))
            self.pending = ""

    def renderer(*, base_url, job_id, output_path, on_phase) -> Path:
        assert base_url == "http://127.0.0.1:5173"
        assert job_id == "job-1"
        assert output_path == tmp_path / "share.png"
        on_phase("rendering")
        assert events == [{"phase": "rendering"}]
        if error is not None:
            raise RuntimeError(error)
        return output_path.with_suffix(".zip")

    monkeypatch.setattr(share_image_export.sys, "stdout", ProtocolOutput())
    monkeypatch.setattr(share_image_export.sys, "argv", [
        "share_image_export",
        "--base-url", "http://127.0.0.1:5173",
        "--job-id", "job-1",
        "--output", str(tmp_path / "share.png"),
    ])
    monkeypatch.setattr(share_image_export, "render_share_image", renderer)

    previous_disable = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        assert share_image_export._run_cli() == (1 if error is not None else 0)
    finally:
        logging.disable(previous_disable)
    expected_result = (
        {"error": error}
        if error is not None
        else {"phase": "completed", "filename": "share.zip"}
    )
    assert events == [{"phase": "rendering"}, expected_result]


@pytest.mark.parametrize("failure_stage", ["write", "flush"])
def test_share_image_cli_propagates_output_failures_and_closes_handler(
    monkeypatch: pytest.MonkeyPatch,
    failure_stage: str,
) -> None:
    failure = OSError("protocol_output_failed")
    handlers = []
    closed_handlers = []
    handler_class = share_image_export._CliEventHandler

    class FailingOutput:
        def write(self, text: str) -> int:
            if failure_stage == "write":
                raise failure
            return len(text)

        def flush(self) -> None:
            if failure_stage == "flush":
                raise failure

    class TrackingHandler(handler_class):
        def __init__(self, stream):
            super().__init__(stream)
            handlers.append(self)

        def close(self) -> None:
            closed_handlers.append(self)
            super().close()

    monkeypatch.setattr(share_image_export.sys, "stdout", FailingOutput())
    monkeypatch.setattr(share_image_export, "_CliEventHandler", TrackingHandler)

    with pytest.raises(OSError) as raised:
        share_image_export._write_cli_event({"phase": "rendering"})

    assert raised.value is failure
    assert len(handlers) == 1
    assert closed_handlers == handlers


def test_share_image_export_job_freezes_snapshot_and_publishes_result() -> None:
    rendered: dict[str, object] = {}

    def renderer(*, base_url, job_id, output_path, on_phase, render_auth) -> Path:
        rendered.update(
            base_url=base_url,
            job_id=job_id,
            output_path=output_path,
            render_auth=render_auth,
        )
        on_phase("rendering")
        output_path.write_bytes(b"png-result")
        return output_path

    manager = ShareImageExportManager(renderer=renderer)
    try:
        created = manager.create_job(
            session_id="session-1",
            snapshot={"session_id": "session-1", "records": [{"role": "user", "content": "hello"}]},
            filename="share.png",
            locale="en",
            base_url="http://127.0.0.1:5173",
        )
        status = _wait_for_terminal_state(manager, created["job_id"])

        assert status == {
            "job_id": created["job_id"],
            "session_id": "session-1",
            "filename": "share.png",
            "state": "completed",
            "phase": "completed",
            "error": None,
            "renderer": "server",
            "progress": None,
            "locale": "en",
        }
        snapshot_path = manager.get_snapshot_path(created["job_id"])
        assert snapshot_path is not None
        assert json.loads(snapshot_path.read_text(encoding="utf-8")) == {
            "filename": "share.png",
            "locale": "en",
            "snapshot": {
                "session_id": "session-1",
                "records": [{"role": "user", "content": "hello"}],
            },
        }
        result = manager.get_result(created["job_id"])
        assert result is not None
        assert result[0].read_bytes() == b"png-result"
        assert rendered["base_url"] == "http://127.0.0.1:5173"
        assert rendered["job_id"] == created["job_id"]
        assert rendered["render_auth"] is None
    finally:
        _cleanup_manager(manager)


def test_share_image_export_job_reports_renderer_failure() -> None:
    def renderer(**_kwargs) -> Path:
        raise RuntimeError("renderer_failed")

    manager = ShareImageExportManager(renderer=renderer)
    try:
        created = manager.create_job(
            session_id="session-2",
            snapshot={"session_id": "session-2", "records": []},
            filename="share.png",
            locale="zh",
            base_url="http://127.0.0.1:5173",
        )
        status = _wait_for_terminal_state(manager, created["job_id"])

        assert status["state"] == "failed"
        assert status["phase"] == "failed"
        assert status["error"] == "renderer_failed"
        assert manager.get_result(created["job_id"]) is None
        assert manager.get_snapshot_path(created["job_id"]) is not None
    finally:
        _cleanup_manager(manager)


def test_share_image_export_filename_cannot_escape_job_directory() -> None:
    def renderer(*, output_path: Path, **_kwargs) -> Path:
        output_path.write_bytes(b"png-result")
        return output_path

    manager = ShareImageExportManager(renderer=renderer)
    try:
        created = manager.create_job(
            session_id="session-3",
            snapshot={"session_id": "session-3", "records": []},
            filename="../../outside.png",
            locale="zh",
            base_url="http://127.0.0.1:5173",
        )
        status = _wait_for_terminal_state(manager, created["job_id"])
        result = manager.get_result(created["job_id"])
        assert status["filename"] == "outside.png"
        assert result is not None
        assert result[0].name == "outside.png"
        assert result[0].parent == manager.get_snapshot_path(created["job_id"]).parent

        created = manager.create_job(
            session_id="session-4",
            snapshot={"session_id": "session-4", "records": []},
            filename="..",
            locale="zh",
            base_url="http://127.0.0.1:5173",
        )
        status = _wait_for_terminal_state(manager, created["job_id"])
        result = manager.get_result(created["job_id"])
        assert status["filename"] == "jiuwenswarm-share.png"
        assert result is not None
        assert result[0].parent == manager.get_snapshot_path(created["job_id"]).parent
    finally:
        _cleanup_manager(manager)


def test_share_image_export_reuses_active_job_for_same_session() -> None:
    started = threading.Event()
    release = threading.Event()

    def renderer(*, output_path: Path, **_kwargs) -> Path:
        started.set()
        assert release.wait(timeout=2)
        output_path.write_bytes(b"png-result")
        return output_path

    manager = ShareImageExportManager(renderer=renderer)
    try:
        first = manager.create_job(
            session_id="session-5",
            snapshot={"session_id": "session-5", "records": [{"content": "first"}]},
            filename="first.png",
            locale="zh",
            base_url="http://127.0.0.1:5173",
        )
        assert started.wait(timeout=2)
        duplicate = manager.create_job(
            session_id="session-5",
            snapshot={"session_id": "session-5", "records": [{"content": "duplicate"}]},
            filename="duplicate.png",
            locale="zh",
            base_url="http://127.0.0.1:5173",
        )

        assert first["reused"] is False
        assert duplicate["reused"] is True
        assert duplicate["job_id"] == first["job_id"]
        assert manager.get_active_status("session-5")["job_id"] == first["job_id"]

        release.set()
        status = _wait_for_terminal_state(manager, first["job_id"])
        assert status["state"] == "completed"
        assert manager.get_active_status("session-5") is None
    finally:
        release.set()
        _cleanup_manager(manager)


def test_share_image_export_job_publishes_renderer_selected_archive() -> None:
    def renderer(*, output_path: Path, **_kwargs) -> Path:
        archive_path = output_path.with_suffix(".zip")
        archive_path.write_bytes(b"zip-result")
        return archive_path

    manager = ShareImageExportManager(renderer=renderer)
    try:
        created = manager.create_job(
            session_id="session-6",
            snapshot={"session_id": "session-6", "records": []},
            filename="share.png",
            locale="zh",
            base_url="http://127.0.0.1:5173",
        )
        status = _wait_for_terminal_state(manager, created["job_id"])

        assert status["state"] == "completed"
        assert status["filename"] == "share.zip"
        result = manager.get_result(created["job_id"])
        assert result is not None
        assert result[0].read_bytes() == b"zip-result"
        assert result[1] == "share.zip"
    finally:
        _cleanup_manager(manager)


def test_share_image_export_validates_png_height_and_zip_parts(tmp_path: Path) -> None:
    png_path = tmp_path / "share.png"
    png_path.write_bytes(_test_png())
    _validate_png(png_path)

    max_height_path = tmp_path / "max-height.png"
    max_height_path.write_bytes(_test_png(height=128_000))
    _validate_png(max_height_path)

    too_tall_path = tmp_path / "too-tall.png"
    too_tall_path.write_bytes(_test_png(height=128_001))
    with pytest.raises(RuntimeError, match="share_export_png_height_unsupported"):
        _validate_png(too_tall_path)

    archive_path = tmp_path / "share.zip"
    archive_path.write_bytes(_test_zip_bytes())
    _validate_zip(archive_path)

    invalid_archive_path = tmp_path / "invalid.zip"
    with zipfile.ZipFile(invalid_archive_path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("share.png", _test_png())
    with pytest.raises(RuntimeError, match="share_export_zip_parts_missing"):
        _validate_zip(invalid_archive_path)

    out_of_order_archive_path = tmp_path / "out-of-order.zip"
    with zipfile.ZipFile(out_of_order_archive_path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("share-part-02-of-02.png", _test_png())
        archive.writestr("share-part-01-of-02.png", _test_png())
    with pytest.raises(RuntimeError, match="share_export_zip_part_invalid"):
        _validate_zip(out_of_order_archive_path)

    inconsistent_archive_path = tmp_path / "inconsistent.zip"
    with zipfile.ZipFile(inconsistent_archive_path, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("share-part-01-of-03.png", _test_png())
        archive.writestr("share-part-02-of-03.png", _test_png())
    with pytest.raises(RuntimeError, match="share_export_zip_part_invalid"):
        _validate_zip(inconsistent_archive_path)


class _FakeResponse:
    def __init__(self, status: int) -> None:
        self.status = status


class _FakeDownload:
    def __init__(self, writer: Callable[[Path], None]) -> None:
        self._writer = writer

    def save_as(self, path: Path) -> None:
        self._writer(Path(path))

    def failure(self):
        return None


class _FakeDownloadWaiter:
    def __init__(self, download: _FakeDownload) -> None:
        self.value = download

    def __enter__(self) -> "_FakeDownloadWaiter":
        return self

    def __exit__(self, *exc_info) -> bool:
        return False


class _FakePage:
    def __init__(
        self,
        *,
        goto_status: int | None = 200,
        states: list[dict | None] | None = None,
        state_fn: Callable[[int], dict | None] | None = None,
        download_writer: Callable[[Path], None] | None = None,
        page_error: str | None = None,
    ) -> None:
        self.goto_calls: list[tuple[str, str | None, int | None]] = []
        self.evaluate_calls: list[str] = []
        self.wait_calls: list[int] = []
        self.pageerror_handler: Callable[[str], None] | None = None
        self._goto_status = goto_status
        self._states = list(states or [])
        self._state_fn = state_fn
        self._download_writer = download_writer
        self._page_error = page_error
        self._evaluate_index = 0

    def on(self, event: str, handler: Callable[[str], None]) -> None:
        if event == "pageerror":
            self.pageerror_handler = handler

    def _emit_page_error(self) -> None:
        if self._page_error is not None and self.pageerror_handler is not None:
            self.pageerror_handler(self._page_error)

    def goto(self, url: str, wait_until=None, timeout=None):
        self.goto_calls.append((url, wait_until, timeout))
        self._emit_page_error()
        if self._goto_status is None:
            return None
        return _FakeResponse(self._goto_status)

    def evaluate(self, script: str):
        self.evaluate_calls.append(script)
        index = self._evaluate_index
        self._evaluate_index += 1
        if self._state_fn is not None:
            return self._state_fn(index)
        if not self._states:
            return None
        if index < len(self._states):
            return self._states[index]
        return self._states[-1]

    def wait_for_timeout(self, ms: int) -> None:
        self.wait_calls.append(ms)
        time.sleep(ms / 1000)

    def expect_download(self, timeout=None) -> _FakeDownloadWaiter:
        assert self._download_writer is not None
        return _FakeDownloadWaiter(_FakeDownload(self._download_writer))


class _FakeContext:
    def __init__(self, page: _FakePage) -> None:
        self.page = page
        self.cookies: list[dict] = []
        self.events: list[str] = []
        self.closed = False

    def add_cookies(self, cookies: list[dict]) -> None:
        self.events.append("add_cookies")
        self.cookies.extend(cookies)

    def new_page(self) -> _FakePage:
        self.events.append("new_page")
        return self.page

    def close(self) -> None:
        self.closed = True


class _FakeBrowser:
    def __init__(self, context: _FakeContext) -> None:
        self.context = context
        self.new_context_kwargs: dict = {}
        self.closed = False

    def new_context(self, **kwargs) -> _FakeContext:
        self.new_context_kwargs = kwargs
        return self.context

    def close(self) -> None:
        self.closed = True


class _FakeChromium:
    def __init__(self, browser: _FakeBrowser) -> None:
        self._browser = browser
        self.launch_kwargs: dict = {}

    def launch(self, **kwargs) -> _FakeBrowser:
        self.launch_kwargs = kwargs
        return self._browser


class _FakePlaywright:
    def __init__(self, browser: _FakeBrowser) -> None:
        self.chromium = _FakeChromium(browser)


class _FakeSyncPlaywright:
    def __init__(self, browser: _FakeBrowser) -> None:
        self._browser = browser

    def __enter__(self) -> _FakePlaywright:
        return _FakePlaywright(self._browser)

    def __exit__(self, *exc_info) -> bool:
        return False


@dataclass
class _RenderHarness:
    page: _FakePage
    context: _FakeContext
    browser: _FakeBrowser
    output_path: Path


def _make_render_harness(
    monkeypatch: pytest.MonkeyPatch,
    page: _FakePage,
    tmp_path: Path,
) -> _RenderHarness:
    context = _FakeContext(page)
    browser = _FakeBrowser(context)
    playwright_api = types.ModuleType("playwright.sync_api")
    playwright_api.sync_playwright = lambda: _FakeSyncPlaywright(browser)  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "playwright", types.ModuleType("playwright"))
    monkeypatch.setitem(sys.modules, "playwright.sync_api", playwright_api)
    monkeypatch.setattr(share_image_export, "_configured_browser", lambda: (None, "chrome"))
    return _RenderHarness(
        page=page,
        context=context,
        browser=browser,
        output_path=tmp_path / "share.png",
    )


def _call_render(harness: _RenderHarness, **kwargs) -> Path:
    return share_image_export.render_share_image(
        base_url="http://127.0.0.1:5173",
        job_id="job-render-1",
        output_path=harness.output_path,
        **kwargs,
    )


_READY_ZIP_STATE: list[dict] = [{"status": "ready", "filename": "share.zip"}]


def test_render_without_auth_opens_runner_and_validates_download(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    page = _FakePage(states=_READY_ZIP_STATE, download_writer=_write_test_zip)
    harness = _make_render_harness(monkeypatch, page, tmp_path)

    result = _call_render(harness)

    assert result.is_file()
    assert result.suffix == ".zip"
    _validate_zip(result)
    assert harness.context.events == ["new_page"]
    assert harness.context.cookies == []
    assert harness.context.closed
    assert harness.browser.closed
    assert page.goto_calls[0][0].startswith(
        "http://127.0.0.1:5173/share-export-runner?job_id=job-render-1",
    )
    assert harness.browser.new_context_kwargs["accept_downloads"] is True


@pytest.mark.parametrize("status", [403, 404, 500])
def test_render_http_error_fails_fast(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    status: int,
) -> None:
    page = _FakePage(goto_status=status)
    harness = _make_render_harness(monkeypatch, page, tmp_path)

    started = time.monotonic()
    with pytest.raises(RuntimeError, match=f"share_export_runner_http_{status}"):
        _call_render(harness)

    assert time.monotonic() - started < 5
    assert page.evaluate_calls == []
    assert harness.context.closed
    assert harness.browser.closed


def test_render_missing_goto_response_fails_fast(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    page = _FakePage(goto_status=None)
    harness = _make_render_harness(monkeypatch, page, tmp_path)

    with pytest.raises(RuntimeError, match="share_export_runner_response_missing"):
        _call_render(harness)


def test_render_runner_not_initialized_fails_within_init_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    page = _FakePage(states=[None])
    harness = _make_render_harness(monkeypatch, page, tmp_path)

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="share_export_runner_not_initialized"):
        _call_render(harness, init_timeout_seconds=0.2, poll_interval_seconds=0.05)

    assert time.monotonic() - started < 5
    assert harness.context.closed


def test_render_page_error_during_init_fails_fast_and_stays_single_line(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    page = _FakePage(states=[None], page_error="Uncaught TypeError:\nboom\nat bundle.js:1")
    harness = _make_render_harness(monkeypatch, page, tmp_path)

    with pytest.raises(RuntimeError, match="share_export_page_error") as exc_info:
        _call_render(harness, init_timeout_seconds=5, poll_interval_seconds=0.05)

    message = str(exc_info.value)
    assert "boom" in message
    assert "\n" not in message


def test_render_runner_error_status_fails_immediately(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    page = _FakePage(states=[
        {"status": "rendering"},
        {"status": "error", "error": "share_export_snapshot_http_404"},
    ])
    harness = _make_render_harness(monkeypatch, page, tmp_path)

    started = time.monotonic()
    with pytest.raises(RuntimeError, match="share_export_snapshot_http_404"):
        _call_render(harness, poll_interval_seconds=0.05)

    assert time.monotonic() - started < 5
    assert page.wait_calls == [50]


def test_render_defaults_allow_long_generation_without_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = {"value": 0.0}
    page = _FakePage(
        states=[
            {"status": "rendering"},
            {"status": "rendering"},
            {"status": "ready", "filename": "share.zip"},
        ],
        download_writer=_write_test_zip,
    )
    harness = _make_render_harness(monkeypatch, page, tmp_path)
    monkeypatch.setattr(share_image_export.time, "monotonic", lambda: clock["value"])
    monkeypatch.setattr(
        page,
        "wait_for_timeout",
        lambda _ms: clock.update(value=clock["value"] + 86_400),
    )

    result = _call_render(harness)

    assert clock["value"] == 2 * 86_400
    assert result.is_file()
    assert harness.context.closed
    assert harness.browser.closed


@contextmanager
def _browser_job() -> Iterator[tuple[ShareImageExportManager, dict]]:
    def unexpected_renderer(**kwargs):
        raise AssertionError("A browser-mode job must not launch a server browser")

    manager = ShareImageExportManager(renderer=unexpected_renderer)
    snapshot = {"session_id": "remote", "records": [{"content": "captured"}]}
    created = manager.create_job(
        session_id="remote",
        snapshot=snapshot,
        filename="share.png",
        locale="zh",
        base_url="http://localhost",
        renderer="browser",
    )
    snapshot["records"].append({"content": "later"})
    try:
        yield manager, created
    finally:
        _cleanup_manager(manager)


def _claim_browser_download(manager: ShareImageExportManager, created: dict) -> str:
    claim = manager.update_browser_job(created["job_id"], {
        "client_token": created["client_token"],
        "claim_download": True,
    })
    assert claim["download_claimed"] is True
    attempt_id = claim["download_attempt_id"]
    assert len(attempt_id) == 32
    assert all(char in "0123456789abcdef" for char in attempt_id)
    return attempt_id


def test_browser_export_owns_fixed_snapshot_and_authenticated_progress() -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        assert created["state"] == "queued"
        assert created["renderer"] == "browser"
        assert "client_token" not in manager.get_status(job_id)
        saved = json.loads(manager.get_snapshot_path(job_id).read_text(encoding="utf-8"))
        assert saved["snapshot"]["records"] == [{"content": "captured"}]

        status = manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "running",
            "phase": "rendering",
            "progress": 40,
        })
        assert status["progress"] == 40
        png = _test_png()
        status = manager.store_browser_result(
            job_id, token, "share.png", io.BytesIO(png), len(png),
        )
        assert status["state"] == "completed"
        assert manager.get_active_status("remote") is None
        assert manager.get_result(job_id)[0].read_bytes() == png

        late = manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "failed",
            "phase": "failed",
        })
        assert late["state"] == "completed"
        attempt_id = _claim_browser_download(manager, created)
        assert manager.mark_browser_download_started(job_id, attempt_id) is True
        assert manager.get_status(job_id)["phase"] == "download_started"


@pytest.mark.parametrize("token", ["wrong", "中文", None])
def test_browser_export_rejects_unauthenticated_updates(token) -> None:
    with _browser_job() as (manager, created):
        with pytest.raises(PermissionError):
            manager.update_browser_job(created["job_id"], {
                "client_token": token,
                "state": "completed",
                "phase": "completed",
            })
        assert manager.get_status(created["job_id"])["state"] == "queued"


@pytest.mark.parametrize("update", [
    {"state": {}, "phase": "rendering"},
    {"state": "running", "phase": []},
    {"state": "completed", "phase": "rendering"},
    {"state": "running", "phase": "rendering", "progress": True},
    {"state": "running", "phase": "rendering", "progress": 101},
])
def test_browser_export_validates_transition_payload(update: dict) -> None:
    with _browser_job() as (manager, created):
        with pytest.raises(ValueError):
            manager.update_browser_job(created["job_id"], {
                **update,
                "client_token": created["client_token"],
            })
        assert manager.get_status(created["job_id"])["state"] == "queued"


def test_browser_export_abandoned_snapshot_expires(monkeypatch: pytest.MonkeyPatch) -> None:
    with _browser_job() as (manager, created):
        snapshot = manager.get_snapshot_path(created["job_id"])
        now = share_image_export.time.time()
        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 3_601)

        assert manager.get_status(created["job_id"]) is None
        assert not snapshot.parent.exists()


def test_missing_explicit_browser_does_not_install_or_probe_other_browsers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        share_image_export,
        "get_config",
        lambda: {"browser": {"chrome_path": "/missing-export-browser"}},
    )
    assert share_image_export.available_share_browser() is None


def test_available_configured_browser_is_reused(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    executable = tmp_path / "chromium"
    executable.touch()
    executable.chmod(0o755)
    monkeypatch.setattr(
        share_image_export,
        "get_config",
        lambda: {"browser": {"chrome_path": str(executable)}},
    )
    assert share_image_export.available_share_browser() == {
        "headless": True,
        "executable_path": str(executable),
    }


@pytest.mark.parametrize("data,size", [
    (b"not a png", 9),
    (_test_png(), 10**10),
    (_test_png(), len(_test_png()) + 1),
])
def test_browser_result_upload_rejects_invalid_or_incomplete_files(data: bytes, size: int) -> None:
    with _browser_job() as (manager, created):
        with pytest.raises((ValueError, RuntimeError)):
            manager.store_browser_result(
                created["job_id"],
                created["client_token"],
                "share.png",
                io.BytesIO(data),
                size,
            )
        assert manager.get_result(created["job_id"]) is None
        snapshot_path = manager.get_snapshot_path(created["job_id"])
        assert snapshot_path is not None
        assert not list(snapshot_path.parent.glob("*.partial"))


def test_duplicate_browser_upload_cannot_overwrite_completed_result() -> None:
    with _browser_job() as (manager, created):
        data = _test_png()
        manager.store_browser_result(
            created["job_id"], created["client_token"], "share.png", io.BytesIO(data), len(data),
        )
        with pytest.raises(ValueError, match="already_finished"):
            manager.store_browser_result(
                created["job_id"],
                created["client_token"],
                "share.png",
                io.BytesIO(b"corrupt"),
                7,
            )
        assert manager.get_status(created["job_id"])["state"] == "completed"
        assert manager.get_result(created["job_id"])[0].read_bytes() == data


def test_browser_renderer_claim_is_atomic_and_duplicate_cannot_fail_the_owner() -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        claim = {
            "client_token": token,
            "state": "running",
            "phase": "loading_snapshot",
            "claim": True,
        }
        assert manager.update_browser_job(job_id, claim)["state"] == "running"
        with pytest.raises(ValueError, match="already_started"):
            manager.update_browser_job(job_id, claim)
        assert manager.get_status(job_id)["state"] == "running"
        assert "client_token" not in manager.get_active_status("remote")
        resumed = manager.get_active_status("remote", include_client_token=True)
        assert resumed["client_token"] == token
        assert resumed["job_id"] == job_id


@pytest.mark.parametrize("claim_first", [False, True])
def test_closing_browser_page_cancels_its_own_claim_in_either_request_order(
    claim_first: bool,
) -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        claim_id = "b" * 32
        claim = {
            "client_token": token,
            "state": "running",
            "phase": "loading_snapshot",
            "claim": True,
            "claim_id": claim_id,
        }
        if claim_first:
            manager.update_browser_job(job_id, claim)
        ended = manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "failed",
            "phase": "failed",
            "error": "share_export_tab_closed",
            "expected_claim_id": claim_id,
        })
        assert ended["state"] == "failed"
        assert ended["error"] == "share_export_tab_closed"
        assert manager.get_active_status("remote") is None
        with pytest.raises(ValueError, match="already_started"):
            manager.update_browser_job(job_id, claim)


def test_rejected_browser_page_cannot_cancel_or_fail_the_claim_owner() -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        claim = {
            "client_token": token,
            "state": "running",
            "phase": "loading_snapshot",
            "claim": True,
        }
        owner = manager.update_browser_job(job_id, {**claim, "claim_id": "b" * 32})
        with pytest.raises(ValueError, match="already_started"):
            manager.update_browser_job(job_id, {**claim, "claim_id": "c" * 32})
        for error in ["share_export_tab_closed", "share_export_status_timeout"]:
            ignored = manager.update_browser_job(job_id, {
                "client_token": token,
                "state": "failed",
                "phase": "failed",
                "error": error,
                "expected_claim_id": "c" * 32,
            })
            assert ignored == owner
        assert manager.get_active_status("remote")["job_id"] == job_id


def test_closing_browser_page_cannot_fail_its_published_artifact() -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        claim_id = "b" * 32
        manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "running",
            "phase": "loading_snapshot",
            "claim": True,
            "claim_id": claim_id,
        })
        png = _test_png()
        published = manager.store_browser_result(
            job_id, token, "share.png", io.BytesIO(png), len(png),
        )
        ended = manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "failed",
            "phase": "failed",
            "error": "share_export_tab_closed",
            "expected_claim_id": claim_id,
        })
        assert ended == published
        assert manager.get_result(job_id)[0].read_bytes() == png


@pytest.mark.parametrize("key", ["claim_id", "expected_claim_id"])
@pytest.mark.parametrize("value", [None, "", "x" * 32, "a" * 31, ["a" * 32]])
def test_browser_claim_identity_rejects_invalid_values(key: str, value: object) -> None:
    with _browser_job() as (manager, created):
        with pytest.raises(ValueError, match="client_state_invalid"):
            manager.update_browser_job(created["job_id"], {
                "client_token": created["client_token"],
                "state": "failed",
                "phase": "failed",
                key: value,
            })
        assert manager.get_status(created["job_id"])["state"] == "queued"


def test_closing_unstarted_browser_job_allows_fresh_snapshot_and_rejects_late_results() -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        old_directory = manager.get_snapshot_path(job_id).parent
        ended = manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "failed",
            "phase": "failed",
            "error": "share_export_tab_closed",
            "expected_state": "queued",
        })
        assert ended["state"] == "failed"
        assert manager.get_active_status("remote") is None

        snapshot = {"session_id": "remote", "records": [{"content": "latest conversation"}]}
        fresh = manager.create_job(
            session_id="remote",
            snapshot=snapshot,
            filename="fresh.png",
            locale="zh",
            base_url="http://localhost",
            renderer="browser",
        )
        assert fresh["job_id"] != job_id
        fresh_snapshot = manager.get_snapshot_path(fresh["job_id"])
        assert json.loads(fresh_snapshot.read_text(encoding="utf-8"))["snapshot"] == snapshot

        late = manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "running",
            "phase": "rendering",
            "progress": 50,
        })
        assert late["state"] == "failed"
        with pytest.raises(ValueError, match="already_finished"):
            png = _test_png()
            manager.store_browser_result(job_id, token, "share.png", io.BytesIO(png), len(png))
        assert manager.get_active_status("remote")["job_id"] == fresh["job_id"]
        assert old_directory.exists()


@pytest.mark.parametrize("state", ["running", "completed"])
def test_queued_tab_cancellation_cannot_cancel_a_claimed_or_published_job(state: str) -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "running",
            "phase": "loading_snapshot",
            "claim": True,
        })
        if state == "completed":
            png = _test_png()
            manager.store_browser_result(job_id, token, "share.png", io.BytesIO(png), len(png))
        result = manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "failed",
            "phase": "failed",
            "error": "share_export_tab_closed",
            "expected_state": "queued",
        })
        assert result["state"] == state
        assert result["error"] is None


def test_running_browser_job_survives_while_heartbeat_reports_continue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        now = share_image_export.time.time()
        manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "running",
            "phase": "loading_snapshot",
            "claim": True,
        })
        for elapsed in (43_200, 86_400):
            monkeypatch.setattr(
                share_image_export.time,
                "time",
                lambda elapsed=elapsed: now + elapsed,
            )
            manager.update_browser_job(job_id, {
                "client_token": token,
                "state": "running",
                "phase": "rendering",
                "progress": 50,
            })
            assert manager.get_status(job_id)["state"] == "running"
            assert manager.get_status(job_id)["error"] is None
        assert manager.get_active_status("remote")["job_id"] == job_id


def test_running_browser_job_without_heartbeat_expires_and_unlocks_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        directory = manager.get_snapshot_path(job_id).parent
        now = share_image_export.time.time()
        manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "running",
            "phase": "loading_snapshot",
            "claim": True,
        })
        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 3_601)

        assert manager.get_status(job_id) is None
        assert not directory.exists()
        assert manager.get_active_status("remote") is None
        restarted = manager.create_job(
            session_id="remote",
            snapshot={"session_id": "remote", "records": []},
            filename="share.png",
            locale="zh",
            base_url="http://localhost",
            renderer="browser",
        )
        assert restarted["reused"] is False


def test_server_save_phase_survives_past_the_browser_ttl(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    release = threading.Event()
    clock = {"now": share_image_export.time.time()}
    monkeypatch.setattr(share_image_export.time, "time", lambda: clock["now"])

    def slow_writer(path: Path) -> None:
        assert release.wait(timeout=5)
        _write_test_zip(path)

    page = _FakePage(states=_READY_ZIP_STATE, download_writer=slow_writer)
    _make_render_harness(monkeypatch, page, tmp_path)
    manager = ShareImageExportManager()
    created = manager.create_job(
        session_id="remote",
        snapshot={"session_id": "remote", "records": []},
        filename="share.png",
        locale="zh",
        base_url="http://localhost",
    )
    job_id = created["job_id"]
    directory = manager.get_snapshot_path(job_id).parent
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status = manager.get_status(job_id)
            if status is not None and status["phase"] == "saving":
                break
            time.sleep(0.01)
        assert manager.get_status(job_id)["phase"] == "saving"

        clock["now"] += 2 * share_image_export._JOB_TTL_SECONDS
        time.sleep(0.1)
        assert manager.get_status(job_id) is not None
        release.set()
        assert _wait_for_terminal_state(manager, job_id)["state"] == "completed"
    finally:
        release.set()
        shutil.rmtree(directory, ignore_errors=True)


@pytest.mark.parametrize("state", ["completed", "failed"])
def test_long_running_browser_job_expires_only_after_generation_finishes(
    monkeypatch: pytest.MonkeyPatch,
    state: str,
) -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        directory = manager.get_snapshot_path(job_id).parent
        now = share_image_export.time.time()
        manager.update_browser_job(job_id, {
            "client_token": token,
            "state": "running",
            "phase": "loading_snapshot",
            "claim": True,
        })
        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 86_400)
        if state == "completed":
            data = _test_png()
            manager.store_browser_result(job_id, token, "share.png", io.BytesIO(data), len(data))
        else:
            manager.update_browser_job(job_id, {
                "client_token": token,
                "state": "failed",
                "phase": "failed",
            })
        assert manager.get_status(job_id)["state"] == state

        monkeypatch.setattr(
            share_image_export.time,
            "time",
            lambda: now + 86_400 + 3_601,
        )
        assert manager.get_status(job_id) is None
        assert not directory.exists()


def test_browser_download_claim_is_atomic_expires_and_does_not_extend_artifact_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        data = _test_png()
        now = share_image_export.time.time()
        monkeypatch.setattr(share_image_export.time, "time", lambda: now)
        manager.store_browser_result(job_id, token, "share.png", io.BytesIO(data), len(data))
        barrier = threading.Barrier(3)
        claimed: list[bool] = []

        def claim() -> None:
            barrier.wait()
            claimed.append(manager.update_browser_job(job_id, {
                "client_token": token,
                "claim_download": True,
            })["download_claimed"])

        threads = [threading.Thread(target=claim) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=2)
            assert not thread.is_alive()
        assert sorted(claimed) == [False, True]
        assert "client_token" not in manager.get_status(job_id)
        assert "download_attempt_id" not in manager.get_status(job_id)
        assert manager.get_status(job_id, include_client_token=True)["client_token"] == token

        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 59)
        assert manager.update_browser_job(job_id, {
            "client_token": token,
            "claim_download": True,
        })["download_claimed"] is False
        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 60)
        assert manager.update_browser_job(job_id, {
            "client_token": token,
            "claim_download": True,
        })["download_claimed"] is True
        assert manager.get_result(job_id)[0].read_bytes() == data

        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 3_601)
        assert manager.get_status(job_id) is None


def test_download_started_cannot_be_claimed_again_after_claim_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _browser_job() as (manager, created):
        job_id = created["job_id"]
        data = _test_png()
        now = share_image_export.time.time()
        manager.store_browser_result(
            job_id, created["client_token"], "share.png", io.BytesIO(data), len(data),
        )
        attempt_id = _claim_browser_download(manager, created)
        assert manager.mark_browser_download_started(job_id, attempt_id) is True
        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 61)

        assert manager.update_browser_job(job_id, {
            "client_token": created["client_token"],
            "claim_download": True,
        })["download_claimed"] is False
        assert manager.get_status(job_id)["phase"] == "download_started"


def test_browser_failed_download_can_retry_once_immediately_without_regenerating() -> None:
    with _browser_job() as (manager, created):
        job_id, token = created["job_id"], created["client_token"]
        data = _test_png()
        claim = {"client_token": token, "claim_download": True}
        manager.store_browser_result(job_id, token, "share.png", io.BytesIO(data), len(data))
        attempt_id = _claim_browser_download(manager, created)
        assert manager.mark_browser_download_started(job_id, attempt_id) is True
        manager.mark_browser_download_finished(job_id, succeeded=False, attempt_id=attempt_id)

        assert manager.get_status(job_id)["state"] == "completed"
        assert manager.get_status(job_id)["phase"] == "download_failed"
        assert manager.mark_browser_download_started(job_id, attempt_id) is False
        retried = manager.update_browser_job(job_id, claim)
        assert retried["download_claimed"] is True
        assert retried["download_attempt_id"] != attempt_id
        assert retried["phase"] == "completed"
        assert manager.update_browser_job(job_id, claim)["download_claimed"] is False
        assert manager.get_result(job_id)[0].read_bytes() == data


def test_browser_sent_download_cannot_be_claimed_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _browser_job() as (manager, created):
        job_id = created["job_id"]
        data = _test_png()
        now = share_image_export.time.time()
        manager.store_browser_result(
            job_id, created["client_token"], "share.png", io.BytesIO(data), len(data),
        )
        attempt_id = _claim_browser_download(manager, created)
        assert manager.mark_browser_download_started(job_id, attempt_id) is True
        manager.mark_browser_download_finished(job_id, succeeded=True, attempt_id=attempt_id)
        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 61)

        assert manager.update_browser_job(job_id, {
            "client_token": created["client_token"],
            "claim_download": True,
        })["download_claimed"] is False
        assert manager.get_status(job_id)["phase"] == "download_sent"
        assert manager.get_status(job_id)["state"] == "completed"


def test_browser_in_flight_download_has_no_transfer_deadline_or_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _browser_job() as (manager, created):
        job_id = created["job_id"]
        directory = manager.get_snapshot_path(job_id).parent
        data = _test_png()
        now = share_image_export.time.time()
        manager.store_browser_result(
            job_id, created["client_token"], "share.png", io.BytesIO(data), len(data),
        )
        attempt_id = _claim_browser_download(manager, created)
        assert manager.mark_browser_download_started(job_id, attempt_id) is True
        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 86_400)

        assert manager.get_status(job_id)["phase"] == "download_started"
        assert manager.get_result(job_id)[0].read_bytes() == data
        manager.mark_browser_download_finished(job_id, succeeded=True, attempt_id=attempt_id)
        assert manager.get_status(job_id)["phase"] == "download_sent"
        monkeypatch.setattr(
            share_image_export.time,
            "time",
            lambda: now + 86_400 + 3_601,
        )
        assert manager.get_status(job_id) is None
        assert not directory.exists()


def test_expired_download_claim_cannot_start_or_finish_a_new_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with _browser_job() as (manager, created):
        job_id = created["job_id"]
        data = _test_png()
        now = share_image_export.time.time()
        manager.store_browser_result(
            job_id, created["client_token"], "share.png", io.BytesIO(data), len(data),
        )
        old_attempt = _claim_browser_download(manager, created)
        monkeypatch.setattr(share_image_export.time, "time", lambda: now + 61)
        new_attempt = _claim_browser_download(manager, created)
        assert new_attempt != old_attempt

        assert manager.mark_browser_download_started(job_id, old_attempt) is False
        assert manager.get_status(job_id)["phase"] == "completed"
        assert manager.mark_browser_download_started(job_id, new_attempt) is True
        assert manager.mark_browser_download_started(job_id, new_attempt) is False
        manager.mark_browser_download_finished(job_id, succeeded=False, attempt_id=old_attempt)
        manager.mark_browser_download_finished(job_id, succeeded=True, attempt_id=old_attempt)
        assert manager.get_status(job_id)["phase"] == "download_started"
        manager.mark_browser_download_finished(job_id, succeeded=True, attempt_id=new_attempt)
        manager.mark_browser_download_finished(job_id, succeeded=False, attempt_id=old_attempt)
        assert manager.get_status(job_id)["phase"] == "download_sent"
        assert manager.mark_browser_download_started(job_id, new_attempt) is False


@pytest.mark.parametrize("attempt_id", [None, "", "wrong", "中文"])
def test_browser_download_requires_current_claimed_attempt(attempt_id) -> None:
    with _browser_job() as (manager, created):
        job_id = created["job_id"]
        data = _test_png()
        manager.store_browser_result(
            job_id, created["client_token"], "share.png", io.BytesIO(data), len(data),
        )
        _claim_browser_download(manager, created)

        assert manager.mark_browser_download_started(job_id, attempt_id) is False
        assert manager.get_status(job_id)["phase"] == "completed"


def test_same_download_attempt_can_start_only_one_concurrent_stream() -> None:
    with _browser_job() as (manager, created):
        job_id = created["job_id"]
        data = _test_png()
        manager.store_browser_result(
            job_id, created["client_token"], "share.png", io.BytesIO(data), len(data),
        )
        attempt_id = _claim_browser_download(manager, created)
        barrier = threading.Barrier(3)
        started: list[bool] = []

        def start() -> None:
            barrier.wait()
            started.append(manager.mark_browser_download_started(job_id, attempt_id))

        threads = [threading.Thread(target=start) for _ in range(2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=2)
            assert not thread.is_alive()
        assert sorted(started) == [False, True]
        assert manager.get_status(job_id)["phase"] == "download_started"
        manager.mark_browser_download_finished(job_id, succeeded=True, attempt_id=attempt_id)


@pytest.fixture
def personal_client(monkeypatch: pytest.MonkeyPatch):
    manager = ShareImageExportManager(
        renderer=lambda **kwargs: pytest.fail("Gateway must not launch a server browser"),
    )
    monkeypatch.setattr(api, "_manager", lambda: manager)
    monkeypatch.setattr(
        api,
        "get_share_session_metadata",
        AsyncMock(return_value={"title": "Export"}),
    )
    monkeypatch.setattr(api, "build_share_snapshot", AsyncMock(return_value=(
        {"session_id": "session", "records": [{"role": "assistant", "content": "retained"}]},
        "share.png",
    )))
    app = FastAPI()
    channel = SimpleNamespace(authenticate_handshake=AsyncMock(return_value=None))
    api.register_personal_share_routes(app, channel)
    with TestClient(app) as client:
        client.manager = manager
        client.channel = channel
        yield client
    _cleanup_manager(manager)


def _create_personal(client: TestClient, *, session_id: str = "session") -> dict:
    response = client.post(
        "/share-api/jobs",
        json={"session_id": session_id, "allow_browser_render": True},
    )
    assert response.status_code == 202, response.text
    assert response.headers["cache-control"] == "no-store"
    return response.json()


@pytest.mark.parametrize("target,status,expected", [
    ("/share-api/jobs?session_id=idle-session", 200, None),
    ("/share-api/jobs", 400, {"error": "missing_session_id"}),
    ("/share-api/jobs/" + "a" * 32, 404, {"error": "job_not_found"}),
])
def test_personal_share_job_lookup_preserves_empty_and_error_results(
    personal_client: TestClient,
    target: str,
    status: int,
    expected: dict | None,
) -> None:
    response = personal_client.get(target)

    assert response.status_code == status
    assert response.headers["cache-control"] == "no-store"
    assert response.json() == expected


def test_personal_share_requires_and_selects_browser_renderer(
    personal_client: TestClient,
) -> None:
    rejected = personal_client.post("/share-api/jobs", json={"session_id": "session"})

    assert rejected.status_code == 400
    assert rejected.json() == {"error": "share_export_browser_render_required"}
    api.build_share_snapshot.assert_not_awaited()

    created = _create_personal(personal_client)
    assert created["renderer"] == "browser"
    assert created["state"] == "queued"
    assert personal_client.manager.get_status(created["job_id"])["renderer"] == "browser"


def test_personal_share_zip_upload_and_download_use_one_complete_attempt(
    personal_client: TestClient,
) -> None:
    created = _create_personal(personal_client)
    job_id, token = created["job_id"], created["client_token"]
    archive_body = _test_zip_bytes()
    uploaded = personal_client.post(
        f"/share-api/jobs/{job_id}/browser-result?filename=share.zip",
        content=archive_body,
        headers={"X-Share-Export-Token": token},
    )
    assert uploaded.status_code == 200, uploaded.text
    assert uploaded.json()["state"] == "completed"

    head = personal_client.head(f"/share-api/jobs/{job_id}/download")
    assert head.status_code == 200
    assert head.headers["accept-ranges"] == "none"
    assert personal_client.manager.get_status(job_id)["phase"] == "completed"
    assert personal_client.get(f"/share-api/jobs/{job_id}/download").status_code == 409

    claim = personal_client.post(
        f"/share-api/jobs/{job_id}/browser",
        json={"client_token": token, "claim_download": True},
    ).json()
    url = f"/share-api/jobs/{job_id}/download?download_attempt_id={claim['download_attempt_id']}"
    response = personal_client.get(url, headers={"Range": "bytes=0-9"})
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    assert response.content == archive_body
    assert personal_client.manager.get_status(job_id)["phase"] == "download_sent"
    assert personal_client.get(url).status_code == 409


def test_personal_share_maps_browser_update_and_upload_errors_without_leaking_token(
    personal_client: TestClient,
) -> None:
    created = _create_personal(personal_client)
    job_id, token = created["job_id"], created["client_token"]

    denied = personal_client.post(
        f"/share-api/jobs/{job_id}/browser",
        json={"client_token": "wrong", "state": "running", "phase": "rendering"},
    )
    assert denied.status_code == 403
    assert denied.headers["cache-control"] == "no-store"

    invalid = personal_client.post(
        f"/share-api/jobs/{job_id}/browser",
        json={"client_token": token, "state": "completed", "phase": "rendering"},
    )
    assert invalid.status_code == 400
    assert invalid.json() == {"error": "share_export_client_state_invalid"}
    assert personal_client.manager.get_status(job_id)["state"] == "queued"

    upload = personal_client.post(
        f"/share-api/jobs/{job_id}/browser-result?filename=share.png",
        content=b"not a png",
        headers={"X-Share-Export-Token": token},
    )
    assert upload.status_code == 400
    assert token not in upload.text
    assert personal_client.manager.get_status(job_id)["state"] == "failed"
    assert personal_client.get(f"/share-api/jobs/{job_id}/download").status_code == 409
