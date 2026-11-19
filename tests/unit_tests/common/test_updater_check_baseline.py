"""Regression tests for the update-check baseline and wheel selection.

Daily wheels bump the installed distribution metadata (for example
``workswarm 0.2.9``) without regenerating the checked-in
``_build_config.VERSION`` constant (still ``0.2.8``), so the check must
compare against ``get_runtime_version()``. The PyPI simple index lists every
historical wheel, so the resolved asset must match the release version rather
than the first entry in the list.
"""

import sys

import pytest

from agent_gateway.common import updater
from agent_gateway.common.version_source import ReleaseAsset, ReleaseInfo


@pytest.fixture(autouse=True)
def pip_install_mode(monkeypatch):
    monkeypatch.delattr(sys, "frozen", raising=False)
    monkeypatch.delenv(updater.DESKTOP_ENV_FLAG, raising=False)


def _release(version: str, *names: str) -> ReleaseInfo:
    return ReleaseInfo(
        version=version,
        published_at="2026-11-13T02:36:10Z",
        assets=[
            ReleaseAsset(
                name=name,
                download_url=f"https://mirror.test/packages/{name}",
            )
            for name in names
        ],
        source_type="pypi",
    )


def _service(monkeypatch, release: ReleaseInfo, runtime_version: str):
    seen: dict[str, str] = {}

    class FakeSource:
        def fetch_latest(self, current_version: str = "") -> ReleaseInfo:
            seen["current_version"] = current_version
            return release

    monkeypatch.setattr(updater, "__version__", "0.2.8")
    monkeypatch.setattr(updater, "get_runtime_version", lambda: runtime_version)
    monkeypatch.setattr(
        updater,
        "get_config_raw",
        lambda: {"updater": {"enabled": True}},
    )
    monkeypatch.setattr(
        updater.UpdaterService,
        "_create_version_source",
        staticmethod(lambda config: FakeSource()),
    )
    return updater.UpdaterService(), seen


def test_check_uses_installed_version_as_baseline(monkeypatch):
    # The installed daily wheel is 0.2.9, which is newer than the mirror's
    # newest pre-release 0.2.9b3, so the check must not offer an update.
    release = _release(
        "0.2.9b3",
        "workswarm-0.2.5-py3-none-any.whl",
        "workswarm-0.2.9b3-py3-none-any.whl",
    )
    service, seen = _service(monkeypatch, release, runtime_version="0.2.9")

    status = service.check()

    assert seen["current_version"] == "0.2.9"
    assert status["has_update"] is False
    assert status["state"] == "up_to_date"
    assert status["current_version"] == "0.2.9"


def test_check_picks_wheel_matching_release_version(monkeypatch):
    release = _release(
        "0.2.9b3",
        "workswarm-0.2.5-py3-none-any.whl",
        "workswarm-0.2.8-py3-none-any.whl",
        "workswarm-0.2.9b3-py3-none-any.whl",
    )
    service, seen = _service(monkeypatch, release, runtime_version="0.2.8")

    status = service.check()

    assert seen["current_version"] == "0.2.8"
    assert status["has_update"] is True
    assert status["state"] == "update_available"
    assert status["latest_version"] == "0.2.9b3"
    assert status["matched_asset"] == "workswarm-0.2.9b3-py3-none-any.whl"
    assert status["download_url"].endswith("/workswarm-0.2.9b3-py3-none-any.whl")


def test_resolve_pip_asset_falls_back_to_first_wheel(monkeypatch):
    release = _release("0.3.0", "nightly-build.whl", "other-build.whl")
    service = updater.UpdaterService()

    service._resolve_pip_asset({}, release)  # pylint: disable=protected-access

    status = service.get_status()
    assert status["asset_name"] == "nightly-build.whl"
    assert status["download_url"].endswith("/nightly-build.whl")


def test_wheel_version_key_normalizes_prerelease_spellings():
    assert updater._wheel_version_key(  # pylint: disable=protected-access
        "workswarm-0.2.3b1-py3-none-any.whl"
    ) == updater._wheel_version_key(  # pylint: disable=protected-access
        "workswarm-0.2.3.beta1-py3-none-any.whl"
    )
    assert updater._wheel_version_key("not-a-wheel") is None  # pylint: disable=protected-access
