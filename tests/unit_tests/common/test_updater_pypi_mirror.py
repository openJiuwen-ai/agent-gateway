"""Regression tests for the PyPI mirror index URL handling (issue 6288).

The legacy default ``https://mirrors.aliyun.com/pypi`` is a directory page,
not a PEP 503 simple index, so ``pip install --index-url`` against it fails.
These tests pin three layers of defence:

- ``DEFAULT_SOURCE_CONFIG`` ships a valid ``/pypi/simple/`` index;
- ``normalize_pypi_index_url`` migrates legacy configs saved by older builds
  at config-load time (``UpdaterService._load_config``) and inside
  ``PyPIVersionSource``;
- ``PipExecutor._build_install_args`` normalizes again at the final pip
  boundary, covering configs that bypass the loader.
"""

from urllib.parse import urljoin

import pytest

from agent_gateway.common._build_config import PACKAGE_NAME
from agent_gateway.common.updater import (
    DEFAULT_SOURCE_CONFIG,
    UpdaterService,
)
from agent_gateway.common.upgrade_executor import PipExecutor
from agent_gateway.common.version_source import (
    PYPI_INDEX_URL,
    PyPIVersionSource,
    normalize_pypi_index_url,
    pypi_project_url,
)

LEGACY_MIRROR = "https://mirrors.aliyun.com/pypi"
MIGRATED_MIRROR = "https://mirrors.aliyun.com/pypi/simple/"


def _noop_callback(_: dict) -> None:
    return None


class TestNormalizePypiIndexUrl:
    def test_empty_mirror_returns_empty(self):
        assert normalize_pypi_index_url("") == ""

    def test_whitespace_only_mirror_returns_empty(self):
        assert normalize_pypi_index_url("   \t ") == ""

    def test_legacy_aliyun_base_is_migrated(self):
        assert normalize_pypi_index_url(LEGACY_MIRROR) == MIGRATED_MIRROR

    def test_legacy_aliyun_base_with_trailing_slash_is_migrated(self):
        assert normalize_pypi_index_url(LEGACY_MIRROR + "/") == MIGRATED_MIRROR

    def test_explicit_simple_index_is_preserved(self):
        assert normalize_pypi_index_url(MIGRATED_MIRROR) == MIGRATED_MIRROR
        assert normalize_pypi_index_url(MIGRATED_MIRROR.rstrip("/")) == (
            MIGRATED_MIRROR.rstrip("/")
        )

    def test_non_aliyun_mirror_is_preserved(self):
        mirror = "https://pypi.tuna.tsinghua.edu.cn/simple"
        assert normalize_pypi_index_url(mirror) == mirror

    def test_leading_and_trailing_whitespace_is_stripped_before_migration(self):
        assert normalize_pypi_index_url(f"  {LEGACY_MIRROR}  ") == MIGRATED_MIRROR

    def test_host_matching_is_case_insensitive(self):
        # The migration matches the host case-insensitively while preserving
        # the original netloc spelling (URL hosts are case-insensitive).
        assert normalize_pypi_index_url("https://MIRRORS.ALIYUN.COM/pypi") == (
            "https://MIRRORS.ALIYUN.COM/pypi/simple/"
        )

    def test_aliyun_path_beyond_pypi_is_preserved(self):
        mirror = "https://mirrors.aliyun.com/pypi/other/"
        assert normalize_pypi_index_url(mirror) == mirror


class TestPypiProjectUrl:
    def test_appends_package_with_trailing_slash(self):
        assert (
            pypi_project_url("https://pypi.org/simple/", "workswarm")
            == "https://pypi.org/simple/workswarm/"
        )

    def test_appends_package_without_trailing_slash(self):
        assert (
            pypi_project_url("https://pypi.org/simple", "workswarm")
            == "https://pypi.org/simple/workswarm/"
        )


class TestPyPIVersionSource:
    def test_legacy_mirror_yields_valid_simple_api_url(self):
        source = PyPIVersionSource(package="workswarm", mirror=LEGACY_MIRROR)
        assert source._api_url == "https://mirrors.aliyun.com/pypi/simple/workswarm/"

    def test_empty_mirror_falls_back_to_pypi_org(self):
        source = PyPIVersionSource(package="workswarm", mirror="")
        assert source._api_url == f"{PYPI_INDEX_URL}workswarm/"

    def test_explicit_mirror_is_used_verbatim(self):
        mirror = "https://pypi.tuna.tsinghua.edu.cn/simple"
        source = PyPIVersionSource(package="workswarm", mirror=mirror)
        assert source._api_url == f"{mirror}/workswarm/"

    @pytest.mark.parametrize(
        "mirror,expected_candidate",
        [
            # Publish-time JSON candidates keep the mirror's path prefix so
            # mirrors exposing the JSON API under the same subtree keep
            # working; the upstream default stays at the domain root
            # (https://pypi.org/pypi/<pkg>/<ver>/json).
            (
                "https://pypi.tuna.tsinghua.edu.cn/simple",
                "https://pypi.tuna.tsinghua.edu.cn/simple/pypi/workswarm/0.2.8/json",
            ),
            (
                "https://mirror.test/packages/pypi/simple/",
                "https://mirror.test/packages/pypi/simple/pypi/workswarm/0.2.8/json",
            ),
            ("", "https://pypi.org/pypi/workswarm/0.2.8/json"),
            (
                LEGACY_MIRROR,
                "https://mirrors.aliyun.com/pypi/simple/pypi/workswarm/0.2.8/json",
            ),
        ],
    )
    def test_publish_time_candidate_keeps_mirror_path_prefix(
        self, mirror, expected_candidate
    ):
        source = PyPIVersionSource(package="workswarm", mirror=mirror)
        candidate = urljoin(
            source._base_url + "/", "pypi/workswarm/0.2.8/json"
        )
        assert candidate == expected_candidate


class TestDefaultSourceConfig:
    def test_ships_a_valid_simple_index(self):
        assert DEFAULT_SOURCE_CONFIG["pypi_mirror"] == MIGRATED_MIRROR

    def test_default_is_idempotent_under_normalize(self):
        mirror = DEFAULT_SOURCE_CONFIG["pypi_mirror"]
        assert normalize_pypi_index_url(mirror) == mirror


class TestUpdaterLoadConfig:
    def _load_with_raw(self, monkeypatch, raw, install_mode):
        import agent_gateway.common.updater as updater_module

        monkeypatch.setattr(updater_module, "get_config_raw", lambda: raw)
        monkeypatch.setattr(updater_module, "_detect_install_mode", lambda: install_mode)
        return UpdaterService._load_config()

    def test_legacy_saved_mirror_is_migrated_for_pypi_mode(self, monkeypatch):
        config = self._load_with_raw(
            monkeypatch,
            {"updater": {"pypi_mirror": LEGACY_MIRROR}},
            install_mode="pip",
        )
        assert config["pypi_mirror"] == MIGRATED_MIRROR
        assert config["release_api_url"] == f"{MIGRATED_MIRROR}{PACKAGE_NAME}/"

    def test_missing_mirror_falls_back_to_pypi_org_api(self, monkeypatch):
        config = self._load_with_raw(monkeypatch, {"updater": {}}, install_mode="pip")
        assert config["pypi_mirror"] == ""
        assert config["release_api_url"] == f"{PYPI_INDEX_URL}{PACKAGE_NAME}/"

    def test_legacy_mirror_is_kept_verbatim_in_desktop_mode(self, monkeypatch):
        # Desktop updates resolve assets from release APIs, not the PyPI index;
        # the migration must not rewrite a mirror the desktop flow never uses.
        config = self._load_with_raw(
            monkeypatch,
            {"updater": {"pypi_mirror": LEGACY_MIRROR}},
            install_mode="desktop",
        )
        assert config["pypi_mirror"] == LEGACY_MIRROR


class TestPipExecutorInstallArgs:
    def _build(self, monkeypatch, mirror, uv_cmd=None):
        monkeypatch.setattr(
            PipExecutor, "_resolve_uv_command", staticmethod(lambda: uv_cmd)
        )
        executor = PipExecutor({"pypi_mirror": mirror}, _noop_callback)
        return executor._build_install_args("workswarm", 20)

    def test_legacy_saved_mirror_is_migrated_for_uv(self, monkeypatch):
        args = self._build(monkeypatch, LEGACY_MIRROR, uv_cmd="/usr/bin/uv")
        assert args[:4] == ["/usr/bin/uv", "pip", "install", "--upgrade"]
        assert ("--index-url", MIGRATED_MIRROR) in zip(args, args[1:])

    def test_legacy_saved_mirror_is_migrated_for_pip(self, monkeypatch):
        args = self._build(monkeypatch, LEGACY_MIRROR, uv_cmd=None)
        assert "-i" in args
        assert args[args.index("-i") + 1] == MIGRATED_MIRROR

    def test_empty_mirror_omits_index_flag(self, monkeypatch):
        args = self._build(monkeypatch, "", uv_cmd=None)
        assert "-i" not in args
        assert "--index-url" not in args

    def test_explicit_mirror_is_passed_through(self, monkeypatch):
        mirror = "https://pypi.tuna.tsinghua.edu.cn/simple"
        args = self._build(monkeypatch, mirror, uv_cmd=None)
        assert args[args.index("-i") + 1] == mirror
