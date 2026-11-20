"""Regression tests for target-pinned install arguments.

The update check resolves a target release (beta channel included), but pip
and uv resolve the index themselves when handed an unpinned package, so an
upgrade offered for ``0.2.9b3`` used to install some other version than the
one shown — a newer release published in the meantime, or no-op entirely
because pre-releases are skipped by default ("Requirement already
satisfied"). The installer now pins the requirement to the checked version;
an explicit ``package==target`` also makes both tools accept pre-release
targets without ``--pre`` / ``--prerelease allow``.
"""

from agent_gateway.common.upgrade_executor import PipExecutor


def _noop_callback(_: dict) -> None:
    return None


def _build(monkeypatch, latest_version: str, uv_cmd: str | None):
    monkeypatch.setattr(
        PipExecutor, "_resolve_uv_command", staticmethod(lambda: uv_cmd)
    )
    executor = PipExecutor(
        {"pypi_mirror": "", "latest_version": latest_version},
        _noop_callback,
    )
    return executor._build_install_args("workswarm", 20, latest_version)


def test_pip_pins_prerelease_target(monkeypatch):
    args = _build(monkeypatch, "0.2.9b3", uv_cmd=None)

    assert "workswarm==0.2.9b3" in args
    assert "--pre" not in args


def test_pip_pins_stable_target(monkeypatch):
    args = _build(monkeypatch, "0.2.9", uv_cmd=None)

    assert "workswarm==0.2.9" in args
    assert "--pre" not in args


def test_uv_pins_prerelease_target(monkeypatch):
    args = _build(monkeypatch, "0.2.9b3", uv_cmd="/usr/bin/uv")

    assert "workswarm==0.2.9b3" in args
    assert "--prerelease" not in args


def test_uv_pins_stable_target(monkeypatch):
    args = _build(monkeypatch, "0.2.8", uv_cmd="/usr/bin/uv")

    assert "workswarm==0.2.8" in args
    assert "--prerelease" not in args


def test_empty_latest_version_keeps_unpinned_fallback(monkeypatch):
    pip_args = _build(monkeypatch, "", uv_cmd=None)
    uv_args = _build(monkeypatch, "", uv_cmd="/usr/bin/uv")

    assert "workswarm" in pip_args
    assert "workswarm" in uv_args
    assert all(not arg.startswith("workswarm==") for arg in pip_args + uv_args)
    assert "--pre" not in pip_args
    assert "--prerelease" not in uv_args
