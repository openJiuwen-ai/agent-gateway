# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

from agent_gateway.auth import net
from agent_gateway.common import env_names, paths, request_ext
from agent_gateway.common.log_masking import probes
from agent_gateway.common.security import ws_origin


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for var in env_names.ALL:
        for name in var.names:
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(env_names, "_warned_conflicts", set())
    return monkeypatch


def test_registered_names_use_the_gateway_prefix() -> None:
    names = [name for var in env_names.ALL for name in var.names]

    assert len(names) == len(set(names))
    for var in env_names.ALL:
        assert var.name.startswith("AGENT_GATEWAY_")
        assert var.legacy
        assert all(name.startswith(("JIUWENSWARM_", "JIUWENCLAW_")) for name in var.legacy)


def test_new_name_wins_over_legacy_name() -> None:
    environ = {"AGENT_GATEWAY_SSL_VERIFY": "false", "JIUWENSWARM_SSL_VERIFY": "false"}

    assert env_names.lookup(env_names.SSL_VERIFY, environ=environ) == (
        "AGENT_GATEWAY_SSL_VERIFY",
        "false",
    )


def test_legacy_name_is_used_when_new_name_is_unset() -> None:
    environ = {"JIUWENSWARM_SSL_VERIFY": "false"}

    assert env_names.lookup(env_names.SSL_VERIFY, environ=environ) == (
        "JIUWENSWARM_SSL_VERIFY",
        "false",
    )


def test_default_is_returned_when_no_name_is_set() -> None:
    assert env_names.get(env_names.SSL_VERIFY, "fallback", environ={}) == "fallback"
    assert env_names.get(env_names.SSL_VERIFY, environ={}) is None


def test_empty_values_count_as_unset() -> None:
    environ = {"AGENT_GATEWAY_SSL_VERIFY": "", "JIUWENSWARM_SSL_VERIFY": "false"}

    assert env_names.get(env_names.SSL_VERIFY, "fallback", environ=environ) == "false"
    assert env_names.get(env_names.SSL_VERIFY, "fallback", environ={"AGENT_GATEWAY_SSL_VERIFY": ""}) == "fallback"


@pytest.mark.parametrize(
    ("var", "value"),
    [
        (env_names.ENABLE_ORIGIN_CHECK, "1"),
        (env_names.WS_ALLOWED_ORIGIN_HOSTS, "a.example"),
        (env_names.SUPERVISOR_PID, "4321"),
        (env_names.RUNTIME_WORKSPACE_READY, "1"),
    ],
    ids=["origin-check", "origin-hosts", "supervisor-pid", "workspace-ready"],
)
def test_empty_new_name_falls_back_to_legacy(var: env_names.EnvVar, value: str) -> None:
    # An empty placeholder in a deploy template must not shadow a real legacy value.
    environ = {var.name: "", var.legacy[0]: value}

    assert env_names.get(var, environ=environ) == value


def test_blank_values_fall_through_in_order() -> None:
    environ = {
        "AGENT_GATEWAY_LOG_MASKING_PROBE": " ",
        "JIUWENCLAW_LOG_MASKING_PROBE": "",
        "JIUWENSWARM_LOG_MASKING_PROBE": "1",
    }

    assert env_names.get(env_names.LOG_MASKING_PROBE, "", environ=environ) == "1"


def test_legacy_names_keep_their_original_order() -> None:
    environ = {
        "JIUWENSWARM_REQUEST_EXT_FORWARD_HEADERS": "x-swarm",
        "JIUWENCLAW_REQUEST_EXT_FORWARD_HEADERS": "x-claw",
    }

    assert env_names.get(env_names.REQUEST_EXT_FORWARD_HEADERS, environ=environ) == "x-swarm"


def test_conflicting_path_values_raise() -> None:
    environ = {"AGENT_GATEWAY_DATA_DIR": "/srv/new", "JIUWENSWARM_DATA_DIR": "/srv/old"}

    with pytest.raises(env_names.EnvVarConflictError, match="AGENT_GATEWAY_DATA_DIR"):
        env_names.get(env_names.DATA_DIR, environ=environ)


def test_equivalent_path_spellings_do_not_conflict(tmp_path: Path) -> None:
    spelled_with_separator = f"{tmp_path}{os.sep}"
    environ = {"AGENT_GATEWAY_DATA_DIR": spelled_with_separator, "JIUWENSWARM_DATA_DIR": str(tmp_path)}

    assert env_names.get(env_names.DATA_DIR, environ=environ) == spelled_with_separator


def test_identical_path_values_do_not_fail_startup(tmp_path: Path) -> None:
    # Deployments may temporarily dual-write the new and legacy names; equal
    # values must keep working instead of turning into a startup failure.
    environ = {"AGENT_GATEWAY_DATA_DIR": str(tmp_path), "JIUWENSWARM_DATA_DIR": str(tmp_path)}

    assert env_names.lookup(env_names.DATA_DIR, environ=environ) == ("AGENT_GATEWAY_DATA_DIR", str(tmp_path))


@pytest.mark.parametrize(
    ("environ", "expected"),
    [
        (
            {"AGENT_GATEWAY_DATA_DIR": "", "JIUWENSWARM_DATA_DIR": "/srv/old"},
            ("JIUWENSWARM_DATA_DIR", "/srv/old"),
        ),
        (
            {"AGENT_GATEWAY_DATA_DIR": "/srv/new", "JIUWENSWARM_DATA_DIR": " "},
            ("AGENT_GATEWAY_DATA_DIR", "/srv/new"),
        ),
    ],
    ids=["empty-new-name", "blank-legacy-name"],
)
def test_empty_path_values_count_as_unset(environ: dict[str, str], expected: tuple[str, str]) -> None:
    # Deploy templates leave empty placeholders; an empty path neither wins nor conflicts.
    assert env_names.lookup(env_names.DATA_DIR, environ=environ) == expected


def test_conflicting_plain_values_warn_once_without_values(
    clean_env: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    environ = {
        "AGENT_GATEWAY_WS_ALLOWED_ORIGIN_HOSTS": "new.example",
        "JIUWENSWARM_WS_ALLOWED_ORIGIN_HOSTS": "old.example",
    }

    with caplog.at_level(logging.WARNING, logger="agent_gateway.common.env_names"):
        assert env_names.get(env_names.WS_ALLOWED_ORIGIN_HOSTS, environ=environ) == "new.example"
        assert env_names.get(env_names.WS_ALLOWED_ORIGIN_HOSTS, environ=environ) == "new.example"

    warnings = [
        record for record in caplog.records if "AGENT_GATEWAY_WS_ALLOWED_ORIGIN_HOSTS" in record.getMessage()
    ]
    assert len(warnings) == 1
    assert "new.example" not in caplog.text
    assert "old.example" not in caplog.text


def test_paths_read_new_home_and_data_dir(clean_env: pytest.MonkeyPatch, tmp_path: Path) -> None:
    clean_env.setenv("AGENT_GATEWAY_HOME", str(tmp_path / "home"))
    clean_env.setenv("AGENT_GATEWAY_DATA_DIR", str(tmp_path / "data"))

    assert paths.get_user_home() == tmp_path / "home"
    assert paths.get_user_workspace_dir() == tmp_path / "data"


def test_paths_reject_conflicting_data_dirs(clean_env: pytest.MonkeyPatch, tmp_path: Path) -> None:
    clean_env.setenv("AGENT_GATEWAY_DATA_DIR", str(tmp_path / "new"))
    clean_env.setenv("JIUWENSWARM_DATA_DIR", str(tmp_path / "old"))

    with pytest.raises(env_names.EnvVarConflictError):
        paths.get_user_workspace_dir()


def test_gateway_config_file_reads_new_config_dir(clean_env: pytest.MonkeyPatch, tmp_path: Path) -> None:
    clean_env.setenv("AGENT_GATEWAY_CONFIG_DIR", str(tmp_path))

    assert paths.get_config_file() == tmp_path / "config-gateway.yaml"


_READERS = [
    pytest.param(
        ws_origin.is_origin_check_enabled,
        env_names.ENABLE_ORIGIN_CHECK,
        "1",
        True,
        id="enable-origin-check",
    ),
    pytest.param(
        ws_origin.get_allowed_origin_hosts,
        env_names.WS_ALLOWED_ORIGIN_HOSTS,
        "a.example, B.example",
        {"a.example", "b.example"},
        id="allowed-origin-hosts",
    ),
    pytest.param(net.ssl_verify_enabled, env_names.SSL_VERIFY, "false", False, id="ssl-verify"),
    pytest.param(
        probes.log_masking_probes_enabled,
        env_names.LOG_MASKING_PROBE,
        "on",
        True,
        id="log-masking-probe",
    ),
    pytest.param(
        request_ext._env_forward_headers,
        env_names.REQUEST_EXT_FORWARD_HEADERS,
        "x-a, x-b",
        ["x-a", "x-b"],
        id="request-ext-forward-headers",
    ),
]


@pytest.mark.parametrize("use_new_name", [True, False], ids=["new-name", "legacy-name"])
@pytest.mark.parametrize(("reader", "var", "value", "expected"), _READERS)
def test_readers_accept_new_and_legacy_names(
    clean_env: pytest.MonkeyPatch,
    reader,
    var: env_names.EnvVar,
    value: str,
    expected,
    use_new_name: bool,
) -> None:
    legacy_name = next(name for name in var.legacy if name.startswith("JIUWENSWARM_"))
    clean_env.setenv(var.name if use_new_name else legacy_name, value)

    assert reader() == expected
