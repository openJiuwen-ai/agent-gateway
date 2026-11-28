# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

import json
import sys

import pytest


@pytest.mark.parametrize(
    "arguments,expected",
    [
        (["--workers", "2"], "--workers requires --mode multiprocess"),
        (["--mode", "multiprocess", "--workers", "0"], "--workers must be between 1 and 32"),
        (["--mode", "multiprocess", "--host", "0.0.0.0"], "loopback"),
    ],
)
def test_rejects_unsupported_cli_options(monkeypatch, capsys, arguments, expected):
    from agent_gateway import app_gateway

    monkeypatch.setattr(sys, "argv", ["agent-gateway", *arguments])
    with pytest.raises(SystemExit) as error:
        app_gateway.main()
    assert error.value.code == 2
    assert expected in capsys.readouterr().err


@pytest.mark.parametrize("jobs,blocked", [([], False), ([{"enabled": True}], True), ([None], True)])
async def test_trial_never_consumes_or_migrates_existing_cron_jobs(tmp_path, monkeypatch, jobs, blocked):
    from agent_gateway.application.multiprocess import validate_workspace
    from agent_gateway.common import config, cron_settings

    store = tmp_path / "cron_jobs.json"
    original = json.dumps({"jobs": jobs})
    store.write_text(original, encoding="utf-8")
    monkeypatch.setattr(config, "get_config", dict)
    monkeypatch.setattr(cron_settings, "get_cron_jobs_path", lambda: store)
    if blocked:
        with pytest.raises(ValueError, match="Cron"):
            await validate_workspace()
    else:
        await validate_workspace()
    assert store.read_text(encoding="utf-8") == original
    assert sorted(tmp_path.iterdir()) == [store]
