# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Target-AS model resolution for controlled multi-worker Cron jobs."""

import asyncio
import math
from types import SimpleNamespace

import pytest
from e2a.agent_models import AgentResponse
from e2a.message import ReqMethod

from agent_gateway.application.worker_cron_models import WorkerCronModelResolver


def _model_row(
    model_name: str,
    *,
    model_id: str = "",
    alias: str = "",
    is_free: bool = False,
    is_agentos: bool = False,
    login: bool = False,
) -> dict:
    row = {
        "model_id": model_id,
        "model_name": model_name,
        "alias": alias,
        "is_free": is_free,
        "is_agentos": is_agentos,
    }
    if login:
        row["read_only"] = True
        row["source"] = "huawei-maas-login"
    return row


def _catalog_payload(models: list[dict], active_model: str) -> dict:
    return {"models": models, "model_groups": [], "active_model": active_model}


class Client:
    def __init__(
        self,
        payload: dict | None = None,
        *,
        ok: bool = True,
        delay: float = 0,
        error: Exception | None = None,
    ) -> None:
        self.requests = []
        self.payload = payload or _catalog_payload(
            [
                _model_row("config-model", model_id="mdl_config", alias="Friendly"),
                _model_row("shared-name", model_id="mdl_shared"),
                _model_row("zen-model", model_id="mdl_zen", alias="Zen", is_free=True),
                _model_row("shared-name", is_free=True),
            ],
            "config-model",
        )
        self.ok = ok
        self.delay = delay
        self.error = error

    async def send_request(self, envelope):
        self.requests.append(envelope)
        if self.error is not None:
            raise self.error
        if self.delay:
            await asyncio.sleep(self.delay)
        return AgentResponse(
            request_id=envelope.request_id,
            channel_id=envelope.channel,
            ok=self.ok,
            payload=self.payload,
        )


def _job(**fields) -> SimpleNamespace:
    values = {
        "model_name": None,
        "model_selection": None,
        "credential_ref": "",
        "user_id": "alice",
        **fields,
    }
    return SimpleNamespace(**values)


@pytest.mark.parametrize("timeout_seconds", [0, -1, math.inf, math.nan])
def test_resolver_rejects_non_positive_or_non_finite_timeout(timeout_seconds: float) -> None:
    with pytest.raises(ValueError, match="timeout_seconds must be positive and finite"):
        WorkerCronModelResolver(Client(), timeout_seconds=timeout_seconds)


async def test_resolver_reads_only_the_target_as(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "agent_gateway.common.config.get_model_config",
        lambda *args, **kwargs: pytest.fail("Worker cron resolver read local config"),
    )
    monkeypatch.setattr(
        "agent_gateway.auth.model_catalog.get_models",
        lambda *args, **kwargs: pytest.fail("Worker cron resolver read login catalog"),
    )
    client = Client()
    resolver = WorkerCronModelResolver(client)

    assert await resolver("shared-name", user_id="alice") == ("shared-name", "config")
    envelope, = client.requests
    assert envelope.method == ReqMethod.MODELS_LIST.value
    assert envelope.channel == "web"
    assert envelope.params == {}
    assert envelope.user_id == "alice"


async def test_resolver_prioritizes_model_selection_and_normalizes_alias() -> None:
    resolver = WorkerCronModelResolver(Client())
    selection = {"type": "model", "id": "mdl_config"}

    assert await resolver("zen-model", model_selection=selection) == ("config-model", "config")
    assert await resolver("Friendly") == ("config-model", "config")


async def test_resolver_accepts_selection_only_and_zen_selection() -> None:
    resolver = WorkerCronModelResolver(Client())

    config_result = await resolver(None, model_selection={"type": "model", "id": "mdl_config"})
    zen_result = await resolver(None, model_selection={"type": "model", "id": "zen-model"})

    assert config_result == ("config-model", "config")
    assert zen_result == ("zen-model", "zen")


async def test_resolver_rejects_group_and_non_model_selection() -> None:
    resolver = WorkerCronModelResolver(Client())
    with pytest.raises(ValueError, match="single model"):
        await resolver(None, model_selection={"type": "model_group", "id": "grp"})
    assert not Client().requests


async def test_resolver_keeps_config_priority_for_a_shared_name() -> None:
    rows = [
        _model_row("shared-name", login=True, is_free=True),
        _model_row("shared-name", model_id="mdl_shared"),
        _model_row("shared-name", is_free=True),
    ]
    resolver = WorkerCronModelResolver(Client(_catalog_payload(rows, "shared-name")))

    assert await resolver("shared-name") == ("shared-name", "config")


async def test_resolver_without_credentials_rejects_login() -> None:
    resolver = WorkerCronModelResolver(
        Client(_catalog_payload([_model_row("login-model", login=True)], "login-model"))
    )

    with pytest.raises(ValueError, match="Cron login models are not enabled"):
        await resolver("login-model")


async def test_resolver_rejects_default_active_login_model_explicitly() -> None:
    # ``is_default`` only marks dropdown visibility; it is not the active model.
    config = _model_row("config-model", model_id="mdl_config")
    config["is_default"] = True
    login = _model_row("login-model", login=True)
    login["is_default"] = True
    resolver = WorkerCronModelResolver(Client(_catalog_payload([config, login], "login-model")))

    with pytest.raises(ValueError, match="Cron default login model requires explicit model"):
        await resolver(None)
    assert await resolver("config-model") == ("config-model", "config")
    assert await WorkerCronModelResolver(
        Client(_catalog_payload([login, config], "config-model"))
    )(None) == (None, "")


async def test_resolver_does_not_treat_read_only_agentos_as_login() -> None:
    agentos_row = _model_row("agentos-model", is_agentos=True)
    agentos_row["read_only"] = True
    resolver = WorkerCronModelResolver(
        Client(_catalog_payload([agentos_row], "agentos-model"))
    )

    with pytest.raises(ValueError, match="Unknown cron model"):
        await resolver("agentos-model")
    with pytest.raises(ValueError, match="Unknown cron model"):
        await resolver(None)


async def test_resolver_ignores_rows_with_empty_names_or_non_boolean_flags() -> None:
    valid_config = _model_row("config-model", model_id="mdl_config")
    malformed = [
        {"model_name": "bad-flag", "is_free": "true"},
        {"model_name": "", "is_agentos": False},
    ]
    resolver = WorkerCronModelResolver(
        Client(_catalog_payload([valid_config, *malformed], "config-model"))
    )

    await resolver(None)

    with pytest.raises(ValueError, match="Unknown cron model"):
        await resolver("bad-flag")


async def test_resolver_sanitizes_failures_and_bounds_the_wait() -> None:
    with pytest.raises(ValueError, match="Cron model catalog unavailable") as caught:
        await WorkerCronModelResolver(Client(ok=False))("config-model")
    assert "error" not in str(caught.value)

    secret_error = RuntimeError("upstream failed after sk-live-secret")
    with pytest.raises(ValueError, match="Cron model catalog unavailable") as caught:
        await WorkerCronModelResolver(Client(error=secret_error))("config-model")
    assert "sk-live-secret" not in str(caught.value)

    for payload in (
        {"models": []},
        {"models": [], "active_model": None},
        {"models": [None], "active_model": ""},
        ["not", "an", "object"],
    ):
        with pytest.raises(ValueError, match="Cron model catalog protocol error"):
            await WorkerCronModelResolver(Client(payload=payload))("config-model")

    async with asyncio.timeout(2):
        with pytest.raises(ValueError, match="Cron model catalog unavailable"):
            await WorkerCronModelResolver(Client(delay=5), timeout_seconds=0.01)("config-model")


async def test_validate_job_accepts_unspecified_model_only_for_allowed_active_model() -> None:
    await WorkerCronModelResolver(Client()).validate_job(_job())
    await WorkerCronModelResolver(Client()).validate_job(
        _job(model_name="Friendly", model_selection={"type": "model", "id": "mdl_zen"})
    )
    with pytest.raises(ValueError, match="Cron login models are not enabled"):
        await WorkerCronModelResolver(
            Client(_catalog_payload([_model_row("login-model", login=True)], "login-model"))
        ).validate_job(_job(model_selection={"type": "model", "id": "login-model"}))
    # An active default that is not a listed model cannot be validated either.
    with pytest.raises(ValueError, match="Unknown cron model"):
        await WorkerCronModelResolver(
            Client(_catalog_payload([], ""))
        ).validate_job(_job())


async def test_validate_job_rejects_credential_ref_without_touching_as() -> None:
    client = Client()
    with pytest.raises(ValueError, match="Cron login models are not enabled"):
        await WorkerCronModelResolver(client).validate_job(
            _job(model_name="config-model", credential_ref="credential-ref")
        )
    assert client.requests == []
