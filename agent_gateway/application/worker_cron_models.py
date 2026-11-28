# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Execution-time Cron model validation against one target AgentServer."""

from __future__ import annotations

import asyncio
import math
import time
from typing import Any, Literal
import uuid

from e2a.agent_models import AgentResponse
from e2a.gateway_normalize import e2a_from_agent_fields
from e2a.message import ReqMethod

from agent_gateway.auth.model_catalog import LOGIN_MODEL_SOURCE
from agent_gateway.common.model_selection import ModelSelection
from agent_gateway.common.request_identity import apply_routing_metadata, normalize_routing_identity
from agent_gateway.routing.agent_request_timeout import send_agent_request_with_timeout

CronModelSource = Literal["", "config", "zen"]
_SUPPORTED_SOURCES = frozenset({"config", "zen"})


class WorkerCronModelResolver:
    """Resolve and revalidate Cron models from the target AgentServer only.

    The controller validates a model when a job is created or its model fields
    are edited. A stored job can outlive that check: its model may be removed,
    or a job disabled by single mode may be enabled again. This resolver repeats
    the same ``models.list`` based resolution before such a job runs.

    Login models need an account credential binding that Worker Cron does not
    have, so they are refused here; an AgentOS model row is refused as an
    unsupported source as well. No model name, selection or credential is
    cached between calls, and the local model config is never read.
    """

    def __init__(self, agent_client: Any, *, timeout_seconds: float = 5.0) -> None:
        try:
            timeout = float(timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("timeout_seconds must be positive and finite") from exc
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout_seconds must be positive and finite")
        self._agent_client = agent_client
        self._timeout_seconds = timeout

    async def __call__(
        self,
        raw: Any,
        *,
        model_selection: Any = None,
        user_id: str | None = None,
    ) -> tuple[str | None, CronModelSource]:
        selection = self._normalize_selection(model_selection)
        raw_value = raw.strip() if isinstance(raw, str) else ""
        payload = await self._request_catalog(user_id)

        if selection is not None:
            model_name, source = self._match(payload, selection["id"])
            self._validate_supported_source(source)
            return model_name, source

        if raw_value:
            model_name, source = self._match(payload, raw_value)
            self._validate_supported_source(source)
            return model_name, source

        # Without an explicit model the AgentServer picks its active default.
        # ``is_default`` only marks dropdown visibility, so it cannot tell
        # which model would actually run. An unknown active model is refused.
        active_source = self._active_source(payload)
        if active_source == "login":
            raise ValueError("Cron default login model requires explicit model selection")
        self._validate_supported_source(active_source)
        return None, ""

    async def validate_job(self, job: Any) -> tuple[str | None, CronModelSource]:
        """Fail closed unless every stored model reference still resolves."""
        if str(getattr(job, "credential_ref", "") or "").strip():
            raise ValueError("Cron login models are not enabled")
        return await self(
            getattr(job, "model_name", None),
            model_selection=getattr(job, "model_selection", None),
            user_id=getattr(job, "user_id", None),
        )

    @staticmethod
    def _normalize_selection(value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        selection = ModelSelection.model_validate(value)
        if selection.type != "model":
            raise ValueError("Scheduled tasks only support a single model")
        return selection.model_dump()

    async def _request_catalog(self, user_id: str | None) -> dict[str, Any]:
        """Read the target catalog once, bounded in time.

        This call site deliberately bypasses the shared unary forward helper:
        that helper logs the transport exception text, and a WebSocket close
        carries an AgentServer-chosen reason verbatim. The request holds no
        credential and the response holds model configuration, so neither the
        exception nor its text is logged, chained or returned from here.
        """
        if self._agent_client is None:
            raise ValueError("Cron model catalog unavailable")
        identity = normalize_routing_identity(
            {"user_id": user_id or "", "group_id": "", "bot_id": ""}
        )
        envelope = e2a_from_agent_fields(
            request_id=f"cron-models-{uuid.uuid4().hex}",
            channel_id="web",
            session_id=None,
            req_method=ReqMethod.MODELS_LIST,
            params={},
            is_stream=False,
            timestamp=time.time(),
            user_id=user_id or None,
            metadata=apply_routing_metadata(None, identity),
        )
        failed = False
        try:
            async with asyncio.timeout(self._timeout_seconds):
                response = await send_agent_request_with_timeout(
                    self._agent_client, envelope, label="models.list",
                )
        except Exception:
            failed = True
        # Raised outside ``except`` so the transport exception is not kept as
        # implicit context either.
        if failed:
            raise ValueError("Cron model catalog unavailable")
        if (
            not isinstance(response, AgentResponse)
            or response.request_id != envelope.request_id
            or response.channel_id != envelope.channel
        ):
            raise ValueError("Cron model catalog protocol error")
        if not response.ok:
            raise ValueError("Cron model catalog unavailable")
        self._validate_payload(response.payload)
        return response.payload

    @staticmethod
    def _validate_payload(payload: Any) -> None:
        if (
            type(payload) is not dict
            or type(payload.get("models")) is not list
            or type(payload.get("active_model")) is not str
            or any(type(row) is not dict for row in payload["models"])
        ):
            raise ValueError("Cron model catalog protocol error")

    @classmethod
    def _match(cls, payload: dict[str, Any], value: str) -> tuple[str | None, str]:
        matches: list[tuple[dict[str, Any], str]] = []
        for row in payload["models"]:
            if not cls._valid_model_row(row):
                continue
            if value in cls._row_values(row):
                matches.append((row, cls._row_source(row)))
        # A configured model wins over a same-named free or login entry.
        for source in ("config", "zen"):
            for row, row_source in matches:
                if row_source == source:
                    return cls._text(row.get("model_name")) or None, source
        for row, row_source in matches:
            if row_source in {"login", "agentos"}:
                return cls._text(row.get("model_name")) or None, row_source
        return None, ""

    @classmethod
    def _active_source(cls, payload: dict[str, Any]) -> str:
        active_model = cls._text(payload.get("active_model"))
        if not active_model:
            return ""
        for row in payload["models"]:
            if not cls._valid_model_row(row):
                continue
            if active_model in cls._row_values(row):
                return cls._row_source(row)
        return ""

    @staticmethod
    def _row_source(row: dict[str, Any]) -> str:
        if row.get("source") == LOGIN_MODEL_SOURCE:
            return "login"
        if row.get("is_agentos") is True:
            return "agentos"
        if row.get("is_free") is True:
            return "zen"
        return "config"

    @staticmethod
    def _valid_model_row(row: dict[str, Any]) -> bool:
        model_name = row.get("model_name")
        is_agentos = row.get("is_agentos")
        is_free = row.get("is_free")
        source = row.get("source")
        model_id = row.get("model_id")
        alias = row.get("alias")
        return (
            isinstance(model_name, str)
            and bool(model_name.strip())
            and (is_agentos is None or isinstance(is_agentos, bool))
            and (is_free is None or isinstance(is_free, bool))
            and (source is None or isinstance(source, str))
            and (model_id is None or isinstance(model_id, str))
            and (alias is None or isinstance(alias, str))
        )

    @classmethod
    def _row_values(cls, row: dict[str, Any]) -> set[str]:
        values = {
            cls._text(row.get("model_name")),
            cls._text(row.get("alias")),
            cls._text(row.get("model_id")),
        }
        return {value for value in values if value}

    @staticmethod
    def _text(value: Any) -> str:
        return value.strip() if isinstance(value, str) else ""

    def _validate_supported_source(self, source: str) -> None:
        if source == "login":
            raise ValueError("Cron login models are not enabled")
        if source not in _SUPPORTED_SOURCES:
            raise ValueError("Unknown cron model")
