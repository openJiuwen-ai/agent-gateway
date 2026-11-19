# Gateway 侧远端模型目录适配，配置以 AgentServer 的 models.list 为准。
"""Read model metadata from AgentServer without persisting Gateway copies."""

from __future__ import annotations

import logging
from typing import Any

from e2a.message import ReqMethod

from agent_gateway.auth.model_catalog import LOGIN_MODEL_SOURCE
from agent_gateway.common.model_errors import (
    MODEL_SELECTION_NOT_FOUND,
    ModelSelectionError,
)
from agent_gateway.common.model_selection import ModelSelection
from agent_gateway.routing.e2a_proxy import fetch_agent_unary

logger = logging.getLogger(__name__)



async def fetch_agent_model_rows(
    *,
    agent_client: Any,
    session_id: str | None = None,
    user_id: str | None = None,
    auth_session: str | None = None,
    metadata: dict[str, Any] | None = None,
    channel_id: str = "web",
) -> list[dict[str, Any]] | None:
    """Return the AgentServer model rows, or ``None`` when the query failed."""
    if agent_client is None:
        return None
    params: dict[str, Any] = {}
    if auth_session:
        from e2a.constants import E2A_AUTH_SESSION_PARAM_KEY

        params[E2A_AUTH_SESSION_PARAM_KEY] = auth_session
    ok, payload = await fetch_agent_unary(
        agent_client=agent_client,
        req_method=ReqMethod.MODELS_LIST,
        params=params,
        session_id=session_id,
        user_id=user_id,
        channel_id=channel_id,
        metadata=metadata,
        label="models.list",
    )
    if not ok:
        logger.warning(
            "[models.list] Gateway remote catalog unavailable: %s",
            payload.get("error") if isinstance(payload, dict) else payload,
        )
        return None
    rows = payload.get("models") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return None
    return [row for row in rows if isinstance(row, dict)]


def resolve_remote_cron_model(
    raw: Any, rows: list[dict[str, Any]]
) -> tuple[str | None, str]:
    """Resolve a Cron model name/alias from models.list rows."""
    if raw is None:
        return None, ""
    value = str(raw).strip()
    if not value:
        return None, ""

    matching_rows = [
        row
        for row in rows
        if str(row.get("model_name") or "").strip()
        and value
        in {
            str(row.get("model_name") or "").strip(),
            str(row.get("alias") or "").strip(),
        }
    ]
    configured_rows = [
        row for row in matching_rows if row.get("source") != LOGIN_MODEL_SOURCE
    ]
    if configured_rows:
        return str(configured_rows[0].get("model_name") or "").strip(), "config"
    if matching_rows:
        return str(matching_rows[0].get("model_name") or "").strip(), "login"

    available = [
        str(row.get("model_name") or "").strip()
        for row in rows
        if str(row.get("model_name") or "").strip()
    ]
    hint = ", ".join(available[:20]) if available else "(no models configured)"
    if len(available) > 20:
        hint += f" ... and {len(available) - 20} more"
    raise ValueError(f"Unknown model {value!r}. Available models: {hint}")


class RemoteModelCatalog:
    """Minimal ModelCatalog-compatible view backed by models.list rows."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        by_id: dict[str, dict[str, Any]] = {}
        public: list[dict[str, Any]] = []
        for row in rows:
            model_id = str(row.get("model_id") or "").strip()
            model_name = str(row.get("model_name") or "").strip()
            if not model_id or not model_name:
                continue
            source = "agentos" if row.get("is_agentos") else "defaults"
            entry = {
                "model_id": model_id,
                "alias": str(row.get("alias") or ""),
                "model_client_config": {
                    "model_name": model_name,
                    "client_provider": str(row.get("model_provider") or ""),
                    "api_base": str(row.get("api_base") or ""),
                    "api_key": str(row.get("api_key") or ""),
                },
                "model_config_obj": {
                    "context_window": row.get("context_window_tokens"),
                },
                "is_default": bool(row.get("is_default")),
            }
            by_id[model_id] = {"source": source, "entry": entry}
            public.append(
                {
                    "model_id": model_id,
                    "alias": entry["alias"],
                    "model_name": model_name,
                    "provider": entry["model_client_config"]["client_provider"],
                    "source": source,
                    "is_agentos": source == "agentos",
                    "is_default": bool(row.get("is_default")),
                    "enabled": True,
                    "context_window": row.get("context_window_tokens"),
                }
            )
        self.snapshot = {
            "defaults": [],
            "agentos": [],
            "groups": [],
            "by_id": by_id,
        }
        self._public = public

    def get_model(self, model_id: str) -> dict[str, Any]:
        hit = self.snapshot["by_id"].get(model_id)
        if hit is None:
            raise ModelSelectionError(
                MODEL_SELECTION_NOT_FOUND, f"unknown model_id {model_id!r}"
            )
        return hit

    def has_selection(self, selection: ModelSelection) -> bool:
        if selection.type == "model":
            return selection.id in self.snapshot["by_id"]
        return False

    def list_public_models(self) -> list[dict[str, Any]]:
        return list(self._public)

    def get_group(self, group_id: str) -> dict[str, Any]:
        raise ModelSelectionError(
            MODEL_SELECTION_NOT_FOUND, f"unknown model_group_id {group_id!r}"
        )
