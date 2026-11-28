# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Select northbound business policies when wiring a deployment."""

from typing import Any, Callable

from agent_gateway.agent_client_mode import is_runtime_agent_client
from agent_gateway.routing.e2a_proxy import (
    is_agentos_routing_client,
    is_legacy_shared_directory_client,
)


def select_web_frame_projector(client: Any) -> Callable[[Any], Any]:
    if is_agentos_routing_client(client):
        from agent_gateway.channel_manager.web.agentos_compat import project_agentos_web_frame

        return project_agentos_web_frame
    return _identity_frame


def _identity_frame(frame: Any) -> Any:
    return frame


def select_attachment_overwrite(client: Any) -> bool:
    """Avoid overwriting files in isolated per-user workspaces."""
    return not is_agentos_routing_client(client)


def select_im_attachment_guard(client: Any) -> Callable[[], None]:
    if is_agentos_routing_client(client):
        return _require_authenticated_im_route
    return _allow_im_attachment


def _require_authenticated_im_route() -> None:
    raise RuntimeError("IM attachment routing requires an authenticated AgentOS user_id")


def _allow_im_attachment() -> None:
    pass


def select_gateway_config_location(client: Any) -> bool:
    """Whether enterprise Gateway owns config/models for this deployment."""
    return (
        client is None
        or is_runtime_agent_client(client)
        or is_legacy_shared_directory_client(client)
    )


def select_container_file_backend(client: Any) -> Any:
    """Return the backend admitted to same-port container file routes."""
    if not is_agentos_routing_client(client):
        return None
    return client
