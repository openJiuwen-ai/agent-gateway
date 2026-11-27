# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Runtime extension assembly and compatibility entry point."""

from agent_gateway.extensions.runtime_management_extension.agent_authorization import authorize_agent
from agent_gateway.extensions.runtime_management_extension.invoke_ids import apply_invoke_ids_to_envelope
from agent_gateway.extensions.runtime_management_extension.pod_http_client import HttpSseAgentServerClient
from agent_gateway.southbound.orchestration import delegated_router
from agent_gateway.southbound.orchestration.delegated_router import DelegatedRouter


class RuntimeRoutedAgentClient(DelegatedRouter):
    """Supply Runtime-specific dependencies at the extension boundary."""

    def __init__(self, **kwargs):
        super().__init__(
            authorize=authorize_agent,
            prepare_envelope=apply_invoke_ids_to_envelope,
            http_factory=HttpSseAgentServerClient,
            **kwargs,
        )


def __getattr__(name):
    return getattr(delegated_router, name)
