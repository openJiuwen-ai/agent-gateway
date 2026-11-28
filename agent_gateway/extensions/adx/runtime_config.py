# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""ADX-specific launch bindings owned by the AgentOS Router implementation."""
from __future__ import annotations


def runtime_environment_settings() -> dict:
    return {
        'preserve_env': ['JIUWENSWARM_ADX_NODE_INGRESS_URL', 'JIUWENSWARM_ADX_NODE_CONTEXT_FILE',
                         'ADX_TOKEN'],
    }


def managed_sandbox_settings() -> dict:
    return {
        'type': 'adx-environment',
        'managed_connection': {
            'url_env': 'JIUWENSWARM_ADX_NODE_INGRESS_URL',
            'context_file_env': 'JIUWENSWARM_ADX_NODE_CONTEXT_FILE',
            'replace_connection_fields': ['server_address', 'gateway_address', 'token', 'token_provider',
                                           'use_tls', 'gateway_use_tls', 'verify_tls'],
            'credential_fields': ['ADX_TOKEN', 'ADX_GATEWAY_TOKEN', 'JIUWENSWARM_ADX_API_TOKEN'],
        },
        'launcher_config': {'workspace_mounts': True, 'shared_scope': 'default'},
    }
