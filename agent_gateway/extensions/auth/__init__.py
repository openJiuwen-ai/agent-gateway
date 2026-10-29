# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""AgentOS authentication modules."""

from agent_gateway.extensions.auth.credential_authenticator import (
    AuthContext,
    AuthResult,
    CredentialAuthenticator,
)
from agent_gateway.extensions.auth.ssh_authenticator import SshPublicKeyAuthenticator
from agent_gateway.extensions.auth.ssh_key_issuer import (
    AgentOSSshKeyIssuer,
    SshKeyIssuer,
)
from agent_gateway.extensions.auth.ssh_key_registry import (
    KeyRegistry,
    KeyRegistryEntry,
)

__all__ = [
    "AuthContext",
    "AuthResult",
    "CredentialAuthenticator",
    "KeyRegistry",
    "KeyRegistryEntry",
    "SshKeyIssuer",
    "AgentOSSshKeyIssuer",
    "SshPublicKeyAuthenticator",
]
