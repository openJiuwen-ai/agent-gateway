# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Gateway-side package management for Extensions V2.

The Gateway shares ``extension_pkgs`` with AgentServer. This package owns the
Gateway half: copying Gateway zips into the shared store during its own install
flow, publishing and releasing them on the live runtime, and reconciling the
Gateway domain from the shared install records. Catalog and request dispatch
belong to the AgentServer management entry.
"""

from agent_gateway.extensions_v2.management.registry import GatewayPackageManager

__all__ = ["GatewayPackageManager"]
