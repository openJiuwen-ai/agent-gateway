# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Fixed-target control plane; no remote resource lifecycle."""

from dataclasses import dataclass

from agent_gateway.southbound.contracts import ConnectionTarget
from agent_gateway.southbound.control.interfaces import StaticControl


@dataclass(frozen=True)
class FixedTargetControl(StaticControl):
    target: ConnectionTarget

    def get_target(self) -> ConnectionTarget:
        return self.target
