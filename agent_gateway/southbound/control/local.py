# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Fixed-target control plane; no remote resource lifecycle."""

from dataclasses import dataclass

from agent_gateway.southbound.contracts import ConnectionTarget


@dataclass(frozen=True)
class LocalControl:
    target: ConnectionTarget

    def get_target(self) -> ConnectionTarget:
        return self.target
