# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Old import path kept for the jiuwenswarm develop that Gateway ships with.

New code imports ``route_binding_shared``.
"""

from agent_gateway.enterprise.team.route_binding_shared import *  # noqa: F403
from agent_gateway.enterprise.team.route_binding_shared import SharedTeamRouteBindingStore

MysqlTeamRouteBindingStore = SharedTeamRouteBindingStore
