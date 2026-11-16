# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Old import path kept for the jiuwenswarm develop that Gateway ships with.

New code imports ``team_store_actor``. This module only supplies the names
that develop still imports. ``use_team_mysql`` stays true only for MySQL,
because those callers still run MySQL SQL.
"""

from agent_gateway.enterprise.db.team_store_actor import *  # noqa: F403
from agent_gateway.enterprise.db.team_store_actor import (
    TeamStoreActor,
    _gateway_db_kind,
    get_team_store_actor,
    reset_team_store_actor,
)

TeamMysqlActor = TeamStoreActor
get_team_mysql_actor = get_team_store_actor
reset_team_mysql_actor = reset_team_store_actor


def use_team_mysql() -> bool:
    """True only when ``GATEWAY_DB_TYPE`` is exactly ``mysql``."""
    return _gateway_db_kind() == "mysql"
