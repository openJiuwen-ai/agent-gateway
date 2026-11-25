# coding: utf-8
# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""企业版 Gateway 本地库表初始化。"""

from __future__ import annotations

from openjiuwen_runtime.foundation.db.handler import DBHandler
from openjiuwen_runtime.foundation.db.table_def import TableDefinition
from openjiuwen_runtime.foundation.log import get_logger

from .a2a_migration import ensure_dispatch_user_column, ensure_user_state_identity
from .a2a_models import (
    A2A_OUTBOUND_DISPATCH_TABLE_DEF,
    A2A_OUTBOUND_RUNTIME_STATE_TABLE_DEF,
    A2A_OUTBOUND_USER_STATE_TABLE_DEF,
)
from .application_config_models import (
    ADVANCED_CONFIG_TABLE_DEF,
    AUDIT_LOG_CONFIG_TABLE_DEF,
    CHANNEL_CONFIG_TABLE_DEF,
    CRON_POLICY_TABLE_DEF,
    LOG_MASKING_RULE_TABLE_DEF,
    LOGGING_CONFIG_TABLE_DEF,
    MEMORY_CONFIG_TABLE_DEF,
    TASK_MEMORY_CONFIG_TABLE_DEF,
    USER_PREFERENCE_TABLE_DEF,
)
from .cron_job_models import CRON_JOB_TABLE_DEF
from .instance_resource_models import INSTANCE_AGENT_RESOURCE_TABLE_DEF
from .key_models import (
    GATEWAY_ENC_KEYPAIR_TABLE_DEF,
    GATEWAY_SIGN_KEYPAIR_TABLE_DEF,
    MANAGER_SIGN_PUBKEY_TABLE_DEF,
)
from .quota_models import QUOTA_TABLE_DEFINITIONS
from .session_map_models import SESSION_MAP_TABLE_DEF
from .template_models import (
    A2A_ACCESS_POLICY_TEMPLATE_TABLE_DEF,
    A2A_OUTBOUND_TEMPLATE_TABLE_DEF,
    AGENT_TEMPLATE_TABLE_DEF,
    EMBEDDING_TEMPLATE_TABLE_DEF,
    EXTENSION_CONFIG_TEMPLATE_TABLE_DEF,
    MCP_TEMPLATE_TABLE_DEF,
    MODEL_TEMPLATE_TABLE_DEF,
    PERMISSIONS_TEMPLATE_TABLE_DEF,
    REACT_TEMPLATE_TABLE_DEF,
    SKILL_PREBUILT_TEMPLATE_TABLE_DEF,
    EXPERT_PACKAGE_TEMPLATE_TABLE_DEF,
)

_log = get_logger(__name__)

# Team control and quota tables are created by TeamStoreActor.start().
# Initializing them here as well takes a metadata lock while a chat
# pre-reserve transaction is open on the same tables.
ALL_TABLE_DEFINITIONS: tuple[TableDefinition, ...] = (
    GATEWAY_ENC_KEYPAIR_TABLE_DEF,
    GATEWAY_SIGN_KEYPAIR_TABLE_DEF,
    MANAGER_SIGN_PUBKEY_TABLE_DEF,
    MODEL_TEMPLATE_TABLE_DEF,
    EMBEDDING_TEMPLATE_TABLE_DEF,
    EXTENSION_CONFIG_TEMPLATE_TABLE_DEF,
    SKILL_PREBUILT_TEMPLATE_TABLE_DEF,
    EXPERT_PACKAGE_TEMPLATE_TABLE_DEF,
    MCP_TEMPLATE_TABLE_DEF,
    PERMISSIONS_TEMPLATE_TABLE_DEF,
    REACT_TEMPLATE_TABLE_DEF,
    AGENT_TEMPLATE_TABLE_DEF,
    A2A_OUTBOUND_TEMPLATE_TABLE_DEF,
    A2A_ACCESS_POLICY_TEMPLATE_TABLE_DEF,
    A2A_OUTBOUND_USER_STATE_TABLE_DEF,
    A2A_OUTBOUND_RUNTIME_STATE_TABLE_DEF,
    A2A_OUTBOUND_DISPATCH_TABLE_DEF,
    INSTANCE_AGENT_RESOURCE_TABLE_DEF,
    CHANNEL_CONFIG_TABLE_DEF,
    LOG_MASKING_RULE_TABLE_DEF,
    LOGGING_CONFIG_TABLE_DEF,
    TASK_MEMORY_CONFIG_TABLE_DEF,
    MEMORY_CONFIG_TABLE_DEF,
    ADVANCED_CONFIG_TABLE_DEF,
    AUDIT_LOG_CONFIG_TABLE_DEF,
    USER_PREFERENCE_TABLE_DEF,
    CRON_POLICY_TABLE_DEF,
    *QUOTA_TABLE_DEFINITIONS,
    CRON_JOB_TABLE_DEF,
    SESSION_MAP_TABLE_DEF,
)


async def _ensure_user_preference_extras_column(handler: DBHandler) -> None:
    """旧库补 ``user_preference.extras``（init_table 不会给已有表加列）。"""
    from sqlalchemy import inspect, text
    from sqlalchemy.exc import DBAPIError

    table_name = USER_PREFERENCE_TABLE_DEF.table_name

    def upgrade(connection) -> None:
        inspector = inspect(connection)
        if not inspector.has_table(table_name):
            return
        cols = {col["name"] for col in inspector.get_columns(table_name)}
        if "extras" in cols:
            return
        quote = connection.dialect.identifier_preparer.quote
        dialect = connection.dialect.name
        if dialect == "postgresql":
            col_type = "JSONB"
        elif dialect == "mysql":
            col_type = "JSON"
        else:
            col_type = "TEXT"
        connection.execute(
            text(
                f"ALTER TABLE {quote(table_name)} "
                f"ADD COLUMN {quote('extras')} {col_type} NULL"
            )
        )

    for attempt in range(3):
        try:
            async with handler.get_engine().begin() as connection:
                await connection.run_sync(upgrade)
            return
        except DBAPIError as exc:
            dialect = handler.get_engine().dialect.name
            sqlstate = getattr(exc.orig, "sqlstate", None) or getattr(
                exc.orig, "pgcode", None
            )
            errno = exc.orig.args[0] if exc.orig.args else None
            column_race = (
                (dialect == "postgresql" and sqlstate == "42701")
                or (dialect == "mysql" and errno == 1060)
                or "duplicate column" in str(exc.orig).lower()
            )
            if not column_race or attempt == 2:
                raise


async def _drop_model_template_retired_flags(handler: DBHandler) -> None:
    """删除已下线的 model_template 布尔列，避免旧库 NOT NULL 无默认值导致写入失败。

    ``enable_streaming`` / ``enable_function_calling`` 已从 TableDefinition 移除，
    但既有 MariaDB/MySQL 表仍可能保留这两列且无 DEFAULT。

    DROP 失败只记 warning 并继续启动：新代码已不再读写这两列。
    """
    from sqlalchemy import inspect, text
    from sqlalchemy.exc import DBAPIError

    retired = ("enable_streaming", "enable_function_calling")
    table_name = MODEL_TEMPLATE_TABLE_DEF.table_name

    def migrate(connection) -> None:
        inspector = inspect(connection)
        if not inspector.has_table(table_name):
            return
        columns = {col["name"] for col in inspector.get_columns(table_name)}
        quote = connection.dialect.identifier_preparer.quote
        dropped: list[str] = []
        for column in retired:
            if column not in columns:
                continue
            connection.execute(
                text(
                    f"ALTER TABLE {quote(table_name)} "
                    f"DROP COLUMN {quote(column)}"
                )
            )
            dropped.append(column)
        if dropped:
            _log.info("dropped model_template columns: %s", ", ".join(dropped))

    try:
        async with handler.get_engine().begin() as connection:
            await connection.run_sync(migrate)
    except DBAPIError as exc:
        dialect = handler.get_engine().dialect.name
        sqlstate = getattr(exc.orig, "sqlstate", None) or getattr(
            exc.orig, "pgcode", None
        )
        errno = exc.orig.args[0] if exc.orig.args else None
        message = str(exc.orig).lower()
        already_dropped = (
            (dialect == "postgresql" and sqlstate == "42703")
            or (dialect == "mysql" and errno == 1091)
            or "no such column" in message
            or "does not exist" in message
        )
        if not already_dropped:
            _log.warning(
                "drop model_template.enable_streaming/enable_function_calling failed: %s",
                exc,
            )


async def init_all_tables(handler: DBHandler) -> None:
    """对已连接的 ``handler`` 依次 ``init_table``，幂等（表已存在则跳过创建逻辑）。"""
    for table_def in ALL_TABLE_DEFINITIONS:
        if table_def is A2A_OUTBOUND_USER_STATE_TABLE_DEF:
            await ensure_user_state_identity(handler.get_engine())
        if table_def is A2A_OUTBOUND_DISPATCH_TABLE_DEF:
            await ensure_dispatch_user_column(handler.get_engine())
        await handler.init_table(table_def)
    await _ensure_user_preference_extras_column(handler)
    await _drop_model_template_retired_flags(handler)
