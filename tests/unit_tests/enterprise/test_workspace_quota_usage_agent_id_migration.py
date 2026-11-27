# Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved

"""PR #426 P0：``_ensure_workspace_quota_usage_agent_id_column`` 迁移幂等性单测。

覆盖三条分支：表无 agent_id 列 → ADD COLUMN；表已有 agent_id 列 → 不重复添加；
表不存在 → 直接返回不报错。同时静态验证 ``WORKSPACE_QUOTA_USAGE_TABLE_DEF``
在 ``table_init`` 模块已导入（避免 NameError 启动崩）。
"""

from __future__ import annotations

from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
)
from sqlalchemy.ext.asyncio import create_async_engine

from agent_gateway.config.enterprise.tables import table_init
from agent_gateway.config.enterprise.tables.table_init import (
    _ensure_workspace_quota_usage_agent_id_column,
)


class _AsyncHandler:
    """最小 DBHandler stub：迁移函数只用到 ``get_engine()``（async engine）。"""

    def __init__(self, engine) -> None:
        self._engine = engine

    def get_engine(self):
        return self._engine


def test_table_init_module_imports_workspace_quota_usage_table_def() -> None:
    """P0 静态保护：``WORKSPACE_QUOTA_USAGE_TABLE_DEF`` 必须在 ``table_init`` 可解析。

    回归用例：上一版提交只在函数体引用该符号但未导入，企业版启动 ``init_all_tables``
    时必然 ``NameError``。本用例锁定导入不回退。
    """
    assert table_init.WORKSPACE_QUOTA_USAGE_TABLE_DEF is not None
    assert (
        table_init.WORKSPACE_QUOTA_USAGE_TABLE_DEF.table_name
        == "workspace_quota_usage"
    )


async def test_adds_agent_id_column_when_missing(tmp_path) -> None:
    # 用文件库：sync 建表 → async engine 跑迁移，确保表数据跨 engine 可见。
    db_file = tmp_path / "test_add.db"
    sync_url = f"sqlite:///{db_file}"
    async_url = f"sqlite+aiosqlite:///{db_file}"
    sync_engine = create_engine(sync_url)
    _create_usage_table_sync(sync_engine, with_agent_id=False)
    sync_engine.dispose()

    engine = create_async_engine(async_url)
    handler = _AsyncHandler(engine)
    await _ensure_workspace_quota_usage_agent_id_column(handler)
    await engine.dispose()

    with create_engine(sync_url).connect() as conn:
        cols = {col["name"] for col in inspect(conn).get_columns("workspace_quota_usage")}
    assert "agent_id" in cols


async def test_idempotent_when_agent_id_column_already_exists(tmp_path) -> None:
    db_file = tmp_path / "test_idempotent.db"
    sync_url = f"sqlite:///{db_file}"
    async_url = f"sqlite+aiosqlite:///{db_file}"
    sync_engine = create_engine(sync_url)
    _create_usage_table_sync(sync_engine, with_agent_id=True)
    sync_engine.dispose()

    engine = create_async_engine(async_url)
    handler = _AsyncHandler(engine)
    # 重复调用两次都不应报错（幂等）。
    await _ensure_workspace_quota_usage_agent_id_column(handler)
    await _ensure_workspace_quota_usage_agent_id_column(handler)
    await engine.dispose()

    with create_engine(sync_url).connect() as conn:
        cols = [col["name"] for col in inspect(conn).get_columns("workspace_quota_usage")]
    # 仍只有一列 agent_id（未重复 ADD）。
    assert cols.count("agent_id") == 1


async def test_noop_when_table_does_not_exist(tmp_path) -> None:
    db_file = tmp_path / "test_noop.db"
    async_url = f"sqlite+aiosqlite:///{db_file}"
    # 不创建 workspace_quota_usage 表。
    engine = create_async_engine(async_url)
    handler = _AsyncHandler(engine)
    # 表不存在时迁移函数应直接返回，不抛异常。
    await _ensure_workspace_quota_usage_agent_id_column(handler)
    await engine.dispose()

    with create_engine(f"sqlite:///{db_file}").connect() as conn:
        inspector = inspect(conn)
        assert not inspector.has_table("workspace_quota_usage")


def _create_usage_table_sync(sync_engine, *, with_agent_id: bool) -> None:
    cols = [
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("user_id", String(64), nullable=False),
        Column("group_id", String(64), nullable=False, default=""),
        Column("bot_id", String(64), nullable=False),
        Column("used_bytes", Integer, nullable=False),
        Column("reported_at", String(32), nullable=False),
        Column("created_at", String(32), nullable=False),
        Column("updated_at", String(32), nullable=False),
    ]
    if with_agent_id:
        cols.append(Column("agent_id", String(64), nullable=False, default=""))
    metadata = MetaData()
    Table("workspace_quota_usage", metadata, *cols)
    metadata.create_all(sync_engine)
