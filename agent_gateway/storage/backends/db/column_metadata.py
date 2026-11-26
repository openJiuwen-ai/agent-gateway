# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Shared database column and dialect metadata helpers."""

from __future__ import annotations

from typing import Any

_PG_DIALECTS = frozenset({"postgresql", "postgres", "pg", "gaussdb", "opengauss"})


def dialect_name(handler: Any) -> str:
    return handler.get_engine().dialect.name


def column_names(handler: Any, table_name: str, column_type: type[Any]) -> set[str]:
    get_table = getattr(handler, "get_table", None)
    if not callable(get_table):
        return set()
    try:
        table = get_table(table_name)
    except (ValueError, AttributeError):
        return set()
    return {
        column.name
        for column in table.columns
        if isinstance(column.type, column_type)
    }


def is_postgres_dialect(name: str) -> bool:
    return name in _PG_DIALECTS
