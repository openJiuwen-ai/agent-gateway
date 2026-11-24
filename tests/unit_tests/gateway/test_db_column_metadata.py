"""Tests for shared persistent DB column metadata helpers."""

from types import SimpleNamespace

from sqlalchemy import Boolean, Column, DateTime, Integer, MetaData, Table

from agent_gateway.storage.backends.db.column_metadata import (
    column_names,
    dialect_name,
    is_postgres_dialect,
)


class _Handler:
    def __init__(self, table: Table, dialect: str = "sqlite") -> None:
        self._table = table
        self._dialect = dialect

    def get_engine(self):
        return SimpleNamespace(dialect=SimpleNamespace(name=self._dialect))

    def get_table(self, table_name: str) -> Table:
        if table_name != self._table.name:
            raise ValueError(table_name)
        return self._table


def _table() -> Table:
    return Table(
        "records",
        MetaData(),
        Column("created_at", DateTime()),
        Column("enabled", Boolean()),
        Column("name", Integer()),
    )


def test_column_names_selects_columns_by_sqlalchemy_type() -> None:
    handler = _Handler(_table())

    assert column_names(handler, "records", DateTime) == {"created_at"}
    assert column_names(handler, "records", Boolean) == {"enabled"}
    assert column_names(handler, "missing", DateTime) == set()


def test_column_names_handles_handlers_without_table_lookup() -> None:
    assert column_names(object(), "records", DateTime) == set()


def test_dialect_helpers_preserve_supported_postgres_aliases() -> None:
    handler = _Handler(_table(), "gaussdb")

    assert dialect_name(handler) == "gaussdb"
    assert all(
        is_postgres_dialect(name)
        for name in ("postgresql", "postgres", "pg", "gaussdb", "opengauss")
    )
    assert not is_postgres_dialect("mysql")
