"""DuckDB target implementation."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import duckdb
import pyarrow as pa

from ..core import Cursor, MergeStrategy


class DuckDBTarget:
    """Write an Arrow stream to a table in a local DuckDB database."""

    def __init__(self, target: Mapping[str, Any]) -> None:
        self._database = _required_string(target, "path")
        self._table = _table_name(target)

    def read_cursor(self, field: str) -> Cursor | None:
        _column_name(field)
        connection = duckdb.connect(self._database)
        try:
            if not _table_exists(connection, self._table):
                return None
            value = connection.execute(
                f"SELECT max({_quoted_identifier(field)}) FROM {_quoted_identifier(self._table)}"
            ).fetchone()[0]
        finally:
            connection.close()

        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"Cursor field {field!r} in the DuckDB table must be an integer.")
        return Cursor(field, value)

    def load(self, stream: pa.RecordBatchReader, strategy: MergeStrategy) -> int:
        connection = duckdb.connect(self._database)
        try:
            connection.register("ingester_stream", stream)
            if strategy is MergeStrategy.REPLACE:
                connection.execute(
                    f"CREATE OR REPLACE TABLE {_quoted_identifier(self._table)} AS "
                    "SELECT * FROM ingester_stream"
                )
                row_count = _row_count(connection, self._table)
            elif strategy is MergeStrategy.APPEND:
                if _table_exists(connection, self._table):
                    existing_row_count = _row_count(connection, self._table)
                    connection.execute(
                        f"INSERT INTO {_quoted_identifier(self._table)} "
                        "SELECT * FROM ingester_stream"
                    )
                    row_count = _row_count(connection, self._table) - existing_row_count
                else:
                    connection.execute(
                        f"CREATE TABLE {_quoted_identifier(self._table)} AS "
                        "SELECT * FROM ingester_stream"
                    )
                    row_count = _row_count(connection, self._table)
            else:
                raise ValueError(f"DuckDBTarget does not support merge strategy {strategy!r}.")
        finally:
            connection.close()

        if not isinstance(row_count, int):
            raise AssertionError("DuckDB row count query did not return an integer.")
        return row_count


def _required_string(target: Mapping[str, Any], name: str) -> str:
    value = target.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"target.{name} must be a non-empty string.")
    return value


def _table_name(target: Mapping[str, Any]) -> str:
    table = _required_string(target, "table")
    _column_name(table, "target.table")
    return table


def _column_name(value: str, name: str = "cursor field") -> None:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{name} must be an unqualified SQL identifier.")


def _quoted_identifier(identifier: str) -> str:
    return f'"{identifier}"'


def _table_exists(connection: duckdb.DuckDBPyConnection, table: str) -> bool:
    return (
        connection.execute(
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = current_schema() AND table_name = ?"
            ")",
            [table],
        ).fetchone()[0]
        is True
    )


def _row_count(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    row_count = connection.execute(f"SELECT count(*) FROM {_quoted_identifier(table)}").fetchone()[
        0
    ]
    if not isinstance(row_count, int):
        raise AssertionError("DuckDB row count query did not return an integer.")
    return row_count
