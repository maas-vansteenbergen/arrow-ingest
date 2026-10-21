"""DuckDB target implementation."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import duckdb
import pyarrow as pa

from ..core import MergeStrategy


class DuckDBTarget:
    """Write an Arrow stream to a table in a local DuckDB database."""

    def __init__(self, target: Mapping[str, Any]) -> None:
        self._database = _required_string(target, "path")
        self._table = _table_name(target)

    def load(self, stream: pa.RecordBatchReader, strategy: MergeStrategy) -> int:
        if strategy is not MergeStrategy.REPLACE:
            raise ValueError(f"DuckDBTarget does not support merge strategy {strategy!r}.")

        connection = duckdb.connect(self._database)
        try:
            connection.register("ingester_stream", stream)
            connection.execute(
                f"CREATE OR REPLACE TABLE {_quoted_identifier(self._table)} AS "
                "SELECT * FROM ingester_stream"
            )
            row_count = connection.execute(
                f"SELECT count(*) FROM {_quoted_identifier(self._table)}"
            ).fetchone()[0]
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
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
        raise ValueError("target.table must be an unqualified SQL identifier.")
    return table


def _quoted_identifier(identifier: str) -> str:
    return f'"{identifier}"'
