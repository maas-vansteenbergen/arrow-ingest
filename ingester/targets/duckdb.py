"""DuckDB target implementation."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

import duckdb
import pyarrow as pa

from ..core import Cursor, MergeConfiguration, MergeStrategy
from ._duckdb_relations import row_count as duckdb_row_count
from ._duckdb_relations import table_exists, table_schema
from ._scd2 import apply_scd2, scd2_configuration
from ._upsert import (
    key_match_condition,
    quoted_identifier,
    validate_incoming_keys,
    validate_key_fields,
)


class DuckDBTarget:
    """Write an Arrow stream to a table in a local DuckDB database.

    Args:
        target: Target configuration containing the database ``path`` and
            destination ``table``.
    """

    def __init__(self, target: Mapping[str, Any]) -> None:
        self._database = _required_string(target, "path")
        self._table = _table_name(target)

    def read_cursor(self, field: str) -> Cursor | None:
        """Read the largest persisted integer value for a table field.

        Args:
            field: The table column from which to derive a high-watermark.

        Returns:
            The current high-watermark, or ``None`` when the table is absent or
            contains no values.

        Raises:
            ValueError: If the field is invalid or does not contain integers.
        """
        _column_name(field)
        connection = duckdb.connect(self._database)
        try:
            if not table_exists(connection, self._table):
                return None
            value = connection.execute(
                f"SELECT max({quoted_identifier(field)}) FROM {quoted_identifier(self._table)}"
            ).fetchone()[0]
        finally:
            connection.close()

        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"Cursor field {field!r} in the DuckDB table must be an integer.")
        return Cursor(field, value)

    def load(self, stream: pa.RecordBatchReader, merge: MergeConfiguration) -> int:
        """Load an Arrow stream into the configured DuckDB table.

        Args:
            stream: The incoming Arrow record batches.
            merge: The merge strategy and its validated settings.

        Returns:
            The number of incoming rows written to the table.

        Raises:
            ValueError: If the merge strategy, input keys, or schemas are
                incompatible with the destination.
        """
        connection = duckdb.connect(self._database)
        try:
            connection.register("ingester_stream", stream)
            if merge.strategy is MergeStrategy.REPLACE:
                connection.execute(
                    f"CREATE OR REPLACE TABLE {quoted_identifier(self._table)} AS "
                    "SELECT * FROM ingester_stream"
                )
                row_count = duckdb_row_count(connection, self._table)
            elif merge.strategy is MergeStrategy.APPEND:
                if table_exists(connection, self._table):
                    existing_row_count = duckdb_row_count(connection, self._table)
                    connection.execute(
                        f"INSERT INTO {quoted_identifier(self._table)} "
                        "SELECT * FROM ingester_stream"
                    )
                    row_count = duckdb_row_count(connection, self._table) - existing_row_count
                else:
                    connection.execute(
                        f"CREATE TABLE {quoted_identifier(self._table)} AS "
                        "SELECT * FROM ingester_stream"
                    )
                    row_count = duckdb_row_count(connection, self._table)
            elif merge.strategy is MergeStrategy.UPSERT:
                row_count = self._upsert_stream(connection, stream.schema, merge.keys)
            elif merge.strategy is MergeStrategy.SCD2:
                row_count = apply_scd2(
                    connection,
                    self._table,
                    stream.schema,
                    merge.keys,
                    scd2_configuration(merge),
                )
            else:
                raise ValueError(
                    f"DuckDBTarget does not support merge strategy {merge.strategy!r}."
                )
        finally:
            connection.close()

        if not isinstance(row_count, int):
            raise AssertionError("DuckDB row count query did not return an integer.")
        return row_count

    def _upsert_stream(
        self,
        connection: duckdb.DuckDBPyConnection,
        schema: pa.Schema,
        keys: tuple[str, ...],
    ) -> int:
        validate_key_fields(schema, keys)
        connection.execute("CREATE TEMP TABLE ingester_input AS SELECT * FROM ingester_stream")
        validate_incoming_keys(connection, "ingester_input", keys)
        row_count = duckdb_row_count(connection, "ingester_input")

        if not table_exists(connection, self._table):
            connection.execute(
                f"CREATE TABLE {quoted_identifier(self._table)} AS SELECT * FROM ingester_input"
            )
            return row_count
        if table_schema(connection, self._table) != table_schema(connection, "ingester_input"):
            raise ValueError(
                "Cannot upsert an Arrow stream with a different schema to DuckDB output."
            )

        self._replace_matching_rows(connection, keys)
        return row_count

    def _replace_matching_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        keys: tuple[str, ...],
    ) -> None:
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                f"DELETE FROM {quoted_identifier(self._table)} AS destination "
                "USING ingester_input AS incoming "
                f"WHERE {key_match_condition('destination', 'incoming', keys)}"
            )
            connection.execute(
                f"INSERT INTO {quoted_identifier(self._table)} SELECT * FROM ingester_input"
            )
        except duckdb.Error:
            connection.execute("ROLLBACK")
            raise
        else:
            connection.execute("COMMIT")


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
