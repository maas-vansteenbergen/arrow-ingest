"""Shared SCD2 merge operation for Arrow-backed destinations."""

from __future__ import annotations

from collections.abc import Sequence

import duckdb
import pyarrow as pa

from ..core import MergeConfiguration, SCD2Configuration
from ._duckdb_relations import row_count as duckdb_row_count
from ._duckdb_relations import table_exists, table_schema
from ._upsert import key_match_condition, quoted_identifier, validate_incoming_keys


def apply_scd2(
    connection: duckdb.DuckDBPyConnection,
    destination: str,
    schema: pa.Schema,
    keys: Sequence[str],
    configuration: SCD2Configuration,
) -> int:
    _validate_source_schema(schema, keys, configuration)
    connection.execute("CREATE TEMP TABLE ingester_input AS SELECT * FROM ingester_stream")
    validate_incoming_keys(connection, "ingester_input", keys)
    row_count = duckdb_row_count(connection, "ingester_input")

    if not table_exists(connection, destination):
        connection.execute(
            f"CREATE TABLE {quoted_identifier(destination)} AS "
            f"{_new_version_rows('ingester_input', configuration)}"
        )
        return row_count

    connection.execute(
        f"CREATE TEMP TABLE ingester_scd2_shape AS "
        f"{_new_version_rows('ingester_input', configuration)} LIMIT 0"
    )
    if table_schema(connection, destination) != table_schema(connection, "ingester_scd2_shape"):
        raise ValueError("Cannot apply SCD2 with a different source schema or metadata columns.")

    current = _current_rows(destination, configuration)
    _validate_current_rows(connection, current, keys, configuration)
    _validate_effective_order(connection, current, keys, configuration)
    connection.execute(
        "CREATE TEMP TABLE ingester_scd2_changes AS "
        f"{_changed_rows('ingester_input', current, keys, configuration)}"
    )

    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute(
            f"UPDATE {quoted_identifier(destination)} AS existing "
            f"SET {quoted_identifier(configuration.valid_to)} = changes."
            f"{quoted_identifier(configuration.effective_at)}, "
            f"{quoted_identifier(configuration.is_current)} = FALSE "
            "FROM ingester_scd2_changes AS changes "
            f"WHERE existing.{quoted_identifier(configuration.is_current)} "
            f"AND {key_match_condition('existing', 'changes', keys)}"
        )
        connection.execute(
            f"INSERT INTO {quoted_identifier(destination)} "
            f"{_new_version_rows('ingester_scd2_changes', configuration)}"
        )
    except duckdb.Error:
        connection.execute("ROLLBACK")
        raise
    else:
        connection.execute("COMMIT")
    return row_count


def _validate_source_schema(
    schema: pa.Schema,
    keys: Sequence[str],
    configuration: SCD2Configuration,
) -> None:
    required_fields = [*keys, *configuration.tracked_fields, configuration.effective_at]
    missing_fields = [field for field in required_fields if schema.get_field_index(field) == -1]
    if missing_fields:
        missing = ", ".join(dict.fromkeys(missing_fields))
        raise ValueError(f"SCD2 fields are missing from the incoming schema: {missing}.")
    if schema.field(configuration.effective_at).type != pa.int64():
        raise ValueError("SCD2 effective_at must have declared type 'int64'.")

    metadata_fields = {
        configuration.valid_from,
        configuration.valid_to,
        configuration.is_current,
    }
    conflicting_fields = metadata_fields.intersection(schema.names)
    if conflicting_fields:
        fields = ", ".join(sorted(conflicting_fields))
        raise ValueError(f"SCD2 metadata fields must not exist in the incoming schema: {fields}.")


def _current_rows(destination: str, configuration: SCD2Configuration) -> str:
    return (
        f"(SELECT * FROM {quoted_identifier(destination)} "
        f"WHERE {quoted_identifier(configuration.is_current)})"
    )


def _validate_current_rows(
    connection: duckdb.DuckDBPyConnection,
    current: str,
    keys: Sequence[str],
    configuration: SCD2Configuration,
) -> None:
    if connection.execute(
        f"SELECT 1 FROM {current} "
        f"WHERE {quoted_identifier(configuration.valid_to)} IS NOT NULL LIMIT 1"
    ).fetchone():
        raise ValueError("Current SCD2 rows must have a null valid_to value.")

    key_columns = ", ".join(quoted_identifier(key) for key in keys)
    if connection.execute(
        f"SELECT 1 FROM ("
        f"SELECT {key_columns} FROM {current} "
        f"GROUP BY {key_columns} HAVING count(*) > 1"
        f") LIMIT 1"
    ).fetchone():
        raise ValueError("SCD2 output must have at most one current row per key.")


def _validate_effective_order(
    connection: duckdb.DuckDBPyConnection,
    current: str,
    keys: Sequence[str],
    configuration: SCD2Configuration,
) -> None:
    if connection.execute(
        f"SELECT 1 FROM ingester_input AS incoming "
        f"JOIN {current} AS existing "
        f"ON {key_match_condition('incoming', 'existing', keys)} "
        f"WHERE incoming.{quoted_identifier(configuration.effective_at)} <= "
        f"existing.{quoted_identifier(configuration.valid_from)} LIMIT 1"
    ).fetchone():
        raise ValueError("Incoming SCD2 effective_at values must be greater than current versions.")


def _changed_rows(
    incoming: str,
    current: str,
    keys: Sequence[str],
    configuration: SCD2Configuration,
) -> str:
    tracked_difference = " OR ".join(
        f"incoming.{quoted_identifier(field)} IS DISTINCT FROM existing.{quoted_identifier(field)}"
        for field in configuration.tracked_fields
    )
    return (
        f"SELECT incoming.* FROM {incoming} AS incoming "
        f"LEFT JOIN {current} AS existing "
        f"ON {key_match_condition('incoming', 'existing', keys)} "
        f"WHERE existing.{quoted_identifier(keys[0])} IS NULL OR ({tracked_difference})"
    )


def _new_version_rows(relation: str, configuration: SCD2Configuration) -> str:
    return (
        f"SELECT source.*, source.{quoted_identifier(configuration.effective_at)} "
        f"AS {quoted_identifier(configuration.valid_from)}, "
        f"CAST(NULL AS BIGINT) AS {quoted_identifier(configuration.valid_to)}, "
        f"TRUE AS {quoted_identifier(configuration.is_current)} "
        f"FROM {relation} AS source"
    )


def scd2_configuration(merge: MergeConfiguration) -> SCD2Configuration:
    if merge.scd2 is None:
        raise AssertionError("SCD2 merge configuration is missing SCD2 settings.")
    return merge.scd2
