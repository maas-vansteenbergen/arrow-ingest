"""Shared upsert validation and SQL helpers."""

from __future__ import annotations

from collections.abc import Sequence

import duckdb
import pyarrow as pa


def validate_key_fields(schema: pa.Schema, keys: Sequence[str]) -> None:
    missing_keys = [key for key in keys if schema.get_field_index(key) == -1]
    if missing_keys:
        missing = ", ".join(missing_keys)
        raise ValueError(f"Upsert key fields are missing from the incoming schema: {missing}.")


def validate_incoming_keys(
    connection: duckdb.DuckDBPyConnection,
    relation: str,
    keys: Sequence[str],
) -> None:
    null_predicate = " OR ".join(f"{quoted_identifier(key)} IS NULL" for key in keys)
    if connection.execute(f"SELECT 1 FROM {relation} WHERE {null_predicate} LIMIT 1").fetchone():
        raise ValueError("Incoming upsert keys must not be null.")

    key_columns = ", ".join(quoted_identifier(key) for key in keys)
    if connection.execute(
        f"SELECT 1 FROM ("
        f"SELECT {key_columns} FROM {relation} "
        f"GROUP BY {key_columns} HAVING count(*) > 1"
        f") LIMIT 1"
    ).fetchone():
        raise ValueError("Incoming upsert keys must be unique within a load.")


def key_match_condition(
    left_relation: str,
    right_relation: str,
    keys: Sequence[str],
) -> str:
    return " AND ".join(
        f"{left_relation}.{quoted_identifier(key)} = {right_relation}.{quoted_identifier(key)}"
        for key in keys
    )


def quoted_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
