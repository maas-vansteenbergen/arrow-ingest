"""DuckDB relation helpers for Arrow-backed targets."""

from __future__ import annotations

import duckdb

from ._upsert import quoted_identifier


def table_exists(connection: duckdb.DuckDBPyConnection, table: str) -> bool:
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


def row_count(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    count = connection.execute(f"SELECT count(*) FROM {quoted_identifier(table)}").fetchone()[0]
    if not isinstance(count, int):
        raise AssertionError("DuckDB row count query did not return an integer.")
    return count


def table_schema(connection: duckdb.DuckDBPyConnection, table: str) -> list[tuple[str, str]]:
    return [
        (column[0], column[1])
        for column in connection.execute(f"DESCRIBE {quoted_identifier(table)}").fetchall()
    ]
