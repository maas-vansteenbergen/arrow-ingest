"""Parquet target implementation."""

from __future__ import annotations

import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from ..core import Cursor, MergeConfiguration, MergeStrategy
from ._upsert import key_match_condition, validate_incoming_keys, validate_key_fields


class ParquetTarget:
    """Write an Arrow stream to a Parquet file."""

    def __init__(self, target: Mapping[str, Any]) -> None:
        self._destination = Path(target["path"])

    def read_cursor(self, field: str) -> Cursor | None:
        if not self._destination.exists():
            return None

        parquet_file = pq.ParquetFile(self._destination)
        field_index = parquet_file.schema_arrow.get_field_index(field)
        if field_index == -1:
            raise ValueError(f"Cursor field {field!r} does not exist in the Parquet output.")
        if parquet_file.schema_arrow.field(field_index).type != pa.int64():
            raise ValueError(f"Cursor field {field!r} in the Parquet output must be int64.")

        value: int | None = None
        for batch in parquet_file.iter_batches(columns=[field]):
            batch_maximum = pc.max(batch.column(0)).as_py()
            if batch_maximum is not None and (value is None or batch_maximum > value):
                value = batch_maximum
        return Cursor(field, value) if value is not None else None

    def load(self, stream: pa.RecordBatchReader, merge: MergeConfiguration) -> int:
        self._destination.parent.mkdir(parents=True, exist_ok=True)
        if merge.strategy is MergeStrategy.REPLACE:
            return self._write_stream(self._destination, stream)
        if merge.strategy is MergeStrategy.APPEND:
            return self._append_stream(stream)
        if merge.strategy is MergeStrategy.UPSERT:
            return self._upsert_stream(stream, merge.keys)
        raise ValueError(f"ParquetTarget does not support merge strategy {merge.strategy!r}.")

    def _append_stream(self, stream: pa.RecordBatchReader) -> int:
        if not self._destination.exists():
            return self._write_stream(self._destination, stream)

        existing_file = pq.ParquetFile(self._destination)
        if existing_file.schema_arrow != stream.schema:
            raise ValueError(
                "Cannot append an Arrow stream with a different schema to Parquet output."
            )

        with tempfile.NamedTemporaryFile(
            dir=self._destination.parent,
            suffix=".parquet",
            delete=False,
        ) as temporary_file:
            temporary_destination = Path(temporary_file.name)

        try:
            row_count = 0
            with pq.ParquetWriter(temporary_destination, stream.schema) as writer:
                for batch in existing_file.iter_batches():
                    writer.write_batch(batch)
                for batch in stream:
                    writer.write_batch(batch)
                    row_count += batch.num_rows
            temporary_destination.replace(self._destination)
            return row_count
        finally:
            temporary_destination.unlink(missing_ok=True)

    def _upsert_stream(self, stream: pa.RecordBatchReader, keys: tuple[str, ...]) -> int:
        validate_key_fields(stream.schema, keys)
        incoming_destination = _temporary_path(self._destination.parent)
        output_destination = _temporary_path(self._destination.parent)
        try:
            row_count = self._write_stream(incoming_destination, stream)
            output_destination.unlink()

            connection = duckdb.connect()
            try:
                connection.execute(
                    "CREATE VIEW incoming AS SELECT * FROM "
                    f"read_parquet({_sql_string(incoming_destination)})",
                )
                validate_incoming_keys(connection, "incoming", keys)

                if self._destination.exists():
                    existing_file = pq.ParquetFile(self._destination)
                    if (
                        existing_file.schema_arrow
                        != pq.ParquetFile(incoming_destination).schema_arrow
                    ):
                        raise ValueError(
                            "Cannot upsert an Arrow stream with a different schema to Parquet output."
                        )
                    connection.execute(
                        "CREATE VIEW existing AS SELECT * FROM "
                        f"read_parquet({_sql_string(self._destination)})",
                    )
                    query = (
                        "SELECT existing.* FROM existing "
                        "WHERE NOT EXISTS ("
                        "SELECT 1 FROM incoming "
                        f"WHERE {key_match_condition('existing', 'incoming', keys)}"
                        ") UNION ALL SELECT * FROM incoming"
                    )
                else:
                    query = "SELECT * FROM incoming"
                connection.execute(
                    f"COPY ({query}) TO {_sql_string(output_destination)} (FORMAT PARQUET)",
                )
            finally:
                connection.close()

            output_destination.replace(self._destination)
            return row_count
        finally:
            incoming_destination.unlink(missing_ok=True)
            output_destination.unlink(missing_ok=True)

    @staticmethod
    def _write_stream(destination: Path, stream: pa.RecordBatchReader) -> int:
        row_count = 0
        with pq.ParquetWriter(destination, stream.schema) as writer:
            for batch in stream:
                writer.write_batch(batch)
                row_count += batch.num_rows
        return row_count


def _temporary_path(directory: Path) -> Path:
    with tempfile.NamedTemporaryFile(
        dir=directory, suffix=".parquet", delete=False
    ) as temporary_file:
        return Path(temporary_file.name)


def _sql_string(value: Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"
