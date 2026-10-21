"""Parquet target implementation."""

from __future__ import annotations

import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..core import MergeStrategy


class ParquetTarget:
    """Write an Arrow stream to a Parquet file."""

    def __init__(self, target: Mapping[str, Any]) -> None:
        self._destination = Path(target["path"])

    def load(self, stream: pa.RecordBatchReader, strategy: MergeStrategy) -> int:
        self._destination.parent.mkdir(parents=True, exist_ok=True)
        if strategy is MergeStrategy.REPLACE:
            return self._write_stream(self._destination, stream)
        if strategy is MergeStrategy.APPEND:
            return self._append_stream(stream)
        raise ValueError(f"ParquetTarget does not support merge strategy {strategy!r}.")

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

    @staticmethod
    def _write_stream(destination: Path, stream: pa.RecordBatchReader) -> int:
        row_count = 0
        with pq.ParquetWriter(destination, stream.schema) as writer:
            for batch in stream:
                writer.write_batch(batch)
                row_count += batch.num_rows
        return row_count
