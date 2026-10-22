"""Parquet connector implementation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from ..core import Cursor

ROWS_PER_BATCH = 1_000


class ParquetConnector:
    """Read a local Parquet file as an Arrow stream."""

    def __init__(self, connector: Mapping[str, Any]) -> None:
        self._connector = connector

    def destination_cursor_field(self) -> str | None:
        if self._connector.get("cursor") is not None:
            raise ValueError("ParquetConnector does not support connector.cursor.")
        return None

    def read(self, cursor: Cursor | None = None) -> pa.RecordBatchReader:
        if cursor is not None:
            raise ValueError("ParquetConnector does not support cursors.")

        path = self._connector.get("path")
        if not isinstance(path, str) or not path:
            raise ValueError("connector.path must be a non-empty string.")

        parquet_file = pq.ParquetFile(path)
        return pa.RecordBatchReader.from_batches(
            parquet_file.schema_arrow,
            parquet_file.iter_batches(batch_size=ROWS_PER_BATCH),
        )
