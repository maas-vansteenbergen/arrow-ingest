"""Parquet connector implementation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

ROWS_PER_BATCH = 1_000


class ParquetConnector:
    """Read a local Parquet file as an Arrow stream."""

    def __init__(self, connector: Mapping[str, Any]) -> None:
        self._connector = connector

    def read(self) -> pa.RecordBatchReader:
        path = self._connector.get("path")
        if not isinstance(path, str) or not path:
            raise ValueError("connector.path must be a non-empty string.")

        parquet_file = pq.ParquetFile(path)
        return pa.RecordBatchReader.from_batches(
            parquet_file.schema_arrow,
            parquet_file.iter_batches(batch_size=ROWS_PER_BATCH),
        )
