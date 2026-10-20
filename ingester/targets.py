"""Target implementations."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .core import MergeStrategy


class ParquetTarget:
    """Write an Arrow stream to a Parquet file."""

    def __init__(self, target: Mapping[str, Any]) -> None:
        self._destination = Path(target["path"])

    def load(self, stream: pa.RecordBatchReader, strategy: MergeStrategy) -> int:
        if strategy is not MergeStrategy.REPLACE:
            raise ValueError(f"ParquetTarget does not support merge strategy {strategy!r}.")

        self._destination.parent.mkdir(parents=True, exist_ok=True)
        row_count = 0
        with pq.ParquetWriter(self._destination, stream.schema) as writer:
            for batch in stream:
                writer.write_batch(batch)
                row_count += batch.num_rows
        return row_count
