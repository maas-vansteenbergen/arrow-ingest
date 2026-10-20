from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from ingester.core import MergeStrategy
from ingester.targets import ParquetTarget


def test_persists_multiple_arrow_batches(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [
            pa.RecordBatch.from_pydict({"id": [1, 2]}, schema=schema),
            pa.RecordBatch.from_pydict({"id": [3]}, schema=schema),
        ],
    )
    destination = tmp_path / "nested" / "rows.parquet"

    row_count = ParquetTarget({"path": str(destination)}).load(
        reader,
        MergeStrategy.REPLACE,
    )

    assert row_count == 3
    assert pq.read_table(destination).column("id").to_pylist() == [1, 2, 3]
