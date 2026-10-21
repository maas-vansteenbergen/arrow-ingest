from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

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


def test_appends_arrow_batches_to_existing_output(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    destination = tmp_path / "rows.parquet"
    pq.write_table(pa.table({"id": [1]}, schema=schema), destination)
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [2, 3]}, schema=schema)],
    )

    row_count = ParquetTarget({"path": str(destination)}).load(reader, MergeStrategy.APPEND)

    assert row_count == 2
    assert pq.read_table(destination).column("id").to_pylist() == [1, 2, 3]


def test_rejects_append_with_a_different_schema(tmp_path) -> None:
    destination = tmp_path / "rows.parquet"
    pq.write_table(pa.table({"id": [1]}), destination)
    reader = pa.RecordBatchReader.from_batches(
        pa.schema([pa.field("title", pa.string())]),
        [pa.RecordBatch.from_pydict({"title": ["new"]})],
    )

    with pytest.raises(
        ValueError,
        match="Cannot append an Arrow stream with a different schema to Parquet output.",
    ):
        ParquetTarget({"path": str(destination)}).load(reader, MergeStrategy.APPEND)
