from __future__ import annotations

import duckdb
import pyarrow as pa

from ingester.core import MergeStrategy
from ingester.targets import DuckDBTarget


def test_persists_multiple_arrow_batches(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [
            pa.RecordBatch.from_pydict({"id": [1, 2]}, schema=schema),
            pa.RecordBatch.from_pydict({"id": [3]}, schema=schema),
        ],
    )
    database = tmp_path / "warehouse.duckdb"

    row_count = DuckDBTarget({"path": str(database), "table": "products"}).load(
        reader,
        MergeStrategy.REPLACE,
    )

    with duckdb.connect(str(database)) as connection:
        rows = connection.execute("SELECT id FROM products ORDER BY id").fetchall()

    assert row_count == 3
    assert rows == [(1,), (2,), (3,)]
