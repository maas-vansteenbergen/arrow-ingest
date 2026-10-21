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


def test_appends_arrow_batches_to_existing_table(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE products (id BIGINT)")
        connection.execute("INSERT INTO products VALUES (1)")
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [2, 3]}, schema=schema)],
    )

    row_count = DuckDBTarget({"path": str(database), "table": "products"}).load(
        reader,
        MergeStrategy.APPEND,
    )

    with duckdb.connect(str(database)) as connection:
        rows = connection.execute("SELECT id FROM products ORDER BY id").fetchall()

    assert row_count == 2
    assert rows == [(1,), (2,), (3,)]
