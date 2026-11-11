from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from arrow_ingest.core import MergeConfiguration, MergeStrategy, SCD2Configuration, Target
from arrow_ingest.targets import DuckDBTarget, ParquetTarget

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64()),
        pa.field("title", pa.string()),
        pa.field("version", pa.int64()),
    ]
)
MERGE = MergeConfiguration(
    MergeStrategy.SCD2,
    ("id",),
    SCD2Configuration(
        tracked_fields=("title",),
        effective_at="version",
        valid_from="valid_from",
        valid_to="valid_to",
        is_current="is_current",
    ),
)


def _target(kind: str, path: Path) -> Target:
    if kind == "duckdb":
        return DuckDBTarget({"path": str(path), "table": "products"})
    return ParquetTarget({"path": str(path)})


def _reader(rows: dict[str, list[object]]) -> pa.RecordBatchReader:
    batch = pa.RecordBatch.from_pydict(rows, schema=SCHEMA)
    return pa.RecordBatchReader.from_batches(SCHEMA, [batch])


def _rows(kind: str, path: Path) -> list[tuple]:
    if kind == "duckdb":
        with duckdb.connect(str(path)) as connection:
            return connection.execute(
                "SELECT id, title, version, valid_from, valid_to, is_current "
                "FROM products ORDER BY id"
            ).fetchall()
    table = pq.read_table(path)
    return [
        tuple(
            row[name] for name in ("id", "title", "version", "valid_from", "valid_to", "is_current")
        )
        for row in table.to_pylist()
    ]


@pytest.mark.parametrize("kind", ["duckdb", "parquet"])
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.parametrize("null_key", [1, 2])
def test_rejects_null_effective_values_without_changing_destination(
    tmp_path: Path, kind: str, existing: bool, null_key: int
) -> None:
    path = tmp_path / ("products.duckdb" if kind == "duckdb" else "products.parquet")
    target = _target(kind, path)
    if existing:
        target.load(_reader({"id": [1], "title": ["original"], "version": [10]}), MERGE)

    versions = [None, 20] if null_key == 1 else [20, None]
    with pytest.raises(ValueError, match="Incoming SCD2 effective_at values must not be null"):
        target.load(
            _reader({"id": [1, 2], "title": ["updated", "new"], "version": versions}),
            MERGE,
        )

    if existing:
        assert _rows(kind, path) == [(1, "original", 10, 10, None, True)]
    elif kind == "parquet":
        assert not path.exists()
    else:
        with duckdb.connect(str(path)) as connection:
            assert connection.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'products'"
            ).fetchone() == (0,)


@pytest.mark.parametrize("kind", ["duckdb", "parquet"])
def test_rejects_existing_current_rows_with_null_valid_from(tmp_path: Path, kind: str) -> None:
    path = tmp_path / ("products.duckdb" if kind == "duckdb" else "products.parquet")
    target = _target(kind, path)
    target.load(_reader({"id": [1], "title": ["original"], "version": [10]}), MERGE)
    if kind == "duckdb":
        with duckdb.connect(str(path)) as connection:
            connection.execute("UPDATE products SET version = NULL, valid_from = NULL")
    else:
        table = pq.read_table(path)
        rows = table.to_pylist()
        rows[0]["version"] = None
        rows[0]["valid_from"] = None
        nullable_schema = pa.schema([field.with_nullable(True) for field in table.schema])
        pq.write_table(pa.Table.from_pylist(rows, schema=nullable_schema), path)

    with pytest.raises(ValueError, match="Current SCD2 rows must have a non-null valid_from"):
        target.load(_reader({"id": [1], "title": ["updated"], "version": [20]}), MERGE)

    assert _rows(kind, path) == [(1, "original", None, None, None, True)]
