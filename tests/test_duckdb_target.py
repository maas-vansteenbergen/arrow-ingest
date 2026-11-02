from __future__ import annotations

import duckdb
import pyarrow as pa
import pytest

from arrow_ingest.core import Cursor, MergeConfiguration, MergeStrategy, SCD2Configuration
from arrow_ingest.targets import DuckDBTarget


@pytest.mark.parametrize(
    ("config", "error"),
    [
        (
            {"path": "", "table": "products"},
            "target.path must be a non-empty string.",
        ),
        (
            {"path": ":memory:", "table": ""},
            "target.table must be a non-empty string.",
        ),
        (
            {"path": ":memory:", "table": "catalog.products"},
            "target.table must be an unqualified SQL identifier.",
        ),
    ],
)
def test_rejects_invalid_target_configuration(
    config: dict[str, object],
    error: str,
) -> None:
    with pytest.raises(ValueError, match=error):
        DuckDBTarget(config)


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
        MergeConfiguration(MergeStrategy.REPLACE),
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
        MergeConfiguration(MergeStrategy.APPEND),
    )

    with duckdb.connect(str(database)) as connection:
        rows = connection.execute("SELECT id FROM products ORDER BY id").fetchall()

    assert row_count == 2
    assert rows == [(1,), (2,), (3,)]


def test_reads_high_watermark_cursor_from_existing_table(tmp_path) -> None:
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE products (id BIGINT)")
        connection.execute("INSERT INTO products VALUES (2), (4), (3)")

    cursor = DuckDBTarget({"path": str(database), "table": "products"}).read_cursor("id")

    assert cursor == Cursor(field="id", value=4)


def test_upserts_arrow_batches_by_key(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("title", pa.string())])
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE products (id BIGINT, title VARCHAR)")
        connection.execute("INSERT INTO products VALUES (1, 'one'), (2, 'two')")
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [2, 3], "title": ["updated", "three"]})],
    )

    row_count = DuckDBTarget({"path": str(database), "table": "products"}).load(
        reader,
        MergeConfiguration(MergeStrategy.UPSERT, ("id",)),
    )

    with duckdb.connect(str(database)) as connection:
        rows = connection.execute("SELECT id, title FROM products ORDER BY id").fetchall()

    assert row_count == 2
    assert rows == [(1, "one"), (2, "updated"), (3, "three")]


def test_rejects_upsert_with_null_keys(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [None]}, schema=schema)],
    )

    with pytest.raises(ValueError, match="Incoming upsert keys must not be null."):
        DuckDBTarget({"path": str(tmp_path / "warehouse.duckdb"), "table": "products"}).load(
            reader,
            MergeConfiguration(MergeStrategy.UPSERT, ("id",)),
        )


def test_upserts_by_composite_key(tmp_path) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("version", pa.int64()),
            pa.field("title", pa.string()),
        ]
    )
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE products (id BIGINT, version BIGINT, title VARCHAR)")
        connection.execute("INSERT INTO products VALUES (1, 1, 'old'), (1, 2, 'current')")
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [
            pa.RecordBatch.from_pydict(
                {"id": [1, 2], "version": [2, 1], "title": ["updated", "new"]},
                schema=schema,
            )
        ],
    )

    DuckDBTarget({"path": str(database), "table": "products"}).load(
        reader,
        MergeConfiguration(MergeStrategy.UPSERT, ("id", "version")),
    )

    with duckdb.connect(str(database)) as connection:
        rows = connection.execute(
            "SELECT id, version, title FROM products ORDER BY id, version"
        ).fetchall()

    assert rows == [(1, 1, "old"), (1, 2, "updated"), (2, 1, "new")]


def test_rejects_upsert_with_a_different_schema(tmp_path) -> None:
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE products (id BIGINT)")
        connection.execute("INSERT INTO products VALUES (1)")
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("title", pa.string())])
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [1], "title": ["updated"]}, schema=schema)],
    )

    with pytest.raises(
        ValueError,
        match="Cannot upsert an Arrow stream with a different schema to DuckDB output.",
    ):
        DuckDBTarget({"path": str(database), "table": "products"}).load(
            reader,
            MergeConfiguration(MergeStrategy.UPSERT, ("id",)),
        )


def test_rolls_back_failed_upsert(tmp_path) -> None:
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE products (id BIGINT, title VARCHAR NOT NULL)")
        connection.execute("INSERT INTO products VALUES (1, 'old')")
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("title", pa.string())])
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [1], "title": [None]}, schema=schema)],
    )

    with pytest.raises(duckdb.ConstraintException):
        DuckDBTarget({"path": str(database), "table": "products"}).load(
            reader,
            MergeConfiguration(MergeStrategy.UPSERT, ("id",)),
        )

    with duckdb.connect(str(database)) as connection:
        rows = connection.execute("SELECT id, title FROM products").fetchall()

    assert rows == [(1, "old")]


def test_tracks_scd2_history(tmp_path) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("title", pa.string()),
            pa.field("version", pa.int64()),
        ]
    )
    database = tmp_path / "warehouse.duckdb"
    target = DuckDBTarget({"path": str(database), "table": "products"})
    merge = _scd2_merge()

    target.load(
        pa.RecordBatchReader.from_batches(
            schema,
            [
                pa.RecordBatch.from_pydict(
                    {"id": [1, 2], "title": ["one", "two"], "version": [10, 10]},
                    schema=schema,
                )
            ],
        ),
        merge,
    )
    row_count = target.load(
        pa.RecordBatchReader.from_batches(
            schema,
            [
                pa.RecordBatch.from_pydict(
                    {"id": [1, 2], "title": ["updated", "two"], "version": [20, 20]},
                    schema=schema,
                )
            ],
        ),
        merge,
    )

    with duckdb.connect(str(database)) as connection:
        rows = connection.execute(
            "SELECT id, title, version, valid_from, valid_to, is_current "
            "FROM products ORDER BY id, valid_from"
        ).fetchall()

    assert row_count == 2
    assert rows == [
        (1, "one", 10, 10, 20, False),
        (1, "updated", 20, 20, None, True),
        (2, "two", 10, 10, None, True),
    ]


def test_rejects_out_of_order_scd2_versions(tmp_path) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("title", pa.string()),
            pa.field("version", pa.int64()),
        ]
    )
    database = tmp_path / "warehouse.duckdb"
    target = DuckDBTarget({"path": str(database), "table": "products"})
    merge = _scd2_merge()
    target.load(
        pa.RecordBatchReader.from_batches(
            schema,
            [
                pa.RecordBatch.from_pydict(
                    {"id": [1], "title": ["one"], "version": [10]},
                    schema=schema,
                )
            ],
        ),
        merge,
    )

    with pytest.raises(
        ValueError,
        match="Incoming SCD2 effective_at values must be greater than current versions.",
    ):
        target.load(
            pa.RecordBatchReader.from_batches(
                schema,
                [
                    pa.RecordBatch.from_pydict(
                        {"id": [1], "title": ["updated"], "version": [10]},
                        schema=schema,
                    )
                ],
            ),
            merge,
        )

    with duckdb.connect(str(database)) as connection:
        rows = connection.execute("SELECT id, title, version FROM products").fetchall()

    assert rows == [(1, "one", 10)]


def _scd2_merge() -> MergeConfiguration:
    return MergeConfiguration(
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
