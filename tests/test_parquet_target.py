from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingester.core import Cursor, MergeConfiguration, MergeStrategy, SCD2Configuration
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
        MergeConfiguration(MergeStrategy.REPLACE),
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

    row_count = ParquetTarget({"path": str(destination)}).load(
        reader,
        MergeConfiguration(MergeStrategy.APPEND),
    )

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
        ParquetTarget({"path": str(destination)}).load(
            reader,
            MergeConfiguration(MergeStrategy.APPEND),
        )


def test_reads_high_watermark_cursor_from_existing_output(tmp_path) -> None:
    destination = tmp_path / "rows.parquet"
    pq.write_table(pa.table({"id": [2, 4, 3]}), destination)

    cursor = ParquetTarget({"path": str(destination)}).read_cursor("id")

    assert cursor == Cursor(field="id", value=4)


def test_upserts_arrow_batches_by_key(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("title", pa.string())])
    destination = tmp_path / "rows.parquet"
    pq.write_table(
        pa.table({"id": [1, 2], "title": ["one", "two"]}, schema=schema),
        destination,
    )
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [2, 3], "title": ["updated", "three"]})],
    )

    row_count = ParquetTarget({"path": str(destination)}).load(
        reader,
        MergeConfiguration(MergeStrategy.UPSERT, ("id",)),
    )

    assert row_count == 2
    assert pq.read_table(destination).to_pylist() == [
        {"id": 1, "title": "one"},
        {"id": 2, "title": "updated"},
        {"id": 3, "title": "three"},
    ]


def test_rejects_upsert_with_duplicate_keys(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    destination = tmp_path / "rows.parquet"
    pq.write_table(pa.table({"id": [2]}), destination)
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [1, 1]}, schema=schema)],
    )

    with pytest.raises(ValueError, match="Incoming upsert keys must be unique within a load."):
        ParquetTarget({"path": str(destination)}).load(
            reader,
            MergeConfiguration(MergeStrategy.UPSERT, ("id",)),
        )

    assert pq.read_table(destination).column("id").to_pylist() == [2]


def test_rejects_upsert_with_a_different_schema(tmp_path) -> None:
    destination = tmp_path / "rows.parquet"
    pq.write_table(pa.table({"id": [1]}), destination)
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("title", pa.string())])
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [pa.RecordBatch.from_pydict({"id": [1], "title": ["updated"]}, schema=schema)],
    )

    with pytest.raises(
        ValueError,
        match="Cannot upsert an Arrow stream with a different schema to Parquet output.",
    ):
        ParquetTarget({"path": str(destination)}).load(
            reader,
            MergeConfiguration(MergeStrategy.UPSERT, ("id",)),
        )


def test_tracks_scd2_history(tmp_path) -> None:
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("title", pa.string()),
            pa.field("version", pa.int64()),
        ]
    )
    destination = tmp_path / "rows.parquet"
    target = ParquetTarget({"path": str(destination)})
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
    target.load(
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

    assert pq.read_table(destination).to_pylist() == [
        {
            "id": 1,
            "title": "one",
            "version": 10,
            "valid_from": 10,
            "valid_to": 20,
            "is_current": False,
        },
        {
            "id": 2,
            "title": "two",
            "version": 10,
            "valid_from": 10,
            "valid_to": None,
            "is_current": True,
        },
        {
            "id": 1,
            "title": "updated",
            "version": 20,
            "valid_from": 20,
            "valid_to": None,
            "is_current": True,
        },
    ]


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
