from __future__ import annotations

from io import BytesIO
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingester import connectors
from ingester.connectors import RestConnector
from ingester.core import MergeStrategy, parse_merge_strategy
from ingester.targets import ParquetTarget


def test_rest_connector_selects_records_and_applies_declared_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "response": {
            "items": [
                {"id": 1, "title": "first"},
                {"id": 2, "title": "second"},
            ]
        }
    }
    monkeypatch.setattr(
        connectors,
        "urlopen",
        lambda request: BytesIO(json.dumps(payload).encode()),
    )

    reader = RestConnector(
        {
            "url": "https://example.test/items",
            "records_path": "response.items",
            "schema": {"id": "int64", "title": "string"},
        }
    ).read()
    table = reader.read_all()

    assert table.schema == pa.schema([pa.field("id", pa.int64()), pa.field("title", pa.string())])
    assert table.to_pylist() == [{"id": 1, "title": "first"}, {"id": 2, "title": "second"}]


def test_parquet_target_persists_multiple_arrow_batches(tmp_path) -> None:
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


def test_unsupported_merge_strategy_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match=r"Unsupported merge strategy 'append'. Supported strategies: replace.",
    ):
        parse_merge_strategy("append")
