from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from arrow_ingest.connectors import ParquetConnector


def test_emits_native_arrow_batches(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("title", pa.string())])
    source = tmp_path / "products.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"id": 1, "title": "first"}, {"id": 2, "title": "second"}],
            schema=schema,
        ),
        source,
    )

    reader = ParquetConnector({"path": str(source)}).read()

    assert reader.schema == schema
    assert reader.read_all().to_pylist() == [
        {"id": 1, "title": "first"},
        {"id": 2, "title": "second"},
    ]


@pytest.mark.parametrize("path", [None, ""])
def test_rejects_missing_or_empty_configured_path(path: object) -> None:
    with pytest.raises(ValueError, match="connector.path must be a non-empty string."):
        ParquetConnector({"path": path}).read()


def test_rejects_configured_cursor() -> None:
    connector = ParquetConnector(
        {
            "path": "input.parquet",
            "cursor": {"type": "high_watermark"},
        }
    )

    with pytest.raises(ValueError, match="ParquetConnector does not support connector.cursor."):
        connector.destination_cursor_field()
