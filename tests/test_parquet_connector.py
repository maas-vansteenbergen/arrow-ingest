from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from ingester.connectors import ParquetConnector


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
