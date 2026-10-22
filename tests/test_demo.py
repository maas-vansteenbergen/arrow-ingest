from __future__ import annotations

import pytest

from demo import _connector_from_config, _cursor_from_destination, _target_from_config
from ingester.connectors import ParquetConnector, RestConnector
from ingester.core import MergeStrategy
from ingester.targets import DuckDBTarget, ParquetTarget


@pytest.mark.parametrize(
    ("config", "expected_type"),
    [
        ({"type": "rest"}, RestConnector),
        ({"type": "parquet"}, ParquetConnector),
    ],
)
def test_selects_configured_connector(config, expected_type) -> None:
    assert isinstance(_connector_from_config(config), expected_type)


@pytest.mark.parametrize(
    ("config", "expected_type"),
    [
        ({"type": "parquet", "path": "output.parquet"}, ParquetTarget),
        ({"type": "duckdb", "path": "warehouse.duckdb", "table": "products"}, DuckDBTarget),
    ],
)
def test_selects_configured_target(config, expected_type) -> None:
    assert isinstance(_target_from_config(config), expected_type)


def test_rejects_unknown_connector_type() -> None:
    with pytest.raises(ValueError, match=r"connector.type must be 'rest' or 'parquet'."):
        _connector_from_config({"type": "csv"})


def test_rejects_unknown_target_type() -> None:
    with pytest.raises(ValueError, match=r"target.type must be 'parquet' or 'duckdb'."):
        _target_from_config({"type": "sqlite"})


def test_derives_cursor_from_destination_for_append(tmp_path) -> None:
    connector = RestConnector(
        {
            "url": "https://example.test/items",
            "records_path": "items",
            "schema": {"id": "int64"},
            "cursor": {
                "type": "high_watermark",
                "field": "id",
                "request_param": "after_id",
            },
        }
    )
    destination = DuckDBTarget({"path": str(tmp_path / "warehouse.duckdb"), "table": "items"})

    assert _cursor_from_destination(connector, destination, MergeStrategy.APPEND) is None
    assert _cursor_from_destination(connector, destination, MergeStrategy.UPSERT) is None
    assert _cursor_from_destination(connector, destination, MergeStrategy.SCD2) is None


def test_rejects_destination_cursor_without_append() -> None:
    connector = RestConnector(
        {
            "url": "https://example.test/items",
            "records_path": "items",
            "schema": {"id": "int64"},
            "cursor": {
                "type": "high_watermark",
                "field": "id",
                "request_param": "after_id",
            },
        }
    )
    destination = DuckDBTarget({"path": ":memory:", "table": "items"})

    with pytest.raises(
        ValueError,
        match="A destination-derived cursor requires merge strategy 'append', 'upsert', or 'scd2'.",
    ):
        _cursor_from_destination(connector, destination, MergeStrategy.REPLACE)
