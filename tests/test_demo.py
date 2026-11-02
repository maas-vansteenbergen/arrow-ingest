from __future__ import annotations

import pytest

from arrow_ingest.connectors import ParquetConnector, RestConnector
from arrow_ingest.core import MergeStrategy
from arrow_ingest.targets import DuckDBTarget, ParquetTarget
from demo import _connector_from_config, _cursor_from_destination, _target_from_config


@pytest.fixture
def high_watermark_connector() -> RestConnector:
    return RestConnector(
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


@pytest.mark.parametrize(
    "strategy",
    [MergeStrategy.APPEND, MergeStrategy.UPSERT, MergeStrategy.SCD2],
)
def test_derives_cursor_from_destination_for_incremental_merges(
    tmp_path,
    high_watermark_connector: RestConnector,
    strategy: MergeStrategy,
) -> None:
    destination = DuckDBTarget({"path": str(tmp_path / "warehouse.duckdb"), "table": "items"})

    assert _cursor_from_destination(high_watermark_connector, destination, strategy) is None


def test_rejects_destination_cursor_without_incremental_merge(
    high_watermark_connector: RestConnector,
) -> None:
    destination = DuckDBTarget({"path": ":memory:", "table": "items"})

    with pytest.raises(
        ValueError,
        match="A destination-derived cursor requires merge strategy 'append', 'upsert', or 'scd2'.",
    ):
        _cursor_from_destination(high_watermark_connector, destination, MergeStrategy.REPLACE)
