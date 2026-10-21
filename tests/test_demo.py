from __future__ import annotations

import pytest

from demo import _connector_from_config, _target_from_config
from ingester.connectors import ParquetConnector, RestConnector
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
