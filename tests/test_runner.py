from __future__ import annotations

import gzip
import json
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from arrow_ingest.connectors import ParquetConnector, RestConnector, rest
from arrow_ingest.core import MergeStrategy
from arrow_ingest.runner import (
    _connector_from_config,
    _cursor_from_destination,
    _target_from_config,
)
from arrow_ingest.targets import DuckDBTarget, ParquetTarget


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


def test_runs_pipeline_with_paths_relative_to_configuration(tmp_path) -> None:
    project_directory = tmp_path / "project"
    input_directory = project_directory / "data"
    pipeline_directory = project_directory / "pipelines"
    input_directory.mkdir(parents=True)
    pipeline_directory.mkdir()
    pq.write_table(
        pa.table({"id": [1, 2], "title": ["first", "second"]}),
        input_directory / "products.parquet",
    )
    configuration_path = pipeline_directory / "products.yaml"
    configuration_path.write_text(
        """
connector:
  type: parquet
  path: ../data/products.parquet
merge: replace
target:
  type: parquet
  path: ../output/products.parquet
""".lstrip(),
        encoding="utf-8",
    )

    from arrow_ingest import run_pipeline

    result = run_pipeline(configuration_path)

    destination = project_directory / "output" / "products.parquet"
    assert result.rows_written == 2
    assert result.column_count == 2
    assert result.destination == str(destination)
    assert pq.read_table(destination).to_pylist() == [
        {"id": 1, "title": "first"},
        {"id": 2, "title": "second"},
    ]


def test_rejects_malformed_yaml_configuration(tmp_path) -> None:
    configuration = tmp_path / "pipeline.yaml"
    configuration.write_text("connector: [", encoding="utf-8")

    from arrow_ingest import run_pipeline

    with pytest.raises(ValueError, match="Pipeline configuration is not valid YAML."):
        run_pipeline(configuration)


def test_runs_rest_yaml_with_compression_and_response_backoff(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    configuration = tmp_path / "pipeline.yaml"
    configuration.write_text(
        """
connector:
  type: rest
  url: https://example.test/items
  records_path: items
  schema:
    id: int64
  headers:
    Accept-Encoding: gzip
  request:
    timeout_seconds: 1
    response_backoff_field: delay
merge: append
target:
  type: parquet
  path: output.parquet
""".lstrip(),
        encoding="utf-8",
    )
    delays: list[float] = []

    class Response(BytesIO):
        headers = {"Content-Encoding": "gzip"}

    def fake_urlopen(request, timeout: float) -> Response:
        assert timeout == 1
        assert request.get_header("Accept-encoding") == "gzip"
        return Response(gzip.compress(json.dumps({"items": [{"id": 1}], "delay": 2}).encode()))

    monkeypatch.setattr(rest, "urlopen", fake_urlopen)
    monkeypatch.setattr(rest, "sleep", delays.append)

    from arrow_ingest import run_pipeline

    result = run_pipeline(configuration)

    assert delays == [2]
    assert result.rows_written == 1
    assert pq.read_table(tmp_path / "output.parquet").to_pylist() == [{"id": 1}]
