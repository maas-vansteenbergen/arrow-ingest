"""Run a configured Arrow ingestion."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from ingester.connectors import ParquetConnector, RestConnector
from ingester.core import (
    Connector,
    Cursor,
    MergeStrategy,
    Target,
    parse_merge_configuration,
)
from ingester.targets import DuckDBTarget, ParquetTarget


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a minimal Arrow ingestion.")
    parser.add_argument(
        "config", nargs="?", default="connector.yaml", help="Path to the connector YAML."
    )
    args = parser.parse_args()

    with Path(args.config).open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)
    connector_config = config["connector"]
    target = config["target"]
    merge = parse_merge_configuration(config["merge"])

    connector = _connector_from_config(connector_config)
    destination = _target_from_config(target)
    cursor = _cursor_from_destination(connector, destination, merge.strategy)
    stream = connector.read(cursor)
    row_count = destination.load(stream, merge)
    print(f"Wrote {row_count} rows with {len(stream.schema)} columns to {target['path']}")


def _connector_from_config(config: Mapping[str, Any]) -> Connector:
    connector_type = config.get("type")
    if connector_type == "rest":
        return RestConnector(config)
    if connector_type == "parquet":
        return ParquetConnector(config)
    raise ValueError("connector.type must be 'rest' or 'parquet'.")


def _target_from_config(config: Mapping[str, Any]) -> Target:
    target_type = config.get("type")
    if target_type == "parquet":
        return ParquetTarget(config)
    if target_type == "duckdb":
        return DuckDBTarget(config)
    raise ValueError("target.type must be 'parquet' or 'duckdb'.")


def _cursor_from_destination(
    connector: Connector,
    destination: Target,
    strategy: MergeStrategy,
) -> Cursor | None:
    field = connector.destination_cursor_field()
    if field is None:
        return None
    if strategy not in (MergeStrategy.APPEND, MergeStrategy.SCD2, MergeStrategy.UPSERT):
        raise ValueError(
            "A destination-derived cursor requires merge strategy 'append', 'upsert', or 'scd2'."
        )
    return destination.read_cursor(field)


if __name__ == "__main__":
    main()
