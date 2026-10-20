"""Fetch a configured JSON collection as Arrow and write it to Parquet."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from ingester.connectors import ParquetConnector, RestConnector
from ingester.core import Connector, Target, parse_merge_strategy
from ingester.targets import ParquetTarget


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a minimal Arrow-to-Parquet ingestion.")
    parser.add_argument(
        "config", nargs="?", default="connector.yaml", help="Path to the connector YAML."
    )
    args = parser.parse_args()

    with Path(args.config).open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)
    connector_config = config["connector"]
    target = config["target"]
    strategy = parse_merge_strategy(config["merge"])

    connector = _connector_from_config(connector_config)
    destination: Target = ParquetTarget(target)
    stream = connector.read()
    row_count = destination.load(stream, strategy)
    print(f"Wrote {row_count} rows with {len(stream.schema)} columns to {target['path']}")


def _connector_from_config(config: Mapping[str, Any]) -> Connector:
    connector_type = config.get("type")
    if connector_type == "rest":
        return RestConnector(config)
    if connector_type == "parquet":
        return ParquetConnector(config)
    raise ValueError("connector.type must be 'rest' or 'parquet'.")


if __name__ == "__main__":
    main()
