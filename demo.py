"""Fetch a configured JSON collection as Arrow and write it to Parquet."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml

from ingester.connectors import RestConnector
from ingester.core import Connector, Target, parse_merge_strategy
from ingester.targets import ParquetTarget


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a minimal JSON-to-Arrow-to-Parquet ingestion.")
    parser.add_argument("config", nargs="?", default="connector.yaml", help="Path to the connector YAML.")
    args = parser.parse_args()

    with Path(args.config).open(encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)
    connector_config = config["connector"]
    target = config["target"]
    strategy = parse_merge_strategy(config["merge"])

    connector: Connector = RestConnector(connector_config)
    destination: Target = ParquetTarget(target)
    stream = connector.read()
    row_count = destination.load(stream, strategy)
    print(f"Wrote {row_count} rows with {len(stream.schema)} columns to {target['path']}")


if __name__ == "__main__":
    main()
