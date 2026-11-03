"""Run configured Arrow ingestions."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .connectors import ParquetConnector, RestConnector
from .core import Connector, Cursor, MergeStrategy, Target, parse_merge_configuration
from .targets import DuckDBTarget, ParquetTarget


@dataclass(frozen=True)
class LoadResult:
    """Describe a completed configured ingestion.

    Attributes:
        rows_written: Number of incoming rows written to the destination.
        column_count: Number of columns in the incoming Arrow schema.
        destination: Resolved destination path from the target configuration.
    """

    rows_written: int
    column_count: int
    destination: str


def run_pipeline(config_path: str | Path) -> LoadResult:
    """Run a YAML-configured ingestion.

    Local Parquet connector and target paths are resolved relative to the
    configuration file.

    Args:
        config_path: Path to the pipeline YAML file.

    Returns:
        Metadata describing the completed load.

    Raises:
        ValueError: If the pipeline configuration is invalid.
        OSError: If the configuration or local data cannot be read or written.
    """
    path = Path(config_path)
    configuration = _load_configuration(path)
    configuration = _resolve_local_paths(configuration, path.parent)
    connector_configuration = _required_mapping(configuration, "connector")
    target_configuration = _required_mapping(configuration, "target")
    merge = parse_merge_configuration(_required_value(configuration, "merge"))

    connector = _connector_from_config(connector_configuration)
    destination = _target_from_config(target_configuration)
    cursor = _cursor_from_destination(connector, destination, merge.strategy)
    stream = connector.read(cursor)
    rows_written = destination.load(stream, merge)

    return LoadResult(
        rows_written=rows_written,
        column_count=len(stream.schema),
        destination=str(target_configuration["path"]),
    )


def _load_configuration(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as configuration_file:
            configuration = yaml.safe_load(configuration_file)
    except yaml.YAMLError as error:
        raise ValueError("Pipeline configuration is not valid YAML.") from error
    if not isinstance(configuration, dict):
        raise ValueError("Pipeline configuration must be a mapping.")
    return configuration


def _required_mapping(configuration: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = _required_value(configuration, name)
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping.")
    return value


def _required_value(configuration: Mapping[str, Any], name: str) -> Any:
    try:
        return configuration[name]
    except KeyError as error:
        raise ValueError(f"Pipeline configuration must define {name}.") from error


def _resolve_local_paths(configuration: dict[str, Any], base_directory: Path) -> dict[str, Any]:
    resolved = deepcopy(configuration)
    connector = resolved.get("connector")
    if isinstance(connector, dict) and connector.get("type") == "parquet":
        _resolve_path(connector, base_directory)

    target = resolved.get("target")
    if isinstance(target, dict) and target.get("type") in {"duckdb", "parquet"}:
        _resolve_path(target, base_directory)
    return resolved


def _resolve_path(configuration: dict[str, Any], base_directory: Path) -> None:
    path = configuration.get("path")
    if not isinstance(path, str) or not path or path == ":memory:":
        return
    configured_path = Path(path)
    if not configured_path.is_absolute():
        configured_path = base_directory / configured_path
    configuration["path"] = str(configured_path.resolve())


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
