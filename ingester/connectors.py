"""Connector implementations."""

from __future__ import annotations

import json
from typing import Any, Mapping
from urllib.request import Request, urlopen

import pyarrow as pa


ARROW_TYPES = {
    "int64": pa.int64(),
    "float64": pa.float64(),
    "string": pa.string(),
    "bool": pa.bool_(),
}
ROWS_PER_BATCH = 1_000


class RestConnector:
    """Fetch a JSON collection from a configured REST endpoint."""

    def __init__(self, connector: Mapping[str, Any]) -> None:
        self._connector = connector

    def read(self) -> pa.RecordBatchReader:
        request = Request(
            self._connector["url"],
            headers={"User-Agent": "arrow-ingest-demo/0.1"},
        )
        with urlopen(request) as response:  # noqa: S310 - the connector URL is explicit user config.
            payload = json.load(response)

        records = _get_path(payload, self._connector["records_path"])
        if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
            raise ValueError("connector.records_path must resolve to a list of JSON objects.")

        schema = pa.schema(
            [
                pa.field(name, _arrow_type(type_name))
                for name, type_name in self._connector["schema"].items()
            ]
        )
        batches = (
            pa.RecordBatch.from_pylist(records[start : start + ROWS_PER_BATCH], schema=schema)
            for start in range(0, len(records), ROWS_PER_BATCH)
        )
        return pa.RecordBatchReader.from_batches(schema, batches)


def _arrow_type(type_name: str) -> pa.DataType:
    try:
        return ARROW_TYPES[type_name]
    except KeyError as error:
        supported = ", ".join(ARROW_TYPES)
        raise ValueError(f"Unsupported Arrow type {type_name!r}. Supported types: {supported}.") from error


def _get_path(payload: dict[str, Any], path: str) -> Any:
    value: Any = payload
    for segment in path.split("."):
        if not isinstance(value, dict) or segment not in value:
            raise ValueError(f"Could not resolve records_path {path!r}.")
        value = value[segment]
    return value
