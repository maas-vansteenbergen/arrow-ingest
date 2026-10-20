"""Connector implementations."""

from __future__ import annotations

import json
from typing import Any, Iterator, Mapping
from urllib.request import Request, urlopen
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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
        schema = pa.schema(
            [
                pa.field(name, _arrow_type(type_name))
                for name, type_name in self._connector["schema"].items()
            ]
        )
        pagination = self._connector.get("pagination")
        if pagination is None:
            payload = self._fetch_payload(self._connector["url"])
            batches = self._record_batches(self._records(payload), schema)
        else:
            offset_param, limit_param, page_size, total_path = _offset_pagination(pagination)
            batches = self._offset_batches(
                schema,
                offset_param=offset_param,
                limit_param=limit_param,
                page_size=page_size,
                total_path=total_path,
            )
        return pa.RecordBatchReader.from_batches(schema, batches)

    def _fetch_payload(self, url: str) -> dict[str, Any]:
        request = Request(
            url,
            headers={"User-Agent": "arrow-ingest-demo/0.1"},
        )
        with urlopen(request) as response:  # noqa: S310 - the connector URL is explicit user config.
            return json.load(response)

    def _records(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        records = _get_path(payload, self._connector["records_path"])
        if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
            raise ValueError("connector.records_path must resolve to a list of JSON objects.")
        return records

    def _record_batches(
        self,
        records: list[dict[str, Any]],
        schema: pa.Schema,
    ) -> Iterator[pa.RecordBatch]:
        yield from (
            pa.RecordBatch.from_pylist(records[start : start + ROWS_PER_BATCH], schema=schema)
            for start in range(0, len(records), ROWS_PER_BATCH)
        )

    def _offset_batches(
        self,
        schema: pa.Schema,
        *,
        offset_param: str,
        limit_param: str,
        page_size: int,
        total_path: str,
    ) -> Iterator[pa.RecordBatch]:
        offset = 0
        while True:
            payload = self._fetch_payload(
                _with_query_parameters(
                    self._connector["url"],
                    {offset_param: offset, limit_param: page_size},
                )
            )
            total = _get_path(payload, total_path)
            if not isinstance(total, int) or isinstance(total, bool) or total < 0:
                raise ValueError("connector.pagination.total_path must resolve to a non-negative integer.")
            if offset >= total:
                return

            records = self._records(payload)
            if not records:
                raise ValueError("connector pagination returned no records before reaching the total.")

            yield from self._record_batches(records, schema)
            offset += len(records)
            if offset >= total:
                return


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


def _offset_pagination(pagination: Any) -> tuple[str, str, int, str]:
    if not isinstance(pagination, Mapping):
        raise ValueError("connector.pagination must be a mapping.")
    if pagination.get("type") != "offset":
        raise ValueError("connector.pagination.type must be 'offset'.")

    offset_param = pagination.get("offset_param")
    limit_param = pagination.get("limit_param")
    total_path = pagination.get("total_path")
    if not all(isinstance(value, str) and value for value in (offset_param, limit_param, total_path)):
        raise ValueError(
            "connector.pagination must define non-empty offset_param, limit_param, and total_path values."
        )

    page_size = pagination.get("page_size")
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size <= 0:
        raise ValueError("connector.pagination.page_size must be a positive integer.")
    return offset_param, limit_param, page_size, total_path


def _with_query_parameters(url: str, parameters: Mapping[str, int]) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({name: str(value) for name, value in parameters.items()})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
