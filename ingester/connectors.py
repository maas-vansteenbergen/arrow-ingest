"""Connector implementations."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from time import sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

import pyarrow as pa

ARROW_TYPES = {
    "int64": pa.int64(),
    "float64": pa.float64(),
    "string": pa.string(),
    "bool": pa.bool_(),
}
ROWS_PER_BATCH = 1_000


@dataclass(frozen=True)
class _RequestConfiguration:
    url: str
    headers: Mapping[str, str]
    controls: _RequestControls


@dataclass(frozen=True)
class _RequestControls:
    timeout_seconds: float | None
    retries: int
    backoff_seconds: float


class RestConnector:
    """Fetch a JSON collection from a configured REST endpoint."""

    def __init__(self, connector: Mapping[str, Any]) -> None:
        self._connector = connector

    def read(self) -> pa.RecordBatchReader:
        request = _request_configuration(self._connector)
        schema = _schema(self._connector["schema"])
        schema_policy = _schema_policy(self._connector.get("schema_policy"))
        records_path = self._connector["records_path"]
        pagination = self._connector.get("pagination")
        if pagination is None:
            payload = _fetch_payload(request)
            batches = _record_batches(
                _records(payload, records_path, schema, schema_policy),
                schema,
            )
        else:
            offset_param, limit_param, page_size, total_path = _offset_pagination(pagination)
            batches = _offset_batches(
                schema,
                request,
                records_path,
                schema_policy,
                offset_param=offset_param,
                limit_param=limit_param,
                page_size=page_size,
                total_path=total_path,
            )
        return pa.RecordBatchReader.from_batches(schema, batches)


def _request_configuration(connector: Mapping[str, Any]) -> _RequestConfiguration:
    return _RequestConfiguration(
        url=_with_query_parameters(
            connector["url"],
            _static_query_parameters(connector.get("params")),
        ),
        headers=_request_headers(
            connector.get("headers"),
            connector.get("auth"),
        ),
        controls=_request_controls(connector.get("request")),
    )


def _schema(schema: Mapping[str, str]) -> pa.Schema:
    return pa.schema([pa.field(name, _arrow_type(type_name)) for name, type_name in schema.items()])


def _fetch_payload(request_configuration: _RequestConfiguration) -> dict[str, Any]:
    controls = request_configuration.controls
    for attempt in range(controls.retries + 1):
        request = Request(
            request_configuration.url,
            headers=request_configuration.headers,
        )
        try:
            if controls.timeout_seconds is None:
                response = urlopen(request)  # noqa: S310 - the connector URL is explicit user config.
            else:
                response = urlopen(  # noqa: S310 - the connector URL is explicit user config.
                    request,
                    timeout=controls.timeout_seconds,
                )
            with response:
                return json.load(response)
        except HTTPError as error:
            if error.code != 429 and not 500 <= error.code < 600:
                raise
            if attempt == controls.retries:
                raise
        except (TimeoutError, URLError):
            if attempt == controls.retries:
                raise

        delay = controls.backoff_seconds * (2**attempt)
        if delay:
            sleep(delay)

    raise AssertionError("Request retry loop exhausted unexpectedly.")


def _records(
    payload: dict[str, Any],
    records_path: str,
    schema: pa.Schema,
    schema_policy: str,
) -> list[dict[str, Any]]:
    records = _get_path(payload, records_path)
    if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
        raise ValueError("connector.records_path must resolve to a list of JSON objects.")
    if schema_policy == "strict":
        _validate_strict_records(records, schema)
    return records


def _record_batches(
    records: list[dict[str, Any]],
    schema: pa.Schema,
) -> Iterator[pa.RecordBatch]:
    yield from (
        pa.RecordBatch.from_pylist(records[start : start + ROWS_PER_BATCH], schema=schema)
        for start in range(0, len(records), ROWS_PER_BATCH)
    )


def _offset_batches(
    schema: pa.Schema,
    request: _RequestConfiguration,
    records_path: str,
    schema_policy: str,
    *,
    offset_param: str,
    limit_param: str,
    page_size: int,
    total_path: str,
) -> Iterator[pa.RecordBatch]:
    offset = 0
    while True:
        payload = _fetch_payload(
            _RequestConfiguration(
                url=_with_query_parameters(
                    request.url,
                    {offset_param: offset, limit_param: page_size},
                ),
                headers=request.headers,
                controls=request.controls,
            )
        )
        total = _get_path(payload, total_path)
        if not isinstance(total, int) or isinstance(total, bool) or total < 0:
            raise ValueError(
                "connector.pagination.total_path must resolve to a non-negative integer."
            )
        if offset >= total:
            return

        records = _records(payload, records_path, schema, schema_policy)
        if not records:
            raise ValueError("connector pagination returned no records before reaching the total.")

        yield from _record_batches(records, schema)
        offset += len(records)
        if offset >= total:
            return


def _arrow_type(type_name: str) -> pa.DataType:
    try:
        return ARROW_TYPES[type_name]
    except KeyError as error:
        supported = ", ".join(ARROW_TYPES)
        raise ValueError(
            f"Unsupported Arrow type {type_name!r}. Supported types: {supported}."
        ) from error


def _schema_policy(value: Any) -> str:
    if value is None:
        return "strict"
    if value != "strict":
        raise ValueError("connector.schema_policy must be 'strict'.")
    return value


def _validate_strict_records(records: list[dict[str, Any]], schema: pa.Schema) -> None:
    expected_fields = set(schema.names)
    for record_index, record in enumerate(records):
        missing_fields = expected_fields.difference(record)
        if missing_fields:
            fields = ", ".join(sorted(missing_fields))
            raise ValueError(f"Record {record_index} is missing declared fields: {fields}.")

        for field in schema:
            _validate_strict_value(record_index, field, record[field.name])


def _validate_strict_value(record_index: int, field: pa.Field, value: Any) -> None:
    if value is None:
        if field.nullable:
            return
        raise ValueError(f"Record {record_index} field {field.name!r} cannot be null.")

    valid_type = (
        (field.type == pa.int64() and type(value) is int)
        or (field.type == pa.float64() and type(value) is float)
        or (field.type == pa.string() and type(value) is str)
        or (field.type == pa.bool_() and type(value) is bool)
    )
    if valid_type:
        return
    raise ValueError(
        f"Record {record_index} field {field.name!r} must be {field.type}, "
        f"got {type(value).__name__}."
    )


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
    if not all(
        isinstance(value, str) and value for value in (offset_param, limit_param, total_path)
    ):
        raise ValueError(
            "connector.pagination must define non-empty offset_param, limit_param, and total_path values."
        )

    page_size = pagination.get("page_size")
    if not isinstance(page_size, int) or isinstance(page_size, bool) or page_size <= 0:
        raise ValueError("connector.pagination.page_size must be a positive integer.")
    return offset_param, limit_param, page_size, total_path


def _request_headers(configured_headers: Any, auth: Any) -> dict[str, str]:
    headers = {"User-Agent": "arrow-ingest-demo/0.1"}
    if configured_headers is not None:
        if not isinstance(configured_headers, Mapping):
            raise ValueError("connector.headers must be a mapping.")
        for name, value in configured_headers.items():
            if not isinstance(name, str) or not name:
                raise ValueError("connector.headers must use non-empty string names.")
            if not isinstance(value, str):
                raise ValueError("connector.headers must use string values.")
            if auth is not None and name.casefold() == "authorization":
                raise ValueError(
                    "connector.headers cannot define Authorization when connector.auth is configured."
                )
            headers[name] = value

    if auth is None:
        return headers
    if not isinstance(auth, Mapping):
        raise ValueError("connector.auth must be a mapping.")
    if auth.get("type") != "bearer":
        raise ValueError("connector.auth.type must be 'bearer'.")

    token_env = auth.get("token_env")
    if not isinstance(token_env, str) or not token_env:
        raise ValueError("connector.auth.token_env must be a non-empty environment variable name.")
    token = os.environ.get(token_env, "").strip()
    if not token:
        raise ValueError(
            f"Environment variable {token_env!r} for connector bearer authentication is not set."
        )

    headers["Authorization"] = f"Bearer {token}"
    return headers


def _request_controls(request: Any) -> _RequestControls:
    if request is None:
        return _RequestControls(
            timeout_seconds=None,
            retries=0,
            backoff_seconds=0,
        )
    if not isinstance(request, Mapping):
        raise ValueError("connector.request must be a mapping.")

    timeout_seconds = request.get("timeout_seconds")
    if timeout_seconds is not None and (
        not isinstance(timeout_seconds, (int, float))
        or isinstance(timeout_seconds, bool)
        or timeout_seconds <= 0
    ):
        raise ValueError("connector.request.timeout_seconds must be a positive number.")

    retries = request.get("retries", 0)
    if not isinstance(retries, int) or isinstance(retries, bool) or retries < 0:
        raise ValueError("connector.request.retries must be a non-negative integer.")

    backoff_seconds = request.get("backoff_seconds", 0)
    if (
        not isinstance(backoff_seconds, (int, float))
        or isinstance(backoff_seconds, bool)
        or backoff_seconds < 0
    ):
        raise ValueError("connector.request.backoff_seconds must be a non-negative number.")

    return _RequestControls(
        timeout_seconds=float(timeout_seconds) if timeout_seconds is not None else None,
        retries=retries,
        backoff_seconds=float(backoff_seconds),
    )


def _static_query_parameters(parameters: Any) -> dict[str, str]:
    if parameters is None:
        return {}
    if not isinstance(parameters, Mapping):
        raise ValueError("connector.params must be a mapping.")

    query_parameters: dict[str, str] = {}
    for name, value in parameters.items():
        if not isinstance(name, str) or not name:
            raise ValueError("connector.params must use non-empty string names.")
        if not isinstance(value, str):
            raise ValueError("connector.params must use string values.")
        query_parameters[name] = value
    return query_parameters


def _with_query_parameters(url: str, parameters: Mapping[str, str | int]) -> str:
    parts = urlsplit(url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update({name: str(value) for name, value in parameters.items()})
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))
