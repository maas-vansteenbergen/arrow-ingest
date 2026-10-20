from __future__ import annotations

from io import BytesIO
import json
from urllib.parse import parse_qs, urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingester import connectors
from ingester.connectors import RestConnector
from ingester.core import MergeStrategy, parse_merge_strategy
from ingester.targets import ParquetTarget


def test_rest_connector_selects_records_and_applies_declared_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "response": {
            "items": [
                {"id": 1, "title": "first"},
                {"id": 2, "title": "second"},
            ]
        }
    }
    monkeypatch.setattr(
        connectors,
        "urlopen",
        lambda request: BytesIO(json.dumps(payload).encode()),
    )

    reader = RestConnector(
        {
            "url": "https://example.test/items",
            "records_path": "response.items",
            "schema": {"id": "int64", "title": "string"},
        }
    ).read()
    table = reader.read_all()

    assert table.schema == pa.schema([pa.field("id", pa.int64()), pa.field("title", pa.string())])
    assert table.to_pylist() == [{"id": 1, "title": "first"}, {"id": 2, "title": "second"}]


def test_rest_connector_sends_bearer_token_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PRODUCTS_API_TOKEN", "test-token")
    authorization_headers: list[str | None] = []
    monkeypatch.setattr(
        connectors,
        "urlopen",
        lambda request: (
            authorization_headers.append(request.get_header("Authorization"))
            or BytesIO(b'{"items": [{"id": 1}]}')
        ),
    )

    reader = RestConnector(
        {
            "url": "https://example.test/items",
            "records_path": "items",
            "schema": {"id": "int64"},
            "auth": {"type": "bearer", "token_env": "PRODUCTS_API_TOKEN"},
        }
    ).read()

    assert [batch.column("id").to_pylist() for batch in reader] == [[1]]
    assert authorization_headers == ["Bearer test-token"]


def test_rest_connector_rejects_missing_bearer_token_before_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MISSING_API_TOKEN", raising=False)
    monkeypatch.setattr(
        connectors,
        "urlopen",
        lambda request: pytest.fail("request should not be sent without a bearer token"),
    )

    with pytest.raises(
        ValueError,
        match=r"Environment variable 'MISSING_API_TOKEN' for connector bearer authentication is not set.",
    ):
        RestConnector(
            {
                "url": "https://example.test/items",
                "records_path": "items",
                "schema": {"id": "int64"},
                "auth": {"type": "bearer", "token_env": "MISSING_API_TOKEN"},
            }
        ).read()


def test_rest_connector_uses_configured_offset_pagination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payloads = {
        0: {"products": [{"id": 1}, {"id": 2}], "total": 5},
        2: {"products": [{"id": 3}, {"id": 4}], "total": 5},
        4: {"products": [{"id": 5}], "total": 5},
    }
    requests: list[dict[str, list[str]]] = []
    authorization_headers: list[str | None] = []
    monkeypatch.setenv("PRODUCTS_API_TOKEN", "test-token")

    def urlopen(request):
        query = parse_qs(urlsplit(request.full_url).query)
        requests.append(query)
        authorization_headers.append(request.get_header("Authorization"))
        return BytesIO(json.dumps(payloads[int(query["skip"][0])]).encode())

    monkeypatch.setattr(connectors, "urlopen", urlopen)

    reader = RestConnector(
        {
            "url": "https://example.test/products",
            "records_path": "products",
            "schema": {"id": "int64"},
            "auth": {"type": "bearer", "token_env": "PRODUCTS_API_TOKEN"},
            "pagination": {
                "type": "offset",
                "offset_param": "skip",
                "limit_param": "limit",
                "page_size": 2,
                "total_path": "total",
            },
        }
    ).read()

    batches = list(reader)

    assert [batch.column("id").to_pylist() for batch in batches] == [[1, 2], [3, 4], [5]]
    assert requests == [
        {"skip": ["0"], "limit": ["2"]},
        {"skip": ["2"], "limit": ["2"]},
        {"skip": ["4"], "limit": ["2"]},
    ]
    assert authorization_headers == ["Bearer test-token"] * 3


def test_rest_connector_rejects_unsupported_pagination_type() -> None:
    with pytest.raises(ValueError, match=r"connector.pagination.type must be 'offset'."):
        RestConnector(
            {
                "url": "https://example.test/items",
                "records_path": "items",
                "schema": {"id": "int64"},
                "pagination": {"type": "cursor"},
            }
        ).read()


def test_parquet_target_persists_multiple_arrow_batches(tmp_path) -> None:
    schema = pa.schema([pa.field("id", pa.int64())])
    reader = pa.RecordBatchReader.from_batches(
        schema,
        [
            pa.RecordBatch.from_pydict({"id": [1, 2]}, schema=schema),
            pa.RecordBatch.from_pydict({"id": [3]}, schema=schema),
        ],
    )
    destination = tmp_path / "nested" / "rows.parquet"

    row_count = ParquetTarget({"path": str(destination)}).load(
        reader,
        MergeStrategy.REPLACE,
    )

    assert row_count == 3
    assert pq.read_table(destination).column("id").to_pylist() == [1, 2, 3]


def test_unsupported_merge_strategy_is_rejected() -> None:
    with pytest.raises(
        ValueError,
        match=r"Unsupported merge strategy 'append'. Supported strategies: replace.",
    ):
        parse_merge_strategy("append")
