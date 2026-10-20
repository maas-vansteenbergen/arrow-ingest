from __future__ import annotations

import json
from collections.abc import Callable
from io import BytesIO
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlsplit

import pyarrow as pa
import pytest

from ingester import connectors
from ingester.connectors import RestConnector


def _json_response(payload: dict[str, Any]) -> BytesIO:
    return BytesIO(json.dumps(payload).encode())


@pytest.fixture
def rest_config() -> Callable[..., dict[str, Any]]:
    def build(**overrides: Any) -> dict[str, Any]:
        config = {
            "url": "https://example.test/items",
            "records_path": "items",
            "schema": {"id": "int64", "title": "string"},
        }
        config.update(overrides)
        return config

    return build


@pytest.fixture
def mock_json_response(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[dict[str, Any]], None]:
    def set_response(payload: dict[str, Any]) -> None:
        monkeypatch.setattr(
            connectors,
            "urlopen",
            lambda request: _json_response(payload),
        )

    return set_response


def test_selects_records_and_applies_declared_schema(
    rest_config: Callable[..., dict[str, Any]],
    mock_json_response: Callable[[dict[str, Any]], None],
) -> None:
    mock_json_response(
        {
            "response": {
                "items": [
                    {"id": 1, "title": "first"},
                    {"id": 2, "title": "second"},
                ]
            }
        }
    )

    table = RestConnector(rest_config(records_path="response.items")).read().read_all()

    assert table.schema == pa.schema([pa.field("id", pa.int64()), pa.field("title", pa.string())])
    assert table.to_pylist() == [{"id": 1, "title": "first"}, {"id": 2, "title": "second"}]


def test_strict_schema_policy_allows_null_for_nullable_fields(
    rest_config: Callable[..., dict[str, Any]],
    mock_json_response: Callable[[dict[str, Any]], None],
) -> None:
    mock_json_response({"items": [{"id": 1, "title": None}]})

    table = RestConnector(rest_config(schema_policy="strict")).read().read_all()

    assert table.to_pylist() == [{"id": 1, "title": None}]


def test_strict_schema_policy_rejects_missing_declared_fields(
    rest_config: Callable[..., dict[str, Any]],
    mock_json_response: Callable[[dict[str, Any]], None],
) -> None:
    mock_json_response({"items": [{"id": 1}]})

    with pytest.raises(ValueError, match=r"Record 0 is missing declared fields: title."):
        RestConnector(rest_config(schema_policy="strict")).read()


def test_strict_schema_policy_ignores_undeclared_source_fields(
    rest_config: Callable[..., dict[str, Any]],
    mock_json_response: Callable[[dict[str, Any]], None],
) -> None:
    mock_json_response({"items": [{"id": 1, "title": "first", "unexpected": "value"}]})

    table = RestConnector(rest_config(schema_policy="strict")).read().read_all()

    assert table.to_pylist() == [{"id": 1, "title": "first"}]


def test_strict_schema_policy_rejects_type_mismatches(
    rest_config: Callable[..., dict[str, Any]],
    mock_json_response: Callable[[dict[str, Any]], None],
) -> None:
    mock_json_response({"items": [{"id": "1", "title": "first"}]})

    with pytest.raises(
        ValueError,
        match=r"Record 0 field 'id' must be int64, got str.",
    ):
        RestConnector(rest_config(schema_policy="strict")).read()


def test_rejects_unsupported_schema_policy(
    rest_config: Callable[..., dict[str, Any]],
) -> None:
    with pytest.raises(ValueError, match=r"connector.schema_policy must be 'strict'."):
        RestConnector(rest_config(schema_policy="cast")).read()


def test_sends_bearer_token_from_environment(
    monkeypatch: pytest.MonkeyPatch,
    rest_config: Callable[..., dict[str, Any]],
) -> None:
    monkeypatch.setenv("PRODUCTS_API_TOKEN", "test-token")
    authorization_headers: list[str | None] = []
    monkeypatch.setattr(
        connectors,
        "urlopen",
        lambda request: (
            authorization_headers.append(request.get_header("Authorization"))
            or _json_response({"items": [{"id": 1}]})
        ),
    )

    reader = RestConnector(
        rest_config(
            schema={"id": "int64"},
            auth={"type": "bearer", "token_env": "PRODUCTS_API_TOKEN"},
        )
    ).read()

    assert [batch.column("id").to_pylist() for batch in reader] == [[1]]
    assert authorization_headers == ["Bearer test-token"]


def test_rejects_missing_bearer_token_before_request(
    monkeypatch: pytest.MonkeyPatch,
    rest_config: Callable[..., dict[str, Any]],
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
            rest_config(
                schema={"id": "int64"},
                auth={"type": "bearer", "token_env": "MISSING_API_TOKEN"},
            )
        ).read()


def test_uses_configured_offset_pagination(
    monkeypatch: pytest.MonkeyPatch,
    rest_config: Callable[..., dict[str, Any]],
) -> None:
    payloads = {
        0: {"products": [{"id": 1}, {"id": 2}], "total": 5},
        2: {"products": [{"id": 3}, {"id": 4}], "total": 5},
        4: {"products": [{"id": 5}], "total": 5},
    }
    requests: list[dict[str, list[str]]] = []
    authorization_headers: list[str | None] = []
    accept_headers: list[str | None] = []
    monkeypatch.setenv("PRODUCTS_API_TOKEN", "test-token")

    def urlopen(request: Any) -> BytesIO:
        query = parse_qs(urlsplit(request.full_url).query)
        requests.append(query)
        authorization_headers.append(request.get_header("Authorization"))
        accept_headers.append(request.get_header("Accept"))
        return _json_response(payloads[int(query["skip"][0])])

    monkeypatch.setattr(connectors, "urlopen", urlopen)

    reader = RestConnector(
        rest_config(
            url="https://example.test/products",
            records_path="products",
            schema={"id": "int64"},
            auth={"type": "bearer", "token_env": "PRODUCTS_API_TOKEN"},
            headers={"Accept": "application/json"},
            params={"locale": "en-US"},
            pagination={
                "type": "offset",
                "offset_param": "skip",
                "limit_param": "limit",
                "page_size": 2,
                "total_path": "total",
            },
        )
    ).read()

    assert [batch.column("id").to_pylist() for batch in reader] == [[1, 2], [3, 4], [5]]
    assert requests == [
        {"locale": ["en-US"], "skip": ["0"], "limit": ["2"]},
        {"locale": ["en-US"], "skip": ["2"], "limit": ["2"]},
        {"locale": ["en-US"], "skip": ["4"], "limit": ["2"]},
    ]
    assert authorization_headers == ["Bearer test-token"] * 3
    assert accept_headers == ["application/json"] * 3


def test_rejects_authorization_header_with_bearer_auth(
    monkeypatch: pytest.MonkeyPatch,
    rest_config: Callable[..., dict[str, Any]],
) -> None:
    monkeypatch.setenv("PRODUCTS_API_TOKEN", "test-token")

    with pytest.raises(
        ValueError,
        match=r"connector.headers cannot define Authorization when connector.auth is configured.",
    ):
        RestConnector(
            rest_config(
                auth={"type": "bearer", "token_env": "PRODUCTS_API_TOKEN"},
                headers={"Authorization": "Bearer another-token"},
            )
        ).read()


def test_retries_retryable_http_failures_with_exponential_backoff(
    monkeypatch: pytest.MonkeyPatch,
    rest_config: Callable[..., dict[str, Any]],
) -> None:
    timeouts: list[float] = []
    delays: list[float] = []

    def urlopen(request: Any, timeout: float) -> BytesIO:
        timeouts.append(timeout)
        if len(timeouts) < 3:
            raise HTTPError(request.full_url, 503, "service unavailable", hdrs=None, fp=None)
        return _json_response({"items": [{"id": 1}]})

    monkeypatch.setattr(connectors, "urlopen", urlopen)
    monkeypatch.setattr(connectors, "sleep", delays.append)

    reader = RestConnector(
        rest_config(
            schema={"id": "int64"},
            request={"timeout_seconds": 2.5, "retries": 2, "backoff_seconds": 1},
        )
    ).read()

    assert [batch.column("id").to_pylist() for batch in reader] == [[1]]
    assert timeouts == [2.5, 2.5, 2.5]
    assert delays == [1, 2]


def test_retries_transport_failures(
    monkeypatch: pytest.MonkeyPatch,
    rest_config: Callable[..., dict[str, Any]],
) -> None:
    attempts = 0
    delays: list[float] = []

    def urlopen(request: Any, timeout: float) -> BytesIO:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise URLError("temporary network failure")
        return _json_response({"items": [{"id": 1}]})

    monkeypatch.setattr(connectors, "urlopen", urlopen)
    monkeypatch.setattr(connectors, "sleep", delays.append)

    reader = RestConnector(
        rest_config(
            schema={"id": "int64"},
            request={"timeout_seconds": 2, "retries": 1, "backoff_seconds": 0.5},
        )
    ).read()

    assert [batch.column("id").to_pylist() for batch in reader] == [[1]]
    assert attempts == 2
    assert delays == [0.5]


def test_does_not_retry_non_retryable_http_errors(
    monkeypatch: pytest.MonkeyPatch,
    rest_config: Callable[..., dict[str, Any]],
) -> None:
    attempts = 0
    delays: list[float] = []

    def urlopen(request: Any, timeout: float) -> BytesIO:
        nonlocal attempts
        attempts += 1
        raise HTTPError(request.full_url, 401, "unauthorized", hdrs=None, fp=None)

    monkeypatch.setattr(connectors, "urlopen", urlopen)
    monkeypatch.setattr(connectors, "sleep", delays.append)

    with pytest.raises(HTTPError, match="HTTP Error 401: unauthorized"):
        RestConnector(
            rest_config(
                schema={"id": "int64"},
                request={"timeout_seconds": 2, "retries": 3, "backoff_seconds": 1},
            )
        ).read()

    assert attempts == 1
    assert delays == []


def test_rejects_unsupported_pagination_type(
    rest_config: Callable[..., dict[str, Any]],
) -> None:
    with pytest.raises(ValueError, match=r"connector.pagination.type must be 'offset'."):
        RestConnector(
            rest_config(
                schema={"id": "int64"},
                pagination={"type": "cursor"},
            )
        ).read()
