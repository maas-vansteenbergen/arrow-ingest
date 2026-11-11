from __future__ import annotations

import gzip
import json
import shutil
from io import BytesIO
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request

import duckdb
import pyarrow.parquet as pq
import pytest

from arrow_ingest import run_pipeline
from arrow_ingest.connectors import rest

PRODUCT_INGESTION_EXAMPLE_DIRECTORY = Path(__file__).parents[1] / "examples" / "product-ingestion"
STACKOVERFLOW_EXAMPLE_DIRECTORY = Path(__file__).parents[1] / "examples" / "stackoverflow"


def test_rest_example_writes_to_duckdb(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_directory = tmp_path / "product-ingestion"
    shutil.copytree(
        PRODUCT_INGESTION_EXAMPLE_DIRECTORY,
        project_directory,
        ignore=shutil.ignore_patterns("output"),
    )
    monkeypatch.setattr(
        rest,
        "urlopen",
        lambda request: BytesIO(
            json.dumps(
                {
                    "products": [
                        {
                            "id": 1,
                            "title": "Widget",
                            "price": 10.0,
                            "category": "tools",
                            "stock": 5,
                        }
                    ]
                }
            ).encode()
        ),
    )

    result = run_pipeline(project_directory / "pipelines" / "products.yaml")

    with duckdb.connect(result.destination) as connection:
        rows = connection.execute(
            "SELECT id, title, price, category, stock FROM products"
        ).fetchall()

    assert result.rows_written == 1
    assert rows == [(1, "Widget", 10.0, "tools", 5)]


def test_stackoverflow_example_resumes_from_destination_watermark(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project_directory = tmp_path / "stackoverflow"
    shutil.copytree(
        STACKOVERFLOW_EXAMPLE_DIRECTORY,
        project_directory,
        ignore=shutil.ignore_patterns("output"),
    )
    requests: list[dict[str, list[str]]] = []
    delays: list[float] = []
    records = [
        {"question_id": 1, "creation_date": 100, "link": "https://stackoverflow.com/questions/1"},
        {"question_id": 2, "creation_date": 200, "link": "https://stackoverflow.com/questions/2"},
    ]

    class Response(BytesIO):
        headers = {"Content-Encoding": "gzip"}

    def fake_urlopen(request: Request, timeout: float) -> Response:
        assert timeout == 30
        assert request.get_header("Accept-encoding") == "gzip"
        parameters = parse_qs(urlsplit(request.full_url).query)
        requests.append(parameters)
        items = records[:1] if len(requests) == 1 else records
        return Response(gzip.compress(json.dumps({"items": items, "backoff": 2}).encode()))

    monkeypatch.setattr(rest, "urlopen", fake_urlopen)
    monkeypatch.setattr(rest, "sleep", delays.append)
    pipeline = project_directory / "pipelines" / "questions.yaml"
    first = run_pipeline(pipeline)
    second = run_pipeline(pipeline)

    assert first.rows_written == 1
    assert second.rows_written == 2
    assert requests == [
        {"site": ["stackoverflow"], "sort": ["creation"], "order": ["asc"], "pagesize": ["30"]},
        {
            "site": ["stackoverflow"],
            "sort": ["creation"],
            "order": ["asc"],
            "pagesize": ["30"],
            "min": ["100"],
        },
    ]
    assert delays == [2, 2]
    with duckdb.connect(second.destination) as connection:
        rows = connection.execute(
            "SELECT question_id, creation_date, link FROM questions ORDER BY question_id"
        ).fetchall()
    assert rows == [(row["question_id"], row["creation_date"], row["link"]) for row in records]


def test_scd2_example_creates_history_from_static_parquet_snapshots(tmp_path) -> None:
    project_directory = tmp_path / "product-ingestion"
    shutil.copytree(
        PRODUCT_INGESTION_EXAMPLE_DIRECTORY,
        project_directory,
        ignore=shutil.ignore_patterns("output"),
    )

    first_result = run_pipeline(project_directory / "pipelines" / "products-scd2-initial.yaml")
    second_result = run_pipeline(project_directory / "pipelines" / "products-scd2-update.yaml")

    history = pq.read_table(project_directory / "output" / "products-history.parquet")

    assert first_result.rows_written == 2
    assert second_result.rows_written == 2
    assert history.to_pylist() == [
        {
            "id": 1,
            "title": "Widget",
            "price": 10.0,
            "effective_at": 1,
            "valid_from": 1,
            "valid_to": 2,
            "is_current": False,
        },
        {
            "id": 2,
            "title": "Gadget",
            "price": 20.0,
            "effective_at": 1,
            "valid_from": 1,
            "valid_to": None,
            "is_current": True,
        },
        {
            "id": 1,
            "title": "Widget Plus",
            "price": 12.0,
            "effective_at": 2,
            "valid_from": 2,
            "valid_to": None,
            "is_current": True,
        },
    ]
