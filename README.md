# arrow-ingest

[![CI](https://github.com/maas-vansteenbergen/arrow-ingest/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/maas-vansteenbergen/arrow-ingest/actions/workflows/ci.yml)

`arrow-ingest` is a small, Arrow-native ingestion library. It moves data from
a supported source to a supported target through an Arrow stream, while keeping
configuration explicit and avoiding a pipeline runtime or hidden metadata
columns.

It currently supports:

- REST JSON APIs and local Parquet files as sources.
- Local Parquet files and DuckDB tables as targets.
- Replace, append, upsert, and SCD2 merge strategies.

## Why this exists

Ingesting data from a source to a database is often a much smaller
problem than the tooling around it.

Many ingestion frameworks introduce their own runtime, state management,
metadata storage, or internal data model. These abstractions can be
useful, but they also add complexity to simple ingestion workloads.

`arrow-ingest` takes a different approach. Connectors produce
`pyarrow.RecordBatchReader` streams, merge strategies define how data is
applied, and targets consume the stream.

Arrow is the only data contract within the library. This keeps data
movement explicit, avoids multiple internal representations, and allows
large datasets to stream without loading everything into memory.

## Architecture

```mermaid
flowchart LR
    Connector["Connector<br/>Reads from an external system"]
    Stream["Arrow stream"]
    Target["Target<br/>Writes to a destination using a merge strategy"]

    Connector --> Stream
    Stream --> Target
```

## Install

Install Python 3.12 or later, then synchronize the development dependencies:

```console
uv sync --dev
```

## Run the sample pipeline

Run the included configuration:

```console
uv run arrow-ingest run pipeline.yaml
```

[pipeline.yaml](./pipeline.yaml) reads a paginated product API and writes a
Parquet file.

## Configure a pipeline

Each pipeline file has a source connector, a merge strategy, and a target:

```yaml
connector:
  type: rest
  url: https://api.example.com/products
  records_path: products
  schema:
    id: int64
    title: string
merge: replace
target:
  type: parquet
  path: ./output/products.parquet
```

For a complete runnable REST-to-Parquet example, see
[pipeline.yaml](./pipeline.yaml).

Use the REST connector for JSON APIs and the Parquet connector for local
Parquet files. Use the Parquet target for one output file or the DuckDB target
for one table.

Choose a merge strategy that matches the load:

- Use `replace` for a first or full refresh.
- Use `append` to retain existing rows.
- Use `upsert` to replace rows with matching keys.
- Use `scd2` to retain a history of changes.

You can add bearer authentication, offset pagination, request retries, and a
destination-derived high-watermark cursor to a REST connector. See the
[YAML configuration reference](./docs/configuration-reference.md) for all
supported fields, defaults, validation rules, and complete examples.

Run a pipeline from Python when you integrate it into an orchestration task:

```python
from arrow_ingest import run_pipeline

result = run_pipeline("pipelines/products.yaml")
print(result.rows_written)
```

The function returns a `LoadResult` with the number of written rows, the
incoming column count, and the resolved destination path.

## Organize a project

Keep pipeline YAML files, local input data, and generated output in separate
directories. See [Organize an ingestion project](./docs/project-structure.md)
for the recommended layout and
[product ingestion example](./examples/product-ingestion/) for a small
starting point.

The same [product ingestion example](./examples/product-ingestion/) also
includes static Parquet snapshots and SCD2 history loading.

## Project status

The project is in the `0.x` alpha series. Read [CHANGELOG.md](./CHANGELOG.md)
for release notes and known limitations.

## Contributing

Read [CONTRIBUTING.md](./CONTRIBUTING.md) for the development workflow,
required checks, design guidelines, documentation standards, and release
process.
