# ingester

`ingester` is a small, Arrow-native ingestion library. Connectors produce
`pyarrow.RecordBatchReader` streams, and targets consume those streams without
introducing a pipeline runtime or hidden metadata columns.

The project currently supports:

- REST JSON and local Parquet connectors;
- local Parquet files and DuckDB tables as targets; and
- replace, append, upsert, and SCD2 merge strategies.

## Install

The project requires Python 3.12 or later.

```console
uv sync --dev
```

## Run an ingestion

Configure the connector, merge strategy, and target in YAML, then run the
included command-line entry point:

```console
uv run python demo.py connector.yaml
```

The repository's [`connector.yaml`](./connector.yaml) is a runnable example
that reads paginated product data and writes a Parquet file.

## Configuration

### REST connector

```yaml
connector:
  type: rest
  url: https://api.example.com/products
  records_path: data.products
  schema:
    id: int64
    title: string
    price: float64
  schema_policy: strict
  params:
    locale: en
  headers:
    Accept: application/json
  cursor:
    type: high_watermark
    field: id
    request_param: after_id
  pagination:
    type: offset
    offset_param: offset
    limit_param: limit
    page_size: 100
    total_path: meta.total
  request:
    timeout_seconds: 30
    retries: 3
    backoff_seconds: 1
```

`records_path` and `total_path` support dot-separated paths into JSON objects.
The strict schema policy requires every declared field in each record to have
the configured primitive Arrow type. Extra fields are ignored. Supported
schema values are `int64`, `float64`, `string`, and `bool`.

REST requests can use bearer authentication without storing the secret in
YAML:

```yaml
auth:
  type: bearer
  token_env: PRODUCTS_API_TOKEN
```

Set the named environment variable before running the command. When bearer
authentication is configured, do not also configure an `Authorization` header.

### Parquet connector

```yaml
connector:
  type: parquet
  path: ./input/products.parquet
```

Parquet connectors stream local files and do not support cursors.

### Targets

```yaml
target:
  type: parquet
  path: ./output/products.parquet
```

```yaml
target:
  type: duckdb
  path: ./output/warehouse.duckdb
  table: products
```

Both targets can derive a high-watermark from an existing integer destination
field for incremental REST loads.

### Merge strategies

Use `replace` to overwrite the destination or `append` to add incoming rows:

```yaml
merge: replace
```

Use `upsert` to replace destination rows that share the configured key tuple.
Incoming key values must be non-null and unique, and the input schema must
match the existing destination schema.

```yaml
merge:
  strategy: upsert
  keys: [id]
```

Use SCD2 to preserve row history. A new version is created only when a tracked
field changes, and every incoming version must be later than the current one
for its key.

```yaml
merge:
  strategy: scd2
  keys: [id]
  tracked_fields: [title, price]
  effective_at: version
  valid_from: valid_from
  valid_to: valid_to
  is_current: is_current
```

## Library API

The public package exports `Connector` and `Target` protocols, the
`Cursor`, `MergeConfiguration`, and `SCD2Configuration` data classes, merge
strategy parsing helpers, and the built-in connector and target
implementations. Connectors return Arrow stream readers; targets accept them
and return the number of incoming rows written.

## Development

Run the tests and lint checks with:

```console
uv run pytest
uv run ruff check .
```
