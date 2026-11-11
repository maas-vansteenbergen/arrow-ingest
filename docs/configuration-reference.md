# YAML configuration reference

Use this reference after you create a basic pipeline. For a concise example,
see the [README](../README.md). For a runnable REST-to-Parquet configuration,
see [`pipeline.yaml`](../pipeline.yaml).

## Pipeline structure

Each pipeline has a `connector`, `merge`, and `target`:

```yaml
connector:
  # Read records from a source.
merge: replace
target:
  # Write records to a destination.
```

## REST connector

Use the REST connector to read a JSON collection from an HTTP endpoint.

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
```

| Field | Required | Description |
|---|---:|---|
| `connector.type` | Yes | Set to `rest`. |
| `connector.url` | Yes | The endpoint URL. |
| `connector.records_path` | Yes | A dot-separated path that resolves to a list of JSON objects. |
| `connector.schema` | Yes | A mapping of field names to `int64`, `float64`, `string`, or `bool`. |
| `connector.schema_policy` | No | Set to `strict`. This is the default and only supported policy. |
| `connector.params` | No | A mapping of non-empty string names to string query-parameter values. |
| `connector.headers` | No | A mapping of non-empty string names to string header values. |
| `connector.auth` | No | Bearer authentication settings. |
| `connector.cursor` | No | High-watermark cursor settings. |
| `connector.pagination` | No | Offset pagination settings. |
| `connector.request` | No | Timeout and retry settings. |

With the `strict` schema policy, every declared field must be present and have
its declared type. Null values are valid. Fields that aren't in the declared
schema are ignored. The connector decodes each JSON response before it forms
Arrow batches.

### Authenticate with a bearer token

```yaml
connector:
  auth:
    type: bearer
    token_env: PRODUCTS_API_TOKEN
```

| Field | Required | Description |
|---|---:|---|
| `connector.auth.type` | Yes | Set to `bearer`. |
| `connector.auth.token_env` | Yes | The non-empty name of the environment variable that contains the token. |

Set the environment variable before you run the pipeline. The connector sends
the value as an `Authorization: Bearer <token>` header. When you use
`connector.auth`, don't also set an `Authorization` header in
`connector.headers`.

### Load incrementally

```yaml
connector:
  cursor:
    type: high_watermark
    field: id
    request_param: after_id
```

| Field | Required | Description |
|---|---:|---|
| `connector.cursor.type` | Yes | Set to `high_watermark`. |
| `connector.cursor.field` | Yes | A declared `int64` schema field. |
| `connector.cursor.request_param` | Yes | The non-empty query-parameter name that the API expects. |

Use a high-watermark cursor with `append`, `upsert`, or `scd2`. It doesn't work
with `replace`. When the destination has no cursor value, the connector doesn't
add the cursor query parameter. Otherwise, the target supplies the maximum
stored value for the field.

The API defines how it interprets that parameter. Make sure its boundary,
ordering, and update behavior match your data: a maximum stored value doesn't
by itself account for ties, late-arriving records, or updates below that value.
With SCD2, unchanged tracked fields don't create a new destination row, so an
incoming cursor value might not be retained and can be requested again.

The connector derives cursor state from destination data. It doesn't persist
opaque source checkpoints.

### Paginate with offsets

```yaml
connector:
  pagination:
    type: offset
    offset_param: offset
    limit_param: limit
    page_size: 100
    total_path: meta.total
```

| Field | Required | Description |
|---|---:|---|
| `connector.pagination.type` | Yes | Set to `offset`. |
| `connector.pagination.offset_param` | Yes | The non-empty name of the offset query parameter. |
| `connector.pagination.limit_param` | Yes | The non-empty name of the page-size query parameter. |
| `connector.pagination.page_size` | Yes | A positive integer. |
| `connector.pagination.total_path` | Yes | A dot-separated path that resolves to a non-negative integer in every response. |

Offset pagination is the only supported pagination mode. Changes to the source
while pages are being read can affect which records the API returns.

### Control requests

```yaml
connector:
  request:
    timeout_seconds: 30
    retries: 3
    backoff_seconds: 1
```

| Field | Required | Default | Description |
|---|---:|---:|---|
| `connector.request.timeout_seconds` | No | No timeout | A positive number of seconds. |
| `connector.request.retries` | No | `0` | A non-negative integer. |
| `connector.request.backoff_seconds` | No | `0` | A non-negative number of seconds. |
| `connector.request.response_backoff_field` | No | No response delay | A top-level JSON field containing a non-negative number of seconds. |

The connector retries transport failures and HTTP `429` and `5xx` responses.
It doesn't retry configuration errors, invalid response shapes, or other HTTP
responses.

The REST connector accepts gzip- and deflate-encoded HTTP responses. Configure
an `Accept-Encoding` header when the API requires one.

### Wait for a response-provided delay

Some APIs include a delay in their JSON responses. To wait for that delay,
set `response_backoff_field` to the non-empty name of the top-level field:

```yaml
connector:
  request:
    response_backoff_field: backoff
```

The field must contain a non-negative finite number of seconds. The connector
waits after reading the response, even when it contains no records. If the
field is absent, the connector doesn't wait. This delay is separate from the
retry delay configured by `backoff_seconds`.

The connector also checks HTTP error response bodies for the field. If it can't
read an error body as a JSON object, it fails without retrying.

The connector doesn't persist delays or coordinate them across processes. If
the API requires a shared delay, coordinate concurrent requests separately.

## Parquet connector

Use the Parquet connector to read one local Parquet file:

```yaml
connector:
  type: parquet
  path: ./input/products.parquet
```

| Field | Required | Description |
|---|---:|---|
| `connector.type` | Yes | Set to `parquet`. |
| `connector.path` | Yes | A non-empty local file path. |

Parquet sources don't support `connector.cursor`.

## Targets

### Parquet target

```yaml
target:
  type: parquet
  path: ./output/products.parquet
```

| Field | Required | Description |
|---|---:|---|
| `target.type` | Yes | Set to `parquet`. |
| `target.path` | Yes | A non-empty local output path, including the output filename. |

The target writes to a temporary file and replaces the destination after the
write succeeds. Append rewrites the existing file along with the incoming
data.

### DuckDB target

```yaml
target:
  type: duckdb
  path: ./output/warehouse.duckdb
  table: products
```

| Field | Required | Description |
|---|---:|---|
| `target.type` | Yes | Set to `duckdb`. |
| `target.path` | Yes | A non-empty database path. You can use `:memory:`. |
| `target.table` | Yes | An unqualified identifier matching `[A-Za-z_][A-Za-z0-9_]*`. |

DuckDB upsert and SCD2 updates use transactions.

Both targets support destination-derived high-watermark cursors when the
destination field is `int64`.

## Merge strategies

### Replace

Use `replace` to overwrite the destination:

```yaml
merge: replace
```

### Append

Use `append` to add incoming rows:

```yaml
merge: append
```

### Upsert

Use `upsert` to replace destination rows with matching keys:

```yaml
merge:
  strategy: upsert
  keys: [id]
```

`keys` must be a non-empty list of unique field names. Incoming key values must
be non-null and unique within a load; the target doesn't select a winner for
duplicate keys. The incoming and destination schemas must match. Upsert retains
destination rows that aren't present in the incoming data; it doesn't
synchronize deletions or evolve the destination schema.

### SCD2

Use `scd2` to retain a history of changes:

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

| Field | Required | Description |
|---|---:|---|
| `merge.strategy` | Yes | Set to `scd2`. |
| `merge.keys` | Yes | A non-empty list of unique field names. |
| `merge.tracked_fields` | Yes | A non-empty list of unique field names that don't overlap `keys`. |
| `merge.effective_at` | Yes | An `int64` source field that orders versions. |
| `merge.valid_from` | Yes | The output field that stores the version start. |
| `merge.valid_to` | Yes | The output field that stores the version end. |
| `merge.is_current` | Yes | The output field that identifies the current row. |

`effective_at`, `valid_from`, `valid_to`, and `is_current` must be distinct,
non-empty field names. The scalar merge form supports only `replace` and
`append`; use the mapping form for `upsert` and `scd2`.

Incoming key values must be non-null and unique within a load, so a load can't
contain multiple versions of the same key. Incoming `effective_at` values and
current rows' `valid_from` values must be non-null. For an existing current
key, the incoming `effective_at` value must be greater than the current
version's `valid_from` value. The target doesn't repair out-of-order versions.

When a tracked field changes, SCD2 closes the current row at the incoming
effective value and creates a new current row. When tracked fields don't
change, it doesn't create a new version. The configured output fields must not
already exist in the incoming schema.

SCD2 doesn't support timestamp effective values, schema evolution, or
deletion and tombstone synchronization. Rows absent from a load remain in the
destination.

## Resolve configuration errors

Configuration errors can be reported during setup or while a load runs. Source
records, incoming schemas, and destination state are validated when they are
available. Use the field or component named in an error to correct the
configuration or data. Don't rely on implicit type conversion.
