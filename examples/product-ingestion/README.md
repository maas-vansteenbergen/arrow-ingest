# Product ingestion example

This example shows a small `arrow-ingest` project:

```text
product-ingestion/
├── pipelines/
│   ├── products.yaml
│   ├── products-scd2-initial.yaml
│   └── products-scd2-update.yaml
├── data/
│   ├── products-v1.parquet
│   └── products-v2.parquet
└── output/
```

## Run a REST-to-DuckDB load

The `products.yaml` file reads a REST JSON collection and writes a local
DuckDB table:

```console
arrow-ingest run pipelines/products.yaml
```

Its database path resolves to `output/products.duckdb` relative to the
configuration file. It writes the `products` table.

## Load SCD2 history

The project also includes two static Parquet snapshots. Run the first snapshot:

```console
arrow-ingest run pipelines/products-scd2-initial.yaml
```

Then load the later snapshot:

```console
arrow-ingest run pipelines/products-scd2-update.yaml
```

The second load closes the first version of product `1` and creates its new
current version. Product `2` is unchanged, so it remains one current row. The
SCD2 pipeline writes `output/products-history.parquet`.

Read [Organize an ingestion project](../../docs/project-structure.md) for the
recommended structure and path behavior.
