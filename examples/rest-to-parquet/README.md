# REST-to-Parquet example

This example reads a paginated product API and writes its records to a local
Parquet file:

```text
rest-to-parquet/
├── pipeline.yaml
└── output/
    └── products.parquet
```

Run the following command from the `examples/rest-to-parquet` directory:

```console
uv run arrow-ingest run pipeline.yaml
```

The pipeline reads products from the public DummyJSON API in pages of 10 and
writes them to `output/products.parquet`.
