# Organize an ingestion project

Use a project directory to keep pipeline configurations, local input files, and
generated output separate from application code. Start with this structure:

```text
my-ingestion-project/
├── pipelines/
│   └── products.yaml
├── data/
├── output/
├── .gitignore
└── README.md
```

Keep one pipeline configuration in `pipelines/`. Start with a descriptive file
name, such as `products.yaml` or `customers.yaml`.

Store local source files in `data/`. Keep generated Parquet files and DuckDB
databases in `output/`, and add `output/` to `.gitignore`. Don't store
credentials in the project directory or in YAML. Reference environment
variables from the configuration instead.

## Create your first project

With `arrow-ingest` installed, create the directories and move into the project:

```console
mkdir -p my-ingestion-project/pipelines my-ingestion-project/data my-ingestion-project/output
cd my-ingestion-project
```

Create `pipelines/products.yaml` with the following configuration:

```yaml
connector:
  type: rest
  url: https://dummyjson.com/products
  records_path: products
  schema:
    id: int64
    title: string
merge: replace
target:
  type: parquet
  path: ../output/products.parquet
```

Run the configuration:

```console
arrow-ingest run pipelines/products.yaml
```

The command reads products from the public API and writes
`output/products.parquet`. It requires an internet connection. Paths in the
configuration are relative to the YAML file that contains them.

## Add another configuration

To add another pipeline, copy the configuration:

```console
cp pipelines/products.yaml pipelines/products-copy.yaml
```

In `pipelines/products-copy.yaml`, change `target.path` to
`../output/products-copy.parquet`. Then run it:

```console
arrow-ingest run pipelines/products-copy.yaml
```

The two configurations now write to separate files. If configurations share a
destination, choose their merge strategies deliberately: `replace`
overwrites the destination, while `append` adds rows and can duplicate data
when a load is repeated.

The [product ingestion example](../examples/product-ingestion/) shows the same
layout with supported pipeline configurations.
