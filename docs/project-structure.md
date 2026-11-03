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

Run a pipeline with the installed command. Paths in a pipeline configuration
are relative to the YAML file that contains them. For example:

```console
cd my-ingestion-project
arrow-ingest run pipelines/products.yaml
```

The [product ingestion example](../examples/product-ingestion/) shows the same
layout with supported pipeline configurations. As the project grows, add
pipeline files rather than adding configuration to application code.
