# Contributing to arrow-ingest

Thank you for contributing to `arrow-ingest`.

## Getting started

This project requires Python 3.12 or later and uses
[uv](https://docs.astral.sh/uv/) for dependency management.

```console
uv sync --dev
```

Run the sample ingestion pipeline with:

```console
uv run python demo.py pipeline.yaml
```

## Development workflow

1. Create a focused branch from `main`.
2. Keep changes small and include tests for changed behavior.
3. Run the required checks before opening a pull request:

   ```console
   uv run pytest
   uv run ruff check .
   uv run ruff format --check .
   ```

4. Describe the behavior change, tests run, and any documentation updates in
   the pull request.

## Design guidelines

- Preserve the Arrow stream boundary: connectors emit
  `pyarrow.RecordBatchReader` and targets consume it.
- Keep the public abstraction surface small. Do not introduce pipeline,
  registry, or state-manager abstractions unless a concrete requirement
  cannot be met by the existing connector-target contract.
- Preserve explicit configuration and validation. Do not add hidden metadata
  columns or implicit persistence.
- Add or update tests in `tests/` for every behavior change.
- Update the README when changing supported configuration, public APIs, or
  merge behavior.

## Security

Do not disclose suspected vulnerabilities in a public issue. Contact the
repository owner privately through GitHub with reproduction steps, affected
versions, and potential impact.
