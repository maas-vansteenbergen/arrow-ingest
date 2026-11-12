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
uv run arrow-ingest run pipeline.yaml
```

## Development workflow

1. Create a focused branch from `main`.
2. Keep changes small and include tests for changed behavior.
3. Install [just](https://just.systems/) and run the required checks before
   opening a pull request:

   ```console
   just check
   ```

4. Describe the behavior change, tests run, and any documentation updates in
   the pull request.

Run `just coverage` to generate branch coverage reports. Run
`just release-check` to run the required checks and build the source
distribution and wheel. Read [RELEASING.md](./RELEASING.md) for the release
process.

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

## Documentation style

Use the [Microsoft Writing Style Guide](https://learn.microsoft.com/style-guide/welcome/)
as the default when you write or update documentation. Adapt it when the
project's audience, Markdown format, or established repository conventions
provide a clearer approach.

- Use sentence-style headings, active voice, and direct language.
- Write task-oriented guidance that tells readers what to do and why.
- Keep the README concise; put detailed field-by-field behavior in focused
  reference pages.
- Keep examples complete, accurate, and aligned with tested behavior.

## Security

Do not disclose suspected vulnerabilities in a public issue. Contact the
repository owner privately through GitHub with reproduction steps, affected
versions, and potential impact.
