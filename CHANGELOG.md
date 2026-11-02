# Changelog

All notable changes are documented in this file. Release tags follow
[Semantic Versioning](https://semver.org/); Python package versions use the
equivalent [PEP 440](https://peps.python.org/pep-0440/) representation.

## [0.1.0-alpha.1] - 2026-11-02

### Added

- Arrow-native connector and target contracts based on
  `pyarrow.RecordBatchReader`.
- REST JSON and local Parquet connectors.
- Local Parquet and DuckDB targets with replace, append, upsert, and SCD2
  merge strategies.
- Destination-derived REST high-watermark cursors.
- CI, coverage reporting, Dependabot, CodeQL, security-reporting guidance,
  and contributor documentation.

### Known limitations

- The public API and declarative configuration are experimental during the
  `0.x` alpha series.
- Supported REST capabilities are bearer authentication, offset pagination,
  and strict primitive schemas.
- CSV, JSON Lines, ADBC, object storage, opaque cursors, transformations, and
  additional targets are not yet implemented.
